-- ============================================================
-- Fase 2: Schema para Parsing e Análise de Faturas
-- Executar após 001_initial_schema.sql
-- ============================================================

-- ── Tabela de faturas parseadas ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS faturas_parsed (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Vínculo com a extração
    extraction_id       UUID REFERENCES extraction_requests(id) ON DELETE SET NULL,
    tenant_id           UUID,

    -- Identificação
    uc                  TEXT NOT NULL,
    mes_referencia      TEXT NOT NULL,       -- "03/2026"
    vencimento          TEXT,                -- "26/04/2026"
    nota_fiscal         TEXT,
    data_emissao        TEXT,
    cliente_nome        TEXT,

    -- Dados da UC
    grupo               TEXT,               -- "A"
    subgrupo            TEXT,               -- "A4"
    classe              TEXT,               -- "COMERCIAL"
    modalidade          TEXT,               -- "HOROSAZONAL VERDE"
    numero_medidor      TEXT,
    tensao_contratada_v NUMERIC,

    -- Leituras
    data_leitura_anterior TEXT,
    data_leitura_atual    TEXT,
    dias_consumo          INT,

    -- Energia (kWh)
    consumo_ponta_kwh           NUMERIC,
    consumo_fora_ponta_kwh      NUMERIC,
    consumo_total_kwh           NUMERIC,
    energia_reversa_kwh         NUMERIC,    -- GD injetada
    media_12_meses_kwh          NUMERIC,
    historico_kwh               NUMERIC[],  -- últimos 13 meses

    -- Demanda (kW)
    demanda_contratada_ponta_kw     NUMERIC,
    demanda_contratada_fora_ponta_kw NUMERIC,
    demanda_medida_ponta_kw         NUMERIC,
    demanda_medida_fora_ponta_kw    NUMERIC,
    demanda_reativa_ponta_kw        NUMERIC,
    demanda_reativa_fora_ponta_kw   NUMERIC,
    ufer_ponta_kvarh                NUMERIC,
    ufer_fora_ponta_kvarh           NUMERIC,

    -- Tarifas
    tarifa_consumo_ponta      NUMERIC,
    tarifa_consumo_fora_ponta NUMERIC,
    tarifa_demanda            NUMERIC,
    bandeira_tarifaria        TEXT,         -- "Verde" | "Amarela" | "Vermelha 1" | "Vermelha 2"
    bandeira_valor_kwh        NUMERIC,

    -- Tributos
    icms_st            BOOLEAN DEFAULT FALSE,
    pis_aliquota       NUMERIC,
    cofins_aliquota    NUMERIC,

    -- Valores financeiros
    cosip_valor        NUMERIC,
    credito_geracao    NUMERIC,
    total_a_pagar      NUMERIC,

    -- Detalhes JSON
    itens_faturados    JSONB,   -- array de itens
    dados_leitura      JSONB,   -- array de grandezas

    -- Metadados
    source_pdf_path    TEXT,
    parsed_at          TIMESTAMPTZ DEFAULT NOW(),
    
    -- Constraint de unicidade: uma UC só tem uma fatura por mês
    UNIQUE (uc, mes_referencia)
);

-- ── Tabela de análises ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS faturas_analise (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    fatura_id           UUID NOT NULL REFERENCES faturas_parsed(id) ON DELETE CASCADE,
    tenant_id           UUID,
    
    uc                  TEXT NOT NULL,
    mes_referencia      TEXT NOT NULL,
    total_fatura        NUMERIC,

    -- Resultado da análise
    score_eficiencia        INT CHECK (score_eficiencia BETWEEN 0 AND 100),
    potencial_economia_mensal  NUMERIC,
    potencial_economia_anual   NUMERIC,
    resumo_executivo           TEXT,

    -- Alertas em JSON
    alertas             JSONB,   -- array de alertas com código, severidade, descrição, economia
    
    -- Análise enriquecida pela Claude API (Fase 3)
    analise_claude      TEXT,    -- resposta textual da Claude com recomendações
    relatorio_pdf_url   TEXT,    -- link para o relatório gerado

    analyzed_at         TIMESTAMPTZ DEFAULT NOW(),
    
    UNIQUE (fatura_id)
);

-- ── Índices ───────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_faturas_parsed_uc_mes
    ON faturas_parsed (uc, mes_referencia);

CREATE INDEX IF NOT EXISTS idx_faturas_parsed_tenant
    ON faturas_parsed (tenant_id, parsed_at DESC);

CREATE INDEX IF NOT EXISTS idx_faturas_analise_uc
    ON faturas_analise (uc, mes_referencia);

CREATE INDEX IF NOT EXISTS idx_faturas_analise_score
    ON faturas_analise (score_eficiencia ASC);

CREATE INDEX IF NOT EXISTS idx_faturas_analise_economia
    ON faturas_analise (potencial_economia_anual DESC)
    WHERE potencial_economia_anual > 0;

-- ── Trigger: atualiza timestamps ──────────────────────────────────────────────
-- (reutiliza a função criada em 001)

CREATE TRIGGER trigger_faturas_parsed_updated_at
    BEFORE UPDATE ON faturas_parsed
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ── Row Level Security ────────────────────────────────────────────────────────
ALTER TABLE faturas_parsed  ENABLE ROW LEVEL SECURITY;
ALTER TABLE faturas_analise ENABLE ROW LEVEL SECURITY;

CREATE POLICY "service_role_full_access_parsed" ON faturas_parsed
    FOR ALL USING (auth.role() = 'service_role');

CREATE POLICY "service_role_full_access_analise" ON faturas_analise
    FOR ALL USING (auth.role() = 'service_role');

-- ── View de resumo (útil para o dashboard) ────────────────────────────────────
CREATE OR REPLACE VIEW vw_dashboard_uc AS
SELECT
    fp.uc,
    fp.cliente_nome,
    fp.subgrupo,
    fp.modalidade,
    COUNT(*)                    AS total_faturas,
    MAX(fp.mes_referencia)      AS ultimo_mes,
    AVG(fp.total_a_pagar)       AS media_valor_mensal,
    SUM(fp.total_a_pagar)       AS total_gasto_periodo,
    AVG(fp.consumo_total_kwh)   AS media_consumo_kwh,
    MAX(fa.potencial_economia_anual) AS maior_economia_potencial,
    MIN(fa.score_eficiencia)         AS pior_score,
    AVG(fa.score_eficiencia)         AS score_medio
FROM faturas_parsed fp
LEFT JOIN faturas_analise fa ON fa.fatura_id = fp.id
GROUP BY fp.uc, fp.cliente_nome, fp.subgrupo, fp.modalidade;

-- ── Comentários descritivos ───────────────────────────────────────────────────
COMMENT ON TABLE faturas_parsed  IS 'Dados estruturados extraídos do PDF de cada fatura';
COMMENT ON TABLE faturas_analise IS 'Resultado da análise e recomendações por fatura';
COMMENT ON VIEW  vw_dashboard_uc IS 'Visão consolidada por UC para o dashboard';
