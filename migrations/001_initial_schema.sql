-- ============================================================
-- Migração: Schema para o Invoice Extraction Worker
-- Executar no SQL Editor do Supabase (uma vez)
-- ============================================================

-- Habilita extensão para UUIDs automáticos
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ── Tabela principal de requisições ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS extraction_requests (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Dados da Unidade Consumidora / Cliente
    concessionaria      TEXT NOT NULL DEFAULT 'amazonas_energia',
    unidade_consumidora TEXT,
    cliente_nome        TEXT,
    
    -- Credenciais criptografadas (recomendado: criptografar no app antes de salvar)
    credentials         JSONB NOT NULL DEFAULT '{}',
    -- Estrutura esperada:
    -- { "cpf_cnpj": "000.000.000-00", "senha": "****", "unidade_consumidora": "12345-6" }
    
    -- Controle de fluxo
    status              TEXT NOT NULL DEFAULT 'pendente'
                        CHECK (status IN (
                            'pendente',
                            'em_progresso',
                            'concluido',
                            'erro_extracao',
                            'credenciais_invalidas'
                        )),
    status_detail       TEXT,           -- Mensagem de erro ou detalhe
    
    -- Resultado
    pdf_links           JSONB,          -- Array de { mes_referencia, storage_url, filename }
    
    -- Auditoria
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at        TIMESTAMPTZ,
    
    -- Metadados opcionais (para multi-tenant SaaS)
    tenant_id           UUID,
    requested_by        UUID            -- FK para tabela de usuários, se houver
);

-- ── Índices para performance do polling ──────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_extraction_requests_status
    ON extraction_requests (status, created_at ASC)
    WHERE status = 'pendente';

CREATE INDEX IF NOT EXISTS idx_extraction_requests_tenant
    ON extraction_requests (tenant_id, status);

-- ── Trigger: atualiza updated_at automaticamente ──────────────────────────────
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    IF NEW.status IN ('concluido', 'erro_extracao', 'credenciais_invalidas') THEN
        NEW.completed_at = NOW();
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_extraction_requests_updated_at
    BEFORE UPDATE ON extraction_requests
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ── Row Level Security (para multi-tenant) ────────────────────────────────────
ALTER TABLE extraction_requests ENABLE ROW LEVEL SECURITY;

-- Policy: service_role tem acesso total (usado pelo worker)
CREATE POLICY "service_role_full_access" ON extraction_requests
    FOR ALL
    USING (auth.role() = 'service_role');

-- ── Bucket de Storage ─────────────────────────────────────────────────────────
-- Executar via Supabase Dashboard > Storage > New Bucket
-- Nome: faturas | Tipo: Private (não público por padrão)
-- Ou via SQL:
INSERT INTO storage.buckets (id, name, public, file_size_limit, allowed_mime_types)
VALUES (
    'faturas',
    'faturas',
    false,
    10485760,   -- 10 MB por arquivo
    ARRAY['application/pdf', 'image/png', 'image/jpeg']
) ON CONFLICT (id) DO NOTHING;

-- ── Dados de exemplo para testar o worker ────────────────────────────────────
-- (Remover em produção)
INSERT INTO extraction_requests (concessionaria, unidade_consumidora, credentials)
VALUES (
    'amazonas_energia',
    '123456-7',
    '{"cpf_cnpj": "000.000.000-00", "senha": "senha_teste", "unidade_consumidora": "123456-7"}'::jsonb
);
