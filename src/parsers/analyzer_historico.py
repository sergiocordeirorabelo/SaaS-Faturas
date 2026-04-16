"""
Motor de Análise Histórica — Agrega N faturas de uma UC.

Usa AnalisadorFatura como fonte única de verdade para as regras de negócio.
Retorna ResultadoHistorico — dataclass completa que alimenta tanto o dashboard
quanto o gerar_estudo.py (estudo técnico em PDF).

Antes: gerar_estudo.py tinha sua própria função _analisar() que recalculava
       demanda, reativo, COSIP etc. duplicando a lógica do AnalisadorFatura.
Agora: AnalisadorHistorico delega as regras ao AnalisadorFatura e agrega
       os resultados de N meses num único objeto estruturado.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

from src.parsers.analyzer_fatura import AnalisadorFatura, Alerta

logger = logging.getLogger(__name__)


# ── Helpers de formatação ─────────────────────────────────────────────────────

def _f(v, d=2):
    try:
        return f"{float(v):,.{d}f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "—"


def _fi(v):
    return _f(v, 0)


# ── Dataclass de resultado ────────────────────────────────────────────────────

@dataclass
class ResultadoHistorico:
    """
    Resultado consolidado da análise de N faturas de uma UC.
    Alimenta diretamente o gerar_estudo.py sem nenhum recálculo adicional.
    """
    uc: str
    nome: str
    cnpj: str
    subgrupo: str
    modalidade: str
    n_faturas: int

    # ── Financeiro ─────────────────────────────────────────────────────────
    custo_medio: float = 0.0          # R$/mês (média dos N meses)
    custo_anual: float = 0.0          # custo_medio × 12

    # ── Desperdícios detectados (soma dos N meses) ──────────────────────────
    demanda_ociosa_r: float = 0.0     # demanda contratada vs medida
    demanda_ultrapassagem_r: float = 0.0
    reativo_r: float = 0.0            # multas UFER / fator de potência baixo
    multas_atraso_r: float = 0.0      # multas por pagamento tardio
    icms_recuperavel_r: float = 0.0   # ICMS sobre TUSD (~60% do ICMS pago)
    cosip_total: float = 0.0
    gd_creditos_r: float = 0.0        # créditos de geração distribuída

    # ── Totais de desperdício ───────────────────────────────────────────────
    potencial_periodo: float = 0.0    # soma de tudo nos N meses
    potencial_anual: float = 0.0      # potencial_periodo / n * 12

    # ── Demanda ────────────────────────────────────────────────────────────
    demanda_contratada: float = 0.0
    demanda_media_medida: float = 0.0
    demanda_pico: float = 0.0
    utilizacao_demanda: int = 0        # %

    # ── Flags ──────────────────────────────────────────────────────────────
    elegivel_mercado_livre: bool = False
    tem_gd: bool = False

    # ── Pontos de atenção para o PPTX (max 6) ──────────────────────────────
    pontos_atencao: list = field(default_factory=list)

    # ── Cronograma de ações para o PPTX (max 8) ────────────────────────────
    # Cada item: (titulo_curto_multiline, descricao_acao)
    acoes: list = field(default_factory=list)

    # ── Alertas consolidados por código (do AnalisadorFatura) ──────────────
    alertas_por_codigo: dict = field(default_factory=dict)

    # ── Referência à fatura mais recente ───────────────────────────────────
    fatura_mais_recente: dict = field(default_factory=dict)

    @property
    def cosip_media(self) -> float:
        return self.cosip_total / self.n_faturas if self.n_faturas > 0 else 0.0


# ── Motor ─────────────────────────────────────────────────────────────────────

class AnalisadorHistorico:
    """
    Recebe lista de faturas (dicts do Supabase) + alertas abertos + CNPJ.
    Retorna ResultadoHistorico pronto para consumo pelo gerar_estudo.py e pela API.
    """

    def analisar(
        self,
        faturas: list[dict],
        alertas_abertos: Optional[list[dict]] = None,
        cnpj: str = "",
    ) -> ResultadoHistorico:
        if not faturas:
            raise ValueError("Nenhuma fatura fornecida para análise")

        alertas_abertos = alertas_abertos or []
        n = len(faturas)
        f0 = faturas[0]

        # ── 1. Rodar AnalisadorFatura em cada fatura ────────────────────────
        motor = AnalisadorFatura()
        resultados_por_fatura = [motor.analisar(f) for f in faturas]

        # ── 2. Consolidar alertas por código (pega o de maior economia) ─────
        alertas_por_codigo: dict[str, Alerta] = {}
        for res in resultados_por_fatura:
            for alerta in res.alertas:
                cod = alerta.codigo
                if cod not in alertas_por_codigo:
                    alertas_por_codigo[cod] = alerta
                else:
                    # Mantém o de maior economia estimada
                    ec_novo = alerta.economia_mensal_r or 0
                    ec_ant  = alertas_por_codigo[cod].economia_mensal_r or 0
                    if ec_novo > ec_ant:
                        alertas_por_codigo[cod] = alerta

        # ── 3. Métricas financeiras ──────────────────────────────────────────
        totais = [float(f.get("total_a_pagar") or 0) for f in faturas]
        custo_medio = sum(totais) / n

        # ── 4. Desperdícios acumulados nos N meses ───────────────────────────
        # 4a. Demanda ociosa (contratada vs medida × tarifa)
        demanda_ociosa_r = 0.0
        for f in faturas:
            dc = float(f.get("demanda_contratada_fora_ponta_kw") or 0)
            dm = float(f.get("demanda_medida_fora_ponta_kw") or 0)
            td = float(f.get("tarifa_demanda") or 0)
            if dc > dm and td > 0:
                demanda_ociosa_r += (dc - dm) * td

        # 4b. Demanda ultrapassada
        demanda_ultrapassagem_r = 0.0
        for f in faturas:
            for it in (f.get("itens_faturados") or []):
                d = (it.get("descricao") or "").lower()
                if "ultrapass" in d:
                    demanda_ultrapassagem_r += abs(float(it.get("valor") or 0))

        # 4c. Energia reativa (UFER)
        reativo_r = 0.0
        for f in faturas:
            for it in (f.get("itens_faturados") or []):
                d = (it.get("descricao") or "").lower()
                if "exc" in d and ("en r" in d or "r exc" in d):
                    reativo_r += abs(float(it.get("valor") or 0))
            # Fallback: se não há itens_faturados, usa ufer diretamente
            if not f.get("itens_faturados"):
                u = float(f.get("ufer_fora_ponta_kvarh") or 0)
                if u > 0:
                    reativo_r += u * 0.349

        # 4d. Multas por atraso
        multas_atraso_r = 0.0
        for f in faturas:
            for it in (f.get("itens_faturados") or []):
                d = (it.get("descricao") or "").lower()
                if "multa" in d or "juros" in d or "mora" in d:
                    multas_atraso_r += abs(float(it.get("valor") or 0))

        # 4e. COSIP
        cosip_total = sum(float(f.get("cosip_valor") or 0) for f in faturas)

        # 4f. GD (créditos de geração distribuída)
        gd_creditos_r = 0.0
        for f in faturas:
            for it in (f.get("itens_faturados") or []):
                d = (it.get("descricao") or "").lower()
                if "credito" in d and "gera" in d:
                    gd_creditos_r += abs(float(it.get("valor") or 0))

        # 4g. ICMS sobre TUSD (~60% é sobre a parte da distribuição, recuperável)
        icms_recuperavel_r = 0.0
        for f in faturas:
            iv = float(f.get("icms_valor") or 0)
            if iv > 0:
                icms_recuperavel_r += iv * 0.60
            else:
                tp = float(f.get("total_a_pagar") or 0)
                if tp > 5000:
                    icms_recuperavel_r += tp * 0.63 * 0.18  # estimativa TUSD × alíquota

        # ── 5. Totais de potencial ───────────────────────────────────────────
        potencial_periodo = (
            demanda_ociosa_r
            + demanda_ultrapassagem_r
            + reativo_r
            + multas_atraso_r
            + icms_recuperavel_r
        )
        potencial_anual = (potencial_periodo / n * 12) if n > 0 else 0.0

        # ── 6. Demanda ───────────────────────────────────────────────────────
        demanda_contratada = max(
            max(float(f.get("demanda_contratada_ponta_kw") or 0),
                float(f.get("demanda_contratada_fora_ponta_kw") or 0))
            for f in faturas
        )
        dms = [
            max(float(f.get("demanda_medida_ponta_kw") or 0),
                float(f.get("demanda_medida_fora_ponta_kw") or 0))
            for f in faturas
        ]
        demanda_media_medida = sum(dms) / n
        demanda_pico = max(dms)
        utilizacao_demanda = (
            round(demanda_media_medida / demanda_contratada * 100)
            if demanda_contratada > 0 else 0
        )

        # ── 7. Flags ─────────────────────────────────────────────────────────
        elegivel_mercado_livre = (
            (f0.get("subgrupo") or "").startswith("A")
            or demanda_contratada >= 300
        )
        tem_gd = gd_creditos_r > 0 or any(
            float(f.get("energia_reversa_kwh") or 0) > 0 for f in faturas
        )

        # ── 8. Pontos de atenção (max 6, textos para o PPTX) ─────────────────
        pontos = self._montar_pontos_atencao(
            demanda_contratada, demanda_media_medida, utilizacao_demanda,
            reativo_r, gd_creditos_r, cosip_total / n if n > 0 else 0,
            multas_atraso_r, icms_recuperavel_r, elegivel_mercado_livre,
            f0, alertas_abertos
        )

        # ── 9. Cronograma de ações (max 8) ───────────────────────────────────
        acoes = self._montar_acoes(
            demanda_contratada, demanda_media_medida, utilizacao_demanda,
            elegivel_mercado_livre, reativo_r, gd_creditos_r,
            cosip_total / n if n > 0 else 0, icms_recuperavel_r
        )

        return ResultadoHistorico(
            uc=f0.get("uc", ""),
            nome=f0.get("cliente_nome", ""),
            cnpj=cnpj,
            subgrupo=f0.get("subgrupo", ""),
            modalidade=f0.get("modalidade", ""),
            n_faturas=n,
            custo_medio=custo_medio,
            custo_anual=custo_medio * 12,
            demanda_ociosa_r=demanda_ociosa_r,
            demanda_ultrapassagem_r=demanda_ultrapassagem_r,
            reativo_r=reativo_r,
            multas_atraso_r=multas_atraso_r,
            icms_recuperavel_r=icms_recuperavel_r,
            cosip_total=cosip_total,
            gd_creditos_r=gd_creditos_r,
            potencial_periodo=potencial_periodo,
            potencial_anual=potencial_anual,
            demanda_contratada=demanda_contratada,
            demanda_media_medida=demanda_media_medida,
            demanda_pico=demanda_pico,
            utilizacao_demanda=utilizacao_demanda,
            elegivel_mercado_livre=elegivel_mercado_livre,
            tem_gd=tem_gd,
            pontos_atencao=pontos,
            acoes=acoes[:8],
            alertas_por_codigo=alertas_por_codigo,
            fatura_mais_recente=f0,
        )

    # ── Helpers internos ──────────────────────────────────────────────────────

    def _montar_pontos_atencao(
        self, dcm, dmd, ut, dr, gd, cosip_m, dm2, icms, el, f0, alertas_abertos
    ) -> list[str]:
        """Monta até 6 pontos de atenção para os slides do PPTX."""
        pts = []

        # pt1 — sempre curto (caixa pequena no template)
        pts.append("Demanda contratada atual.")

        # pt2 — demanda
        if ut < 85 and dcm > 0:
            pts.append(
                f"Demanda não utilizada recorrente: {_fi(dcm)} kW contratada vs "
                f"{_fi(dmd)} kW utilizada ({ut}%). Pode ser ajustada com sazonalidade."
            )
        else:
            pts.append(
                f"Demanda de {_fi(dcm)} kW com utilização de {ut}%. "
                "Dentro dos parâmetros aceitáveis."
            )

        # pt3 — reativo ou GD
        if dr > 0:
            pts.append(
                f"Energia reativa (UFER): R$ {_f(dr)}. "
                "Multa por baixo fator de potência. "
                "Corrigir com Banco de Capacitores e estudar Filtro Capacitivo."
            )
        elif gd > 0:
            pts.append(
                f"Geração Distribuída ativa. Créditos de R$ {_f(gd)}. "
                "Verificar potencial de expansão do sistema."
            )
        else:
            pts.append(
                "Sem cobrança de energia reativa. "
                "Fator de potência dentro dos limites da ANEEL."
            )

        # pt4 — COSIP ou multas atraso
        if cosip_m > 300:
            pts.append(
                f"COSIP elevada: R$ {_f(cosip_m)}/mês. "
                "Contestar valor junto à Prefeitura e verificar base de cálculo."
            )
        elif dm2 > 0:
            pts.append(
                f"Multas por atrasos: R$ {_f(dm2)}. "
                "Requer gestão ativa nas contas para evitar encargos."
            )
        else:
            pts.append(
                "Sem multas por atraso identificadas. "
                "Manter gestão preventiva dos vencimentos."
            )

        # pt5 — ICMS ou Mercado Livre ou multas
        if icms > 0:
            pts.append(
                f"ICMS sobre TUSD: R$ {_f(icms)} potencialmente recuperável. "
                "Elaborar laudo técnico para separar produção."
            )
        elif el:
            pts.append(
                f"Elegível para o Mercado Livre ({f0.get('subgrupo','?')}, "
                f"{_fi(dcm)} kW). Economia estimada de 15-25% via comercializadora."
            )
        else:
            pts.append(
                "Auditar leituras do medidor para identificar "
                "cobranças indevidas nas últimas 120 faturas."
            )

        # pt6 — alertas abertos ou fallback
        for al in alertas_abertos:
            t = al.get("titulo", "")
            if t and len(pts) < 6:
                pts.append(f"{t}. {al.get('descricao', '')[:80]}")

        while len(pts) < 6:
            pts.append(
                "Auditar registros do medidor para "
                "identificar cobranças indevidas retroativas."
            )

        return pts[:6]

    def _montar_acoes(
        self, dcm, dmd, ut, el, dr, gd, cosip_m, icms
    ) -> list[tuple]:
        """Monta até 8 ações para o cronograma do PPTX. Cada item: (titulo, descricao)."""
        acoes = []

        acoes.append((
            "Auditoria\nRetroativa\ndos últimos\n120 meses",
            "Buscar pagamentos\nindevidos e pedir\nrestituição"
        ))

        if ut < 85 and dcm > 0:
            acoes.append((
                "Ajustar a\ndemanda\ncontratada\nociosa",
                f"Reduzir de {_fi(dcm)} kW\npara ~{_fi(dmd)} kW\n(economia imediata)"
            ))

        if el:
            acoes.append((
                "Migração\npara o\nMercado Livre",
                "Economia de 15-25%\nna tarifa de energia\nvia comercializadora"
            ))

        if dr > 0:
            acoes.append((
                "Corrigir\nfator de\npotência",
                "Instalar/ajustar\nBanco de Capacitores\nURGENTE"
            ))

        acoes.append((
            "Laudo de\nICMS para\ncréditos\nda energia",
            "Fazer laudo para\nseparar o que é\nprodução"
        ))

        if gd > 0:
            acoes.append((
                "Otimizar\nGeração\nDistribuída",
                "Gestão de créditos\ne estudo de\nexpansão"
            ))

        if cosip_m > 300:
            acoes.append((
                "Contestar\nCOSIP junto\nà Prefeitura",
                f"Valor médio\nR$ {_fi(cosip_m)}/mês\nacima do padrão"
            ))

        acoes.append((
            "Relatório\nMensal de\nResultado",
            "Prestação de contas\ncom economia\ne metas"
        ))

        while len(acoes) < 8:
            acoes.append((
                "Vistoria\nTécnica da\nInstalação",
                "Inspeção da rede\nelétrica para\nmelhorias"
            ))

        return acoes[:8]


# ── Função de conveniência ────────────────────────────────────────────────────

def analisar_historico(
    faturas: list[dict],
    alertas_abertos: Optional[list[dict]] = None,
    cnpj: str = "",
) -> ResultadoHistorico:
    """Recebe lista de faturas do Supabase e retorna ResultadoHistorico."""
    return AnalisadorHistorico().analisar(faturas, alertas_abertos, cnpj)
