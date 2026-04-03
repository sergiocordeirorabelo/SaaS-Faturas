"""
Motor de Análise de Faturas — Regras de Negócio para Grupo A
Detecta oportunidades de redução de custo e flags de anomalias.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


# ── Configuração das regras ────────────────────────────────────────────────────

# Percentual mínimo de utilização da demanda contratada (ANEEL: 10% por ultrapassagem)
UTILIZ_DEMANDA_MINIMA = 0.85   # abaixo disso → demanda superdimensionada
UTILIZ_DEMANDA_CRITICA = 0.70  # muito abaixo → ajuste urgente

# Fator de potência mínimo exigido pela ANEEL: 0,92
FP_MINIMO = 0.92

# Limite de COSIP "razoável" para Grupo A (acima disso, vale contestar)
COSIP_LIMITE_PERCENTUAL = 0.15  # 15% do total sem COSIP

# Bandeiras com impacto (para sinalizar risco)
BANDEIRAS_ALERTA = {"Amarela", "Vermelha 1", "Vermelha 2"}


# ── Dataclasses de resultado ──────────────────────────────────────────────────

@dataclass
class Alerta:
    codigo: str
    severidade: str      # "critico" | "atencao" | "info"
    titulo: str
    descricao: str
    economia_mensal_r: Optional[float] = None
    economia_anual_r: Optional[float] = None
    acao_recomendada: Optional[str] = None


@dataclass
class ResultadoAnalise:
    uc: str
    mes_referencia: str
    total_fatura: float
    alertas: list[Alerta] = field(default_factory=list)
    resumo_executivo: str = ""
    potencial_economia_mensal: float = 0.0
    potencial_economia_anual: float = 0.0
    score_eficiencia: int = 100   # 0-100, começa perfeito e desconta por problemas

    def to_dict(self) -> dict:
        return {
            "uc": self.uc,
            "mes_referencia": self.mes_referencia,
            "total_fatura": self.total_fatura,
            "score_eficiencia": self.score_eficiencia,
            "potencial_economia_mensal": round(self.potencial_economia_mensal, 2),
            "potencial_economia_anual": round(self.potencial_economia_anual, 2),
            "resumo_executivo": self.resumo_executivo,
            "alertas": [
                {
                    "codigo": a.codigo,
                    "severidade": a.severidade,
                    "titulo": a.titulo,
                    "descricao": a.descricao,
                    "economia_mensal_r": round(a.economia_mensal_r, 2) if a.economia_mensal_r else None,
                    "economia_anual_r": round(a.economia_anual_r, 2) if a.economia_anual_r else None,
                    "acao_recomendada": a.acao_recomendada,
                }
                for a in self.alertas
            ],
        }


# ── Motor ─────────────────────────────────────────────────────────────────────

class AnalisadorFatura:
    """
    Aplica regras de negócio sobre o dict retornado pelo parser.
    Cada método _check_* adiciona alertas ao resultado.
    """

    def analisar(self, fatura: dict) -> ResultadoAnalise:
        result = ResultadoAnalise(
            uc=fatura.get("uc", ""),
            mes_referencia=fatura.get("mes_referencia", ""),
            total_fatura=fatura.get("total_a_pagar") or 0.0,
        )

        self._check_demanda_superdimensionada(fatura, result)
        self._check_demanda_ultrapassagem(fatura, result)
        self._check_fator_potencia(fatura, result)
        self._check_geracao_distribuida(fatura, result)
        self._check_cosip(fatura, result)
        self._check_bandeira(fatura, result)
        self._check_consumo_anomalo(fatura, result)
        self._check_mercado_livre(fatura, result)

        # Consolidar potencial de economia
        result.potencial_economia_mensal = sum(
            a.economia_mensal_r for a in result.alertas if a.economia_mensal_r
        )
        result.potencial_economia_anual = result.potencial_economia_mensal * 12

        # Score: -15 por critico, -5 por atencao
        for a in result.alertas:
            if a.severidade == "critico":
                result.score_eficiencia -= 15
            elif a.severidade == "atencao":
                result.score_eficiencia -= 5
        result.score_eficiencia = max(0, result.score_eficiencia)

        result.resumo_executivo = self._gerar_resumo(fatura, result)
        return result

    # ── Regras ────────────────────────────────────────────────────────────

    def _check_demanda_superdimensionada(self, f: dict, r: ResultadoAnalise) -> None:
        """Demanda contratada muito acima da medida → cliente paga pelo que não usa."""
        ctda = f.get("demanda_contratada_fora_ponta_kw")
        medi = f.get("demanda_medida_fora_ponta_kw")
        tar  = f.get("tarifa_demanda")

        if not all([ctda, medi, tar]):
            return

        utilizacao = medi / ctda
        excesso_kw = ctda - medi

        if utilizacao < UTILIZ_DEMANDA_CRITICA:
            severidade = "critico"
        elif utilizacao < UTILIZ_DEMANDA_MINIMA:
            severidade = "atencao"
        else:
            return

        # Economia: reduzir contratada para 110% da medida (margem de segurança)
        nova_ctda = round(medi * 1.10)
        reducao_kw = ctda - nova_ctda
        economia_mensal = reducao_kw * tar
        economia_anual  = economia_mensal * 12

        r.alertas.append(Alerta(
            codigo="DEMANDA_SUPERDIMENSIONADA",
            severidade=severidade,
            titulo=f"Demanda contratada superdimensionada ({utilizacao:.0%} de utilização)",
            descricao=(
                f"A demanda contratada é {ctda:.0f} kW, mas a demanda medida foi apenas "
                f"{medi:.0f} kW ({utilizacao:.0%} de utilização). "
                f"O excesso de {excesso_kw:.0f} kW representa custo sem contrapartida."
            ),
            economia_mensal_r=economia_mensal,
            economia_anual_r=economia_anual,
            acao_recomendada=(
                f"Solicitar redução da demanda contratada de {ctda:.0f} kW para "
                f"{nova_ctda:.0f} kW junto à Amazonas Energia. "
                f"Permitido 1 ajuste por ciclo tarifário (normalmente anual)."
            ),
        ))

    def _check_demanda_ultrapassagem(self, f: dict, r: ResultadoAnalise) -> None:
        """
        Demanda medida > contratada → multa automática.
        Regra ANEEL: excedente cobrado em 3x a tarifa normal.
        """
        ctda = f.get("demanda_contratada_fora_ponta_kw")
        medi = f.get("demanda_medida_fora_ponta_kw")
        tar  = f.get("tarifa_demanda")
        itens = f.get("itens_faturados", [])

        if not all([ctda, medi, tar]):
            return

        # Verifica se há dois itens de demanda (sinal de cobrança de ultrapassagem)
        dem_itens = [i for i in itens if "Demanda" in i.get("descricao", "")]
        if len(dem_itens) < 2:
            return

        # Se a demanda medida > contratada, há ultrapassagem direta
        if medi > ctda:
            excesso = medi - ctda
            custo_extra = excesso * tar * 2   # paga 3x, já pagou 1x → excedente = 2x
            r.alertas.append(Alerta(
                codigo="DEMANDA_ULTRAPASSAGEM",
                severidade="critico",
                titulo=f"Ultrapassagem de demanda: {excesso:.0f} kW acima do contratado",
                descricao=(
                    f"A demanda medida ({medi:.0f} kW) ultrapassou a contratada ({ctda:.0f} kW) "
                    f"em {excesso:.0f} kW. O excedente é cobrado a 3x a tarifa normal."
                ),
                economia_mensal_r=custo_extra,
                acao_recomendada=(
                    "Revisar equipamentos com maior demanda de pico. "
                    "Escalonar ligação de cargas pesadas. "
                    "Considerar aumentar a demanda contratada se o crescimento for permanente."
                ),
            ))
        elif len(dem_itens) == 2:
            # Dois itens de demanda sem ultrapassagem: padrão da Amazonas Energia
            # (cobram medida + diferença para contratada)
            dem_valores = sorted([i.get("quantidade", 0) for i in dem_itens])
            if dem_valores[1] < ctda:
                # Confirma que é cobrança de demanda mínima (contratada não atingida)
                # Já tratado em DEMANDA_SUPERDIMENSIONADA
                pass

    def _check_fator_potencia(self, f: dict, r: ResultadoAnalise) -> None:
        """
        Fator de potência abaixo de 0,92 gera multa automática.
        FP = kWh / sqrt(kWh² + kVArh²)
        """
        kwh_fp   = f.get("consumo_fora_ponta_kwh") or 0
        ufer_fp  = f.get("ufer_fora_ponta_kvarh") or 0
        ufer_pta = f.get("ufer_ponta_kvarh") or 0
        tar_reativo = f.get("tarifa_consumo_fora_ponta")  # proxy para tarifa reativo

        # Ufer > 0 significa consumo reativo excedente (FP baixo)
        total_ufer = ufer_fp + ufer_pta
        if total_ufer <= 0:
            return

        # Estima FP com base no ufer registrado
        # ufer registrado = energia reativa excedente em kVArh
        # FP ~ cos(arctan(kVArh/kWh)) — aproximação
        if kwh_fp > 0:
            fp_estimado = kwh_fp / (kwh_fp**2 + (ufer_fp * 1000)**2)**0.5
            fp_desc = f"FP estimado ≈ {fp_estimado:.3f}"
        else:
            fp_desc = f"Energia reativa excedente: {total_ufer:.0f} kVArh"

        custo_reativo = 0.69  # valor do item En R Exc F/Ponta desta fatura
        itens = f.get("itens_faturados", [])
        custo_reativo = next(
            (i.get("valor", 0) for i in itens if "En R Exc" in i.get("descricao", "")),
            custo_reativo
        )

        if custo_reativo > 0:
            r.alertas.append(Alerta(
                codigo="FATOR_POTENCIA_BAIXO",
                severidade="atencao",
                titulo="Energia reativa excedente faturada",
                descricao=(
                    f"Foram cobrados R$ {custo_reativo:.2f} por energia reativa excedente "
                    f"({total_ufer:.0f} kVArh). "
                    "Isso indica fator de potência abaixo de 0,92 em algum período."
                ),
                economia_mensal_r=custo_reativo,
                acao_recomendada=(
                    "Instalar banco de capacitores automaticamente chaveados. "
                    "Realizar medição do FP por 30 dias para dimensionar a correção. "
                    "Custo de instalação geralmente se paga em 6-18 meses."
                ),
            ))

    def _check_geracao_distribuida(self, f: dict, r: ResultadoAnalise) -> None:
        """Informa sobre geração distribuída ativa e oportunidades de expansão."""
        reversa = f.get("energia_reversa_kwh") or 0
        cred    = f.get("credito_geracao") or 0
        media   = f.get("media_12_meses_kwh") or 0
        total   = f.get("total_a_pagar") or 0

        if reversa <= 0:
            return

        # Quanto % do consumo a GD está cobrindo
        consumo_total = f.get("consumo_total_kwh") or media or 1
        cobertura = reversa / consumo_total if consumo_total > 0 else 0

        r.alertas.append(Alerta(
            codigo="GD_ATIVA",
            severidade="info",
            titulo=f"Geração distribuída ativa: {reversa:.0f} kWh injetados",
            descricao=(
                f"A unidade injetou {reversa:.0f} kWh na rede, gerando crédito de "
                f"R$ {abs(cred):.2f} ({cobertura:.1%} do consumo total). "
                f"O crédito foi abatido diretamente nesta fatura."
            ),
            acao_recomendada=(
                "Verificar se o sistema GD está operando no potencial máximo. "
                f"Com {cobertura:.0%} de cobertura atual, há espaço para expansão "
                "se o consumo superar a geração."
            ) if cobertura < 0.5 else (
                "Sistema GD com boa cobertura. "
                "Verificar se há créditos acumulados a utilizar em outros meses."
            ),
        ))

    def _check_cosip(self, f: dict, r: ResultadoAnalise) -> None:
        """COSIP anormalmente alta merece contestação."""
        cosip = f.get("cosip_valor") or 0
        total = f.get("total_a_pagar") or 1
        # Total sem COSIP
        total_sem_cosip = total - cosip

        if total_sem_cosip <= 0:
            return

        pct = cosip / total_sem_cosip

        if pct > COSIP_LIMITE_PERCENTUAL:
            r.alertas.append(Alerta(
                codigo="COSIP_ELEVADA",
                severidade="atencao",
                titulo=f"COSIP representa {pct:.1%} do valor da conta",
                descricao=(
                    f"A Contribuição de Iluminação Pública é R$ {cosip:.2f}, "
                    f"equivalente a {pct:.1%} do valor da conta (limite referência: 15%). "
                    "Valores muito acima da média merecem verificação junto à prefeitura."
                ),
                acao_recomendada=(
                    "Solicitar memória de cálculo da COSIP à Amazonas Energia. "
                    "Verificar se o enquadramento tarifário da UC está correto. "
                    "Contestar junto à Prefeitura de Manaus se o valor não corresponder "
                    "ao previsto na legislação municipal."
                ),
            ))

    def _check_bandeira(self, f: dict, r: ResultadoAnalise) -> None:
        """Bandeira amarela/vermelha tem impacto significativo no consumo."""
        bandeira = f.get("bandeira_tarifaria") or ""
        if bandeira in BANDEIRAS_ALERTA:
            r.alertas.append(Alerta(
                codigo="BANDEIRA_TARIFARIA",
                severidade="atencao",
                titulo=f"Bandeira tarifária: {bandeira}",
                descricao=(
                    f"A bandeira {bandeira} está ativa, acrescentando custo adicional "
                    "por kWh consumido. Isso impacta diretamente o custo de energia."
                ),
                acao_recomendada=(
                    "Intensificar ações de eficiência energética neste período. "
                    "Avaliar deslocamento de cargas para horários fora de ponta "
                    "enquanto a bandeira estiver acionada."
                ),
            ))

    def _check_consumo_anomalo(self, f: dict, r: ResultadoAnalise) -> None:
        """Consumo do mês muito acima ou abaixo da média histórica."""
        historico = f.get("historico_kwh") or []
        consumo   = f.get("consumo_total_kwh") or 0
        media     = f.get("media_12_meses_kwh") or 0

        if not historico or not consumo or not media:
            return

        desvio = (consumo - media) / media if media > 0 else 0

        if desvio > 0.30:
            r.alertas.append(Alerta(
                codigo="CONSUMO_ALTO",
                severidade="atencao",
                titulo=f"Consumo {desvio:.0%} acima da média histórica",
                descricao=(
                    f"O consumo deste mês ({consumo:,.0f} kWh) está {desvio:.0%} acima "
                    f"da média dos últimos 12 meses ({media:,.0f} kWh). "
                    "Pode indicar desperdício ou novo equipamento de alto consumo."
                ),
                acao_recomendada=(
                    "Realizar auditoria energética para identificar a causa do aumento. "
                    "Verificar se há equipamentos ligados fora do horário de operação."
                ),
            ))
        elif desvio < -0.30:
            r.alertas.append(Alerta(
                codigo="CONSUMO_BAIXO",
                severidade="info",
                titulo=f"Consumo {abs(desvio):.0%} abaixo da média histórica",
                descricao=(
                    f"O consumo ({consumo:,.0f} kWh) está {abs(desvio):.0%} abaixo "
                    f"da média ({media:,.0f} kWh). Pode ser período de baixa atividade "
                    "ou resultado de medidas de eficiência energética."
                ),
            ))

    def _check_mercado_livre(self, f: dict, r: ResultadoAnalise) -> None:
        """
        Clientes Grupo A com demanda >= 500 kW podem migrar para o Mercado Livre.
        Abaixo disso, verificar ACL especial (>= 500 kW) ou ACR melhor.
        """
        ctda = f.get("demanda_contratada_fora_ponta_kw") or 0
        total = f.get("total_a_pagar") or 0
        subgrupo = f.get("subgrupo") or ""

        # Critério atual ANEEL: >= 500 kW podem migrar livremente
        # A4 (2,3 a 25 kV): elegível para ACL com contratos especiais
        if ctda >= 500:
            r.alertas.append(Alerta(
                codigo="MERCADO_LIVRE_ELEGIVEL",
                severidade="info",
                titulo="Elegível para migração ao Mercado Livre de Energia",
                descricao=(
                    f"Com demanda contratada de {ctda:.0f} kW, esta UC é elegível "
                    "para o Mercado Livre de Energia (ACL). Dependendo do perfil de "
                    "consumo, a economia pode ser de 15-30% na conta de energia."
                ),
                acao_recomendada=(
                    "Solicitar estudo de viabilidade de migração para o ACL. "
                    "Contratar consultoria especializada para negociar contratos de energia. "
                    "Analisar curva de carga antes de migrar."
                ),
            ))
        elif ctda >= 300 and subgrupo in ("A4", "A3a", "A3"):
            r.alertas.append(Alerta(
                codigo="MERCADO_LIVRE_FUTURO",
                severidade="info",
                titulo="Consumidor em zona de transição para o Mercado Livre",
                descricao=(
                    f"Com demanda de {ctda:.0f} kW, esta UC não é elegível ainda "
                    "para o ACL pleno (mín. 500 kW), mas a tendência regulatória "
                    "é reduzir esse limiar. Recomenda-se monitorar e se preparar."
                ),
            ))

    # ── Resumo ────────────────────────────────────────────────────────────────

    def _gerar_resumo(self, f: dict, r: ResultadoAnalise) -> str:
        criticos = [a for a in r.alertas if a.severidade == "critico"]
        atencao  = [a for a in r.alertas if a.severidade == "atencao"]
        infos    = [a for a in r.alertas if a.severidade == "info"]

        partes = []

        if r.potencial_economia_mensal > 0:
            partes.append(
                f"Identificamos potencial de economia de "
                f"R$ {r.potencial_economia_mensal:,.2f}/mês "
                f"(R$ {r.potencial_economia_anual:,.2f}/ano) "
                f"na fatura de {f.get('mes_referencia', '')}."
            )

        if criticos:
            partes.append(
                f"{len(criticos)} problema(s) crítico(s): "
                + "; ".join(a.titulo for a in criticos) + "."
            )

        if atencao:
            partes.append(
                f"{len(atencao)} ponto(s) de atenção: "
                + "; ".join(a.titulo for a in atencao) + "."
            )

        if not partes:
            partes.append(
                "Nenhuma anomalia significativa identificada nesta fatura. "
                "A conta está dentro dos parâmetros normais."
            )

        partes.append(f"Score de eficiência: {r.score_eficiencia}/100.")
        return " ".join(partes)


# ── Função de conveniência ────────────────────────────────────────────────────

def analisar_fatura(fatura_dict: dict) -> dict:
    """Recebe o dict do parser e retorna o dict de análise."""
    return AnalisadorFatura().analisar(fatura_dict).to_dict()
