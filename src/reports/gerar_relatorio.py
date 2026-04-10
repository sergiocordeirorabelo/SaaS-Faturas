"""
Gerador de Relatório PDF — SaaS Análise de Energia
Usa ReportLab Platypus para texto justificado e layout profissional.
"""

from __future__ import annotations

import io
from datetime import datetime
from pathlib import Path
from typing import Optional

from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT, TA_JUSTIFY
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    HRFlowable, KeepTogether
)
from reportlab.platypus.flowables import HRFlowable

W, H = A4
MX = 18 * mm  # margem horizontal

# ── Cores ─────────────────────────────────────────────────────────────────────
C_WHITE      = colors.white
C_BG_CARD    = colors.HexColor("#f8f9fa")
C_TEXT       = colors.HexColor("#1a1d23")
C_MUTED      = colors.HexColor("#6b7280")
C_HINT       = colors.HexColor("#9ca3af")
C_BORDER     = colors.HexColor("#e5e7eb")
C_GREEN      = colors.HexColor("#059669")
C_GREEN_BG   = colors.HexColor("#d1fae5")
C_GREEN_DARK = colors.HexColor("#065f46")
C_AMBER      = colors.HexColor("#d97706")
C_AMBER_BG   = colors.HexColor("#fef3c7")
C_RED        = colors.HexColor("#dc2626")
C_RED_BG     = colors.HexColor("#fee2e2")
C_BLUE       = colors.HexColor("#2563eb")
C_ACCENT     = colors.HexColor("#0d2137")

# ── Estilos de parágrafo ──────────────────────────────────────────────────────
def _estilos():
    return {
        "normal": ParagraphStyle("normal", fontName="Helvetica",         fontSize=9,    leading=14, textColor=C_TEXT,  alignment=TA_JUSTIFY),
        "small":  ParagraphStyle("small",  fontName="Helvetica",         fontSize=8,    leading=12, textColor=C_MUTED),
        "bold":   ParagraphStyle("bold",   fontName="Helvetica-Bold",    fontSize=9,    leading=14, textColor=C_TEXT),
        "title":  ParagraphStyle("title",  fontName="Helvetica-Bold",    fontSize=14,   leading=18, textColor=C_TEXT),
        "label":  ParagraphStyle("label",  fontName="Helvetica-Bold",    fontSize=7.5,  leading=10, textColor=C_MUTED, spaceAfter=2),
        "value":  ParagraphStyle("value",  fontName="Helvetica-Bold",    fontSize=13,   leading=16, textColor=C_TEXT),
        "green":  ParagraphStyle("green",  fontName="Helvetica-Bold",    fontSize=13,   leading=16, textColor=C_GREEN),
        "white":  ParagraphStyle("white",  fontName="Helvetica-Bold",    fontSize=10,   leading=14, textColor=C_WHITE),
        "white_s":ParagraphStyle("white_s",fontName="Helvetica",         fontSize=8,    leading=11, textColor=colors.HexColor("#9DB8CC")),
        "ai":     ParagraphStyle("ai",     fontName="Helvetica-Oblique", fontSize=9,    leading=14, textColor=C_MUTED, alignment=TA_JUSTIFY),
        "sec":    ParagraphStyle("sec",    fontName="Helvetica-Bold",    fontSize=7.5,  leading=10, textColor=C_MUTED, spaceAfter=4),
        "prop_l": ParagraphStyle("prop_l", fontName="Helvetica",         fontSize=9,    leading=13, textColor=C_MUTED),
        "prop_v": ParagraphStyle("prop_v", fontName="Helvetica-Bold",    fontSize=9,    leading=13, textColor=C_TEXT,  alignment=TA_RIGHT),
        "prop_g": ParagraphStyle("prop_g", fontName="Helvetica-Bold",    fontSize=9,    leading=13, textColor=C_GREEN, alignment=TA_RIGHT),
    }

S = _estilos()

# ── Header/Footer ─────────────────────────────────────────────────────────────
def _header_footer(canvas, doc):
    canvas.saveState()
    # Topo azul escuro
    canvas.setFillColor(C_ACCENT)
    canvas.rect(0, H - 18*mm, W, 18*mm, fill=1, stroke=0)
    # Faixa verde
    canvas.setFillColor(C_GREEN)
    canvas.rect(0, H - 19.5*mm, W, 1.5*mm, fill=1, stroke=0)
    # Logo
    canvas.setFillColor(C_WHITE)
    canvas.setFont("Helvetica-Bold", 10)
    canvas.drawString(MX, H - 11*mm, "SaaS Analise de Energia")
    # Data
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(colors.HexColor("#9DB8CC"))
    canvas.drawRightString(W - MX, H - 11*mm, f"Emitido em {datetime.now().strftime('%d/%m/%Y')}")
    # Rodapé
    canvas.setFillColor(C_ACCENT)
    canvas.rect(0, 0, W, 9*mm, fill=1, stroke=0)
    canvas.setFillColor(C_WHITE)
    canvas.setFont("Helvetica", 7)
    canvas.drawString(MX, 3.5*mm, "Documento confidencial — SaaS Analise de Energia")
    canvas.drawRightString(W - MX, 3.5*mm, f"Gerado em {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    canvas.restoreState()


# ── Células de métrica ────────────────────────────────────────────────────────
def _card_metric(label: str, value: str, sub: str, value_style="value") -> Table:
    data = [
        [Paragraph(label.upper(), S["label"])],
        [Paragraph(value, S[value_style])],
        [Paragraph(sub,   S["small"])],
    ]
    t = Table(data, colWidths=[(W - 2*MX - 3*3*mm) / 4])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,-1), C_BG_CARD),
        ("ROUNDEDCORNERS", [4,4,4,4]),
        ("TOPPADDING",    (0,0), (-1,-1), 8),
        ("BOTTOMPADDING", (0,0), (-1,-1), 8),
        ("LEFTPADDING",   (0,0), (-1,-1), 10),
        ("RIGHTPADDING",  (0,0), (-1,-1), 10),
    ]))
    return t


def _metricas(d: dict) -> Table:
    def _f(v, default=0):
        try: return float(v) if v is not None else default
        except: return default
    score  = _f(d.get("score_eficiencia"), 100)
    eco_m  = _f(d.get("potencial_economia_mensal"))
    eco_a  = _f(d.get("potencial_economia_anual"))
    total  = _f(d.get("total_a_pagar"))
    ctda   = _f(d.get("demanda_contratada_fora_ponta_kw"))
    medi   = _f(d.get("demanda_medida_fora_ponta_kw"))
    util   = round(medi / ctda * 100) if ctda > 0 else 0

    def fmt(v): return f"R$ {v:,.0f}".replace(",",".")

    score_color = "green" if score >= 85 else ("value" if score >= 70 else "value")
    util_color  = "value" if util >= 70 else "value"

    cw = (W - 2*MX - 9*mm) / 4
    cards = [
        _make_card("Total da fatura",        fmt(total),     d.get("mes_referencia",""),     "value",       cw),
        _make_card("Economia potencial / ano", fmt(eco_a),   f"{fmt(eco_m)}/mes",             "green",       cw),
        _make_card("Score de eficiencia",     f"{score}/100", _score_label(score),            score_color,   cw),
        _make_card("Utilizacao da demanda",   f"{util}%",     f"{medi:.0f} kW de {ctda:.0f} kW", util_color, cw),
    ]

    t = Table([cards], colWidths=[cw]*4, hAlign="LEFT",
              spaceBefore=0, spaceAfter=0,
              style=[
                  ("LEFTPADDING",  (0,0), (-1,-1), 0),
                  ("RIGHTPADDING", (0,0), (-1,-1), 0),
                  ("TOPPADDING",   (0,0), (-1,-1), 0),
                  ("BOTTOMPADDING",(0,0), (-1,-1), 0),
                  ("ALIGN",        (0,0), (-1,-1), "LEFT"),
                  ("COLPADDING",   (0,0), (-1,-1), 3),
              ])
    return t


def _make_card(label, value, sub, vstyle, cw) -> Table:
    data = [
        [Paragraph(label.upper(), S["label"])],
        [Paragraph(value, S[vstyle])],
        [Paragraph(sub,   S["small"])],
    ]
    t = Table(data, colWidths=[cw - 6*mm])
    t.setStyle(TableStyle([
        ("BACKGROUND",    (0,0), (-1,-1), C_BG_CARD),
        ("TOPPADDING",    (0,0), (-1,-1), 8),
        ("BOTTOMPADDING", (0,0), (-1,-1), 8),
        ("LEFTPADDING",   (0,0), (-1,-1), 9),
        ("RIGHTPADDING",  (0,0), (-1,-1), 9),
        ("LINEBELOW",     (0,0), (-1,0),  0.5, C_BORDER),
    ]))
    return t


# ── Barras de demanda ─────────────────────────────────────────────────────────
def _demanda_table(d: dict) -> Table:
    def _f(v, default=0):
        try: return float(v) if v is not None else default
        except: return default
    ctda = _f(d.get("demanda_contratada_fora_ponta_kw"))
    medi = _f(d.get("demanda_medida_fora_ponta_kw"))
    hist_raw = d.get("historico_kwh") or []
    if isinstance(hist_raw, str):
        import json as _json
        try: hist_raw = _json.loads(hist_raw)
        except: hist_raw = []
    hist = [float(x) for x in hist_raw if x]
    cons = _f(d.get("consumo_total_kwh"), 1) or 1
    tar  = _f(d.get("tarifa_demanda"), 22.96) or 22.96

    ratio = medi / cons if cons > 0 else 0
    pico  = round(ratio * max(hist)) if hist else medi
    nova  = round(pico * 1.1)
    eco   = round((ctda - nova) * tar) if nova < ctda else 0
    util  = round(medi / ctda * 100) if ctda > 0 else 0

    # Largura da coluna de barras
    lbl_w  = 38*mm
    bar_w  = 90*mm
    val_w  = 20*mm
    total_w = W - 2*MX

    def barra(val, ref, cor):
        pct = min(val / ref, 1.0) if ref > 0 else 0
        filled = bar_w * pct
        # Tabela de 2 células: preenchida + vazia
        b = Table([[""]], colWidths=[filled], rowHeights=[4*mm])
        b.setStyle(TableStyle([
            ("BACKGROUND",    (0,0), (-1,-1), cor),
            ("TOPPADDING",    (0,0), (-1,-1), 0),
            ("BOTTOMPADDING", (0,0), (-1,-1), 0),
            ("LEFTPADDING",   (0,0), (-1,-1), 0),
            ("RIGHTPADDING",  (0,0), (-1,-1), 0),
        ]))
        # Container cinza
        container = Table([[b, ""]], colWidths=[filled, bar_w - filled], rowHeights=[4*mm])
        container.setStyle(TableStyle([
            ("BACKGROUND",    (0,0), (0,0),  cor),
            ("BACKGROUND",    (1,0), (1,0),  C_BORDER),
            ("TOPPADDING",    (0,0), (-1,-1), 0),
            ("BOTTOMPADDING", (0,0), (-1,-1), 0),
            ("LEFTPADDING",   (0,0), (-1,-1), 0),
            ("RIGHTPADDING",  (0,0), (-1,-1), 0),
            ("ROUNDEDCORNERS",[3,3,3,3]),
        ]))
        return container

    rows = [
        [Paragraph("Contratada", S["small"]),   barra(ctda, ctda, colors.HexColor("#ef444466")), Paragraph(f"<b>{ctda:.0f} kW</b>", S["bold"])],
        [Paragraph("Medida atual", S["small"]),  barra(medi, ctda, C_BLUE),                       Paragraph(f"<b>{medi:.0f} kW</b>", S["bold"])],
        [Paragraph("Pico historico", S["small"]),barra(pico, ctda, C_AMBER),                      Paragraph(f"<b>{pico:.0f} kW</b>", S["bold"])],
    ]

    t = Table(rows, colWidths=[lbl_w, bar_w, val_w], spaceBefore=2*mm)
    t.setStyle(TableStyle([
        ("VALIGN",        (0,0), (-1,-1), "MIDDLE"),
        ("TOPPADDING",    (0,0), (-1,-1), 4),
        ("BOTTOMPADDING", (0,0), (-1,-1), 4),
        ("LEFTPADDING",   (0,0), (-1,-1), 0),
        ("RIGHTPADDING",  (0,0), (-1,-1), 0),
    ]))

    nota = f"Reducao recomendada: {ctda:.0f} kW para {nova} kW  |  Utilizacao atual: {util}%  |  Economia estimada: R$ {eco:,.0f}/mes".replace(",",".")

    nota_t = Table([[Paragraph(nota, ParagraphStyle("n", fontName="Helvetica-Bold", fontSize=8, textColor=C_GREEN))]], 
                   colWidths=[W - 2*MX])
    nota_t.setStyle(TableStyle([
        ("BACKGROUND",    (0,0), (-1,-1), C_GREEN_BG),
        ("TOPPADDING",    (0,0), (-1,-1), 6),
        ("BOTTOMPADDING", (0,0), (-1,-1), 6),
        ("LEFTPADDING",   (0,0), (-1,-1), 8),
        ("RIGHTPADDING",  (0,0), (-1,-1), 8),
        ("ROUNDEDCORNERS",[4,4,4,4]),
    ]))

    return t, nota_t


# ── Alertas ───────────────────────────────────────────────────────────────────
def _alertas_table(alertas: list) -> Table:
    if not alertas:
        return Table([[Paragraph("Nenhum alerta identificado.", S["small"])]])

    rows = []
    for a in alertas[:4]:
        sev   = (a.get("severidade") or "info").lower()
        titulo = a.get("titulo") or ""
        eco   = a.get("economia_mensal_r")
        acao  = a.get("acao_recomendada") or ""

        bg, tc = {
            "critico": (C_RED_BG,   C_RED),
            "atencao": (C_AMBER_BG, C_AMBER),
        }.get(sev, (C_BG_CARD, C_MUTED))

        label = {"critico":"CRITICO","atencao":"ATENCAO"}.get(sev, "INFO")
        badge = Table([[Paragraph(label, ParagraphStyle("b", fontName="Helvetica-Bold", fontSize=7, textColor=tc))]],
                      colWidths=[18*mm])
        badge.setStyle(TableStyle([
            ("BACKGROUND",    (0,0), (-1,-1), bg),
            ("TOPPADDING",    (0,0), (-1,-1), 3),
            ("BOTTOMPADDING", (0,0), (-1,-1), 3),
            ("LEFTPADDING",   (0,0), (-1,-1), 4),
            ("RIGHTPADDING",  (0,0), (-1,-1), 4),
            ("ROUNDEDCORNERS",[3,3,3,3]),
        ]))

        eco_str = f"<font color='#059669'><b>R$ {eco:,.2f}/mes</b></font>".replace(",",".") if eco and eco > 0 else ""
        desc = Paragraph(f"<b>{titulo}</b><br/>{eco_str}<br/><font color='#6b7280'>{acao[:300]}</font>", 
                         ParagraphStyle("d", fontName="Helvetica", fontSize=8.5, leading=13, alignment=TA_JUSTIFY))

        rows.append([badge, desc])

    cw_badge = 20*mm
    cw_desc  = W - 2*MX - cw_badge - 4*mm
    t = Table(rows, colWidths=[cw_badge, cw_desc])
    t.setStyle(TableStyle([
        ("VALIGN",        (0,0), (-1,-1), "TOP"),
        ("TOPPADDING",    (0,0), (-1,-1), 6),
        ("BOTTOMPADDING", (0,0), (-1,-1), 6),
        ("LEFTPADDING",   (0,0), (-1,-1), 0),
        ("RIGHTPADDING",  (0,0), (-1,-1), 0),
        ("LINEBELOW",     (0,0), (-1,-2), 0.5, C_BORDER),
    ]))
    return t


# ── Proposta ──────────────────────────────────────────────────────────────────
def _proposta_table(d: dict) -> Table:
    def _f(v, default=0):
        try: return float(v) if v is not None else default
        except: return default
    eco_a  = _f(d.get("potencial_economia_anual"))
    eco_m  = _f(d.get("potencial_economia_mensal"))
    hon_a  = round(eco_a * 0.30)
    hon_m  = round(eco_m * 0.30)
    ctda   = _f(d.get("demanda_contratada_fora_ponta_kw"))
    modelo = d.get("modelo_recomendado") or "consultoria"

    def fmt(v): return f"R$ {v:,.0f}".replace(",",".")

    cw_l = 95*mm
    cw_r = W - 2*MX - 12*mm - cw_l

    st_lbl = ParagraphStyle("pl", fontName="Helvetica",      fontSize=9, leading=13, textColor=C_MUTED)
    st_val = ParagraphStyle("pv", fontName="Helvetica-Bold", fontSize=9, leading=13, textColor=C_TEXT,  alignment=TA_RIGHT)
    st_grn = ParagraphStyle("pg", fontName="Helvetica-Bold", fontSize=9, leading=13, textColor=C_GREEN, alignment=TA_RIGHT)
    st_hdr = ParagraphStyle("ph", fontName="Helvetica-Bold", fontSize=9, leading=13, textColor=C_GREEN)
    st_sub = ParagraphStyle("ps", fontName="Helvetica",      fontSize=8, leading=12, textColor=C_MUTED)

    def linha(l, v, vs=st_val):
        return [Paragraph(l, st_lbl), Paragraph(v, vs)]

    # ── Serviço 1: Consultoria por resultado ──────────────────────────────────
    rows_consul = [
        [Paragraph("1. Consultoria por resultado", st_hdr), ""],
        linha("Modelo de cobrança",    "30% da economia gerada / ano"),
        linha("Economia estimada / mes", fmt(eco_m)),
        linha("Economia estimada / ano", fmt(eco_a)),
        linha("Seu honorario / ano",   fmt(hon_a), st_grn),
        linha("Seu honorario / mes",   fmt(hon_m), st_grn),
        linha("Pagamento",             "Mensal — baseado na fatura real"),
        linha("Risco para o cliente",  "Zero — so paga se economizar"),
    ]

    # ── Serviço 2: Monitoramento e gestao de energia ──────────────────────────
    uc_count = 1  # UC da fatura atual; expandir quando tivermos todas as UCs
    mon_mes  = 200  # R$ por UC/mês — referência
    rows_mon = [
        [Paragraph("2. Monitoramento e gestao de energia", st_hdr), ""],
        linha("Modelo de cobrança",    "R$ 300 / UC / mes"),
        linha("Servico",               "Analise automatica mensal de cada fatura"),
        linha("Alertas",               "Notificacao imediata de anomalias"),
        linha("Relatorio mensal",      "PDF com evolucao e recomendacoes"),
        linha("Contrato",              "Mensal — sem fidelidade"),
    ]

    # ── Serviço 3: Mercado Livre (se elegível) ────────────────────────────────
    rows_mercado = []
    if ctda >= 300:
        rows_mercado = [
            [Paragraph("3. Intermediacao — Mercado Livre de Energia", st_hdr), ""],
            linha("Elegibilidade",      f"Demanda de {ctda:.0f} kW — elegivel para ACL"),
            linha("Economia estimada",  "15–30% da conta de energia / ano"),
            linha("Modelo de cobranca", "Comissao unica de corretagem"),
            linha("Ticket estimado",    "R$ 20.000 – R$ 80.000 por contrato"),
        ]

    def make_table(rows):
        t = Table(rows, colWidths=[cw_l, cw_r])
        t.setStyle(TableStyle([
            ("SPAN",          (0,0), (-1,0)),
            ("TOPPADDING",    (0,0), (-1,-1), 5),
            ("BOTTOMPADDING", (0,0), (-1,-1), 5),
            ("LEFTPADDING",   (0,0), (-1,-1), 0),
            ("RIGHTPADDING",  (0,0), (-1,-1), 0),
            ("LINEBELOW",     (0,0), (-1,0),  0.5, colors.HexColor("#a7f3d0")),
            ("LINEBELOW",     (0,1), (-1,-2), 0.3, C_BORDER),
        ]))
        return t

    # ── Nota rodapé ───────────────────────────────────────────────────────────
    nota_txt = (
        "Os servicos podem ser contratados individualmente ou em conjunto. "
        "Na consultoria por resultado, o cliente so paga apos comprovacao da economia na fatura."
    )

    # Monta container principal
    inner_rows = [
        [make_table(rows_consul)],
        [Spacer(1, 4*mm)],
        [make_table(rows_mon)],
    ]
    if rows_mercado:
        inner_rows += [
            [Spacer(1, 4*mm)],
            [make_table(rows_mercado)],
        ]
    inner_rows += [
        [Spacer(1, 4*mm)],
        [Paragraph(nota_txt, ParagraphStyle("nota", fontName="Helvetica-Oblique",
                                             fontSize=8, leading=12, textColor=C_GREEN_DARK))],
    ]

    inner = Table(inner_rows, colWidths=[W - 2*MX - 24*mm])
    inner.setStyle(TableStyle([
        ("LEFTPADDING",   (0,0), (-1,-1), 0),
        ("RIGHTPADDING",  (0,0), (-1,-1), 0),
        ("TOPPADDING",    (0,0), (-1,-1), 0),
        ("BOTTOMPADDING", (0,0), (-1,-1), 0),
    ]))

    container = Table([
        [Paragraph("<b>Servicos disponíveis</b>", ParagraphStyle(
            "ch", fontName="Helvetica-Bold", fontSize=10, leading=14, textColor=C_GREEN))],
        [inner],
    ], colWidths=[W - 2*MX])
    container.setStyle(TableStyle([
        ("BACKGROUND",    (0,0), (-1,-1), C_GREEN_BG),
        ("LINEBELOW",     (0,0), (-1,0),  0.5, colors.HexColor("#a7f3d0")),
        ("TOPPADDING",    (0,0), (-1,0),  10),
        ("BOTTOMPADDING", (0,0), (-1,0),  8),
        ("TOPPADDING",    (0,1), (-1,1),  8),
        ("BOTTOMPADDING", (0,1), (-1,1),  10),
        ("LEFTPADDING",   (0,0), (-1,-1), 14),
        ("RIGHTPADDING",  (0,0), (-1,-1), 14),
        ("BOX",           (0,0), (-1,-1), 1, colors.HexColor("#6ee7b7")),
        ("ROUNDEDCORNERS",[5,5,5,5]),
    ]))
    return container


# ── Título de seção ───────────────────────────────────────────────────────────
def _sec(titulo: str):
    return [
        Paragraph(titulo.upper(), S["sec"]),
        HRFlowable(width=W - 2*MX, thickness=0.5, color=C_BORDER, spaceAfter=4),
    ]


# ── Helpers ───────────────────────────────────────────────────────────────────
def _score_label(s):
    if s < 60: return "Critico — acao imediata"
    if s < 80: return "Atencao — oportunidades"
    return "Eficiente"


# ── Gerador principal ─────────────────────────────────────────────────────────
def gerar_relatorio(dados: dict, output_path: Optional[str | Path] = None) -> bytes:
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=A4,
        leftMargin=MX, rightMargin=MX,
        topMargin=26*mm, bottomMargin=16*mm,
        title=f"Analise de Energia — {dados.get('cliente_nome','')}",
    )

    d = dados
    story = []

    # ── Identificação do cliente ──────────────────────────────────────────────
    nome  = (d.get("cliente_nome") or "").strip()
    if len(nome) > 70: nome = nome[:67] + "..."
    uc    = d.get("uc") or ""
    mes   = d.get("mes_referencia") or ""
    sub   = d.get("subgrupo") or ""
    mod   = d.get("modalidade") or ""

    story.append(Paragraph(nome, S["title"]))
    story.append(Spacer(1, 2*mm))
    story.append(Paragraph(
        f"UC {uc}  |  Referencia: {mes}  |  {sub} — {mod}",
        ParagraphStyle("sub", fontName="Helvetica", fontSize=9, textColor=C_MUTED, leading=12)
    ))
    story.append(HRFlowable(width=W - 2*MX, thickness=0.7, color=C_BORDER, spaceBefore=4, spaceAfter=6))

    # ── Métricas ──────────────────────────────────────────────────────────────
    story.append(_metricas(d))
    story.append(Spacer(1, 5*mm))

    # ── Demanda ───────────────────────────────────────────────────────────────
    story += _sec("Demanda contratada vs medida")
    dem_table, nota_table = _demanda_table(d)
    story.append(dem_table)
    story.append(Spacer(1, 2*mm))
    story.append(nota_table)
    story.append(Spacer(1, 5*mm))

    # ── Alertas ───────────────────────────────────────────────────────────────
    alertas = d.get("alertas") or []
    story += _sec(f"Alertas identificados ({len(alertas)})")
    story.append(_alertas_table(alertas))
    story.append(Spacer(1, 5*mm))

    # ── Análise IA ────────────────────────────────────────────────────────────
    texto = d.get("analise_claude") or d.get("resumo_executivo") or ""
    if texto:
        story += _sec("Analise executiva — gerada por inteligencia artificial")
        # Container com barra verde lateral
        ai_t = Table(
            [[Paragraph(f'"{texto}"', S["ai"])]],
            colWidths=[W - 2*MX - 8*mm],
        )
        ai_t.setStyle(TableStyle([
            ("BACKGROUND",    (0,0), (-1,-1), C_BG_CARD),
            ("TOPPADDING",    (0,0), (-1,-1), 8),
            ("BOTTOMPADDING", (0,0), (-1,-1), 8),
            ("LEFTPADDING",   (0,0), (-1,-1), 12),
            ("RIGHTPADDING",  (0,0), (-1,-1), 12),
            ("LINEBEFORE",    (0,0), (-1,-1), 3, C_GREEN),
        ]))
        story.append(ai_t)
        story.append(Spacer(1, 5*mm))

    # ── Proposta ──────────────────────────────────────────────────────────────
    from reportlab.platypus import PageBreak
    story.append(PageBreak())
    story += _sec("Proposta comercial")
    story.append(_proposta_table(d))

    doc.build(story, onFirstPage=_header_footer, onLaterPages=_header_footer)
    pdf_bytes = buf.getvalue()

    if output_path:
        Path(output_path).write_bytes(pdf_bytes)

    return pdf_bytes


if __name__ == "__main__":
    dados_teste = {
        "cliente_nome": "Servico de Apoio as Micros e Pequenas Empresas do Amazonas — SEBRAE AM",
        "uc": "0087346-2", "mes_referencia": "03/2026",
        "subgrupo": "A4", "modalidade": "Horossazonal Verde",
        "total_a_pagar": 25621.71, "consumo_total_kwh": 31692,
        "demanda_contratada_fora_ponta_kw": 225,
        "demanda_medida_fora_ponta_kw": 142, "tarifa_demanda": 22.96,
        "historico_kwh": [33000,34442,35940,28671,36480,41517,40316,33677,29035,23637,26792,22890,31692],
        "score_eficiencia": 80,
        "potencial_economia_mensal": 459.20,
        "potencial_economia_anual": 5510.40,
        "modelo_recomendado": "consultoria",
        "analise_claude": (
            "Prezado cliente, ao revisar sua fatura de energia, identificamos oportunidades significativas de economia. "
            "Sua demanda contratada esta superdimensionada, permitindo uma reducao de 225 kW para 205 kW, o que pode "
            "gerar uma economia de R$ 459,20 por mes, totalizando R$ 5.510,40 por ano. Alem disso, a instalacao de um "
            "banco de capacitores pode corrigir o fator de potencia e evitar custos adicionais. Essas acoes nao apenas "
            "diminuem seus gastos, mas tambem melhoram a eficiencia energetica do seu negocio."
        ),
        "alertas": [
            {"severidade": "critico", "titulo": "Demanda superdimensionada — 63% de utilizacao",
             "economia_mensal_r": 459.20, "acao_recomendada": "Reduzir de 225 kW para 205 kW (110% do pico historico de 186 kW)."},
            {"severidade": "atencao", "titulo": "Energia reativa excedente faturada",
             "economia_mensal_r": 0.69,   "acao_recomendada": "Instalar banco de capacitores automatico."},
            {"severidade": "info",    "titulo": "Geracao distribuida ativa — 2.203 kWh injetados",
             "economia_mensal_r": None,   "acao_recomendada": "Cobertura de 7%. Espaco para expansao do sistema solar."},
        ],
    }
    gerar_relatorio(dados_teste, "/tmp/relatorio_v2.pdf")
    print("PDF gerado: /tmp/relatorio_v2.pdf")
