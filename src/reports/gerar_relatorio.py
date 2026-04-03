"""
Gerador de Relatório PDF — SaaS Análise de Energia
Design claro, profissional, adequado para impressão e envio ao cliente.
"""

from __future__ import annotations

import io
from datetime import datetime
from pathlib import Path
from typing import Optional

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm

from reportlab.pdfgen import canvas

W, H = A4

# ── Paleta clara ──────────────────────────────────────────────────────────────
C_WHITE      = colors.white
C_BG_LIGHT   = colors.HexColor("#f8f9fa")
C_BG_CARD    = colors.HexColor("#f1f3f5")
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
C_BLUE_BG    = colors.HexColor("#dbeafe")
C_ACCENT     = colors.HexColor("#0d2137")  # azul escuro — cor institucional


class RelatorioPDF:

    def __init__(self, dados: dict):
        self.d   = dados
        self.buf = io.BytesIO()
        self.c   = canvas.Canvas(self.buf, pagesize=A4)
        self.c.setTitle(f"Analise de Energia — {dados.get('cliente_nome','')}")

    def gerar(self) -> bytes:
        self._pagina()
        self.c.save()
        self.buf.seek(0)
        return self.buf.read()

    def salvar(self, path: str | Path) -> Path:
        path = Path(path)
        path.write_bytes(self.gerar())
        return path

    # ── Página ────────────────────────────────────────────────────────────────

    def _pagina(self):
        c = self.c
        d = self.d

        # Fundo branco
        c.setFillColor(C_WHITE)
        c.rect(0, 0, W, H, fill=1, stroke=0)

        # Barra de topo institucional
        c.setFillColor(C_ACCENT)
        c.rect(0, H - 18*mm, W, 18*mm, fill=1, stroke=0)

        # Faixa verde fina abaixo da barra
        c.setFillColor(C_GREEN)
        c.rect(0, H - 20*mm, W, 2*mm, fill=1, stroke=0)

        y = H - 28*mm
        y = self._header(y)
        y -= 5*mm
        y = self._metricas(y)
        y -= 5*mm
        y = self._demanda(y)
        y -= 5*mm
        y = self._alertas(y)
        y -= 5*mm
        y = self._analise_ia(y)
        y -= 5*mm
        y = self._proposta(y)
        self._rodape()

    # ── Header ────────────────────────────────────────────────────────────────

    def _header(self, y: float) -> float:
        c  = self.c
        d  = self.d
        mx = 18*mm

        # Logo no topo
        c.setFillColor(C_WHITE)
        c.setFont("Helvetica-Bold", 11)
        c.drawString(mx, H - 12*mm, "SaaS Analise de Energia")
        c.setFont("Helvetica", 8)
        data_str = datetime.now().strftime("%d/%m/%Y")
        c.drawRightString(W - mx, H - 12*mm, f"Emitido em {data_str}")

        # Nome do cliente
        c.setFillColor(C_ACCENT)
        c.setFont("Helvetica-Bold", 15)
        nome = d.get("cliente_nome", "Cliente")
        if len(nome) > 50: nome = nome[:47] + "..."
        c.drawString(mx, y, nome)

        # Subtítulo
        y -= 6*mm
        c.setFillColor(C_MUTED)
        c.setFont("Helvetica", 9)
        info = (
            f"UC {d.get('uc','')}  |  "
            f"Referencia: {d.get('mes_referencia','')}  |  "
            f"{d.get('subgrupo','')} — {d.get('modalidade','')}"
        )
        c.drawString(mx, y, info)

        # Linha
        y -= 5*mm
        c.setStrokeColor(C_BORDER)
        c.setLineWidth(0.7)
        c.line(mx, y, W - mx, y)

        return y - 3*mm

    # ── Métricas ──────────────────────────────────────────────────────────────

    def _metricas(self, y: float) -> float:
        c  = self.c
        d  = self.d
        mx = 18*mm
        gap    = 4*mm
        card_w = (W - 2*mx - 3*gap) / 4
        card_h = 22*mm

        score = d.get("score_eficiencia", 100)
        eco_m = d.get("potencial_economia_mensal", 0) or 0
        eco_a = d.get("potencial_economia_anual", 0) or 0
        total = d.get("total_a_pagar", 0) or 0
        ctda  = d.get("demanda_contratada_fora_ponta_kw", 0) or 0
        medi  = d.get("demanda_medida_fora_ponta_kw", 0) or 0
        util  = round(medi / ctda * 100) if ctda > 0 else 0

        def fmt(v): return f"R$ {v:,.0f}".replace(",",".")

        cards = [
            {"label": "Total da fatura",        "val": fmt(total),     "sub": d.get("mes_referencia",""),         "vcolor": C_TEXT,    "bg": C_BG_CARD},
            {"label": "Economia potencial/ano",  "val": fmt(eco_a),     "sub": f"{fmt(eco_m)}/mes",                "vcolor": C_GREEN,   "bg": C_GREEN_BG},
            {"label": "Score de eficiencia",     "val": f"{score}/100", "sub": self._score_label(score),           "vcolor": self._score_color(score), "bg": C_BG_CARD},
            {"label": "Utilizacao da demanda",   "val": f"{util}%",     "sub": f"{medi:.0f} kW de {ctda:.0f} kW", "vcolor": self._util_color(util),   "bg": C_BG_CARD},
        ]

        x = mx
        for card in cards:
            c.setFillColor(card["bg"])
            c.setStrokeColor(C_BORDER)
            c.setLineWidth(0.5)
            c.roundRect(x, y - card_h, card_w, card_h, 2.5*mm, fill=1, stroke=1)

            c.setFillColor(C_MUTED)
            c.setFont("Helvetica", 7.5)
            c.drawString(x + 3*mm, y - 5*mm, card["label"])

            c.setFillColor(card["vcolor"])
            c.setFont("Helvetica-Bold", 13)
            c.drawString(x + 3*mm, y - 13*mm, card["val"])

            c.setFillColor(C_HINT)
            c.setFont("Helvetica", 7.5)
            c.drawString(x + 3*mm, y - 19*mm, card["sub"])

            x += card_w + gap

        return y - card_h - 2*mm

    # ── Demanda ───────────────────────────────────────────────────────────────

    def _demanda(self, y: float) -> float:
        c  = self.c
        d  = self.d
        mx = 18*mm
        box_h = 33*mm

        ctda  = d.get("demanda_contratada_fora_ponta_kw", 0) or 0
        medi  = d.get("demanda_medida_fora_ponta_kw", 0) or 0
        hist  = d.get("historico_kwh") or []
        cons  = d.get("consumo_total_kwh") or 1
        pico  = round(medi / cons * max(hist)) if hist and cons > 0 else medi
        nova  = round(pico * 1.1)
        tar   = d.get("tarifa_demanda") or 22.96
        eco_dem = round((ctda - nova) * tar) if nova < ctda else 0

        c.setFillColor(C_BG_CARD)
        c.setStrokeColor(C_BORDER)
        c.setLineWidth(0.5)
        c.roundRect(mx, y - box_h, W - 2*mx, box_h, 2.5*mm, fill=1, stroke=1)

        # Título seção
        c.setFillColor(C_ACCENT)
        c.setFont("Helvetica-Bold", 8)
        c.drawString(mx + 4*mm, y - 5*mm, "DEMANDA CONTRATADA VS MEDIDA")

        bar_x     = mx + 48*mm
        bar_w_max = W - 2*mx - 62*mm
        bar_h     = 4.5*mm
        lbl_x     = mx + 4*mm

        rows = [
            ("Contratada",    ctda, ctda, C_RED,   f"{ctda:.0f} kW"),
            ("Medida atual",  medi, ctda, C_BLUE,  f"{medi:.0f} kW"),
            ("Pico historico",pico, ctda, C_AMBER, f"{pico:.0f} kW"),
        ]

        ry = y - 12*mm
        for (lbl, val, ref, col, vstr) in rows:
            c.setFillColor(C_MUTED)
            c.setFont("Helvetica", 8)
            c.drawString(lbl_x, ry, lbl)

            c.setFillColor(C_BORDER)
            c.roundRect(bar_x, ry - 1.5*mm, bar_w_max, bar_h, 1*mm, fill=1, stroke=0)

            fw = min((val / ref * bar_w_max) if ref > 0 else 0, bar_w_max)
            c.setFillColor(col)
            c.roundRect(bar_x, ry - 1.5*mm, fw, bar_h, 1*mm, fill=1, stroke=0)

            c.setFillColor(C_TEXT)
            c.setFont("Helvetica-Bold", 8)
            c.drawRightString(W - mx - 2*mm, ry, vstr)

            ry -= 7.5*mm

        # Nota
        if eco_dem > 0:
            nota = f"Reducao recomendada: {ctda:.0f} kW para {nova} kW  |  Economia estimada: R$ {eco_dem:,.0f}/mes".replace(",",".")
            c.setFillColor(C_GREEN)
            c.setFont("Helvetica-Bold", 7.5)
            c.drawString(mx + 4*mm, y - box_h + 4*mm, nota)

        return y - box_h - 2*mm

    # ── Alertas ───────────────────────────────────────────────────────────────

    def _alertas(self, y: float) -> float:
        c       = self.c
        d       = self.d
        mx      = 18*mm
        alertas = (d.get("alertas") or [])[:4]
        if not alertas: return y

        box_h = 8*mm + len(alertas) * 9*mm

        c.setFillColor(C_WHITE)
        c.setStrokeColor(C_BORDER)
        c.setLineWidth(0.5)
        c.roundRect(mx, y - box_h, W - 2*mx, box_h, 2.5*mm, fill=1, stroke=1)

        c.setFillColor(C_ACCENT)
        c.setFont("Helvetica-Bold", 8)
        c.drawString(mx + 4*mm, y - 5*mm, "ALERTAS IDENTIFICADOS")

        ay = y - 10*mm
        for alerta in alertas:
            sev   = alerta.get("severidade", "info")
            titulo = (alerta.get("titulo") or "")[:85]
            eco   = alerta.get("economia_mensal_r")

            bg_c, txt_c = {
                "critico": (C_RED_BG,   C_RED),
                "atencao": (C_AMBER_BG, C_AMBER),
            }.get(sev, (C_BG_CARD, C_MUTED))

            bw = 14*mm
            c.setFillColor(bg_c)
            c.roundRect(mx + 4*mm, ay - 2.5*mm, bw, 5*mm, 1.5*mm, fill=1, stroke=0)
            c.setFillColor(txt_c)
            c.setFont("Helvetica-Bold", 6.5)
            lbl = "CRITICO" if sev=="critico" else "ATENCAO" if sev=="atencao" else "INFO"
            c.drawCentredString(mx + 4*mm + bw/2, ay, lbl)

            c.setFillColor(C_TEXT)
            c.setFont("Helvetica", 8.5)
            c.drawString(mx + 20*mm, ay, titulo)

            if eco and eco > 0:
                c.setFillColor(C_GREEN)
                c.setFont("Helvetica-Bold", 8)
                c.drawRightString(W - mx - 2*mm, ay, f"R$ {eco:,.2f}/mes".replace(",","."))

            ay -= 9*mm

        return y - box_h - 2*mm

    # ── Análise IA ────────────────────────────────────────────────────────────

    def _analise_ia(self, y: float) -> float:
        c    = self.c
        d    = self.d
        mx   = 18*mm
        texto = d.get("analise_claude") or d.get("resumo_executivo") or ""
        if not texto: return y

        words = texto.split()
        lines, line = [], ""
        for w in words:
            if len(line) + len(w) + 1 > 105:
                lines.append(line.strip())
                line = w
            else:
                line += " " + w
        if line: lines.append(line.strip())

        box_h = 8*mm + len(lines) * 5.5*mm + 5*mm

        c.setFillColor(C_BG_LIGHT)
        c.setStrokeColor(C_BORDER)
        c.setLineWidth(0.5)
        c.roundRect(mx, y - box_h, W - 2*mx, box_h, 2.5*mm, fill=1, stroke=1)

        # Barra lateral verde
        c.setFillColor(C_GREEN)
        c.roundRect(mx, y - box_h, 1.5*mm, box_h, 0.75*mm, fill=1, stroke=0)

        c.setFillColor(C_ACCENT)
        c.setFont("Helvetica-Bold", 8)
        c.drawString(mx + 5*mm, y - 5*mm, "ANALISE EXECUTIVA — gerada por inteligencia artificial")

        ly = y - 11*mm
        for line in lines:
            c.setFillColor(C_MUTED)
            c.setFont("Helvetica-Oblique", 8.5)
            c.drawString(mx + 5*mm, ly, line)
            ly -= 5.5*mm

        return y - box_h - 2*mm

    # ── Proposta ──────────────────────────────────────────────────────────────

    def _proposta(self, y: float) -> float:
        c     = self.c
        d     = self.d
        mx    = 18*mm
        box_h = 40*mm

        eco_a  = d.get("potencial_economia_anual", 0) or 0
        eco_m  = d.get("potencial_economia_mensal", 0) or 0
        hon_a  = round(eco_a * 0.25)
        hon_m  = round(eco_m * 0.25)
        modelo = d.get("modelo_recomendado", "consultoria")

        # Box verde claro
        c.setFillColor(C_GREEN_BG)
        c.setStrokeColor(C_GREEN)
        c.setLineWidth(0.8)
        c.roundRect(mx, y - box_h, W - 2*mx, box_h, 2.5*mm, fill=1, stroke=1)

        # Cabeçalho da proposta
        c.setFillColor(C_GREEN_DARK)
        c.setFont("Helvetica-Bold", 10)
        c.drawString(mx + 5*mm, y - 7*mm, "PROPOSTA COMERCIAL")

        modelo_label = {
            "consultoria": "Consultoria por resultado — 25% da economia gerada",
            "assinatura":  "Monitoramento mensal por UC",
            "mercado":     "Intermediacao para o Mercado Livre de Energia",
        }.get(modelo, "Consultoria por resultado — 25% da economia gerada")

        c.setFillColor(C_GREEN)
        c.setFont("Helvetica", 8.5)
        c.drawString(mx + 5*mm, y - 13*mm, modelo_label)

        # Linha divisória
        c.setStrokeColor(colors.HexColor("#a7f3d0"))
        c.setLineWidth(0.5)
        c.line(mx + 5*mm, y - 16*mm, W - mx - 5*mm, y - 16*mm)

        def fmt(v): return f"R$ {v:,.0f}".replace(",",".")

        col1 = mx + 5*mm
        col2 = mx + (W - 2*mx) * 0.36
        col3 = mx + (W - 2*mx) * 0.64

        rows1_y = y - 24*mm
        rows2_y = y - 34*mm

        items = [
            (col1, rows1_y, "Economia estimada / mes",   fmt(eco_m),                C_TEXT),
            (col2, rows1_y, "Economia estimada / ano",   fmt(eco_a),                C_TEXT),
            (col3, rows1_y, "Seu honorario / ano (25%)", fmt(hon_a),                C_GREEN_DARK),
            (col1, rows2_y, "Seu honorario / mes",       fmt(hon_m),                C_GREEN_DARK),
            (col2, rows2_y, "Pagamento",                 "Mensal — fatura real",    C_TEXT),
            (col3, rows2_y, "Risco p/ o cliente",        "Zero — so paga se economizar", C_TEXT),
        ]

        for (x, vy, lbl, val, vc) in items:
            c.setFillColor(C_MUTED)
            c.setFont("Helvetica", 7)
            c.drawString(x, vy, lbl)
            c.setFillColor(vc)
            c.setFont("Helvetica-Bold", 9.5)
            c.drawString(x, vy - 5.5*mm, val)

        return y - box_h - 2*mm

    # ── Rodapé ────────────────────────────────────────────────────────────────

    def _rodape(self):
        c  = self.c
        mx = 18*mm
        y  = 10*mm

        c.setFillColor(C_ACCENT)
        c.rect(0, 0, W, 8*mm, fill=1, stroke=0)

        c.setFillColor(C_WHITE)
        c.setFont("Helvetica", 7)
        c.drawString(mx, 3*mm, "SaaS Analise de Energia — Documento confidencial")
        c.drawRightString(W - mx, 3*mm, f"Gerado em {datetime.now().strftime('%d/%m/%Y %H:%M')}")

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _score_color(self, s):
        if s < 60: return C_RED
        if s < 80: return C_AMBER
        return C_GREEN

    def _score_label(self, s):
        if s < 60: return "Critico — acao imediata"
        if s < 80: return "Atencao — oportunidades"
        return "Eficiente"

    def _util_color(self, u):
        if u < 70: return C_RED
        if u < 85: return C_AMBER
        return C_GREEN


def gerar_relatorio(dados: dict, output_path: Optional[str | Path] = None) -> bytes:
    rel = RelatorioPDF(dados)
    pdf_bytes = rel.gerar()
    if output_path:
        Path(output_path).write_bytes(pdf_bytes)
    return pdf_bytes


if __name__ == "__main__":
    dados_teste = {
        "cliente_nome": "Servico de Apoio as Micros e Pequenas Empresas do AM — SEBRAE AM",
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
            "Sua unidade consumidora esta pagando por 83 kW de demanda que nunca utiliza. "
            "Considerando o pico historico de agosto de 186 kW, a reducao segura da demanda "
            "contratada de 225 kW para 205 kW geraria R$ 459 por mes — R$ 5.510 ao longo de um ano, "
            "sem qualquer risco de ultrapassagem nos meses de maior consumo. "
            "A geracao distribuida ativa contribui com R$ 1.092 de credito mensal, cobrindo 7% do consumo total."
        ),
        "alertas": [
            {"severidade": "critico", "titulo": "Demanda superdimensionada — 63% de utilizacao", "economia_mensal_r": 459.20},
            {"severidade": "atencao", "titulo": "Energia reativa excedente faturada", "economia_mensal_r": 0.69},
            {"severidade": "info",    "titulo": "Geracao distribuida ativa — 2.203 kWh injetados", "economia_mensal_r": None},
        ],
    }
    gerar_relatorio(dados_teste, "/tmp/relatorio_sebrae.pdf")
    print("PDF gerado: /tmp/relatorio_sebrae.pdf")
