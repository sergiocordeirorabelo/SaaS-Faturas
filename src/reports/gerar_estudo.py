"""
Gerador de Estudo Técnico + Proposta Comercial — VOLTIX ENERGIA
PDF direto via ReportLab canvas. Sem LibreOffice, sem PPTX.
ReportLab canvas — layout tipo apresentação (landscape A4).
Todos os valores das faturas parseadas. Sem LibreOffice.
"""
from __future__ import annotations
import io, logging
from datetime import datetime
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas
from typing import Optional
from src.parsers.analyzer_historico import ResultadoHistorico

logger = logging.getLogger(__name__)
W, H = landscape(A4)

# Cores
NAVY=colors.HexColor("#1B2A4A"); DARK_NAVY=colors.HexColor("#0F1B33")
ORANGE=colors.HexColor("#F5A623"); GREEN=colors.HexColor("#059669")
RED=colors.HexColor("#DC2626"); RED_LT=colors.HexColor("#FEE2E2")
WHITE=colors.white; OFF_WHITE=colors.HexColor("#F8FAFC")
GRAY=colors.HexColor("#64748B"); GRAY_LT=colors.HexColor("#E2E8F0")
TEXT_CLR=colors.HexColor("#1E293B"); MUTED=colors.HexColor("#94A3B8")
MX=18*mm; MY=14*mm

def _fmt(v,d=2):
    try: return f"{float(v):,.{d}f}".replace(",","X").replace(".",",").replace("X",".")
    except: return "—"
def _fmtI(v): return _fmt(v,0)

def _rect(c,x,y,w,h,fill=None,stroke=None):
    if fill: c.setFillColor(fill)
    if stroke: c.setStrokeColor(stroke); c.setLineWidth(1)
    c.rect(x,y,w,h,fill=1 if fill else 0,stroke=1 if stroke else 0)

def _text(c,x,y,txt,size=10,bold=False,color=TEXT_CLR,align='left',maxw=None):
    font="Helvetica-Bold" if bold else "Helvetica"
    c.setFont(font,size); c.setFillColor(color)
    if align=='right' and maxw:
        c.drawString(x+maxw-c.stringWidth(txt,font,size),y,txt)
    elif align=='center' and maxw:
        c.drawString(x+(maxw-c.stringWidth(txt,font,size))/2,y,txt)
    else:
        c.drawString(x,y,txt)

def _wrap(c,x,y,txt,size=10,bold=False,color=TEXT_CLR,maxw=250*mm,lead=None):
    font="Helvetica-Bold" if bold else "Helvetica"
    if lead is None: lead=size*1.5
    c.setFont(font,size); c.setFillColor(color)
    words=txt.split(' '); line=''; cy=y
    for w in words:
        test=line+(' ' if line else '')+w
        if c.stringWidth(test,font,size)>maxw and line:
            c.drawString(x,cy,line); cy-=lead; line=w
        else: line=test
    if line: c.drawString(x,cy,line); cy-=lead
    return cy

def _topbar(c):
    c.setFillColor(NAVY); c.rect(0,H-12*mm,W,12*mm,fill=1,stroke=0)
    c.setFillColor(WHITE); c.setFont("Helvetica-Bold",11); c.drawString(MX,H-8.5*mm,"VOLTIX ENERGIA")
    c.setFillColor(MUTED); c.setFont("Helvetica",7); c.drawRightString(W-MX,H-7*mm,"Estudo Técnico + Proposta Comercial")

def _footer(c,pg):
    c.setFillColor(GRAY_LT); c.rect(0,0,W,7*mm,fill=1,stroke=0)
    c.setFillColor(MUTED); c.setFont("Helvetica",6)
    c.drawString(MX,2.5*mm,"VOLTIX ENERGIA · Gestão Inteligente de Energia · Manaus, AM")
    c.drawRightString(W-MX,2.5*mm,f"Página {pg}")

def _sidebar(c): _rect(c,0,0,3*mm,H,ORANGE)

# ══════════════════════════════════════════════════════════════════════════════


# ══════════════════════════════════════════════════════════════════════════════
# PÁGINAS
# ══════════════════════════════════════════════════════════════════════════════
def _pg_capa(c,d,cnpj):
    _rect(c,0,0,W,H,DARK_NAVY); _rect(c,0,0,3*mm,H,ORANGE)
    _text(c,MX+10*mm,H-55*mm,"Estudo Técnico",36,True,WHITE)
    _text(c,MX+10*mm,H-70*mm,"+ Proposta Comercial",22,False,MUTED)
    _rect(c,MX+10*mm,H-80*mm,180*mm,0.7*mm,ORANGE)
    _text(c,MX+10*mm,H-92*mm,d["nome"][:60],14,True,WHITE)
    if cnpj: _text(c,MX+10*mm,H-100*mm,f"CNPJ: {cnpj}",11,False,MUTED)
    _text(c,MX+10*mm,H-108*mm,f"UC: {d['uc']}",11,False,MUTED)
    _text(c,W-65*mm,H-30*mm,"VOLTIX",28,True,WHITE)
    _text(c,W-65*mm,H-38*mm,"E N E R G I A",10,False,ORANGE)
    _text(c,MX+10*mm,20*mm,f"Gerado em {datetime.now().strftime('%d/%m/%Y')}",8,False,MUTED)

def _pg_valor(c,d):
    _topbar(c); _sidebar(c); _footer(c,2)
    y=H-28*mm
    _text(c,MX+8*mm,y,"Transformando custo em estratégia",24,True,NAVY); y-=14*mm
    _rect(c,MX+8*mm,y-72*mm,180*mm,80*mm,OFF_WHITE,GRAY_LT)
    txts=["A energia elétrica, que tradicionalmente é tratada como um custo fixo e inevitável, passa a ser gerida como um ativo estratégico com a VOLTIX ENERGIA.",
          "Atuamos de forma técnica e contínua na análise das suas faturas, contratos e perfil de consumo, identificando oportunidades reais de redução de custos.",
          "Nosso trabalho não é pontual, é gestão recorrente, com inteligência de dados e acompanhamento especializado.",
          "Com a VOLTIX, sua empresa deixa de apenas pagar energia e passa a controlar, prever e economizar."]
    ty=y-5*mm
    for t in txts: ty=_wrap(c,MX+12*mm,ty,t,10,False,TEXT_CLR,170*mm,14); ty-=4*mm
    _wrap(c,MX+8*mm,ty-8*mm,"Resultado: redução de despesas, aumento de eficiência e previsibilidade financeira.",10,True,GREEN,200*mm)
    _text(c,MX+8*mm,22*mm,"Foque no que importa e deixe a energia com a gente!",11,False,RED)

def _pg_fatura(c,d):
    _topbar(c); _footer(c,3)
    y=H-26*mm
    _text(c,MX,y,"Fatura da Distribuidora",22,True,NAVY); y-=12*mm

    # ── Header: demanda contratada e período ──────────────────────────────
    _rect(c,MX,y-8*mm,165*mm,10*mm,NAVY)
    dc_pta = float(d['f0'].get('demanda_contratada_ponta_kw') or 0)
    _text(c,MX+3*mm,y-5*mm,f"D. Ctda Pta: {_fmtI(dc_pta)}       D. Ctda F.Pta: {_fmtI(d['dc_max'])}       Período: {d['mes']}",9,True,WHITE)
    y-=14*mm

    # ── Classificar itens → match dinâmico com Pontos de Atenção ─────────
    itens=d["itens"]
    pontos = d["pontos"]
    item_markers = {}  # {item_index: point_number}

    # Mapear cada ponto ao seu tipo
    def _achar_ponto(keywords):
        """Acha o número do ponto que contém alguma das keywords."""
        for pi, pt in enumerate(pontos):
            pt_lower = pt.lower()
            if any(kw in pt_lower for kw in keywords):
                return pi + 1  # 1-indexed
        return None

    for i, item in enumerate(itens):
        desc_lower = (item.get("descricao","") or "").lower()
        if any(x in desc_lower for x in ["não utilizada","nao utilizada","ociosa"]):
            num = _achar_ponto(["demanda não utilizada","demanda nao utilizada","ociosa","superdimensionada"])
            if num: item_markers[i] = num
        elif "demanda" in desc_lower and "não" not in desc_lower and "nao" not in desc_lower:
            num = _achar_ponto(["demanda contratada atual"])
            if num: item_markers[i] = num
        elif any(x in desc_lower for x in ["en r exc","ufer","reativ"]):
            num = _achar_ponto(["reativa","ufer","fator de potência","fator de potencia"])
            if num: item_markers[i] = num
        elif any(x in desc_lower for x in ["multa","juros","mora"]):
            num = _achar_ponto(["multa","atraso","juros"])
            if num: item_markers[i] = num
        elif any(x in desc_lower for x in ["cosip","iluminação","iluminacao"]):
            num = _achar_ponto(["cosip","iluminação","iluminacao"])
            if num: item_markers[i] = num

    if itens:
        cols=[90*mm,35*mm,40*mm]
        tw = sum(cols)
        _rect(c,MX,y-7*mm,tw,8*mm,NAVY)
        _text(c,MX+8*mm,y-4.5*mm,"Itens Faturados",8,True,WHITE)
        _text(c,MX+cols[0]+2*mm,y-4.5*mm,"Tar. sem Impostos",8,True,WHITE,'right',cols[1]-4*mm)
        _text(c,MX+cols[0]+cols[1]+2*mm,y-4.5*mm,"Valor",8,True,WHITE,'right',cols[2]-4*mm)
        y-=8*mm
        for i,item in enumerate(itens):
            desc=item.get("descricao",""); tar=item.get("tarifa")
            val=float(item.get("valor") or 0)
            has_marker = i in item_markers

            # Cor de fundo baseada no tipo
            desc_lower = desc.lower()
            if any(x in desc_lower for x in ["não utilizada","nao utilizada"]):
                bg = RED_LT
            elif any(x in desc_lower for x in ["en r exc","ufer","multa","juros","mora"]):
                bg = colors.HexColor("#FEF3C7")
            elif any(x in desc_lower for x in ["credito","crédito","geração","geracao"]):
                bg = colors.HexColor("#D1FAE5")
            else:
                bg = OFF_WHITE if i%2==0 else WHITE

            _rect(c,MX,y-6*mm,tw,7*mm,bg)

            # Marcador numerado (círculo laranja com número)
            if has_marker:
                num = item_markers[i]
                c.setFillColor(ORANGE); c.circle(MX+4*mm, y-2.5*mm, 3*mm, fill=1, stroke=0)
                _text(c, MX+1.5*mm, y-4*mm, str(num), 7, True, WHITE, 'center', 5*mm)
                _text(c,MX+10*mm,y-3.5*mm,desc[:52],8,False,TEXT_CLR)
            else:
                _text(c,MX+4*mm,y-3.5*mm,desc[:55],8,False,TEXT_CLR)

            if tar: _text(c,MX+cols[0]+2*mm,y-3.5*mm,f"{tar:.6f}",8,False,GRAY,'right',cols[1]-4*mm)
            cor=GREEN if val<0 else TEXT_CLR
            _text(c,MX+cols[0]+cols[1]+2*mm,y-3.5*mm,f"R$ {_fmt(val)}",8,True,cor,'right',cols[2]-4*mm)
            y-=7*mm

        # Total
        _rect(c,MX,y-7*mm,tw,8*mm,NAVY)
        _text(c,MX+cols[0]+2*mm,y-4.5*mm,"Valor a Pagar",9,True,WHITE,'right',cols[1]-4*mm)
        _text(c,MX+cols[0]+cols[1]+2*mm,y-4.5*mm,f"R$ {_fmt(d['total'])}",11,True,ORANGE,'right',cols[2]-4*mm)
        y -= 10*mm

    # ── Dados da Leitura (compacto, abaixo dos itens) ─────────────────────
    import json as _json
    dados_leitura = d["f0"].get("dados_leitura") or []
    if isinstance(dados_leitura, str):
        try: dados_leitura = _json.loads(dados_leitura)
        except: dados_leitura = []

    if dados_leitura and y > 40*mm:
        _rect(c,MX,y-2*mm,165*mm,0.5*mm,GRAY_LT)
        y -= 6*mm
        dl_cols=[50*mm,28*mm,28*mm,28*mm,28*mm]
        dl_w=sum(dl_cols)
        _rect(c,MX,y-7*mm,dl_w,8*mm,GRAY_LT)
        for hi,h in enumerate(["Descrição da Grandeza","Leit. Atual","Leit. Anterior","Constante","Registrado"]):
            _text(c,MX+sum(dl_cols[:hi])+2*mm,y-4.5*mm,h,7,True,NAVY)
        y-=8*mm
        for i,item in enumerate(dados_leitura[:8]):
            if not isinstance(item, dict): continue
            bg=OFF_WHITE if i%2==0 else WHITE
            _rect(c,MX,y-6*mm,dl_w,7*mm,bg)
            vals=[str(item.get("descricao","")),str(item.get("leit_atual","")),
                  str(item.get("leit_anterior","")),str(item.get("constante","")),
                  str(item.get("registrado",item.get("valor","")))]
            for vi,v in enumerate(vals):
                _text(c,MX+sum(dl_cols[:vi])+2*mm,y-3.5*mm,v[:18],7,False,TEXT_CLR)
            y-=7*mm

        # Linha de médias
        media = float(d["f0"].get("media_12_meses_kwh") or 0)
        _rect(c,MX,y-7*mm,dl_w,8*mm,GRAY_LT)
        _text(c,MX+2*mm,y-4.5*mm,f"Média 12 meses: {_fmtI(media)} kWh     D.Ctda Pta: {_fmtI(dc_pta)}     D.Ctda F.Pta: {_fmtI(d['dc_max'])}",8,True,NAVY)

    # ── Painel direito: Pontos de Atenção ─────────────────────────────────
    px=185*mm; pw=W-px-MX; py_top=H-26*mm
    _rect(c,px,MY+7*mm,pw,py_top-MY-7*mm,NAVY)
    _text(c,px+8*mm,py_top-8*mm,"Pontos de Atenção",14,True,WHITE)
    _text(c,px+pw-12*mm,py_top-7*mm,"⚠",16,False,ORANGE)
    py=py_top-22*mm
    for i,ponto in enumerate(d["pontos"][:6]):
        c.setFillColor(ORANGE); c.circle(px+14*mm,py+2*mm,4*mm,fill=1,stroke=0)
        _text(c,px+11.5*mm,py-0.5*mm,str(i+1),9,True,WHITE,'center',5*mm)
        py=_wrap(c,px+22*mm,py+1*mm,ponto,8,False,WHITE,pw-28*mm,11); py-=5*mm

def _pg_analise_detalhe(c, d):
    """Página de análise visual: tabelas da fatura com highlights nos problemas."""
    _topbar(c); _sidebar(c); _footer(c, 4)
    y = H - 26*mm
    _text(c, MX+8*mm, y, "Análise Detalhada da Fatura", 22, True, NAVY)
    _text(c, MX+8*mm, y-10*mm, f"UC {d['uc']}  ·  {d['mes']}  ·  Identificação de oportunidades", 9, False, GRAY)
    y -= 22*mm

    # ── TABELA: Descrição da Conta ────────────────────────────────────────
    itens = d.get("itens") or []
    if itens:
        _text(c, MX+8*mm, y, "DESCRIÇÃO DA CONTA", 9, True, NAVY); y -= 6*mm
        cols = [115*mm, 30*mm, 30*mm, 50*mm]
        total_w = sum(cols)
        # Header
        _rect(c, MX+8*mm, y-7*mm, total_w, 8*mm, NAVY)
        _text(c, MX+10*mm, y-4.5*mm, "Itens Faturados", 8, True, WHITE)
        _text(c, MX+8*mm+cols[0]+2*mm, y-4.5*mm, "Tarifa", 8, True, WHITE)
        _text(c, MX+8*mm+cols[0]+cols[1]+2*mm, y-4.5*mm, "Valor (R$)", 8, True, WHITE, 'right', cols[2]-4*mm)
        _text(c, MX+8*mm+cols[0]+cols[1]+cols[2]+2*mm, y-4.5*mm, "Diagnóstico", 8, True, WHITE)
        y -= 8*mm

        for i, item in enumerate(itens):
            desc = str(item.get("descricao", ""))
            tar = item.get("tarifa")
            val = float(item.get("valor") or 0)
            desc_lower = desc.lower()

            # Classificar o item
            bg = OFF_WHITE if i % 2 == 0 else WHITE
            diag = ""
            diag_cor = TEXT_CLR

            if any(x in desc_lower for x in ["não utilizada", "nao utilizada", "ociosa"]):
                bg = RED_LT
                diag = "⚠ DESPERDÍCIO — demanda paga sem uso"
                diag_cor = RED
            elif any(x in desc_lower for x in ["ufer", "reativ", "exc f/p", "en r exc"]):
                bg = colors.HexColor("#FEF3C7")
                diag = "⚠ Fator de potência baixo"
                diag_cor = colors.HexColor("#D97706")
            elif any(x in desc_lower for x in ["cosip", "iluminação", "iluminacao"]):
                bg = colors.HexColor("#FEF3C7")
                diag = "⚠ Verificar enquadramento"
                diag_cor = colors.HexColor("#D97706")
            elif any(x in desc_lower for x in ["credito", "crédito", "geração", "geracao"]):
                bg = colors.HexColor("#D1FAE5")
                diag = "✓ Economia ativa (GD)"
                diag_cor = GREEN
            elif "demanda" in desc_lower and val > 2000:
                diag = f"Demanda medida — {d['util']}% da contratada"
                diag_cor = GRAY

            rh = 7*mm
            _rect(c, MX+8*mm, y-rh+1*mm, total_w, rh, bg)
            # Borda colorida à esquerda para itens com diagnóstico
            if diag:
                _rect(c, MX+8*mm, y-rh+1*mm, 2*mm, rh, diag_cor)
            _text(c, MX+12*mm, y-4*mm, desc[:60], 8, False, TEXT_CLR)
            if tar: _text(c, MX+8*mm+cols[0]+2*mm, y-4*mm, f"{tar:.6f}", 7, False, GRAY)
            val_cor = GREEN if val < 0 else TEXT_CLR
            _text(c, MX+8*mm+cols[0]+cols[1]+2*mm, y-4*mm, f"R$ {_fmt(val)}", 8, True, val_cor, 'right', cols[2]-4*mm)
            if diag:
                _text(c, MX+8*mm+cols[0]+cols[1]+cols[2]+4*mm, y-4*mm, diag, 7, True, diag_cor)
            y -= rh

        # Total
        _rect(c, MX+8*mm, y-7*mm, total_w, 8*mm, NAVY)
        _text(c, MX+10*mm, y-4.5*mm, "TOTAL A PAGAR", 9, True, WHITE)
        _text(c, MX+8*mm+cols[0]+cols[1]+2*mm, y-4.5*mm, f"R$ {_fmt(d['total'])}", 10, True, ORANGE, 'right', cols[2]-4*mm)
        y -= 14*mm

    # ── Legenda ───────────────────────────────────────────────────────────
    _rect(c, MX+8*mm, y-10*mm, sum(cols), 12*mm, OFF_WHITE, GRAY_LT)
    _text(c, MX+12*mm, y-6*mm, "LEGENDA:", 7, True, NAVY)
    lx = MX+45*mm
    _rect(c, lx, y-7*mm, 3*mm, 5*mm, RED_LT); _rect(c, lx, y-7*mm, 1*mm, 5*mm, RED)
    _text(c, lx+5*mm, y-5*mm, "Desperdício identificado", 7, False, RED); lx += 55*mm
    _rect(c, lx, y-7*mm, 3*mm, 5*mm, colors.HexColor("#FEF3C7")); _rect(c, lx, y-7*mm, 1*mm, 5*mm, colors.HexColor("#D97706"))
    _text(c, lx+5*mm, y-5*mm, "Atenção / verificar", 7, False, colors.HexColor("#D97706")); lx += 50*mm
    _rect(c, lx, y-7*mm, 3*mm, 5*mm, colors.HexColor("#D1FAE5")); _rect(c, lx, y-7*mm, 1*mm, 5*mm, GREEN)
    _text(c, lx+5*mm, y-5*mm, "Economia ativa", 7, False, GREEN)
    y -= 18*mm

    # ── TABELA: Dados da Leitura ──────────────────────────────────────────
    dados_leitura = d["f0"].get("dados_leitura") or []
    if isinstance(dados_leitura, str):
        import json
        try: dados_leitura = json.loads(dados_leitura)
        except: dados_leitura = []

    if dados_leitura:
        _text(c, MX+8*mm, y, "DADOS DA LEITURA", 9, True, NAVY); y -= 6*mm
        dl_cols = [42*mm, 24*mm, 24*mm, 24*mm, 22*mm, 55*mm]
        dl_w = sum(dl_cols)
        _rect(c, MX+8*mm, y-7*mm, dl_w, 8*mm, NAVY)
        headers_dl = ["Grandeza", "Leit. Atual", "Leit. Ant.", "Constante", "Registr.", "Diagnóstico"]
        for hi, h in enumerate(headers_dl):
            hx = MX+8*mm+sum(dl_cols[:hi])+2*mm
            _text(c, hx, y-4.5*mm, h, 7, True, WHITE)
        y -= 8*mm

        dc_pta = float(d["f0"].get("demanda_contratada_ponta_kw") or 0)
        dc_fpta = float(d["dc_max"])

        for i, item in enumerate(dados_leitura):
            if not isinstance(item, dict): continue
            desc = str(item.get("descricao", item.get("grandeza", "")))
            registrado = float(item.get("registrado", item.get("valor", 0)) or 0)
            desc_lower = desc.lower()

            # Classificar o registro
            bg = OFF_WHITE if i % 2 == 0 else WHITE
            diag = ""
            diag_cor = TEXT_CLR
            borda_cor = None

            if "ufer" in desc_lower and registrado > 0:
                bg = colors.HexColor("#FEF3C7")
                borda_cor = colors.HexColor("#D97706")
                diag = "⚠ Reativo excedente — FP baixo"
                diag_cor = colors.HexColor("#D97706")
            elif "ufer" in desc_lower and registrado == 0:
                diag = "✓ Sem excedente reativo"
                diag_cor = GREEN
            elif "dem acum f" in desc_lower and dc_fpta > 0:
                util = round(registrado / dc_fpta * 100) if dc_fpta > 0 else 0
                if util < 75:
                    bg = RED_LT
                    borda_cor = RED
                    diag = f"⚠ Só {util}% — demanda ociosa"
                    diag_cor = RED
                elif util < 90:
                    bg = colors.HexColor("#FEF3C7")
                    borda_cor = colors.HexColor("#D97706")
                    diag = f"Utilização {util}% — monitorar"
                    diag_cor = colors.HexColor("#D97706")
                else:
                    diag = f"✓ Utilização {util}% — adequada"
                    diag_cor = GREEN
            elif "dem acum" in desc_lower and "pta" in desc_lower and dc_pta > 0:
                util = round(registrado / dc_pta * 100) if dc_pta > 0 else 0
                if util < 60:
                    bg = RED_LT
                    borda_cor = RED
                    diag = f"⚠ Ponta {util}% — superdimensionada"
                    diag_cor = RED
                else:
                    diag = f"Ponta {util}%"
                    diag_cor = GRAY
            elif "reversa" in desc_lower and registrado > 0:
                bg = colors.HexColor("#D1FAE5")
                borda_cor = GREEN
                diag = f"✓ GD ativa — {_fmtI(registrado)} kWh injetados"
                diag_cor = GREEN
            elif "reversa" in desc_lower:
                diag = "Sem geração distribuída"
                diag_cor = GRAY

            rh = 7*mm
            _rect(c, MX+8*mm, y-rh+1*mm, dl_w, rh, bg)
            if borda_cor:
                _rect(c, MX+8*mm, y-rh+1*mm, 2*mm, rh, borda_cor)

            vals = [desc, str(item.get("leit_atual", "")),
                    str(item.get("leit_anterior", "")),
                    str(item.get("constante", "")),
                    str(item.get("registrado", item.get("valor", "")))]
            for vi, v in enumerate(vals):
                vx = MX+8*mm+sum(dl_cols[:vi])+2*mm
                _text(c, vx, y-3.5*mm, v[:18], 7, False, TEXT_CLR)
            if diag:
                dx = MX+8*mm+sum(dl_cols[:5])+4*mm
                _text(c, dx, y-3.5*mm, diag, 7, True, diag_cor)
            y -= rh

        # Linha de médias
        media = float(d["f0"].get("media_12_meses_kwh") or 0)
        _rect(c, MX+8*mm, y-7*mm, dl_w, 8*mm, GRAY_LT)
        _text(c, MX+10*mm, y-4.5*mm, f"Média 12 meses: {_fmtI(media)} kWh", 8, True, NAVY)
        _text(c, MX+90*mm, y-4.5*mm, f"D.Ctda Pta: {_fmtI(dc_pta)}", 8, False, GRAY)
        _text(c, MX+140*mm, y-4.5*mm, f"D.Ctda F.Pta: {_fmtI(dc_fpta)}", 8, False, GRAY)

def _pg_resumo(c,d):
    _topbar(c); _sidebar(c); _footer(c,5)
    y=H-26*mm
    _text(c,MX+8*mm,y,"Resumo Técnico",20,True,NAVY); y-=12*mm
    # KPIs compactos
    kw=60*mm; kh=22*mm; gap=4*mm
    kpis=[("Custo Médio/Mês",f"R$ {_fmt(d['custo_medio'])}"),("Projeção Anual",f"R$ {_fmt(d['custo_anual'])}"),("Consumo Médio",f"{_fmtI(d['consumo_medio'])} kWh"),("Utiliz. Demanda",f"{d['util']}%")]
    for i,(label,val) in enumerate(kpis):
        kx=MX+8*mm+i*(kw+gap)
        _rect(c,kx,y-kh,kw,kh,OFF_WHITE,GRAY_LT)
        _text(c,kx+4*mm,y-8*mm,val,12,True,NAVY if d["util"]>=70 or i!=3 else RED,'center',kw-8*mm)
        _text(c,kx+4*mm,y-kh+4*mm,label,7,False,GRAY,'center',kw-8*mm)
    y-=kh+8*mm

    # ── Desperdícios reais ────────────────────────────────────────────────
    _text(c,MX+8*mm,y,f"DESPERDÍCIOS IDENTIFICADOS NA FATURA ({d['n']} meses)",10,True,RED); y-=8*mm
    items=[]
    if d["desp_dem"]>0:
        items.append(("Demanda não utilizada",d["desp_dem"],
            f"{_fmtI(d['dc_max'])} kW contratada vs {_fmtI(d['dm_med'])} kW medida ({d['util']}%)"))
    if d.get("desp_ultra",0)>0:
        items.append(("Demanda ultrapassada",d["desp_ultra"],"Multa 3x a tarifa"))
    if d["desp_reat"]>0:
        items.append(("Energia reativa (UFER)",d["desp_reat"],"FP abaixo de 0,92"))
    if d["desp_multas"]>0:
        items.append(("Multas e juros",d["desp_multas"],"Pagamento após vencimento"))
    total_real = sum(v for _,v,_ in items)
    total_real_anual = total_real / d["n"] * 12 if d["n"]>0 else 0

    for label,val,desc in items:
        _text(c,MX+12*mm,y,f"{label}  ",10,False,TEXT_CLR)
        _text(c,MX+120*mm,y,desc,7,False,GRAY)
        _text(c,MX+12*mm,y,f"R$ {_fmt(val)}",10,True,RED,'right',200*mm); y-=9*mm

    c.setStrokeColor(RED); c.setLineWidth(1); c.line(MX+12*mm,y+4*mm,MX+212*mm,y+4*mm); y-=3*mm
    _text(c,MX+12*mm,y,"Subtotal no período",9,True,RED)
    _text(c,MX+12*mm,y,f"R$ {_fmt(total_real)}",10,True,RED,'right',200*mm); y-=12*mm

    # ── Oportunidades adicionais (estimativas) ────────────────────────────
    _text(c,MX+8*mm,y,"OPORTUNIDADES ADICIONAIS (valores estimados)",10,True,NAVY); y-=8*mm
    oport=[]
    icms_val = d.get("desp_icms",0)
    if icms_val > 0:
        f0 = d["f0"]
        tem_icms = bool(float(f0.get("icms_valor") or 0) > 0)
        obs = "ICMS declarado × 60%" if tem_icms else "Estimado: 63% TUSD × 18% alíquota AM"
        oport.append(("ICMS sobre TUSD", icms_val, f"Tema 956/STF — {obs}"))
    if d["eleg_ml"] and d["custo_anual"]>0:
        ml_eco = d["custo_anual"] * 0.18 / 12 * d["n"]
        oport.append(("Migração Mercado Livre", ml_eco,
            f"Lei 10.848/2004 — economia ~18% ({d['subgrupo']}, {_fmtI(d['dc_max'])} kW)"))
    if d["cosip_medio"]>300:
        cosip_exc = (d["cosip_medio"]-300) * d["n"]
        oport.append(("Contestação COSIP", cosip_exc,
            f"R$ {_fmt(d['cosip_medio'])}/mês — contestar junto à Prefeitura"))

    total_oport = 0
    for label,val,desc in oport:
        _text(c,MX+12*mm,y,label,10,False,TEXT_CLR)
        _text(c,MX+120*mm,y,desc,7,False,GRAY)
        _text(c,MX+12*mm,y,f"R$ {_fmt(val)}",10,True,NAVY,'right',200*mm); y-=9*mm
        total_oport += val

    c.setStrokeColor(NAVY); c.setLineWidth(1); c.line(MX+12*mm,y+4*mm,MX+212*mm,y+4*mm); y-=3*mm
    _text(c,MX+12*mm,y,"Subtotal estimado no período",9,True,NAVY)
    _text(c,MX+12*mm,y,f"R$ {_fmt(total_oport)}",10,True,NAVY,'right',200*mm); y-=14*mm

    # ── TOTAL GERAL ───────────────────────────────────────────────────────
    total_geral = total_real + total_oport
    total_geral_anual = total_geral / d["n"] * 12 if d["n"]>0 else 0
    d["_real_anual"] = total_geral_anual

    _rect(c,MX+8*mm,y-14*mm,210*mm,20*mm,NAVY)
    _text(c,MX+14*mm,y-4*mm,"POTENCIAL TOTAL DE ECONOMIA",13,True,WHITE)
    _text(c,MX+14*mm,y-12*mm,"Desperdícios reais + oportunidades estimadas",7,False,MUTED)
    _text(c,MX+14*mm,y-4*mm,f"R$ {_fmtI(total_geral_anual)} / ano",20,True,ORANGE,'right',198*mm)

def _pg_comp(c,d):
    _topbar(c); _footer(c,6); da=d.get("_real_anual", d["desp_anual"]); mid=W/2
    _text(c,MX+10*mm,H-30*mm,"Com Gestão:",20,True,NAVY)
    y=H-44*mm
    for b in ["✅  Auditoria retroativa e contínua","✅  Monitoramento de oportunidades","✅  Sem desperdícios a partir de hoje"]:
        _text(c,MX+14*mm,y,b,11,False,TEXT_CLR); y-=10*mm
    bw=mid-MX-20*mm
    _rect(c,MX+10*mm,30*mm,bw,50*mm,NAVY)
    _text(c,MX+14*mm,68*mm,"Economia de 1 ano",12,False,WHITE,'center',bw-8*mm)
    _text(c,MX+14*mm,50*mm,f"+R$ {_fmtI(da)}",26,True,GREEN,'center',bw-8*mm)
    _text(c,MX+14*mm,36*mm,"Sem incluir outros meios",8,False,MUTED,'center',bw-8*mm)
    _text(c,mid+10*mm,H-30*mm,"Sem Gestão:",20,True,NAVY)
    y=H-44*mm
    for p in ["❌  Conta continua um passivo","❌  Sem entender o desperdício","❌  Paga mais do que deveria"]:
        _text(c,mid+14*mm,y,p,11,False,TEXT_CLR); y-=10*mm
    _rect(c,mid+10*mm,30*mm,bw,50*mm,RED)
    _text(c,mid+14*mm,68*mm,"Desperdício de 1 ano",12,False,WHITE,'center',bw-8*mm)
    _text(c,mid+14*mm,50*mm,f"-R$ {_fmtI(da)}",26,True,colors.HexColor("#FEF08A"),'center',bw-8*mm)
    _text(c,mid+14*mm,36*mm,"Somente auditoria superficial",8,False,colors.HexColor("#FECACA"),'center',bw-8*mm)


def _pg_cronograma(c, d):
    _topbar(c); _sidebar(c); _footer(c,7)
    y = H - 28*mm
    _text(c, MX+8*mm, y, "Cronograma de Ações", 22, True, NAVY)
    trimestre = f"Primeiro Trimestre de {datetime.now().year}"
    _text(c, MX+8*mm, y-10*mm, trimestre, 11, False, GRAY)
    y -= 26*mm
    acoes = d.get("acoes", [])[:8]
    # Layout: 2 colunas x 4 linhas
    col_w = 120*mm; col_gap = 10*mm
    row_h = 34*mm; row_gap = 4*mm
    for i, (titulo, desc) in enumerate(acoes):
        col = i % 2
        row = i // 2
        ax = MX + 8*mm + col * (col_w + col_gap)
        ay = y - row * (row_h + row_gap)
        titulo_clean = titulo.replace("\n", " ").strip()
        desc_clean = desc.replace("\n", " ").strip()
        # Box escuro com título
        _rect(c, ax, ay - 15*mm, col_w, 16*mm, NAVY)
        _text(c, ax + 6*mm, ay - 10*mm, titulo_clean[:35], 10, True, WHITE)
        # Descrição abaixo
        _wrap(c, ax + 6*mm, ay - 20*mm, desc_clean, 8, False, GRAY, col_w - 12*mm, 11)

def _pg_escopo(c,d):
    _topbar(c); _footer(c,8)
    _text(c,MX+8*mm,H-28*mm,"Escopo das Atividades",22,True,NAVY)
    ativs=["Gestão completa na distribuidora — monitoramento e auditoria mensal.",
        "Auditoria retroativa das 120 faturas para recuperar cobranças indevidas.",
        "Revisão de demanda contratada com análise de sazonalidade.",
        "Correção do fator de potência — banco de capacitores.",
        "Laudo de ICMS da energia para créditos tributários.",
        "Processos administrativos junto à Amazonas Energia, CCEE e ANEEL.",
        "Relatórios mensais de resultado e metas. Dashboard online 24/7.",
        "Contestação de COSIP junto à Prefeitura quando cabível."]
    if d["eleg_ml"]: ativs.insert(2,"Análise de migração ao Mercado Livre de Energia (ACL).")
    if d["tem_gd"]: ativs.append("Otimização da Geração Distribuída e gestão de créditos.")
    y=H-44*mm
    for a in ativs[:10]: _text(c,MX+12*mm,y,f"✅  {a}",10,False,TEXT_CLR); y-=12*mm
    _rect(c,210*mm,MY+7*mm,1.5*mm,H-MY-19*mm,NAVY)
    # Painel direito — Diferenciais VOLTIX
    rx=218*mm; rw=W-rx-MX; ry=H-28*mm
    _text(c,rx,ry,"Por que a VOLTIX?",16,True,NAVY); ry-=16*mm
    difs=[("+ 10 Anos","de experiência no setor elétrico"),
          ("Especialistas","em clientes Grupo A (alta tensão)"),
          ("Dashboard 24/7","monitoramento em tempo real"),
          ("Sem Risco","pagamento só sobre resultados"),
          ("120 Faturas","auditoria retroativa completa"),
          ("Relatórios","prestação de contas mensal")]
    for titulo,desc in difs:
        _rect(c,rx,ry-22*mm,rw,24*mm,OFF_WHITE,GRAY_LT)
        _text(c,rx+6*mm,ry-8*mm,titulo,11,True,NAVY)
        _text(c,rx+6*mm,ry-18*mm,desc,8,False,GRAY)
        ry-=28*mm

def _pg_proposta(c,d,vm=500,cm=30):
    _topbar(c); _footer(c,9)
    _text(c,MX+8*mm,H-28*mm,"Proposta",24,True,NAVY)
    conds=[f"✅  Zero Investimento Inicial",f"✅  R$ {_fmtI(vm)},00/mês por UC",f"✅  {cm}% dos valores recuperados"]
    extras=["✅  Assessoria Técnica e Regulatória","✅  Dashboard de Monitoramento","✅  Gestão Fiscal e Tributária"]
    y=H-44*mm
    for co in conds: _text(c,MX+12*mm,y,co,12,True,TEXT_CLR); y-=11*mm
    y=H-44*mm
    for e in extras: _text(c,W/2+10*mm,y,e,12,True,TEXT_CLR); y-=11*mm
    cards=[("Tempo de Contrato","1 Ano","Reajuste anual IPCA"),("Cancelamento","R$ 0,00","Quando custo > economia"),("Prestação de Contas","Contínua","Relatório + Dashboard")]
    cw=80*mm; ch=55*mm; gap=6*mm; cy=25*mm
    for i,(titulo,valor,sub) in enumerate(cards):
        cx=MX+8*mm+i*(cw+gap); _rect(c,cx,cy,cw,ch,NAVY)
        _text(c,cx+4*mm,cy+ch-12*mm,titulo,10,False,MUTED,'center',cw-8*mm)
        _text(c,cx+4*mm,cy+ch-32*mm,valor,28,True,WHITE,'center',cw-8*mm)
        _text(c,cx+4*mm,cy+8*mm,sub,8,False,MUTED,'center',cw-8*mm)

def _pg_fim(c,d):
    _rect(c,0,0,W,H,DARK_NAVY); _rect(c,0,0,3*mm,H,ORANGE)
    _text(c,0,H/2+25*mm,"VOLTIX",40,True,WHITE,'center',W)
    _text(c,0,H/2+12*mm,"E N E R G I A",14,False,ORANGE,'center',W)
    _text(c,0,H/2-5*mm,"www.voltixenergia.com.br",14,False,GREEN,'center',W)
    _text(c,0,H/2-18*mm,"Gestão Inteligente",12,False,MUTED,'center',W)
    y=H/2-40*mm
    for cr in ["✅  + 10 Anos de experiência","✅  Time de Engenharia Elétrica","✅  Expert no Mercado Livre de Energia"]:
        _text(c,W/2-50*mm,y,cr,11,True,WHITE); y-=12*mm

# ══════════════════════════════════════════════════════════════════════════════
def gerar_estudo_pdf(resultado: ResultadoHistorico, valor_mensal=500, comissao=30, pdf_screenshot_bytes: Optional[bytes]=None):
    """Gera Estudo Técnico + Proposta Comercial em PDF."""
    r = resultado
    f0 = r.fatura_mais_recente
    d = {
        "nome": r.nome, "uc": r.uc, "subgrupo": r.subgrupo,
        "modalidade": r.modalidade, "n": r.n_faturas, "f0": f0,
        "mes": f0.get("mes_referencia",""),
        "total": float(f0.get("total_a_pagar") or 0),
        "custo_medio": r.custo_medio, "custo_periodo": r.custo_medio * r.n_faturas,
        "custo_anual": r.custo_anual,
        "consumo_medio": sum(float(f0.get("consumo_total_kwh") or 0) for _ in [1]),
        "dc_max": r.demanda_contratada, "dm_max": r.demanda_pico,
        "dm_med": r.demanda_media_medida, "util": r.utilizacao_demanda,
        "desp_dem": r.demanda_ociosa_r, "desp_reat": r.reativo_r,
        "desp_multas": r.multas_atraso_r, "desp_ultra": r.demanda_ultrapassagem_r,
        "desp_icms": r.icms_recuperavel_r,
        "desp_total": r.potencial_periodo,
        "desp_anual": r.potencial_anual,
        "cosip_total": r.cosip_total, "cosip_medio": r.cosip_media,
        "gd_total": r.gd_creditos_r, "tem_gd": r.tem_gd,
        "eleg_ml": r.elegivel_mercado_livre,
        "pontos": r.pontos_atencao[:6],
        "itens": f0.get("itens_faturados") or [],
        "acoes": r.acoes[:8],
        "screenshot": pdf_screenshot_bytes,
    }
    cnpj = r.cnpj
    buf=io.BytesIO()
    cv=canvas.Canvas(buf,pagesize=landscape(A4))
    cv.setTitle(f"Estudo Técnico — {d['nome'][:40]}")
    cv.setAuthor("VOLTIX ENERGIA")
    _pg_capa(cv,d,cnpj); cv.showPage()
    _pg_valor(cv,d); cv.showPage()
    _pg_fatura(cv,d); cv.showPage()
    _pg_analise_detalhe(cv,d); cv.showPage()
    _pg_resumo(cv,d); cv.showPage()
    _pg_comp(cv,d); cv.showPage()
    _pg_cronograma(cv,d); cv.showPage()
    _pg_escopo(cv,d); cv.showPage()
    _pg_proposta(cv,d,valor_mensal,comissao); cv.showPage()
    _pg_fim(cv,d); cv.showPage()
    cv.save(); buf.seek(0)
    logger.info(f"Estudo PDF: UC {d['uc']}, {d['n']} faturas, {buf.getbuffer().nbytes} bytes")
    return buf

gerar_estudo_pptx = gerar_estudo_pdf
