"""
Gerador de Estudo Técnico + Proposta Comercial — PDF Direto
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
    c.setFillColor(WHITE); c.setFont("Helvetica-Bold",11); c.drawString(MX,H-8.5*mm,"Trianon Energia")
    c.setFillColor(MUTED); c.setFont("Helvetica",7); c.drawRightString(W-MX,H-7*mm,"Estudo Técnico + Proposta Comercial")

def _footer(c,pg):
    c.setFillColor(GRAY_LT); c.rect(0,0,W,7*mm,fill=1,stroke=0)
    c.setFillColor(MUTED); c.setFont("Helvetica",6)
    c.drawString(MX,2.5*mm,"TRIANON GESTÃO DE ENERGIA LTDA · CNPJ: 58.843.586/0001-36 · Manaus, AM")
    c.drawRightString(W-MX,2.5*mm,f"Página {pg}")

def _sidebar(c): _rect(c,0,0,3*mm,H,ORANGE)

# ══════════════════════════════════════════════════════════════════════════════
def _analisar(faturas, alertas=None):
    alertas=alertas or []; n=len(faturas); f0=faturas[0]
    totais=[float(f.get("total_a_pagar") or 0) for f in faturas]
    custo_medio=sum(totais)/n; custo_periodo=sum(totais)
    desp_dem=0.0
    for f in faturas:
        dc=float(f.get("demanda_contratada_fora_ponta_kw") or 0)
        dm=float(f.get("demanda_medida_fora_ponta_kw") or 0)
        td=float(f.get("tarifa_demanda") or 0)
        if dc>dm and td>0: desp_dem+=(dc-dm)*td
    desp_reat=0.0
    for f in faturas:
        for it in (f.get("itens_faturados") or []):
            d=(it.get("descricao") or "").lower()
            if "exc" in d and ("en r" in d or "r exc" in d):
                desp_reat+=abs(float(it.get("valor") or 0))
        if not f.get("itens_faturados"):
            u=float(f.get("ufer_fora_ponta_kvarh") or 0)
            if u>0: desp_reat+=u*0.349
    desp_multas=0.0
    for f in faturas:
        for it in (f.get("itens_faturados") or []):
            d=(it.get("descricao") or "").lower()
            if "multa" in d or "juros" in d or "mora" in d:
                desp_multas+=abs(float(it.get("valor") or 0))
    cosip_total=sum(float(f.get("cosip_valor") or 0) for f in faturas)
    cosip_medio=cosip_total/n
    gd_total=0.0
    for f in faturas:
        for it in (f.get("itens_faturados") or []):
            d=(it.get("descricao") or "").lower()
            if "credito" in d and "gera" in d: gd_total+=abs(float(it.get("valor") or 0))
    dc_max=max(max(float(f.get("demanda_contratada_ponta_kw") or 0),float(f.get("demanda_contratada_fora_ponta_kw") or 0)) for f in faturas)
    dms=[max(float(f.get("demanda_medida_ponta_kw") or 0),float(f.get("demanda_medida_fora_ponta_kw") or 0)) for f in faturas]
    dm_max=max(dms); dm_med=sum(dms)/n
    util=round(dm_med/dc_max*100) if dc_max>0 else 0
    eleg_ml=(f0.get("subgrupo") or "").startswith("A") or dc_max>=300
    desp_total=desp_dem+desp_reat+desp_multas
    pontos=[]
    if util<85 and dc_max>0: pontos.append(f"Demanda superdimensionada — apenas {util}% de utilização média.")
    if gd_total>0: pontos.append("Geração Distribuída ativa — verificar expansão.")
    if dc_max-dm_med>20: pontos.append("Demanda não utilizada recorrente — ajustar com sazonalidade.")
    if desp_reat>0: pontos.append("Energia reativa (UFER) — corrigir fator de potência.")
    if cosip_medio>300: pontos.append(f"COSIP elevada (R$ {_fmt(cosip_medio)}/mês) — contestar.")
    if eleg_ml: pontos.append(f"Elegível Mercado Livre ({f0.get('subgrupo','?')}, {_fmtI(dc_max)} kW).")
    for al in alertas:
        t=al.get("titulo","")
        if t and t[:25] not in [p[:25] for p in pontos]: pontos.append(t)
    return {"nome":f0.get("cliente_nome",""),"uc":f0.get("uc",""),"subgrupo":f0.get("subgrupo",""),
        "modalidade":f0.get("modalidade",""),"n":n,"f0":f0,"mes":f0.get("mes_referencia",""),
        "total":float(f0.get("total_a_pagar") or 0),"custo_medio":custo_medio,"custo_periodo":custo_periodo,
        "custo_anual":custo_medio*12,"consumo_medio":sum(float(f.get("consumo_total_kwh") or 0) for f in faturas)/n,
        "dc_max":dc_max,"dm_max":dm_max,"dm_med":dm_med,"util":util,
        "desp_dem":desp_dem,"desp_reat":desp_reat,"desp_multas":desp_multas,
        "desp_total":desp_total,"desp_anual":desp_total/n*12,
        "cosip_total":cosip_total,"cosip_medio":cosip_medio,
        "gd_total":gd_total,"tem_gd":gd_total>0,"eleg_ml":eleg_ml,
        "pontos":pontos[:6],"itens":f0.get("itens_faturados") or []}

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
    _text(c,W-65*mm,H-30*mm,"Trianon",28,True,WHITE)
    _text(c,W-65*mm,H-38*mm,"E N E R G I A",10,False,ORANGE)
    _text(c,MX+10*mm,20*mm,f"Gerado em {datetime.now().strftime('%d/%m/%Y')}",8,False,MUTED)

def _pg_valor(c,d):
    _topbar(c); _sidebar(c); _footer(c,2)
    y=H-28*mm
    _text(c,MX+8*mm,y,"Transformando custo em estratégia",24,True,NAVY); y-=14*mm
    _rect(c,MX+8*mm,y-72*mm,180*mm,80*mm,OFF_WHITE,GRAY_LT)
    txts=["A energia elétrica, que tradicionalmente é tratada como um custo fixo e inevitável, passa a ser gerida como um ativo estratégico com a Trianon Energia.",
          "Atuamos de forma técnica e contínua na análise das suas faturas, contratos e perfil de consumo, identificando oportunidades reais de redução de custos.",
          "Nosso trabalho não é pontual, é gestão recorrente, com inteligência de dados e acompanhamento especializado.",
          "Com a Trianon, sua empresa deixa de apenas pagar energia e passa a controlar, prever e economizar."]
    ty=y-5*mm
    for t in txts: ty=_wrap(c,MX+12*mm,ty,t,10,False,TEXT_CLR,170*mm,14); ty-=4*mm
    _wrap(c,MX+8*mm,ty-8*mm,"Resultado: redução de despesas, aumento de eficiência e previsibilidade financeira.",10,True,GREEN,200*mm)
    _text(c,MX+8*mm,22*mm,"Foque no que importa e deixe a energia com a gente!",11,False,RED)

def _pg_fatura(c,d):
    _topbar(c); _footer(c,3)
    y=H-26*mm
    _text(c,MX,y,"Fatura da Distribuidora",22,True,NAVY); y-=12*mm
    _rect(c,MX,y-8*mm,165*mm,10*mm,NAVY)
    _text(c,MX+3*mm,y-5*mm,f"D.Ctda Pta: {_fmtI(d['f0'].get('demanda_contratada_ponta_kw') or 0)}   D.Ctda F.Pta: {_fmtI(d['dc_max'])}   Período: {d['mes']}",9,True,WHITE)
    y-=14*mm
    itens=d["itens"]
    if itens:
        cols=[90*mm,35*mm,40*mm]
        _rect(c,MX,y-7*mm,sum(cols),8*mm,NAVY)
        _text(c,MX+2*mm,y-4.5*mm,"Itens Faturados",8,True,WHITE)
        _text(c,MX+cols[0]+2*mm,y-4.5*mm,"Tarifa",8,True,WHITE,'right',cols[1]-4*mm)
        _text(c,MX+cols[0]+cols[1]+2*mm,y-4.5*mm,"Valor",8,True,WHITE,'right',cols[2]-4*mm)
        y-=8*mm
        for i,item in enumerate(itens):
            desc=item.get("descricao",""); qtd=item.get("quantidade"); tar=item.get("tarifa")
            val=float(item.get("valor") or 0)
            label=f"{desc} {_fmtI(qtd)} a {tar}" if qtd and tar else desc
            bg=OFF_WHITE if i%2==0 else WHITE
            _rect(c,MX,y-6*mm,sum(cols),7*mm,bg)
            _text(c,MX+2*mm,y-3.5*mm,label[:55],8,False,TEXT_CLR)
            if tar: _text(c,MX+cols[0]+2*mm,y-3.5*mm,f"{tar:.6f}",8,False,GRAY,'right',cols[1]-4*mm)
            cor=GREEN if val<0 else TEXT_CLR
            _text(c,MX+cols[0]+cols[1]+2*mm,y-3.5*mm,f"R$ {_fmt(val)}",8,True,cor,'right',cols[2]-4*mm)
            y-=7*mm
        _rect(c,MX,y-7*mm,sum(cols),8*mm,NAVY)
        _text(c,MX+cols[0]+2*mm,y-4.5*mm,"Valor a Pagar",9,True,WHITE,'right',cols[1]-4*mm)
        _text(c,MX+cols[0]+cols[1]+2*mm,y-4.5*mm,f"R$ {_fmt(d['total'])}",11,True,ORANGE,'right',cols[2]-4*mm)
    # Pontos
    px=185*mm; pw=W-px-MX; py_top=H-26*mm
    _rect(c,px,MY+7*mm,pw,py_top-MY-7*mm,NAVY)
    _text(c,px+8*mm,py_top-8*mm,"Pontos de Atenção",14,True,WHITE)
    _text(c,px+pw-12*mm,py_top-7*mm,"⚠",16,False,ORANGE)
    py=py_top-22*mm
    for i,ponto in enumerate(d["pontos"][:6]):
        c.setFillColor(ORANGE); c.circle(px+14*mm,py+2*mm,4*mm,fill=1,stroke=0)
        _text(c,px+11.5*mm,py-0.5*mm,str(i+1),9,True,WHITE,'center',5*mm)
        py=_wrap(c,px+22*mm,py+1*mm,ponto,8,False,WHITE,pw-28*mm,11); py-=5*mm

def _pg_resumo(c,d):
    _topbar(c); _sidebar(c); _footer(c,4)
    y=H-28*mm
    _text(c,MX+8*mm,y,"Resumo Técnico",22,True,NAVY); y-=14*mm
    kw=60*mm; kh=26*mm; gap=4*mm
    kpis=[("Custo Médio/Mês",f"R$ {_fmt(d['custo_medio'])}"),("Projeção Anual",f"R$ {_fmt(d['custo_anual'])}"),("Consumo Médio",f"{_fmtI(d['consumo_medio'])} kWh"),("Utiliz. Demanda",f"{d['util']}%")]
    for i,(label,val) in enumerate(kpis):
        kx=MX+8*mm+i*(kw+gap)
        _rect(c,kx,y-kh,kw,kh,OFF_WHITE,GRAY_LT)
        _text(c,kx+4*mm,y-9*mm,val,13,True,NAVY if d["util"]>=70 or i!=3 else RED,'center',kw-8*mm)
        _text(c,kx+4*mm,y-kh+5*mm,label,8,False,GRAY,'center',kw-8*mm)
    y-=kh+12*mm
    _text(c,MX+8*mm,y,f"DESPERDÍCIO IDENTIFICADO ({d['n']} meses)",11,True,RED); y-=10*mm
    items=[]
    if d["desp_dem"]>0: items.append(("Demanda não utilizada",d["desp_dem"]))
    if d["desp_reat"]>0: items.append(("Energia reativa (UFER)",d["desp_reat"]))
    if d["desp_multas"]>0: items.append(("Multas e juros",d["desp_multas"]))
    for label,val in items:
        _text(c,MX+12*mm,y,label,10,False,TEXT_CLR)
        _text(c,MX+12*mm,y,f"R$ {_fmt(val)}",10,True,RED,'right',200*mm); y-=8*mm
    if items:
        c.setStrokeColor(RED); c.setLineWidth(1.5); c.line(MX+12*mm,y+3*mm,MX+212*mm,y+3*mm); y-=4*mm
        _text(c,MX+12*mm,y,"TOTAL NO PERÍODO",11,True,RED)
        _text(c,MX+12*mm,y,f"R$ {_fmt(d['desp_total'])}",14,True,RED,'right',200*mm); y-=12*mm
        _rect(c,MX+100*mm,y-4*mm,115*mm,14*mm,RED_LT)
        _text(c,MX+12*mm,y,"PROJEÇÃO ANUAL",11,True,RED)
        _text(c,MX+12*mm,y,f"R$ {_fmtI(d['desp_anual'])}",18,True,RED,'right',200*mm)

def _pg_comp(c,d):
    _topbar(c); _footer(c,5); da=d["desp_anual"]; mid=W/2
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

def _pg_escopo(c,d):
    _topbar(c); _footer(c,6)
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

def _pg_proposta(c,d,vm=500,cm=30):
    _topbar(c); _footer(c,7)
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
    _text(c,0,H/2+25*mm,"Trianon",40,True,WHITE,'center',W)
    _text(c,0,H/2+12*mm,"E N E R G I A",14,False,ORANGE,'center',W)
    _text(c,0,H/2-5*mm,"www.trianonenergia.com",14,False,GREEN,'center',W)
    _text(c,0,H/2-18*mm,"Gestão Inteligente",12,False,MUTED,'center',W)
    y=H/2-40*mm
    for cr in ["✅  + 10 Anos de experiência","✅  Time de Engenharia Elétrica","✅  Expert no Mercado Livre de Energia"]:
        _text(c,W/2-50*mm,y,cr,11,True,WHITE); y-=12*mm

# ══════════════════════════════════════════════════════════════════════════════
def gerar_estudo_pdf(faturas,alertas=None,cnpj="",valor_mensal=500,comissao=30):
    if not faturas: raise ValueError("Nenhuma fatura")
    d=_analisar(faturas,alertas)
    buf=io.BytesIO()
    cv=canvas.Canvas(buf,pagesize=landscape(A4))
    cv.setTitle(f"Estudo Técnico — {d['nome'][:40]}")
    cv.setAuthor("Trianon Gestão de Energia")
    _pg_capa(cv,d,cnpj); cv.showPage()
    _pg_valor(cv,d); cv.showPage()
    _pg_fatura(cv,d); cv.showPage()
    _pg_resumo(cv,d); cv.showPage()
    _pg_comp(cv,d); cv.showPage()
    _pg_escopo(cv,d); cv.showPage()
    _pg_proposta(cv,d,valor_mensal,comissao); cv.showPage()
    _pg_fim(cv,d); cv.showPage()
    cv.save(); buf.seek(0)
    logger.info(f"Estudo PDF: UC {d['uc']}, {d['n']} faturas, {buf.getbuffer().nbytes} bytes")
    return buf

gerar_estudo_pptx = gerar_estudo_pdf
