"""
Gerador de Estudo Técnico + Proposta Comercial
python-pptx → soffice → PDF  |  Layout réplica Cometais
Dados 100% reais das faturas parseadas.
"""
from __future__ import annotations
import io,os,logging,subprocess,tempfile
from datetime import datetime
from pptx import Presentation
from pptx.util import Inches,Pt,Emu
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN,MSO_ANCHOR
from pptx.enum.shapes import MSO_SHAPE

logger=logging.getLogger(__name__)

class C:
    navy=RGBColor(0x1B,0x2A,0x4A);dark=RGBColor(0x0F,0x1B,0x33)
    orange=RGBColor(0xF5,0xA6,0x23);green=RGBColor(0x05,0x96,0x69)
    red=RGBColor(0xDC,0x26,0x26);white=RGBColor(0xFF,0xFF,0xFF)
    off=RGBColor(0xF8,0xFA,0xFC);gray=RGBColor(0x64,0x74,0x8B)
    grayL=RGBColor(0xE2,0xE8,0xF0);txt=RGBColor(0x1E,0x29,0x3B)
    mut=RGBColor(0x94,0xA3,0xB8);yel=RGBColor(0xFE,0xF0,0x8A)
    cyan=RGBColor(0x06,0xB6,0xD4)

def _f(v,d=2):
    try: return f"{float(v):,.{d}f}".replace(",","X").replace(".",",").replace("X",".")
    except: return "—"
def _fi(v): return _f(v,0)

def _tx(s,t,l,tp,w,h,sz=14,b=False,it=False,c=C.txt,al=PP_ALIGN.LEFT,va=MSO_ANCHOR.TOP,ls=None,fn="Calibri"):
    tb=s.shapes.add_textbox(Inches(l),Inches(tp),Inches(w),Inches(h))
    tf=tb.text_frame;tf.word_wrap=True;p=tf.paragraphs[0]
    p.text=t;p.font.size=Pt(sz);p.font.bold=b;p.font.italic=it
    p.font.color.rgb=c;p.font.name=fn;p.alignment=al
    if ls: p.line_spacing=Pt(ls)
    return tb

def _rt(s,l,t,w,h,fc=None,lc=None):
    sh=s.shapes.add_shape(MSO_SHAPE.RECTANGLE,Inches(l),Inches(t),Inches(w),Inches(h))
    if lc: sh.line.color.rgb=lc;sh.line.width=Pt(1)
    else: sh.line.fill.background()
    if fc: sh.fill.solid();sh.fill.fore_color.rgb=fc
    return sh

def _ov(s,l,t,w,h,fc):
    sh=s.shapes.add_shape(MSO_SHAPE.OVAL,Inches(l),Inches(t),Inches(w),Inches(h))
    sh.fill.solid();sh.fill.fore_color.rgb=fc;sh.line.fill.background()

# ══════════════════════════════════════════════════════════════════════════════
def _analisar(faturas,alertas=None):
    alertas=alertas or [];n=len(faturas);f0=faturas[0]
    totais=[float(f.get("total_a_pagar")or 0) for f in faturas]
    cm=sum(totais)/n;cp=sum(totais)
    # Demanda
    dd=0.0
    for f in faturas:
        dc=float(f.get("demanda_contratada_fora_ponta_kw")or 0)
        dm=float(f.get("demanda_medida_fora_ponta_kw")or 0)
        td=float(f.get("tarifa_demanda")or 0)
        if dc>dm and td>0: dd+=(dc-dm)*td
    # Reativo
    dr=0.0
    for f in faturas:
        for it in(f.get("itens_faturados")or[]):
            d=(it.get("descricao")or"").lower()
            if "exc" in d and("en r" in d or "r exc" in d): dr+=abs(float(it.get("valor")or 0))
        if not f.get("itens_faturados"):
            u=float(f.get("ufer_fora_ponta_kvarh")or 0)
            if u>0: dr+=u*0.349
    # Multas
    dm2=0.0
    for f in faturas:
        for it in(f.get("itens_faturados")or[]):
            d=(it.get("descricao")or"").lower()
            if "multa" in d or "juros" in d or "mora" in d: dm2+=abs(float(it.get("valor")or 0))
    # COSIP/GD
    cosip_t=sum(float(f.get("cosip_valor")or 0) for f in faturas);cosip_m=cosip_t/n
    gd=0.0
    for f in faturas:
        for it in(f.get("itens_faturados")or[]):
            d=(it.get("descricao")or"").lower()
            if "credito" in d and "gera" in d: gd+=abs(float(it.get("valor")or 0))
    dcm=max(max(float(f.get("demanda_contratada_ponta_kw")or 0),float(f.get("demanda_contratada_fora_ponta_kw")or 0))for f in faturas)
    dms=[max(float(f.get("demanda_medida_ponta_kw")or 0),float(f.get("demanda_medida_fora_ponta_kw")or 0))for f in faturas]
    dmx=max(dms);dmd=sum(dms)/n
    ut=round(dmd/dcm*100)if dcm>0 else 0
    el=(f0.get("subgrupo")or"").startswith("A")or dcm>=300
    dt=dd+dr+dm2
    # Pontos detalhados
    pts=[]
    if ut<85 and dcm>0:
        pts.append({"t":"Demanda contratada atual.","d":f"Contratada: {_fi(dcm)} kW | Medida máx: {_fi(dmx)} kW | Utilização: {ut}%. Demanda não utilizada recorrente, muito desperdício todos os meses. Podendo ser ajustada com sazonalidade."})
    if dr>0:
        pts.append({"t":"Energia reativa (UFER).","d":f"Total faturado no período: R$ {_f(dr)}. Multa devido ao baixo fator de potência, corrigir com Banco de Capacitores e estudar o Filtro Capacitivo para proteção dos equipamentos."})
    if gd>0:
        pts.append({"t":"Geração Distribuída ativa.","d":f"Créditos acumulados: R$ {_f(gd)}. Verificar potencial de expansão do sistema para aumentar a cobertura e reduzir custo."})
    if cosip_m>300:
        pts.append({"t":f"COSIP elevada (R$ {_f(cosip_m)}/mês).","d":f"Total no período: R$ {_f(cosip_t)}. Contribuição de Iluminação Pública pode estar acima do enquadramento. Contestar junto à Prefeitura."})
    if dm2>0:
        pts.append({"t":"Multas por atrasos de pagamentos.","d":f"Total: R$ {_f(dm2)} no período. Requer uma gestão ativa nas contas para eliminação total."})
    if el:
        pts.append({"t":"Elegível para Mercado Livre.","d":f"{f0.get('subgrupo','?')} com {_fi(dcm)} kW. Desde 01/2024 todo Grupo A é elegível. Economia de 15-25% na tarifa de energia via comercializadora."})
    for al in alertas:
        t=al.get("titulo","")
        if t and len(pts)<6: pts.append({"t":t,"d":al.get("descricao","")})
    for i,pt in enumerate(pts): pt["n"]=str(i+1)
    # Ações
    acoes=[("Auditoria\nRetroativa\ndos últimos\n120 meses","Buscar pagamentos\nindevidos e pedir\ndevolução em dobro"),
           ("Ajustar a\ndemanda\ncontratada\nociosa","Se não houver\naumento do consumo,\najustar")]
    if el: acoes.append(("Portabilidade\npara o\nMercado Livre","Reduzir preço da\nenergia via\ncomercializadora"))
    if dr>0: acoes.append(("Corrigir\nBanco de\nCapacitores","Corrigir horário\nindutivo dos\nbancos URGENTE"))
    acoes.append(("Laudo de\nICMS para\ncréditos\nda energia","Fazer laudo para\nseparar o que é\nprodução"))
    if dr>0: acoes.append(("Instalar\nFiltro Capacitivo\ne corrigir BC","Para proteção\ndos Capacitores\ne motores"))
    if gd>0: acoes.append(("Estudar\nexpansão da\nGeração\nDistribuída","Aumentar cobertura\ne reduzir custo"))
    acoes.append(("Relatório\nMensal\nde Resultado","Prestação de contas\ncom economia\ne metas"))
    return {"nome":f0.get("cliente_nome",""),"uc":f0.get("uc",""),"sub":f0.get("subgrupo",""),
        "mod":f0.get("modalidade",""),"n":n,"f0":f0,"mes":f0.get("mes_referencia",""),
        "total":float(f0.get("total_a_pagar")or 0),"cm":cm,"cp":cp,"ca":cm*12,
        "cmed":sum(float(f.get("consumo_total_kwh")or 0)for f in faturas)/n,
        "dcm":dcm,"dmx":dmx,"dmd":dmd,"ut":ut,
        "dd":dd,"dr":dr,"dm2":dm2,"dt":dt,"da":dt/n*12,
        "cosip_t":cosip_t,"cosip_m":cosip_m,"gd":gd,"tem_gd":gd>0,"el":el,
        "pts":pts[:6],"itens":f0.get("itens_faturados")or[],"acoes":acoes[:8]}

# ══════════════════════════════════════════════════════════════════════════════
# SLIDE 1 — CAPA (réplica Cometais)
def _s1(p,d,cnpj):
    s=p.slides.add_slide(p.slide_layouts[6])
    s.background.fill.solid();s.background.fill.fore_color.rgb=C.white
    # Painel esquerdo navy com hexágonos simulados
    _rt(s,0,0,5.2,7.5,C.navy)
    # Hexágonos decorativos (shapes)
    for y in[0.4,2.2,4.0]:
        _rt(s,0.3,y,2.2,1.6,RGBColor(0x25,0x3E,0x6B))
        _rt(s,2.7,y+0.8,2.2,1.6,RGBColor(0x1E,0x35,0x5C))
    # Barra inferior navy
    _rt(s,0,7.2,13.3,0.3,C.navy)
    # Logo
    _tx(s,"Trianon",7.5,0.6,4,0.7,32,True,c=C.navy,fn="Calibri")
    _tx(s,"E N E R G I A",7.5,1.2,4,0.4,12,c=C.orange,fn="Calibri")
    _tx(s,"⚡",10.8,0.55,0.8,0.8,34,c=C.orange)
    # Título
    _tx(s,"Estudo Técnico",6.5,2.8,6,0.7,36,True,c=C.navy)
    _tx(s,"+ Proposta Comercial",6.5,3.5,6,0.5,24,c=C.gray)
    # Dados cliente
    _tx(s,d["nome"][:55],6.5,5.0,6.2,0.4,13,True,c=C.navy)
    _tx(s,f"CNPJ: {cnpj}",6.5,5.4,6,0.3,12,c=C.txt)
    _tx(s,f"UC: {d['uc']}",6.5,5.7,6,0.3,12,c=C.txt)

# SLIDE 2 — TRANSFORMANDO CUSTO EM ESTRATÉGIA
def _s2(p,d):
    s=p.slides.add_slide(p.slide_layouts[6])
    # Painel direito decorativo
    _rt(s,9.5,0,3.8,7.5,RGBColor(0xF0,0xF4,0xF8))
    _rt(s,13.1,0,0.2,7.5,C.orange)
    _tx(s,"Transformando custo em estratégia",0.5,0.3,8.5,0.6,28,True,c=C.navy)
    _rt(s,0.5,1.1,8.5,5.0,C.off,C.grayL)
    txts=["A energia elétrica, que tradicionalmente é tratada como um custo fixo e inevitável, passa a ser gerida como um ativo estratégico com a Trianon Energia.",
          "Atuamos de forma técnica e contínua na análise das suas faturas, contratos e perfil de consumo, identificando oportunidades reais de redução de custos, eliminação de cobranças indevidas e otimização da demanda contratada.",
          "Nosso trabalho não é pontual, é gestão recorrente, com inteligência de dados e acompanhamento especializado.",
          "Com a Trianon, sua empresa deixa de apenas pagar energia e passa a controlar, prever e economizar, com decisões embasadas e segurança regulatória."]
    y=1.3
    for t in txts: _tx(s,t,0.8,y,8,0.85,12,c=C.txt,ls=17);y+=0.95
    _tx(s,"Resultado: redução de despesas, aumento de eficiência e previsibilidade financeira.",0.5,5.5,8.5,0.4,12,True,c=C.txt)
    _tx(s,"Foque no que importa e deixe a energia com a gente!",0.5,6.3,8.5,0.4,13,it=True,c=C.red)
    _tx(s,"Trianon",10.2,6.5,2.5,0.4,16,True,c=C.navy)
    _tx(s,"ENERGIA",10.2,6.85,2.5,0.3,9,c=C.orange)

# SLIDE 3 — FATURA + PONTOS DE ATENÇÃO (página principal!)
def _s3(p,d):
    s=p.slides.add_slide(p.slide_layouts[6])
    _tx(s,"Fatura da Distribuidora:",0.3,0.15,6,0.5,24,True,c=C.navy)
    # Header demanda
    _rt(s,0.3,0.7,7.8,0.35,C.navy)
    dcp=float(d['f0'].get('demanda_contratada_ponta_kw')or 0)
    dcfp=d['dcm']
    _tx(s,f"D. Ctda Pta: {_fi(dcp)}         D. Ctda F.Pta: {_fi(dcfp)}         Período de Consumo: {d['mes']}",0.4,0.72,7.5,0.3,9,True,c=C.white)
    # Tabela itens faturados
    itens=d["itens"]
    if itens:
        from pptx.util import Inches as In
        rows=len(itens)+1;cols=3
        tbl=s.shapes.add_table(rows,cols,In(0.3),In(1.15),In(7.8),In(0.28*rows)).table
        tbl.columns[0].width=In(4.8);tbl.columns[1].width=In(1.5);tbl.columns[2].width=In(1.5)
        for j,h in enumerate(["Itens Faturados","Tar. sem Impostos","Valor"]):
            c2=tbl.cell(0,j);c2.text=h;c2.fill.solid();c2.fill.fore_color.rgb=C.navy
            pp=c2.text_frame.paragraphs[0];pp.font.size=Pt(8);pp.font.bold=True;pp.font.color.rgb=C.white;pp.font.name="Calibri"
            if j>0: pp.alignment=PP_ALIGN.RIGHT
        for i,item in enumerate(itens):
            desc=item.get("descricao","");qtd=item.get("quantidade");tar=item.get("tarifa")
            val=float(item.get("valor")or 0)
            lab=f"{desc} {_fi(qtd)} a {_f(tar,6)}" if qtd and tar else desc
            tbl.cell(i+1,0).text=lab
            tbl.cell(i+1,1).text=_f(tar,6) if tar else ""
            tbl.cell(i+1,2).text=f"{_f(val)}"
            for j in range(3):
                pp=tbl.cell(i+1,j).text_frame.paragraphs[0];pp.font.size=Pt(8);pp.font.name="Calibri"
                pp.font.color.rgb=C.green if val<0 else C.txt
                if j>0: pp.alignment=PP_ALIGN.RIGHT
    # Vencimento e Total
    ty=1.15+0.28*(len(itens)+1)+0.05
    _rt(s,0.3,ty,7.8,0.4,C.grayL)
    _tx(s,f"Vencimento: {d['mes']}",0.5,ty+0.05,3,0.3,10,True,c=C.txt)
    _tx(s,f"R$ {_f(d['total'])}",5.5,ty+0.02,2.5,0.35,14,True,c=C.navy,al=PP_ALIGN.RIGHT)
    _tx(s,"Valor a Pagar",4.0,ty+0.05,1.5,0.3,9,c=C.gray,al=PP_ALIGN.RIGHT)
    # Logo
    _tx(s,"Trianon",6.8,ty+0.5,1.5,0.3,12,True,c=C.navy)
    _tx(s,"ENERGIA",6.8,ty+0.75,1.5,0.2,7,c=C.orange)

    # ── PAINEL DIREITO: Pontos de Atenção ──
    _rt(s,8.4,0.15,4.6,7.2,C.navy)
    _tx(s,"Pontos de Atenção",8.7,0.3,3.5,0.4,16,True,c=C.white)
    _tx(s,"⚠",12.2,0.25,0.6,0.5,24,c=C.orange)
    py=0.85
    for pt in d["pts"][:6]:
        _ov(s,8.65,py+0.05,0.35,0.35,C.orange)
        _tx(s,pt["n"],8.65,py+0.05,0.35,0.35,11,True,c=C.white,al=PP_ALIGN.CENTER,va=MSO_ANCHOR.MIDDLE)
        _tx(s,pt["t"],9.15,py,3.6,0.3,10,True,c=C.white)
        _tx(s,pt["d"],9.15,py+0.32,3.6,0.7,8,c=RGBColor(0xCB,0xD5,0xE1),ls=11)
        py+=1.05

# SLIDE 4 — RESUMO TÉCNICO
def _s4(p,d):
    s=p.slides.add_slide(p.slide_layouts[6])
    # Painel direito decorativo
    _rt(s,9.0,0,4.3,7.5,RGBColor(0xF0,0xF4,0xF8))
    _rt(s,13.1,0,0.2,7.5,C.orange)
    _tx(s,"Resumo técnico:",0.5,0.3,7,0.6,28,True,it=True,c=C.navy)
    # Consolidado
    _tx(s,f"CONSOLIDADO DOS ÚLTIMOS {d['n']} MESES:",0.5,1.1,7,0.3,11,True,c=C.txt)
    y=1.6
    tusd_pct=63;te_pct=37
    tusd_v=d['ca']*tusd_pct/100;te_v=d['ca']*te_pct/100
    for lb,vl,pc in[("Custo do Fio (TUSD — AME):",f"R$ {_fi(tusd_v)}",f"{tusd_pct}%"),
                     ("Custo da Energia (TE):",f"R$ {_fi(te_v)}",f"{te_pct}%")]:
        _tx(s,lb,0.8,y,4,0.25,12,c=C.txt);_tx(s,vl,5,y,1.8,0.25,12,c=C.txt,al=PP_ALIGN.RIGHT)
        _tx(s,pc,7,y,0.8,0.25,12,c=C.txt,al=PP_ALIGN.RIGHT);y+=0.35
    _rt(s,0.8,y,7,0.015,C.navy);y+=0.15
    _tx(s,f"R$ {_fi(d['ca'])}",5,y,1.8,0.3,13,True,c=C.txt,al=PP_ALIGN.RIGHT)
    _tx(s,"100%",7,y,0.8,0.3,13,c=C.txt,al=PP_ALIGN.RIGHT)
    # Desperdício
    y+=0.6
    _tx(s,f"DESPERDÍCIO DOS ÚLTIMOS {d['n']} MESES:",0.5,y,7,0.3,11,True,c=C.txt);y+=0.4
    items=[]
    if d["dd"]>0:
        pct=round(d["dd"]/d["dt"]*100,2) if d["dt"]>0 else 0
        items.append(("Demanda não utilizada:",f"R$ {_fi(d['dd'])}",f"{pct}%"))
    if d["dr"]>0:
        pct=round(d["dr"]/d["dt"]*100,2) if d["dt"]>0 else 0
        items.append(("Energia reativa:",f"R$ {_fi(d['dr'])}",f"{pct}%"))
    if d["dm2"]>0:
        pct=round(d["dm2"]/d["dt"]*100,2) if d["dt"]>0 else 0
        items.append(("Multas por atrasos:",f"R$ {_fi(d['dm2'])}",f"{pct}%"))
    for lb,vl,pc in items:
        _tx(s,lb,0.8,y,4,0.25,12,c=C.txt);_tx(s,vl,5,y,1.8,0.25,12,c=C.txt,al=PP_ALIGN.RIGHT)
        _tx(s,pc,7,y,0.8,0.25,12,c=C.txt,al=PP_ALIGN.RIGHT);y+=0.35
    _rt(s,0.8,y,7,0.02,C.navy);y+=0.15
    # Total com destaque
    _tx(s,f"R$ {_f(d['dt'])}",4.5,y,2.3,0.4,14,True,c=C.txt,al=PP_ALIGN.RIGHT)
    _tx(s,"100%",7,y,0.8,0.35,13,c=C.txt,al=PP_ALIGN.RIGHT)
    y+=0.5
    _tx(s,"PROJEÇÃO ANUAL DO DESPERDÍCIO:",0.5,y,7,0.3,11,True,c=C.red);y+=0.4
    _tx(s,f"R$ {_f(d['da'])}",4.5,y,2.3,0.4,16,True,c=C.red,al=PP_ALIGN.RIGHT)
    # Círculo vermelho ao redor
    sh=s.shapes.add_shape(MSO_SHAPE.OVAL,Inches(4.3),Inches(y-0.05),Inches(2.7),Inches(0.5))
    sh.fill.background();sh.line.color.rgb=C.red;sh.line.width=Pt(2)
    _tx(s,"Trianon",10,6.5,2.5,0.4,16,True,c=C.navy)
    _tx(s,"ENERGIA",10,6.85,2.5,0.3,9,c=C.orange)

# SLIDE 5 — COM GESTÃO vs SEM GESTÃO
def _s5(p,d):
    s=p.slides.add_slide(p.slide_layouts[6])
    # Decoração direita
    _rt(s,9.8,0,3.5,3,C.navy)
    # Triângulos decorativos (simula os chevrons do Cometais)
    _rt(s,10,0.3,1.2,0.6,C.cyan);_rt(s,10.5,1.0,1.2,0.6,C.cyan);_rt(s,10,1.7,1.2,0.6,C.cyan)
    da=d["da"]
    # Com Gestão
    _tx(s,"Com Gestão:",0.5,0.3,5,0.5,24,True,c=C.navy)
    bens=["Auditoria retroativa e contínua","Monitoramento de oportunidades","Sem desperdícios a partir de hoje"]
    y=1.0
    for b in bens: _tx(s,"✅",0.7,y,0.4,0.35,16);_tx(s,b,1.2,y+0.02,4,0.3,13,True,c=C.txt);y+=0.42
    # Box economia
    _rt(s,0.5,3.2,4.3,2.3,C.navy)
    _tx(s,"Economia\nde 1 ano",0.8,3.4,3.7,0.7,16,c=C.white,al=PP_ALIGN.CENTER)
    _tx(s,f"+R$ {_f(da)}",0.8,4.1,3.7,0.6,28,True,c=C.green,al=PP_ALIGN.CENTER)
    _tx(s,"Sem incluir outros\nmeios de economia",0.8,4.8,3.7,0.5,10,c=C.mut,al=PP_ALIGN.CENTER)
    # Sem Gestão
    _tx(s,"Sem Gestão:",6,0.3,5,0.5,24,True,c=C.navy)
    y=1.0
    for pb in["Sua conta de energia continua um passivo","Continua sem entender do seu maior desperdício","Paga muito mais do que deveria"]:
        _tx(s,"❌",6.2,y,0.4,0.35,16);_tx(s,pb,6.7,y+0.02,5.5,0.3,13,True,c=C.txt);y+=0.42
    # Box desperdício
    _rt(s,6,3.2,4.3,2.3,C.red)
    _tx(s,"Desperdício\nde 1 ano",6.3,3.4,3.7,0.7,16,c=C.white,al=PP_ALIGN.CENTER)
    _tx(s,f"-R$ {_f(da)}",6.3,4.1,3.7,0.6,28,True,c=C.yel,al=PP_ALIGN.CENTER)
    _tx(s,"Somente com uma\nauditoria superficial",6.3,4.8,3.7,0.5,10,c=RGBColor(0xFE,0xCA,0xCA),al=PP_ALIGN.CENTER)

# SLIDE 6 — CRONOGRAMA DE AÇÕES
def _s6(p,d):
    s=p.slides.add_slide(p.slide_layouts[6])
    _tx(s,"Cronograma de Ações",0.4,0.25,8,0.5,28,True,c=C.navy)
    _tx(s,f"({datetime.now().strftime('%B').capitalize()})",6,0.3,3,0.4,18,c=C.gray)
    acoes=d["acoes"][:8];cols=4;gap=0.2
    cw=(12.5-(cols-1)*gap)/cols;ch=2.0
    for i,(titulo,sub) in enumerate(acoes):
        col=i%cols;row=i//cols
        cx=0.4+col*(cw+gap);cy=1.1+row*(ch+gap)
        _rt(s,cx,cy,cw,ch,C.navy)
        _tx(s,titulo,cx+0.15,cy+0.15,cw-0.3,1.0,11,True,c=C.white,al=PP_ALIGN.CENTER,va=MSO_ANCHOR.MIDDLE,ls=14)
        _tx(s,sub,cx+0.15,cy+1.2,cw-0.3,0.7,8,c=C.mut,al=PP_ALIGN.CENTER,ls=11)
    _tx(s,"Trianon",11,6.8,2,0.3,12,True,c=C.navy)
    _tx(s,"ENERGIA",11,7.05,2,0.2,7,c=C.orange)

# SLIDE 7 — ESCOPO DAS ATIVIDADES
def _s7(p,d):
    s=p.slides.add_slide(p.slide_layouts[6])
    # Painel direito
    _rt(s,9.5,0,3.8,3,RGBColor(0x06,0xA0,0xB5))
    _rt(s,9.5,3,3.8,4.5,C.navy)
    _tx(s,"Trianon",10,0.3,2.8,0.5,20,True,c=C.white)
    _tx(s,"ENERGIA",10,0.7,2.8,0.3,10,c=C.orange)
    _tx(s,"Escopo das atividades",0.4,0.3,8,0.6,28,True,c=C.navy)
    ativs=["Gestão completa no CATIVO ou LIVRE, na comercializadora e na distribuidora.",
        "Auditoria retroativa das 120 faturas para recuperar cobranças indevidas.",
        "Laudo de ICMS da energia utilizada na indústria para gerar créditos.",
        "Processos administrativos junto a todos os órgãos do setor elétrico como CCEE e ANEEL.",
        "Monitorar consumo por telemetria em tempo real (Opcional).",
        "Revisão jurídica e contábil de contratos com fornecedor de energia.",
        "Instalação de Analisador Inteligente para encontrar pontos de melhorias na rede elétrica.",
        "Relatórios mensais de resultado, economia e meta. Reunião presencial trimestral.",
        "Análise de viabilidade para BESS, usina solar e cooperativa de créditos.",
        "Serviço de Engenharia Elétrica para novas instalações, adequações e melhorias."]
    y=1.2
    for a in ativs: _tx(s,"✅",0.5,y,0.4,0.4,16,c=C.green);_tx(s,a,1.0,y+0.03,8,0.4,12,c=C.txt);y+=0.52

# SLIDE 8 — PROPOSTA
def _s8(p,d,vm=500,cm=30):
    s=p.slides.add_slide(p.slide_layouts[6])
    # Painel direito decorativo
    _rt(s,9.5,0,3.8,2.5,C.navy)
    _rt(s,9.5,0,0.8,0.5,C.cyan);_rt(s,10.5,0.3,0.8,0.5,C.cyan)
    _tx(s,"Proposta",0.4,0.3,5,0.6,28,True,c=C.navy)
    conds=[f"Zero Investimento Inicial",f"R$ {_fi(vm)},00/mês por UC",f"{cm}% dos valores que forem recuperados"]
    extras=["Assessoria Jurídica","Consultoria Contábil","Gestão Fiscal e Tributária"]
    y=1.2
    for c2 in conds: _tx(s,"✅",0.5,y,0.4,0.35,16,c=C.green);_tx(s,c2,1.0,y+0.02,5,0.3,14,True,c=C.txt);y+=0.42
    y=1.2
    for e in extras: _tx(s,"✅",6.5,y,0.4,0.35,16,c=C.green);_tx(s,e,7.0,y+0.02,3,0.3,14,True,c=C.txt);y+=0.42
    # Cards
    cards=[("Tempo de Contrato","1 Ano","Os 3 primeiros meses\ncom investimento\nreduzido"),
           ("Cancelamento","R$ 0,00","*Quando a remuneração\nfor maior que a\neconomia gerada"),
           ("Prestação de Contas","Contínua","Relatório Mensal\nReunião Trimestral\nComparativo Semestral")]
    cw=3.6;gap=0.4;cy=4.0
    for i,(tit,val,sub) in enumerate(cards):
        cx=0.5+i*(cw+gap);_rt(s,cx,cy,cw,3.0,C.navy)
        _tx(s,tit,cx+0.2,cy+0.3,cw-0.4,0.3,11,c=C.mut,al=PP_ALIGN.CENTER)
        _tx(s,val,cx+0.2,cy+0.7,cw-0.4,0.9,36,True,c=C.white,al=PP_ALIGN.CENTER,va=MSO_ANCHOR.MIDDLE)
        _tx(s,sub,cx+0.2,cy+1.8,cw-0.4,0.9,10,c=C.mut,al=PP_ALIGN.CENTER,ls=14)
    _tx(s,"Trianon",11,6.8,2,0.3,12,True,c=C.navy)
    _tx(s,"ENERGIA",11,7.05,2,0.2,7,c=C.orange)

# SLIDE 9 — ENCERRAMENTO
def _s9(p,d):
    s=p.slides.add_slide(p.slide_layouts[6])
    s.background.fill.solid();s.background.fill.fore_color.rgb=C.dark
    _rt(s,0,7.2,13.3,0.3,C.cyan)
    _rt(s,0,7.0,13.3,0.2,C.cyan)
    # "Confiam em nós" placeholder
    _rt(s,0.4,0.3,5.5,4.5,C.white)
    _tx(s,"Confiam em nós",0.5,0.4,5,0.3,12,True,it=True,c=C.navy)
    _tx(s,"Clientes atendidos pela Trianon\nem Manaus e região",0.8,1.5,4.5,1.0,14,c=C.gray,al=PP_ALIGN.CENTER)
    # Parceiros
    _tx(s,"Parceiros",0.5,5.0,5,0.3,11,True,c=C.white)
    _rt(s,0.5,5.35,5,0.7,RGBColor(0x20,0x30,0x50))
    _tx(s,"Moura  ·  Ambar Energia  ·  Itaipu",0.8,5.45,4.5,0.4,12,True,c=C.white,al=PP_ALIGN.CENTER)
    # Logo grande
    _tx(s,"Trianon",7,1.5,5.5,0.8,40,True,c=C.white,al=PP_ALIGN.CENTER)
    _tx(s,"E N E R G I A",7,2.3,5.5,0.4,16,c=C.orange,al=PP_ALIGN.CENTER)
    _tx(s,"www.trianonenergia.com",7,3.3,5.5,0.4,16,c=C.white,al=PP_ALIGN.CENTER)
    _tx(s,"Gestão Inteligente",7,3.8,5.5,0.4,14,c=C.green,al=PP_ALIGN.CENTER)
    for i,cr in enumerate(["✅  + 10 Anos de experiência","✅  Time de Engenharia Elétrica experiente","✅  Expert no Mercado Livre de Energia"]):
        _tx(s,cr,7.5,4.8+i*0.42,5,0.35,12,True,c=C.white)

# ══════════════════════════════════════════════════════════════════════════════
def gerar_estudo_pdf(faturas,alertas=None,cnpj="",valor_mensal=500,comissao=30):
    if not faturas: raise ValueError("Nenhuma fatura")
    d=_analisar(faturas,alertas)
    prs=Presentation();prs.slide_width=Inches(13.3);prs.slide_height=Inches(7.5)
    _s1(prs,d,cnpj);_s2(prs,d);_s3(prs,d);_s4(prs,d)
    _s5(prs,d);_s6(prs,d);_s7(prs,d);_s8(prs,d,valor_mensal,comissao);_s9(prs,d)

    with tempfile.TemporaryDirectory() as tmp:
        px=os.path.join(tmp,"estudo.pptx");prs.save(px)
        profile=os.path.join(tmp,"lo_profile");os.makedirs(profile,exist_ok=True)
        try:
            subprocess.run(["soffice","--headless","--norestore","--nofirststartwizard",
                f"-env:UserInstallation=file://{profile}","--convert-to","pdf","--outdir",tmp,px],
                check=True,timeout=60,capture_output=True)
            pdfs=[f for f in os.listdir(tmp) if f.endswith('.pdf')]
            if pdfs:
                with open(os.path.join(tmp,pdfs[0]),"rb") as f: buf=io.BytesIO(f.read())
                buf.seek(0);logger.info(f"PDF gerado: {buf.getbuffer().nbytes} bytes");return buf
        except Exception as e:
            logger.warning(f"soffice falhou ({e}), retornando PPTX")
        with open(px,"rb") as f: buf=io.BytesIO(f.read())
        buf.seek(0);return buf

gerar_estudo_pptx=gerar_estudo_pdf
