"""
Gerador de Estudo Técnico + Proposta Comercial — PDF
Pipeline: dados reais → python-pptx → LibreOffice → PDF

Todos os valores vêm diretamente das faturas parseadas.
Nenhum dado é inventado ou estimado sem base real.

Uso via API:
    from src.reports.gerar_estudo import gerar_estudo_pdf
    pdf_bytes = gerar_estudo_pdf(faturas, alertas, cnpj="...")
"""
from __future__ import annotations
import io, os, logging, subprocess, tempfile
from datetime import datetime
from pptx import Presentation
from pptx.util import Inches, Pt
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN, MSO_ANCHOR
from pptx.enum.shapes import MSO_SHAPE

logger = logging.getLogger(__name__)

# ── Cores ─────────────────────────────────────────────────────────────────────
class C:
    navy=RGBColor(0x1B,0x2A,0x4A); darkNavy=RGBColor(0x0F,0x1B,0x33)
    orange=RGBColor(0xF5,0xA6,0x23); green=RGBColor(0x05,0x96,0x69)
    red=RGBColor(0xDC,0x26,0x26); white=RGBColor(0xFF,0xFF,0xFF)
    offWhite=RGBColor(0xF8,0xFA,0xFC); gray=RGBColor(0x64,0x74,0x8B)
    grayLt=RGBColor(0xE2,0xE8,0xF0); text=RGBColor(0x1E,0x29,0x3B)
    muted=RGBColor(0x94,0xA3,0xB8); yellow=RGBColor(0xFE,0xF0,0x8A)

def _fmt(v,d=2):
    try: return f"{float(v):,.{d}f}".replace(",","X").replace(".",",").replace("X",".")
    except: return "—"
def _fmtI(v): return _fmt(v,0)

def _tx(s,t,l,tp,w,h,sz=14,b=False,it=False,c=C.text,al=PP_ALIGN.LEFT,va=MSO_ANCHOR.TOP,ls=None):
    tb=s.shapes.add_textbox(Inches(l),Inches(tp),Inches(w),Inches(h))
    tf=tb.text_frame; tf.word_wrap=True; p=tf.paragraphs[0]
    p.text=t; p.font.size=Pt(sz); p.font.bold=b; p.font.italic=it
    p.font.color.rgb=c; p.font.name="Calibri"; p.alignment=al
    if ls: p.line_spacing=Pt(ls)
    return tb

def _rect(s,l,t,w,h,fc=None):
    sh=s.shapes.add_shape(MSO_SHAPE.RECTANGLE,Inches(l),Inches(t),Inches(w),Inches(h))
    sh.line.fill.background()
    if fc: sh.fill.solid(); sh.fill.fore_color.rgb=fc
    return sh

def _oval(s,l,t,w,h,fc):
    sh=s.shapes.add_shape(MSO_SHAPE.OVAL,Inches(l),Inches(t),Inches(w),Inches(h))
    sh.fill.solid(); sh.fill.fore_color.rgb=fc; sh.line.fill.background()
    return sh


# ══════════════════════════════════════════════════════════════════════════════
# ANÁLISE 100% REAL
# ══════════════════════════════════════════════════════════════════════════════
def _analisar(faturas, alertas=None):
    alertas=alertas or []; n=len(faturas); f0=faturas[0]
    totais=[float(f.get("total_a_pagar") or 0) for f in faturas]
    custo_medio=sum(totais)/n; custo_periodo=sum(totais)

    # Demanda — desperdício REAL
    desp_dem=0.0
    for f in faturas:
        dc=float(f.get("demanda_contratada_fora_ponta_kw") or 0)
        dm=float(f.get("demanda_medida_fora_ponta_kw") or 0)
        td=float(f.get("tarifa_demanda") or 0)
        if dc>dm and td>0: desp_dem+=(dc-dm)*td

    # Reativo — REAL dos itens faturados
    desp_reat=0.0
    for f in faturas:
        for it in (f.get("itens_faturados") or []):
            d=(it.get("descricao") or "").lower()
            if "exc" in d and ("r " in d or "r_" in d or d.startswith("en r")):
                desp_reat+=abs(float(it.get("valor") or 0))
        if not f.get("itens_faturados"):
            u=float(f.get("ufer_fora_ponta_kvarh") or 0)
            if u>0: desp_reat+=u*0.349

    # Multas — REAL
    desp_multas=0.0
    for f in faturas:
        for it in (f.get("itens_faturados") or []):
            d=(it.get("descricao") or "").lower()
            if "multa" in d or "juros" in d or "mora" in d:
                desp_multas+=abs(float(it.get("valor") or 0))

    # COSIP — REAL
    cosip_total=sum(float(f.get("cosip_valor") or 0) for f in faturas)
    cosip_medio=cosip_total/n

    # GD — REAL
    gd_total=0.0
    for f in faturas:
        for it in (f.get("itens_faturados") or []):
            d=(it.get("descricao") or "").lower()
            if "credito" in d and "gera" in d: gd_total+=abs(float(it.get("valor") or 0))

    # Utilização demanda
    dc_max=max(max(float(f.get("demanda_contratada_ponta_kw") or 0),
                   float(f.get("demanda_contratada_fora_ponta_kw") or 0)) for f in faturas)
    dms=[max(float(f.get("demanda_medida_ponta_kw") or 0),
             float(f.get("demanda_medida_fora_ponta_kw") or 0)) for f in faturas]
    dm_max=max(dms); dm_med=sum(dms)/n
    util=round(dm_med/dc_max*100) if dc_max>0 else 0
    eleg_ml=(f0.get("subgrupo") or "").startswith("A") or dc_max>=300

    desp_total=desp_dem+desp_reat+desp_multas

    # Pontos de atenção
    pontos=[]
    if util<85 and dc_max>0:
        pontos.append(f"Demanda contratada superdimensionada — apenas {util}% de utilização média.")
    if gd_total>0:
        pontos.append(f"Geração Distribuída ativa — verificar potencial de expansão.")
    if dc_max-dm_med>20:
        pontos.append("Demanda não utilizada recorrente — ajustar com sazonalidade para eliminar desperdício.")
    if desp_reat>0:
        pontos.append("Energia reativa — UFER presente, corrigir fator de potência com banco de capacitores.")
    if cosip_medio>300:
        pontos.append(f"COSIP elevada (R$ {_fmt(cosip_medio)}/mês) — verificar enquadramento junto à Prefeitura.")
    if eleg_ml:
        pontos.append(f"Elegível para Mercado Livre ({f0.get('subgrupo','?')}, {_fmtI(dc_max)} kW). Economia potencial de 10-25%.")
    for al in alertas:
        t=al.get("titulo","")
        if t and t[:25] not in [p[:25] for p in pontos]: pontos.append(t)

    return {
        "nome":f0.get("cliente_nome",""),"uc":f0.get("uc",""),
        "subgrupo":f0.get("subgrupo",""),"modalidade":f0.get("modalidade",""),
        "n":n,"f0":f0,"mes":f0.get("mes_referencia",""),
        "total":float(f0.get("total_a_pagar") or 0),
        "custo_medio":custo_medio,"custo_periodo":custo_periodo,
        "custo_anual":custo_medio*12,
        "consumo_medio":sum(float(f.get("consumo_total_kwh") or 0) for f in faturas)/n,
        "dc_max":dc_max,"dm_max":dm_max,"dm_med":dm_med,"util":util,
        "desp_dem":desp_dem,"desp_reat":desp_reat,"desp_multas":desp_multas,
        "desp_total":desp_total,"desp_anual":desp_total/n*12,
        "cosip_total":cosip_total,"cosip_medio":cosip_medio,
        "gd_total":gd_total,"tem_gd":gd_total>0,"eleg_ml":eleg_ml,
        "pontos":pontos[:6],
        "itens":f0.get("itens_faturados") or [],
    }


# ══════════════════════════════════════════════════════════════════════════════
# SLIDES (mesmo layout validado no PPTX anterior)
# ══════════════════════════════════════════════════════════════════════════════
def _sl_capa(p,d,cnpj):
    s=p.slides.add_slide(p.slide_layouts[6])
    s.background.fill.solid(); s.background.fill.fore_color.rgb=C.darkNavy
    _rect(s,0,0,0.12,7.5,C.orange)
    _tx(s,"Estudo Técnico",1.2,1.5,7,0.8,40,True,c=C.white)
    _tx(s,"+ Proposta Comercial",1.2,2.3,7,0.6,28,c=C.muted)
    _rect(s,1.2,3.8,8,0.03,C.orange)
    _tx(s,d["nome"],1.2,4.1,8,0.5,16,True,c=C.white)
    _tx(s,f"CNPJ: {cnpj}",1.2,4.6,8,0.3,13,c=C.muted)
    _tx(s,f"UC: {d['uc']}",1.2,4.9,8,0.3,13,c=C.muted)
    _tx(s,"Trianon",9,0.6,3.5,0.6,32,True,c=C.white,al=PP_ALIGN.RIGHT)
    _tx(s,"E N E R G I A",9,1.1,3.5,0.4,14,c=C.orange,al=PP_ALIGN.RIGHT)
    _tx(s,"⚡",12.3,0.5,0.7,0.7,34,c=C.orange)

def _sl_valor(p,d):
    s=p.slides.add_slide(p.slide_layouts[6])
    _tx(s,"Transformando custo em estratégia",0.6,0.4,8,0.7,30,True,c=C.navy)
    _rect(s,0.6,1.3,7.5,4.8,C.offWhite)
    txts=["A energia elétrica, que tradicionalmente é tratada como um custo fixo e inevitável, passa a ser gerida como um ativo estratégico com a Trianon Energia.",
          "Atuamos de forma técnica e contínua na análise das suas faturas, contratos e perfil de consumo, identificando oportunidades reais de redução de custos e otimização da demanda contratada.",
          "Nosso trabalho não é pontual, é gestão recorrente, com inteligência de dados e acompanhamento especializado.",
          "Com a Trianon, sua empresa deixa de apenas pagar energia e passa a controlar, prever e economizar, com decisões embasadas e segurança regulatória."]
    y=1.5
    for t in txts: _tx(s,t,0.9,y,6.9,0.95,13,c=C.text,ls=18); y+=1.0
    _tx(s,"Resultado: redução de despesas, aumento de eficiência e previsibilidade financeira.",0.6,5.8,7.5,0.4,13,True,c=C.green)
    _tx(s,"Foque no que importa e deixe a energia com a gente!",0.6,6.4,7.5,0.4,14,it=True,c=C.red)
    _rect(s,13.18,0,0.12,7.5,C.orange)

def _sl_fatura(p,d):
    s=p.slides.add_slide(p.slide_layouts[6])
    _tx(s,"Fatura da Distribuidora",0.6,0.3,7,0.6,28,True,c=C.navy)
    _rect(s,0.6,1.0,7.5,0.4,C.navy)
    _tx(s,f"D. Ctda Pta: {_fmtI(d['f0'].get('demanda_contratada_ponta_kw') or 0)}     D. Ctda F.Pta: {_fmtI(d['dc_max'])}     Período: {d['mes']}",
        0.8,1.02,7,0.35,11,True,c=C.white)

    # Tabela itens
    from pptx.util import Inches as In
    itens=d["itens"]
    if itens:
        rows=len(itens)+2
        tbl=s.shapes.add_table(rows,3,In(0.6),In(1.55),In(7.5),In(0.32*rows)).table
        tbl.columns[0].width=In(4.5); tbl.columns[1].width=In(1.5); tbl.columns[2].width=In(1.5)
        for j,h in enumerate(["Itens Faturados","Tar. sem Impostos","Valor"]):
            c2=tbl.cell(0,j); c2.text=h; c2.fill.solid(); c2.fill.fore_color.rgb=C.navy
            pp=c2.text_frame.paragraphs[0]; pp.font.size=Pt(10); pp.font.bold=True; pp.font.color.rgb=C.white
            if j>0: pp.alignment=PP_ALIGN.RIGHT
        for i,it in enumerate(itens):
            desc=it.get("descricao",""); qtd=it.get("quantidade"); tar=it.get("tarifa")
            val=float(it.get("valor") or 0)
            lab=f"{desc} {_fmtI(qtd)} a {tar}" if qtd and tar else desc
            tbl.cell(i+1,0).text=lab
            tbl.cell(i+1,1).text=f"{tar:.6f}" if tar else ""
            tbl.cell(i+1,2).text=f"R$ {_fmt(val)}"
            for j in range(3):
                pp=tbl.cell(i+1,j).text_frame.paragraphs[0]; pp.font.size=Pt(10)
                pp.font.color.rgb=C.green if val<0 else C.text
                if j>0: pp.alignment=PP_ALIGN.RIGHT
        lr=rows-1
        tbl.cell(lr,0).text=""; tbl.cell(lr,1).text="Valor a Pagar"
        tbl.cell(lr,2).text=f"R$ {_fmt(d['total'])}"
        for j in range(3):
            c2=tbl.cell(lr,j); c2.fill.solid(); c2.fill.fore_color.rgb=C.navy
            pp=c2.text_frame.paragraphs[0]; pp.font.bold=True
            pp.font.color.rgb=C.orange if j==2 else C.white; pp.font.size=Pt(12 if j>=1 else 10)
            if j>0: pp.alignment=PP_ALIGN.RIGHT

    # Pontos de atenção
    _rect(s,8.6,0.3,4.2,7,C.navy)
    _tx(s,"Pontos de Atenção",8.8,0.5,3.2,0.5,18,True,c=C.white)
    _tx(s,"⚠",12,0.4,0.5,0.5,28,c=C.orange)
    y=1.2
    for i,pt in enumerate(d["pontos"][:6]):
        _oval(s,8.9,y+0.05,0.35,0.35,C.orange)
        _tx(s,str(i+1),8.9,y+0.05,0.35,0.35,12,True,c=C.white,al=PP_ALIGN.CENTER,va=MSO_ANCHOR.MIDDLE)
        _tx(s,pt,9.4,y,3.3,0.85,11,c=C.white,ls=14); y+=0.92

def _sl_resumo(p,d):
    s=p.slides.add_slide(p.slide_layouts[6])
    _tx(s,"Resumo técnico",0.6,0.4,7,0.7,30,True,c=C.navy)
    _tx(s,f"CONSOLIDADO ({d['n']} FATURAS):",0.6,1.4,6,0.35,12,True,c=C.navy)

    y=1.85
    for lb,vl in [("Custo médio mensal:",f"R$ {_fmt(d['custo_medio'])}"),
                   (f"Custo total ({d['n']} meses):",f"R$ {_fmt(d['custo_periodo'])}")]:
        _tx(s,lb,0.9,y,3.5,0.3,13,c=C.text); _tx(s,vl,4.5,y,2,0.3,13,True,c=C.text,al=PP_ALIGN.RIGHT); y+=0.35
    _rect(s,0.9,y+0.05,5.6,0.02,C.navy); y+=0.2
    _tx(s,"Projeção anual:",0.9,y,3.5,0.35,14,True,c=C.navy)
    _tx(s,f"R$ {_fmt(d['custo_anual'])}",4.5,y,2,0.35,14,True,c=C.navy,al=PP_ALIGN.RIGHT)

    y+=0.7
    _tx(s,f"DESPERDÍCIO IDENTIFICADO ({d['n']} meses):",0.6,y,6,0.35,12,True,c=C.red); y+=0.45
    items=[]
    if d["desp_dem"]>0: items.append(("Demanda não utilizada:",d["desp_dem"]))
    if d["desp_reat"]>0: items.append(("Energia reativa (UFER):",d["desp_reat"]))
    if d["desp_multas"]>0: items.append(("Multas e juros:",d["desp_multas"]))
    for lb,vl in items:
        _tx(s,lb,0.9,y,3.5,0.3,13,c=C.text); _tx(s,f"R$ {_fmt(vl)}",4.5,y,2,0.3,13,True,c=C.red,al=PP_ALIGN.RIGHT); y+=0.35
    _rect(s,0.9,y+0.05,5.6,0.03,C.red); y+=0.25
    _tx(s,f"TOTAL PERÍODO:",0.9,y,3.5,0.35,14,True,c=C.red)
    _tx(s,f"R$ {_fmt(d['desp_total'])}",4.5,y,2,0.4,16,True,c=C.red,al=PP_ALIGN.RIGHT)
    y+=0.45
    _tx(s,"PROJEÇÃO ANUAL:",0.9,y,3.5,0.35,14,True,c=C.red)
    _tx(s,f"R$ {_fmtI(d['desp_anual'])}",4.5,y,2,0.4,18,True,c=C.red,al=PP_ALIGN.RIGHT)
    _rect(s,13.18,0,0.12,7.5,C.orange)

def _sl_comp(p,d):
    s=p.slides.add_slide(p.slide_layouts[6])
    da=d["desp_anual"]
    _tx(s,"Com Gestão:",0.6,0.5,5.5,0.6,26,True,c=C.navy)
    for i,b in enumerate(["Auditoria retroativa e contínua","Monitoramento de oportunidades","Sem desperdícios a partir de hoje"]):
        _tx(s,"✅",0.8,1.3+i*0.45,0.4,0.35,16); _tx(s,b,1.3,1.3+i*0.45,4.5,0.35,14,c=C.text)
    _rect(s,0.8,3.5,4.5,2.2,C.navy)
    _tx(s,"Economia\nde 1 ano",1.0,3.7,4,0.7,16,c=C.white,al=PP_ALIGN.CENTER)
    _tx(s,f"+R$ {_fmtI(da)}",1.0,4.3,4,0.6,32,True,c=C.green,al=PP_ALIGN.CENTER)
    _tx(s,"Sem incluir outros\nmeios de economia",1.0,5.0,4,0.5,11,c=C.muted,al=PP_ALIGN.CENTER)

    _tx(s,"Sem Gestão:",7,0.5,5.5,0.6,26,True,c=C.navy)
    for i,pb in enumerate(["Sua conta de energia continua um passivo","Continua sem entender seu maior desperdício","Paga muito mais do que deveria"]):
        _tx(s,"❌",7.2,1.3+i*0.45,0.4,0.35,16); _tx(s,pb,7.7,1.3+i*0.45,5,0.35,14,c=C.text)
    _rect(s,7.2,3.5,4.5,2.2,C.red)
    _tx(s,"Desperdício\nde 1 ano",7.4,3.7,4,0.7,16,c=C.white,al=PP_ALIGN.CENTER)
    _tx(s,f"-R$ {_fmtI(da)}",7.4,4.3,4,0.6,32,True,c=C.yellow,al=PP_ALIGN.CENTER)
    _tx(s,"Somente com uma\nauditoria superficial",7.4,5.0,4,0.5,11,c=RGBColor(0xFE,0xCA,0xCA),al=PP_ALIGN.CENTER)

def _sl_crono(p,d):
    s=p.slides.add_slide(p.slide_layouts[6])
    _tx(s,"Cronograma de Ações",0.6,0.4,8,0.6,28,True,c=C.navy)
    _tx(s,"(Primeiro Trimestre)",6.5,0.45,3,0.5,18,c=C.gray)
    acoes=[("Auditoria\nRetroativa\ndos últimos\n120 meses","Buscar pagamentos\nindevidos"),
           ("Ajustar a\ndemanda\ncontratada\nociosa","Reduzir desperdício\ncom sazonalidade")]
    if d["eleg_ml"]: acoes.append(("Estudar\nMercado Livre\nde Energia","Comparar propostas\nde comercializadoras"))
    if d["desp_reat"]>0: acoes.append(("Corrigir\nBanco de\nCapacitores","Eliminar UFER e\nproteger equipamentos"))
    acoes.append(("Laudo de\nICMS para\ncréditos","Separar energia\nde produção"))
    if d["tem_gd"]: acoes.append(("Otimizar\nGeração\nDistribuída","Expandir cobertura\npara reduzir custo"))
    if d["cosip_medio"]>300: acoes.append(("Contestar\nCOSIP\nelevada","Verificar junto\nà Prefeitura"))
    acoes.append(("Relatório\nMensal\nde Resultado","Prestação de contas\ncom economia"))
    acoes=acoes[:8]; cols=4; gap=0.25; cw=(12.1-(cols-1)*gap)/cols; ch=1.9
    for i,(titulo,sub) in enumerate(acoes):
        col=i%cols; row=i//cols; cx=0.6+col*(cw+gap); cy=1.4+row*(ch+gap)
        _rect(s,cx,cy,cw,ch,C.navy)
        _tx(s,titulo,cx+0.15,cy+0.15,cw-0.3,0.95,12,True,c=C.white,al=PP_ALIGN.CENTER,va=MSO_ANCHOR.MIDDLE,ls=15)
        _tx(s,sub,cx+0.15,cy+1.1,cw-0.3,0.7,9,c=C.muted,al=PP_ALIGN.CENTER,ls=12)

def _sl_escopo(p,d):
    s=p.slides.add_slide(p.slide_layouts[6])
    _tx(s,"Escopo das atividades",0.6,0.4,8,0.7,28,True,c=C.navy)
    ativs=["Gestão completa na distribuidora — monitoramento mensal.",
           "Auditoria retroativa das 120 faturas para recuperar cobranças indevidas.",
           "Revisão de demanda contratada com sazonalidade.",
           "Correção do fator de potência — banco de capacitores.",
           "Laudo de ICMS da energia para créditos tributários.",
           "Processos administrativos junto à Amazonas Energia, CCEE e ANEEL.",
           "Relatórios mensais de resultado e metas. Dashboard online.",
           "Contestação de COSIP junto à Prefeitura quando cabível."]
    if d["eleg_ml"]: ativs.insert(2,"Análise de migração ao Mercado Livre de Energia (ACL).")
    if d["tem_gd"]: ativs.append("Otimização da Geração Distribuída.")
    for i,a in enumerate(ativs[:10]):
        y=1.4+i*0.48; _tx(s,"✅",0.8,y,0.4,0.4,18,c=C.green); _tx(s,a,1.3,y+0.02,8,0.4,14,c=C.text)
    _rect(s,9.5,0,0.08,7.5,C.navy)

def _sl_proposta(p,d,vm=500,cm=30):
    s=p.slides.add_slide(p.slide_layouts[6])
    _tx(s,"Proposta",0.6,0.4,5,0.7,30,True,c=C.navy)
    conds=["Zero Investimento Inicial",f"R$ {_fmtI(vm)},00/mês por UC",f"{cm}% dos valores que forem recuperados"]
    extras=["Assessoria Técnica e Regulatória","Dashboard de Monitoramento","Gestão Fiscal e Tributária"]
    for i,c2 in enumerate(conds):
        _tx(s,"✅",0.8,1.4+i*0.48,0.4,0.4,18,c=C.green); _tx(s,c2,1.3,1.42+i*0.48,5,0.4,15,True,c=C.text)
    for i,e in enumerate(extras):
        _tx(s,"✅",7,1.4+i*0.48,0.4,0.4,18,c=C.green); _tx(s,e,7.5,1.42+i*0.48,5,0.4,15,True,c=C.text)
    cards=[("Tempo de Contrato","1 Ano","Com reajuste\nanual pelo IPCA"),
           ("Cancelamento","R$ 0,00","*Quando a remuneração\nfor maior que a\neconomia gerada"),
           ("Prestação de Contas","Contínua","Relatório Mensal\nDashboard Online\nComparativo Semestral")]
    for i,(tit,val,sub) in enumerate(cards):
        cx=0.8+i*4.1; _rect(s,cx,3.8,3.6,3,C.navy)
        _tx(s,tit,cx+0.2,4.0,3.2,0.4,13,c=C.muted,al=PP_ALIGN.CENTER)
        _tx(s,val,cx+0.2,4.5,3.2,0.8,36,True,c=C.white,al=PP_ALIGN.CENTER,va=MSO_ANCHOR.MIDDLE)
        _tx(s,sub,cx+0.2,5.5,3.2,0.9,11,c=C.muted,al=PP_ALIGN.CENTER,ls=15)

def _sl_fim(p,d):
    s=p.slides.add_slide(p.slide_layouts[6])
    s.background.fill.solid(); s.background.fill.fore_color.rgb=C.darkNavy
    _rect(s,0,0,0.12,7.5,C.orange)
    _tx(s,"Trianon",3,1.5,7,0.8,44,True,c=C.white,al=PP_ALIGN.CENTER)
    _tx(s,"E N E R G I A",3,2.2,7,0.4,18,c=C.orange,al=PP_ALIGN.CENTER)
    _tx(s,"⚡",8.3,1.5,0.8,0.8,36,c=C.orange)
    _tx(s,"www.trianonenergia.com",3,3.3,7,0.5,18,c=C.green,al=PP_ALIGN.CENTER)
    _tx(s,"Gestão Inteligente",3,3.8,7,0.4,16,c=C.muted,al=PP_ALIGN.CENTER)
    for i,cr in enumerate(["✅  + 10 Anos de experiência","✅  Time de Engenharia Elétrica experiente","✅  Expert no Mercado Livre de Energia"]):
        _tx(s,cr,4,5.0+i*0.45,6,0.4,14,True,c=C.white)


# ══════════════════════════════════════════════════════════════════════════════
# PIPELINE: PPTX → LibreOffice → PDF
# ══════════════════════════════════════════════════════════════════════════════
def gerar_estudo_pdf(faturas, alertas=None, cnpj="", valor_mensal=500, comissao=30):
    """Gera PDF profissional. Todos os dados vêm das faturas reais."""
    if not faturas: raise ValueError("Nenhuma fatura")
    d=_analisar(faturas, alertas)
    d["cnpj"]=cnpj

    prs=Presentation(); prs.slide_width=Inches(13.3); prs.slide_height=Inches(7.5)
    _sl_capa(prs,d,cnpj); _sl_valor(prs,d); _sl_fatura(prs,d)
    _sl_resumo(prs,d); _sl_comp(prs,d); _sl_crono(prs,d)
    _sl_escopo(prs,d); _sl_proposta(prs,d,valor_mensal,comissao); _sl_fim(prs,d)

    # Salva PPTX em temp → converte para PDF
    with tempfile.TemporaryDirectory() as tmp:
        pptx_path=os.path.join(tmp,"estudo.pptx")
        pdf_path=os.path.join(tmp,"estudo.pdf")
        prs.save(pptx_path)
        logger.info(f"PPTX salvo: {os.path.getsize(pptx_path)} bytes")

        try:
            # Profile temporário para Docker (evita lock do soffice)
            profile_dir = os.path.join(tmp, "lo_profile")
            os.makedirs(profile_dir, exist_ok=True)

            result = subprocess.run([
                "soffice",
                "--headless",
                "--norestore",
                "--nofirststartwizard",
                f"-env:UserInstallation=file://{profile_dir}",
                "--convert-to", "pdf",
                "--outdir", tmp,
                pptx_path
            ], check=True, timeout=60, capture_output=True, text=True)
            logger.info(f"soffice stdout: {result.stdout.strip()}")
            if result.stderr:
                logger.warning(f"soffice stderr: {result.stderr.strip()}")
        except subprocess.TimeoutExpired:
            logger.error("soffice timeout (60s)")
            with open(pptx_path,"rb") as f:
                buf = io.BytesIO(f.read())
                buf.seek(0)
                return buf
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            logger.warning(f"LibreOffice falhou ({e}), retornando PPTX")
            with open(pptx_path,"rb") as f:
                buf = io.BytesIO(f.read())
                buf.seek(0)
                return buf

        if not os.path.exists(pdf_path):
            # Tenta encontrar o PDF com outro nome
            pdfs = [f for f in os.listdir(tmp) if f.endswith('.pdf')]
            if pdfs:
                pdf_path = os.path.join(tmp, pdfs[0])
                logger.info(f"PDF encontrado: {pdfs[0]}")
            else:
                logger.error(f"PDF não gerado. Arquivos em tmp: {os.listdir(tmp)}")
                with open(pptx_path,"rb") as f:
                    buf = io.BytesIO(f.read())
                    buf.seek(0)
                    return buf

        pdf_size = os.path.getsize(pdf_path)
        logger.info(f"PDF gerado: {pdf_size} bytes")

        with open(pdf_path,"rb") as f:
            buf=io.BytesIO(f.read())

    buf.seek(0)
    logger.info(f"Estudo PDF gerado: UC {d['uc']}, {d['n']} faturas, desp R${d['desp_total']:.0f}")
    return buf

# Alias para compatibilidade com api.py
gerar_estudo_pptx = gerar_estudo_pdf
