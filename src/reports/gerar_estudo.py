"""
Gerador de Estudo Técnico — Template-based.
Usa o PPTX original do Cometais como template,
substitui apenas os campos dinâmicos.
Design 100% preservado.
"""
from __future__ import annotations
import io,os,copy,logging,subprocess,tempfile
from pptx import Presentation
from pptx.util import Inches,Pt,Emu
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN

logger=logging.getLogger(__name__)

def _f(v,d=2):
    try: return f"{float(v):,.{d}f}".replace(",","X").replace(".",",").replace("X",".")
    except: return "—"
def _fi(v): return _f(v,0)

def _set_text(shape, text, size=None, bold=None, color=None):
    """Substitui texto mantendo formatação original quando possível."""
    if not shape.has_text_frame: return
    tf = shape.text_frame
    if tf.paragraphs:
        p = tf.paragraphs[0]
        # Preserva formatação do primeiro run
        if p.runs:
            run = p.runs[0]
            run.text = text
            if size: run.font.size = Pt(size)
            if bold is not None: run.font.bold = bold
            if color: run.font.color.rgb = color
            # Remove runs extras
            for r in p.runs[1:]:
                r.text = ""
            # Remove parágrafos extras
            for pp in tf.paragraphs[1:]:
                for r in pp.runs:
                    r.text = ""
        else:
            p.text = text

def _set_multi(shape, lines):
    """Substitui múltiplas linhas mantendo formato do primeiro parágrafo."""
    if not shape.has_text_frame: return
    tf = shape.text_frame
    # Pega formato do primeiro parágrafo/run
    ref_font = None
    if tf.paragraphs and tf.paragraphs[0].runs:
        ref_font = tf.paragraphs[0].runs[0].font

    # Limpa tudo
    for p in tf.paragraphs:
        for r in p.runs:
            r.text = ""

    # Reescreve
    for i, line in enumerate(lines):
        if i == 0:
            p = tf.paragraphs[0]
        else:
            from pptx.oxml.ns import qn
            new_p = copy.deepcopy(tf.paragraphs[0]._p)
            tf._txBody.append(new_p)
            p = tf.paragraphs[-1]
        
        if p.runs:
            p.runs[0].text = line
            for r in p.runs[1:]: r.text = ""
        else:
            p.text = line

def _assets():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)),"assets")

# ══════════════════════════════════════════════════════════════════════════════
def _analisar(faturas,alertas=None):
    alertas=alertas or [];n=len(faturas);f0=faturas[0]
    totais=[float(f.get("total_a_pagar")or 0) for f in faturas]
    cm=sum(totais)/n;cp=sum(totais)
    dd=0.0
    for f in faturas:
        dc=float(f.get("demanda_contratada_fora_ponta_kw")or 0)
        dm=float(f.get("demanda_medida_fora_ponta_kw")or 0)
        td=float(f.get("tarifa_demanda")or 0)
        if dc>dm and td>0: dd+=(dc-dm)*td
    dr=0.0
    for f in faturas:
        for it in(f.get("itens_faturados")or[]):
            d=(it.get("descricao")or"").lower()
            if "exc" in d and("en r" in d or "r exc" in d): dr+=abs(float(it.get("valor")or 0))
        if not f.get("itens_faturados"):
            u=float(f.get("ufer_fora_ponta_kvarh")or 0)
            if u>0: dr+=u*0.349
    dm2=0.0
    for f in faturas:
        for it in(f.get("itens_faturados")or[]):
            d=(it.get("descricao")or"").lower()
            if "multa" in d or "juros" in d or "mora" in d: dm2+=abs(float(it.get("valor")or 0))
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
        pts.append({"t":"Demanda contratada atual.","d":f"Contratada: {_fi(dcm)} kW | Medida máx: {_fi(dmx)} kW | Utilização: {ut}%. Demanda não utilizada recorrente, podendo ser ajustada com sazonalidade."})
    if dr>0:
        pts.append({"t":"Energia reativa (UFER).","d":f"Total: R$ {_f(dr)}. Multa devido ao baixo fator de potência, corrigir com Banco de Capacitores e estudar o Filtro Capacitivo."})
    if gd>0:
        pts.append({"t":"Geração Distribuída ativa.","d":f"Créditos: R$ {_f(gd)}. Verificar potencial de expansão do sistema."})
    if cosip_m>300:
        pts.append({"t":f"COSIP elevada (R$ {_f(cosip_m)}/mês).","d":f"Total: R$ {_f(cosip_t)}. Contestar junto à Prefeitura."})
    if dm2>0:
        pts.append({"t":"Multas por atrasos.","d":f"Total: R$ {_f(dm2)}. Requer gestão ativa nas contas."})
    if el:
        pts.append({"t":"Elegível para Mercado Livre.","d":f"{f0.get('subgrupo','?')} com {_fi(dcm)} kW. Economia de 15-25% via comercializadora."})
    for al in alertas:
        t=al.get("titulo","")
        if t and len(pts)<6: pts.append({"t":t,"d":al.get("descricao","")})
    for i,pt in enumerate(pts): pt["n"]=str(i+1)
    return {"nome":f0.get("cliente_nome",""),"uc":f0.get("uc",""),"sub":f0.get("subgrupo",""),
        "mod":f0.get("modalidade",""),"n":n,"f0":f0,"mes":f0.get("mes_referencia",""),
        "total":float(f0.get("total_a_pagar")or 0),"cm":cm,"cp":cp,"ca":cm*12,
        "dcm":dcm,"dmx":dmx,"dmd":dmd,"ut":ut,
        "dd":dd,"dr":dr,"dm2":dm2,"dt":dt,"da":dt/n*12,
        "cosip_t":cosip_t,"cosip_m":cosip_m,"gd":gd,"tem_gd":gd>0,"el":el,
        "pts":pts[:6],"itens":f0.get("itens_faturados")or[]}


# ══════════════════════════════════════════════════════════════════════════════
def gerar_estudo_pdf(faturas,alertas=None,cnpj="",valor_mensal=500,comissao=30):
    if not faturas: raise ValueError("Nenhuma fatura")
    d=_analisar(faturas,alertas)

    # Abre o template
    tmpl_path = os.path.join(_assets(),"template.pptx")
    if not os.path.exists(tmpl_path):
        raise FileNotFoundError(f"Template não encontrado: {tmpl_path}")

    prs = Presentation(tmpl_path)
    slides = list(prs.slides)

    # ── SLIDE 1: CAPA ──────────────────────────────────────────────────────
    s1 = slides[0]
    shapes1 = list(s1.shapes)
    # shape[4] = nome, CNPJ, UC
    _set_multi(shapes1[4], [
        d["nome"][:65],
        f"CNPJ: {cnpj}" if cnpj else "",
        f"UC: {d['uc']}"
    ])

    # ── SLIDE 3: FATURA + PONTOS ────────────────────────────────────────
    s3 = slides[2]
    shapes3 = list(s3.shapes)
    
    # Pontos de atenção — substitui textos nos shapes existentes
    # shape[2]=título pt1, shape[3]=desc pt2, shape[4]=desc pt3, shape[5]=desc pt4, shape[6]=desc pt5
    pt_title_shapes = [2, None, None, None, None, None]  # Mapear por posição
    pt_desc_shapes = [3, 4, 5, 6, None, None]
    
    # Títulos dos pontos (shapes com texto curto de título)
    # shape[2] = "Demanda contratada atual."
    # shape[3] = "Consumo na Ponta..."
    # shape[4] = "Demanda não utilizada..."
    # shape[5] = "Energia reativa..."
    # shape[6] = "Multas por atrasos..."
    # shape[38] = "6 Descrição de Gradeza..."
    
    # Substituir títulos e descrições dos pontos
    pt_shapes = [
        (2, 3),   # Ponto 1: título shape[2], descrição shape[3]
        (None, 4), # Ponto 2: desc shape[4] (título junto)
        (None, 5), # Ponto 3: desc shape[5]
        (None, 6), # Ponto 4: desc shape[6]
        (None, None), # Ponto 5
        (None, 38),   # Ponto 6
    ]

    # Simples: substituir shapes de descrição dos pontos com novo texto
    desc_shape_indices = [3, 4, 5, 6]
    title_shape_indices = [2]
    
    # Substitui o título do primeiro ponto
    if d["pts"]:
        _set_text(shapes3[2], d["pts"][0]["t"])
    
    # Substitui as descrições dos pontos 2-5
    for i, si in enumerate(desc_shape_indices):
        if i+1 < len(d["pts"]):
            pt = d["pts"][i+1]
            _set_text(shapes3[si], f"{pt['t']} {pt['d']}")
        else:
            _set_text(shapes3[si], "")
    
    # Ponto 6 (shape[38])
    if len(d["pts"]) > 5:
        pt6 = d["pts"][5]
        _set_multi(shapes3[38], [f"6  {pt6['t']} {pt6['d']}"])
    elif len(shapes3) > 38:
        _set_text(shapes3[38], "")

    # ── SLIDE 5: RESUMO TÉCNICO (TABELA) ──────────────────────────────────
    s5 = slides[4]
    shapes5 = list(s5.shapes)
    
    # shape[0] = "CONSOLIDADO DOS ÚLTIMOS 12 MESES:" → atualizar período
    _set_text(shapes5[0], f"CONSOLIDADO DOS ÚLTIMOS {d['n']} MESES:")
    
    # shape[1] = TABLE 10x3 → atualizar valores
    tbl = shapes5[1].table
    tusd_pct=63.43; te_pct=36.57
    tusd_v=d['ca']*tusd_pct/100; te_v=d['ca']*te_pct/100
    
    # Linha 0: Custo do Fio
    tbl.cell(0,0).text = "Custo do Fio (TUSD — AME):"
    tbl.cell(0,1).text = f"R$ {_f(tusd_v)}"
    tbl.cell(0,2).text = f"{tusd_pct:.2f}%"
    # Linha 1: Custo da Energia
    tbl.cell(1,0).text = "Custo da Energia (TE — SAFIRA):"
    tbl.cell(1,1).text = f"R$ {_f(te_v)}"
    tbl.cell(1,2).text = f"{te_pct:.2f}%"
    # Linha 2: Total
    tbl.cell(2,0).text = ""
    tbl.cell(2,1).text = f"R$ {_f(d['ca'])}"
    tbl.cell(2,2).text = "100%"
    # Linha 3: vazia/separador
    tbl.cell(3,0).text = f"DESPERDÍCIO DOS ÚLTIMOS {d['n']} MESES:"
    tbl.cell(3,1).text = ""; tbl.cell(3,2).text = ""
    
    # Linhas de desperdício
    desp_items = []
    if d["dd"]>0:
        pct=round(d["dd"]/d["dt"]*100,2) if d["dt"]>0 else 0
        desp_items.append(("Demanda não utilizada:",f"R$ {_f(d['dd'])}",f"{pct:.2f}%"))
    if d["dr"]>0:
        pct=round(d["dr"]/d["dt"]*100,2) if d["dt"]>0 else 0
        desp_items.append(("Energia reativa:",f"R$ {_f(d['dr'])}",f"{pct:.2f}%"))
    if d["dm2"]>0:
        pct=round(d["dm2"]/d["dt"]*100,2) if d["dt"]>0 else 0
        desp_items.append(("Multas por atrasos:",f"R$ {_f(d['dm2'])}",f"{pct:.2f}%"))
    
    for i in range(6):
        row = 4 + i
        if row < len(tbl.rows):
            if i < len(desp_items):
                tbl.cell(row,0).text = desp_items[i][0]
                tbl.cell(row,1).text = desp_items[i][1]
                tbl.cell(row,2).text = desp_items[i][2]
            elif i == len(desp_items):
                # Total
                tbl.cell(row,0).text = ""
                tbl.cell(row,1).text = f"R$ {_f(d['da'])}"
                tbl.cell(row,2).text = "100%"
            else:
                tbl.cell(row,0).text = ""
                tbl.cell(row,1).text = ""
                tbl.cell(row,2).text = ""

    # ── SLIDE 6: COM GESTÃO vs SEM GESTÃO ──────────────────────────────────
    s6 = slides[5]
    shapes6 = list(s6.shapes)
    # shape[1] = "Desperdício de 1 ano -R$ 183.602,66..."
    _set_multi(shapes6[1], [
        "Desperdício\nde 1 ano",
        f"-R$ {_f(d['da'])}",
        "Somente com uma\nauditoria superficial"
    ])
    # shape[11] = "Economia de 1 ano +R$ 183.602,66..."
    _set_multi(shapes6[11], [
        "Economia\nde 1 ano",
        f"+R$ {_f(d['da'])}",
        "Sem incluir outros\nmeios de economia"
    ])

    # ── SLIDE 10: PROPOSTA ──────────────────────────────────────────────────
    s10 = slides[9]
    shapes10 = list(s10.shapes)
    # shape[5] = pricing text
    _set_multi(shapes10[5], [
        "Zero Investimento Inicial",
        f"R$ {_fi(valor_mensal)},00/mês por UC",
        f"{comissao}% dos valores que forem recuperados"
    ])

    # ── SALVAR E CONVERTER ──────────────────────────────────────────────────
    with tempfile.TemporaryDirectory() as tmp:
        px = os.path.join(tmp,"estudo.pptx")
        prs.save(px)
        logger.info(f"PPTX salvo: {os.path.getsize(px)} bytes")

        profile = os.path.join(tmp,"lo_profile")
        os.makedirs(profile, exist_ok=True)
        try:
            subprocess.run([
                "soffice","--headless","--norestore","--nofirststartwizard",
                f"-env:UserInstallation=file://{profile}",
                "--convert-to","pdf","--outdir",tmp,px
            ], check=True, timeout=60, capture_output=True)
            pdfs = [f for f in os.listdir(tmp) if f.endswith('.pdf')]
            if pdfs:
                with open(os.path.join(tmp,pdfs[0]),"rb") as f:
                    buf = io.BytesIO(f.read())
                buf.seek(0)
                logger.info(f"PDF gerado: {buf.getbuffer().nbytes} bytes")
                return buf
        except Exception as e:
            logger.warning(f"soffice falhou ({e}), retornando PPTX")
        
        with open(px,"rb") as f:
            buf = io.BytesIO(f.read())
        buf.seek(0)
        return buf

gerar_estudo_pptx = gerar_estudo_pdf
