"""
Gerador de Estudo Técnico — Template-based (find & replace).
Abre o PPTX original, faz find/replace nos textos, salva e converte.
Não mexe em formatação, layout ou imagens.
"""
from __future__ import annotations
import io,os,logging,subprocess,tempfile,copy
from pptx import Presentation
from pptx.util import Pt

logger=logging.getLogger(__name__)

def _f(v,d=2):
    try: return f"{float(v):,.{d}f}".replace(",","X").replace(".",",").replace("X",".")
    except: return "—"
def _fi(v): return _f(v,0)

def _assets():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)),"assets")

def _replace_in_shape(shape, old, new):
    """Substitui texto em um shape, funciona mesmo com texto fragmentado em múltiplos runs."""
    if not shape.has_text_frame: return False
    full = shape.text_frame.text
    if old not in full: return False
    found = False
    for para in shape.text_frame.paragraphs:
        # Tenta em run individual primeiro
        for run in para.runs:
            if old in run.text:
                run.text = run.text.replace(old, new)
                found = True
        if found: return True
        # Texto pode estar fragmentado entre runs — reconstrói
        if para.runs:
            combined = "".join(r.text for r in para.runs)
            if old in combined:
                para.runs[0].text = combined.replace(old, new)
                for r in para.runs[1:]: r.text = ""
                return True
    return False

def _replace_all(prs, old, new):
    """Find & replace em toda a apresentação."""
    for slide in prs.slides:
        for shape in slide.shapes:
            _replace_in_shape(shape, old, new)
            if shape.has_table:
                for row in shape.table.rows:
                    for cell in row.cells:
                        if old in cell.text:
                            for p in cell.text_frame.paragraphs:
                                for r in p.runs:
                                    if old in r.text:
                                        r.text = r.text.replace(old, new)

def _set_cell(tbl, row, col, text):
    """Define texto de uma célula da tabela preservando formatação."""
    cell = tbl.cell(row, col)
    for p in cell.text_frame.paragraphs:
        if p.runs:
            p.runs[0].text = str(text)
            for r in p.runs[1:]: r.text = ""
            return
    # Sem runs, define direto
    cell.text = str(text)

# ══════════════════════════════════════════════════════════════════════════════
def _analisar(faturas,alertas=None):
    alertas=alertas or[];n=len(faturas);f0=faturas[0]
    totais=[float(f.get("total_a_pagar")or 0)for f in faturas]
    cm=sum(totais)/n
    dd=0.0
    for f in faturas:
        dc=float(f.get("demanda_contratada_fora_ponta_kw")or 0)
        dm=float(f.get("demanda_medida_fora_ponta_kw")or 0)
        td=float(f.get("tarifa_demanda")or 0)
        if dc>dm and td>0:dd+=(dc-dm)*td
    dr=0.0
    for f in faturas:
        for it in(f.get("itens_faturados")or[]):
            d=(it.get("descricao")or"").lower()
            if "exc" in d and("en r" in d or"r exc" in d):dr+=abs(float(it.get("valor")or 0))
        if not f.get("itens_faturados"):
            u=float(f.get("ufer_fora_ponta_kvarh")or 0)
            if u>0:dr+=u*0.349
    dm2=0.0
    for f in faturas:
        for it in(f.get("itens_faturados")or[]):
            d=(it.get("descricao")or"").lower()
            if "multa" in d or"juros" in d or"mora" in d:dm2+=abs(float(it.get("valor")or 0))
    cosip_t=sum(float(f.get("cosip_valor")or 0)for f in faturas);cosip_m=cosip_t/n
    gd=0.0
    for f in faturas:
        for it in(f.get("itens_faturados")or[]):
            d=(it.get("descricao")or"").lower()
            if "credito" in d and"gera" in d:gd+=abs(float(it.get("valor")or 0))
    dcm=max(max(float(f.get("demanda_contratada_ponta_kw")or 0),float(f.get("demanda_contratada_fora_ponta_kw")or 0))for f in faturas)
    dms=[max(float(f.get("demanda_medida_ponta_kw")or 0),float(f.get("demanda_medida_fora_ponta_kw")or 0))for f in faturas]
    dmx=max(dms);dmd=sum(dms)/n
    ut=round(dmd/dcm*100)if dcm>0 else 0
    el=(f0.get("subgrupo")or"").startswith("A")or dcm>=300
    dt=dd+dr+dm2
    pts=[]
    if ut<85 and dcm>0:
        pts.append(f"Demanda contratada: {_fi(dcm)} kW, utilização {ut}%. Podendo ser ajustada com sazonalidade.")
    if dr>0:
        pts.append(f"Energia reativa: R$ {_f(dr)}. Corrigir fator de potência com Banco de Capacitores.")
    if gd>0:
        pts.append(f"Geração Distribuída ativa. Créditos: R$ {_f(gd)}. Verificar expansão.")
    if cosip_m>300:
        pts.append(f"COSIP elevada: R$ {_f(cosip_m)}/mês. Contestar junto à Prefeitura.")
    if dm2>0:
        pts.append(f"Multas por atrasos: R$ {_f(dm2)}. Requer gestão ativa nas contas.")
    if el:
        pts.append(f"Elegível Mercado Livre ({f0.get('subgrupo','?')}, {_fi(dcm)} kW). Economia 15-25%.")
    for al in alertas:
        t=al.get("titulo","")
        if t and len(pts)<6:pts.append(f"{t}. {al.get('descricao','')}")
    while len(pts)<6:pts.append("")
    return {"nome":f0.get("cliente_nome",""),"uc":f0.get("uc",""),
        "n":n,"f0":f0,"cm":cm,"ca":cm*12,
        "dcm":dcm,"dmx":dmx,"dmd":dmd,"ut":ut,
        "dd":dd,"dr":dr,"dm2":dm2,"dt":dt,"da":dt/n*12,
        "cosip_t":cosip_t,"cosip_m":cosip_m,"gd":gd,
        "pts":pts}


def gerar_estudo_pdf(faturas,alertas=None,cnpj="",valor_mensal=500,comissao=30):
    if not faturas:raise ValueError("Nenhuma fatura")
    d=_analisar(faturas,alertas)

    tmpl=os.path.join(_assets(),"template.pptx")
    if not os.path.exists(tmpl):raise FileNotFoundError(f"Template: {tmpl}")
    prs=Presentation(tmpl)

    # ═══════════════════════════════════════════════════════════════════════
    # FIND & REPLACE — só textos, sem mexer em formatação
    # ═══════════════════════════════════════════════════════════════════════

    # SLIDE 1: Capa — nome, CNPJ, UC
    _replace_all(prs,"COMETAIS INDUSTRIA E COMERCIO DE METAIS LTDA",d["nome"][:60])
    _replace_all(prs,"02.896.727/0003-96",cnpj or"")
    _replace_all(prs,"0502485-4",d["uc"])

    # SLIDE 3: Pontos de atenção (textos descritivos)
    _replace_all(prs,"Demanda contratada atual.",d["pts"][0][:40] if d["pts"][0] else "")
    
    _replace_all(prs,"Consumo na Ponta (Entre as 20h às 23h) muito elevado, podendo ser feito um estudo de BESS",
                 d["pts"][1][:90] if d["pts"][1] else "")
    _replace_all(prs,"(Armazenamento de energia em baterias).","")
    
    _replace_all(prs,"Demanda não utilizada recorrente, muito desperdício todos os meses. Podendo ser ajustada com sazonalidade.",
                 d["pts"][2][:100] if d["pts"][2] else "")
    
    _replace_all(prs,"Energia reativa, multa devido ao baixo fator  de potência, corrigir com Banco de Capacitores e estudar o Filtro Capacitivo para protege-lo e os demais equipamentos da indústria.",
                 d["pts"][3][:120] if d["pts"][3] else "")
    
    _replace_all(prs,"Multas por atrasos de pagamentos constantes, requer uma gestão ativa nas contas.",
                 d["pts"][4][:80] if d["pts"][4] else "")
    
    _replace_all(prs,"Descrição de Gradeza são os registros feitos pelo medidor da UC, aqui acontecem as auditorias retroativas.",
                 d["pts"][5][:100] if d["pts"][5] else "Descrição de Grandeza: registros do medidor para auditorias retroativas.")

    # SLIDE 5: Resumo técnico — tabela
    _replace_all(prs,"CONSOLIDADO DOS ÚLTIMOS 12 MESES:",f"CONSOLIDADO DOS ÚLTIMOS {d['n']} MESES:")

    s5=list(prs.slides)[4]
    for sh in s5.shapes:
        if sh.has_table:
            tbl=sh.table
            tusd_pct=63.43;te_pct=36.57
            tusd_v=d['ca']*tusd_pct/100;te_v=d['ca']*te_pct/100
            _set_cell(tbl,0,1,f"R$ {_f(tusd_v)}")
            _set_cell(tbl,0,2,f"{tusd_pct:.2f}%")
            _set_cell(tbl,1,1,f"R$ {_f(te_v)}")
            _set_cell(tbl,1,2,f"{te_pct:.2f}%")
            _set_cell(tbl,2,1,f"R$ {_f(d['ca'])}")
            # Desperdício
            _set_cell(tbl,3,0,f"DESPERDÍCIO DOS ÚLTIMOS {d['n']} MESES:")
            desp=[]
            if d["dd"]>0:
                p=round(d["dd"]/d["dt"]*100,2)if d["dt"]>0 else 0
                desp.append(("Demanda não utilizada:",f"R$ {_f(d['dd'])}",f"{p:.2f}%"))
            if d["dr"]>0:
                p=round(d["dr"]/d["dt"]*100,2)if d["dt"]>0 else 0
                desp.append(("Energia reativa:",f"R$ {_f(d['dr'])}",f"{p:.2f}%"))
            if d["dm2"]>0:
                p=round(d["dm2"]/d["dt"]*100,2)if d["dt"]>0 else 0
                desp.append(("Multas por atrasos:",f"R$ {_f(d['dm2'])}",f"{p:.2f}%"))
            for i in range(5):
                row=4+i
                if row>=len(tbl.rows):break
                if i<len(desp):
                    _set_cell(tbl,row,0,desp[i][0])
                    _set_cell(tbl,row,1,desp[i][1])
                    _set_cell(tbl,row,2,desp[i][2])
                elif i==len(desp):
                    _set_cell(tbl,row,0,"")
                    _set_cell(tbl,row,1,f"R$ {_f(d['da'])}")
                    _set_cell(tbl,row,2,"100%")
                else:
                    _set_cell(tbl,row,0,"");_set_cell(tbl,row,1,"");_set_cell(tbl,row,2,"")
            break

    # SLIDE 6: Com/Sem Gestão — valores
    _replace_all(prs,"-R$ 183.602,66",f"-R$ {_f(d['da'])}")
    _replace_all(prs,"+R$ 183.602,66",f"+R$ {_f(d['da'])}")

    # SLIDE 10: Proposta — valores
    _replace_all(prs,"R$ 1.500,00 nos três primeiros meses",f"R$ {_fi(valor_mensal)},00/mês por UC")
    _replace_all(prs,"R$ 2.500,00 no quarto mês em diante",f"{comissao}% dos valores recuperados")

    # ═══════════════════════════════════════════════════════════════════════
    # SALVAR E CONVERTER
    # ═══════════════════════════════════════════════════════════════════════
    with tempfile.TemporaryDirectory() as tmp:
        px=os.path.join(tmp,"estudo.pptx")
        prs.save(px)
        logger.info(f"PPTX template: {os.path.getsize(px)} bytes")
        profile=os.path.join(tmp,"lo_profile");os.makedirs(profile,exist_ok=True)
        try:
            subprocess.run(["soffice","--headless","--norestore","--nofirststartwizard",
                f"-env:UserInstallation=file://{profile}","--convert-to","pdf","--outdir",tmp,px],
                check=True,timeout=60,capture_output=True)
            pdfs=[f for f in os.listdir(tmp)if f.endswith('.pdf')]
            if pdfs:
                with open(os.path.join(tmp,pdfs[0]),"rb")as f:buf=io.BytesIO(f.read())
                buf.seek(0);logger.info(f"PDF: {buf.getbuffer().nbytes} bytes");return buf
        except Exception as e:
            logger.warning(f"soffice falhou ({e}), retornando PPTX")
        with open(px,"rb")as f:buf=io.BytesIO(f.read())
        buf.seek(0);return buf

gerar_estudo_pptx=gerar_estudo_pdf
