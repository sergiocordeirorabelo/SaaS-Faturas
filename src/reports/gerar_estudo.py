"""
Gerador de Estudo Técnico — Template-based 100% dinâmico.
Usa PPTX Cometais como template visual, substitui TODOS os dados pelo cliente.
"""
from __future__ import annotations
import io,os,logging,subprocess,tempfile
from datetime import datetime
from pptx import Presentation
from pptx.util import Pt,Inches

logger=logging.getLogger(__name__)

def _f(v,d=2):
    try: return f"{float(v):,.{d}f}".replace(",","X").replace(".",",").replace("X",".")
    except: return "—"
def _fi(v): return _f(v,0)

def _assets():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)),"assets")

def _replace_in_shape(shape, old, new):
    if not shape.has_text_frame: return False
    full = shape.text_frame.text
    if old not in full: return False
    for para in shape.text_frame.paragraphs:
        for run in para.runs:
            if old in run.text:
                run.text = run.text.replace(old, new)
                return True
        if para.runs:
            combined = "".join(r.text for r in para.runs)
            if old in combined:
                para.runs[0].text = combined.replace(old, new)
                for r in para.runs[1:]: r.text = ""
                return True
    return False

def _replace_all(prs, old, new):
    for slide in prs.slides:
        for shape in slide.shapes:
            _replace_in_shape(shape, old, new)
            if hasattr(shape,'has_table') and shape.has_table:
                for row in shape.table.rows:
                    for cell in row.cells:
                        if old in cell.text:
                            for p in cell.text_frame.paragraphs:
                                for r in p.runs:
                                    if old in r.text:
                                        r.text = r.text.replace(old, new)

def _set_cell(tbl, row, col, text):
    cell = tbl.cell(row, col)
    for p in cell.text_frame.paragraphs:
        if p.runs:
            p.runs[0].text = str(text)
            for r in p.runs[1:]: r.text = ""
            return
    cell.text = str(text)

def _delete_slide(prs, index):
    """Remove um slide da apresentação."""
    rId = prs.slides._sldIdLst[index].get('{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id')
    prs.part.drop_rel(rId)
    del prs.slides._sldIdLst[index]

# ══════════════════════════════════════════════════════════════════════════════
def _analisar(faturas, alertas=None):
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
    # Pontos
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
        if t and len(pts)<6:pts.append(f"{t}. {al.get('descricao','')[:80]}")
    while len(pts)<6:pts.append("Auditar registros do medidor para identificar cobranças indevidas retroativas.")
    # Cronograma dinâmico baseado na análise
    acoes=[]
    acoes.append(("Auditoria\nRetroativa\ndos últimos\n120 meses","Buscar pagamentos\nindevidos e pedir\nrestituição"))
    if ut<85 and dcm>0:
        acoes.append(("Ajustar a\ndemanda\ncontratada\nociosa",f"Reduzir de {_fi(dcm)} kW\npara ~{_fi(dmd)} kW\n(economia imediata)"))
    if el:
        acoes.append(("Migração\npara o\nMercado Livre",f"Economia de 15-25%\nna tarifa de energia\nvia comercializadora"))
    if dr>0:
        acoes.append(("Corrigir\nfator de\npotência","Instalar/ajustar\nBanco de Capacitores\nURGENTE"))
    acoes.append(("Laudo de\nICMS para\ncréditos\nda energia","Fazer laudo para\nseparar o que é\nprodução"))
    if gd>0:
        acoes.append(("Otimizar\nGeração\nDistribuída","Gestão de créditos\ne estudo de\nexpansão"))
    if cosip_m>300:
        acoes.append(("Contestar\nCOSIP junto\nà Prefeitura",f"Valor médio\nR$ {_fi(cosip_m)}/mês\nacima do padrão"))
    acoes.append(("Relatório\nMensal de\nResultado","Prestação de contas\ncom economia\ne metas"))
    while len(acoes)<8:
        acoes.append(("Vistoria\nTécnica da\nInstalação","Inspeção da rede\nelétrica para\nmelhorias"))
    return {"nome":f0.get("cliente_nome",""),"uc":f0.get("uc",""),
        "sub":f0.get("subgrupo",""),"mod":f0.get("modalidade",""),
        "n":n,"f0":f0,"mes":f0.get("mes_referencia",""),
        "cm":cm,"ca":cm*12,"dt":dt,"da":dt/n*12,
        "dcm":dcm,"dmx":dmx,"dmd":dmd,"ut":ut,
        "dd":dd,"dr":dr,"dm2":dm2,
        "cosip_t":cosip_t,"cosip_m":cosip_m,"gd":gd,"el":el,
        "pts":pts,"acoes":acoes[:8]}


# ══════════════════════════════════════════════════════════════════════════════
def gerar_estudo_pdf(faturas,alertas=None,cnpj="",valor_mensal=500,comissao=30,pdf_screenshot_bytes=None):
    if not faturas:raise ValueError("Nenhuma fatura")
    d=_analisar(faturas,alertas)
    meses_pt = {1:"Janeiro",2:"Fevereiro",3:"Março",4:"Abril",5:"Maio",6:"Junho",
               7:"Julho",8:"Agosto",9:"Setembro",10:"Outubro",11:"Novembro",12:"Dezembro"}
    mes_atual = meses_pt.get(datetime.now().month, "")

    tmpl=os.path.join(_assets(),"template.pptx")
    if not os.path.exists(tmpl):raise FileNotFoundError(f"Template: {tmpl}")
    prs=Presentation(tmpl)

    # ═══════════════════════════════════════════════════════════════════════
    # SLIDE 1: CAPA — nome, CNPJ, UC
    # ═══════════════════════════════════════════════════════════════════════
    _replace_all(prs,"COMETAIS INDUSTRIA E COMERCIO DE METAIS LTDA",d["nome"][:60])
    _replace_all(prs,"02.896.727/0003-96",cnpj or"")
    _replace_all(prs,"0502485-4",d["uc"])

    # ═══════════════════════════════════════════════════════════════════════
    # SLIDE 3: FATURA — SEMPRE remove imagem do Cometais, coloca do cliente
    # ═══════════════════════════════════════════════════════════════════════
    try:
        s3 = list(prs.slides)[2]
        # Encontra imagem grande da fatura (Picture > 3")
        for shape in list(s3.shapes):
            if shape.shape_type == 13 and shape.width > Inches(3):
                left,top,width,height = shape.left,shape.top,shape.width,shape.height
                shape._element.getparent().remove(shape._element)
                if pdf_screenshot_bytes:
                    s3.shapes.add_picture(io.BytesIO(pdf_screenshot_bytes),left,top,width,height)
                    logger.info("[Estudo] Screenshot da fatura do cliente embutido")
                else:
                    logger.info("[Estudo] Imagem Cometais removida (sem screenshot)")
                break
    except Exception as e:
        logger.warning(f"[Estudo] Erro slide 3: {e}")

    # Pontos de atenção (6 posições no template)
    _replace_all(prs,"Demanda contratada atual.",d["pts"][0][:50])
    
    _replace_all(prs,"Consumo na Ponta (Entre as 20h às 23h) muito elevado, podendo ser feito um estudo de BESS",
                 d["pts"][1][:90])
    _replace_all(prs,"(Armazenamento de energia em baterias).","")
    
    _replace_all(prs,"Demanda não utilizada recorrente, muito desperdício todos os meses. Podendo ser ajustada com sazonalidade.",
                 d["pts"][2][:100])
    
    _replace_all(prs,"Energia reativa, multa devido ao baixo fator  de potência, corrigir com Banco de Capacitores e estudar o Filtro Capacitivo para protege-lo e os demais equipamentos da indústria.",
                 d["pts"][3][:130])
    
    _replace_all(prs,"Multas por atrasos de pagamentos constantes, requer uma gestão ativa nas contas.",
                 d["pts"][4][:80])
    
    _replace_all(prs,"Descrição de Gradeza são os registros feitos pelo medidor da UC, aqui acontecem as auditorias retroativas.",
                 d["pts"][5][:100])

    # ═══════════════════════════════════════════════════════════════════════
    # SLIDE 4: FATURA COMERCIALIZADORA — remover (específico do Cometais)
    # ═══════════════════════════════════════════════════════════════════════
    try:
        _delete_slide(prs, 3)  # Index 3 = slide 4
        logger.info("[Estudo] Slide 4 (Comercializadora) removido")
    except Exception as e:
        logger.warning(f"[Estudo] Erro ao remover slide 4: {e}")

    # Após remoção do slide 4, os índices mudam:
    # Slide 5 (resumo) → index 3
    # Slide 6 (com/sem) → index 4
    # Slide 7 (crono) → index 5
    # etc.

    # ═══════════════════════════════════════════════════════════════════════
    # SLIDE 5→4: RESUMO TÉCNICO — tabela dinâmica
    # ═══════════════════════════════════════════════════════════════════════
    _replace_all(prs,"CONSOLIDADO DOS ÚLTIMOS 12 MESES:",f"CONSOLIDADO DOS ÚLTIMOS {d['n']} MESES:")

    for sl in prs.slides:
        for sh in sl.shapes:
            if hasattr(sh,'has_table') and sh.has_table and len(sh.table.rows)>=8:
                tbl=sh.table
                tusd_pct=63.43;te_pct=36.57
                tusd_v=d['ca']*tusd_pct/100;te_v=d['ca']*te_pct/100
                _set_cell(tbl,0,0,"Custo do Fio (TUSD — AME):")
                _set_cell(tbl,0,1,f"R$ {_f(tusd_v)}")
                _set_cell(tbl,0,2,f"{tusd_pct:.2f}%")
                _set_cell(tbl,1,0,"Custo da Energia (TE — SAFIRA):")
                _set_cell(tbl,1,1,f"R$ {_f(te_v)}")
                _set_cell(tbl,1,2,f"{te_pct:.2f}%")
                _set_cell(tbl,2,0,"")
                _set_cell(tbl,2,1,f"R$ {_f(d['ca'])}")
                _set_cell(tbl,2,2,"100%")
                _set_cell(tbl,3,0,f"DESPERDÍCIO DOS ÚLTIMOS {d['n']} MESES:")
                _set_cell(tbl,3,1,"");_set_cell(tbl,3,2,"")
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
                for i in range(6):
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

    # ═══════════════════════════════════════════════════════════════════════
    # SLIDE 6→5: COM GESTÃO vs SEM GESTÃO — valores dinâmicos
    # ═══════════════════════════════════════════════════════════════════════
    _replace_all(prs,"-R$ 183.602,66",f"-R$ {_f(d['da'])}")
    _replace_all(prs,"+R$ 183.602,66",f"+R$ {_f(d['da'])}")

    # ═══════════════════════════════════════════════════════════════════════
    # SLIDE 7→6: CRONOGRAMA — ações dinâmicas do cliente
    # ═══════════════════════════════════════════════════════════════════════
    # Título do mês
    _replace_all(prs,"Cronograma de Ações (Abril)",f"Cronograma de Ações ({mes_atual})")

    # Ações do Cometais → ações dinâmicas (find & replace individual)
    _replace_all(prs,"Buscar pagamentos indevidos e pedir devolução em dobro",
                 d["acoes"][0][1] if len(d["acoes"])>0 else "")
    _replace_all(prs,"Portabilidade  para Âmbar Energia no Mercado Livre",
                 d["acoes"][2][0] if len(d["acoes"])>2 else "Migração\npara o\nMercado Livre")
    _replace_all(prs,"Atual proprietária da AME. Reduzir preço",
                 d["acoes"][2][1].split("\n")[0] if len(d["acoes"])>2 else "")
    _replace_all(prs,"e estreitar o relacionamento",
                 "\n".join(d["acoes"][2][1].split("\n")[1:]) if len(d["acoes"])>2 else "")
    _replace_all(prs,"Estudar  o consumo no","Ajustar a\ndemanda" if d["ut"]<85 else "Estudar\no consumo no")
    _replace_all(prs,"horário de ponta","contratada\nociosa" if d["ut"]<85 else "horário de\nponta")
    _replace_all(prs,"Instalar analisador, levantar a carga e fazer um estudo do BESS",
                 d["acoes"][1][1] if len(d["acoes"])>1 else "")
    _replace_all(prs,"Ajustar Banco de Capacitores existentes",
                 d["acoes"][3][0] if len(d["acoes"])>3 else "")
    _replace_all(prs,"Corrigir horário indutivo dos bancos URGENTE",
                 d["acoes"][3][1] if len(d["acoes"])>3 else "")
    _replace_all(prs,"Fazer laudo para separar o que é produção",
                 d["acoes"][4][1] if len(d["acoes"])>4 else "")
    _replace_all(prs,"Instalar Filtro Capacitivo","Contestar\nCOSIP" if d["cosip_m"]>300 else "Relatório\nMensal de")
    _replace_all(prs,"e corrigir Banco de Capacitores","junto à\nPrefeitura" if d["cosip_m"]>300 else "Resultado")
    _replace_all(prs,"Para proteção dos Bancos de","Valor médio" if d["cosip_m"]>300 else "Prestação de contas")
    _replace_all(prs,"Capacitores e motores",f"R$ {_fi(d['cosip_m'])}/mês" if d["cosip_m"]>300 else "com economia e metas")
    _replace_all(prs,"Se não houver aumento do consumo, ajustar",
                 d["acoes"][-1][1] if d["acoes"] else "")
    _replace_all(prs,"Ajustar a demanda contratada ociosa",
                 d["acoes"][-1][0] if d["acoes"] else "")

    # ═══════════════════════════════════════════════════════════════════════
    # SLIDE 9→8: SISTEMA DE MONITORAMENTO — nome do cliente
    # ═══════════════════════════════════════════════════════════════════════
    _replace_all(prs,"João Gomes",d["nome"][:25])

    # ═══════════════════════════════════════════════════════════════════════
    # SLIDE 10→9: PROPOSTA — valores dinâmicos
    # ═══════════════════════════════════════════════════════════════════════
    _replace_all(prs,"R$ 1.500,00 nos três primeiros meses",f"R$ {_fi(valor_mensal)},00/mês por UC")
    _replace_all(prs,"R$ 2.500,00 no quarto mês em diante",f"{comissao}% dos valores recuperados")

    # ═══════════════════════════════════════════════════════════════════════
    # SALVAR E CONVERTER
    # ═══════════════════════════════════════════════════════════════════════
    with tempfile.TemporaryDirectory() as tmp:
        px=os.path.join(tmp,"estudo.pptx")
        prs.save(px)
        sz=os.path.getsize(px)
        logger.info(f"[Estudo] PPTX: {sz} bytes, {len(list(prs.slides))} slides")
        profile=os.path.join(tmp,"lo_profile");os.makedirs(profile,exist_ok=True)
        try:
            subprocess.run(["soffice","--headless","--norestore","--nofirststartwizard",
                f"-env:UserInstallation=file://{profile}","--convert-to","pdf","--outdir",tmp,px],
                check=True,timeout=60,capture_output=True)
            pdfs=[f for f in os.listdir(tmp)if f.endswith('.pdf')]
            if pdfs:
                with open(os.path.join(tmp,pdfs[0]),"rb")as f:buf=io.BytesIO(f.read())
                buf.seek(0)
                logger.info(f"[Estudo] PDF: {buf.getbuffer().nbytes} bytes, UC {d['uc']}")
                return buf
        except Exception as e:
            logger.warning(f"[Estudo] soffice falhou ({e}), retornando PPTX")
        with open(px,"rb")as f:buf=io.BytesIO(f.read())
        buf.seek(0);return buf

gerar_estudo_pptx=gerar_estudo_pdf
