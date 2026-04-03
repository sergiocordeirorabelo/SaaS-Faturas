"""
Parser de Faturas — Amazonas Energia
Extrai todos os campos relevantes do PDF detalhado e retorna um dict estruturado.
Layout: FaturaNormal V.29 (2026)
"""

from __future__ import annotations

import re
import logging
from pathlib import Path
from typing import Optional

import pdfplumber

logger = logging.getLogger(__name__)


def _re(pattern: str, text: str, flags: int = 0) -> Optional[str]:
    m = re.search(pattern, text, flags)
    return m.group(1).strip() if m else None


def _float(val: Optional[str]) -> Optional[float]:
    if val is None:
        return None
    try:
        cleaned = val.strip().replace(" ", "")
        negative = cleaned.startswith("-")
        cleaned = cleaned.lstrip("-")
        if "," in cleaned:
            # Formato BR: 25.621,71 → 25621.71
            cleaned = cleaned.replace(".", "").replace(",", ".")
        else:
            # Ponto como separador de milhar: 32.200 → 32200
            cleaned = cleaned.replace(".", "")
        return -float(cleaned) if negative else float(cleaned)
    except (ValueError, AttributeError):
        return None


def _parse_itens(text: str) -> list[dict]:
    """Extrai itens da secao 'Descricao da Conta'."""
    itens = []

    # Padrao: "Consumo Ponta 2.564 kWh a 1,730090 1,730090 4.435,95"
    for m in re.finditer(
        r'(Consumo Ponta|Consumo F/Ponta|Demanda|En R Exc F/Ponta)\s+'
        r'([\d.]+)\s+(kWh|kW|kVAr)\s+a\s+'
        r'([\d,]+)\s+([\d,]+)\s+([\d.,]+)',
        text
    ):
        # Remove separador de milhar da quantidade
        qtd_str = m.group(2).replace(".", "")
        qtd   = float(qtd_str) if qtd_str else None
        tarifa = _float(m.group(5))
        valor  = _float(m.group(6))
        itens.append({
            "descricao": m.group(1).strip(),
            "quantidade": qtd,
            "unidade":    m.group(3),
            "tarifa":     tarifa,
            "valor":      valor,
        })

    # COSIP
    m = re.search(r'Contribuição de Iluminação Pública \(COSIP\)\s+([\d.,]+)', text)
    if m:
        itens.append({
            "descricao": "COSIP",
            "quantidade": None, "unidade": None, "tarifa": None,
            "valor": _float(m.group(1)),
        })

    # Credito Geracao
    m = re.search(r'(Credito De Geracao F/Ponta)\s+(-[\d.,]+)', text)
    if m:
        itens.append({
            "descricao": m.group(1),
            "quantidade": None, "unidade": None, "tarifa": None,
            "valor": _float(m.group(2)),
        })

    return itens


def _parse_leituras(text: str) -> list[dict]:
    """Tabela de grandezas: leit_atual | leit_anterior | constante | registrado."""
    result = []
    pattern = re.compile(
        r'(En Ativa Pta|En Ativa F-Pta|Dem Acum Pta|Dem Acum F-Pta|'
        r'Ufer Pta|Ufer F-Pta|Dmcr Acum Pta|Dmcr Acum F-Pta|En Reversa F-Pt)\s+'
        r'([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+(\d+)'
    )
    for m in pattern.finditer(text):
        result.append({
            "grandeza":         m.group(1).strip(),
            "leitura_atual":    float(m.group(2)),
            "leitura_anterior": float(m.group(3)),
            "constante":        float(m.group(4)),
            "registrado":       float(m.group(5)),
        })
    return result


def _grandeza(leituras: list[dict], nome: str) -> Optional[float]:
    row = next((l for l in leituras if nome in l["grandeza"]), None)
    return row["registrado"] if row else None


def parse_pdf(pdf_path: str | Path) -> dict:
    """Ponto de entrada: recebe caminho do PDF, retorna dict estruturado."""
    path = Path(pdf_path)
    with pdfplumber.open(str(path)) as pdf:
        text = "\n".join(page.extract_text() or "" for page in pdf.pages)
    return parse_text(text)


def parse_text(text: str) -> dict:
    """Processa texto extraido e retorna o dict estruturado."""

    # ── Identificacao ─────────────────────────────────────────────────────
    # Linha: "...MANAUS - AM 0087346-2 26/04/2026 03/2026"
    id_line = re.search(r'(\d{7,8}-\d)\s+(\d{2}/\d{2}/\d{4})\s+(\d{2}/\d{4})', text)
    uc         = id_line.group(1) if id_line else ""
    vencimento = id_line.group(2) if id_line else ""
    mes_ref    = id_line.group(3) if id_line else ""

    nota_fiscal  = _re(r'Nota Fiscal\s*N[o\xba]\s*(\d+)', text)
    data_emissao = _re(r'Data de Emiss[aã]o:\s*(\d{2}/\d{2}/\d{4})', text)
    cliente      = _re(r'^(SERVICO DE APOIO .+?)$', text, re.MULTILINE) or ""

    # ── Dados da UC ───────────────────────────────────────────────────────
    grupo      = _re(r'GRUPO\s+([AB])\b', text) or ""
    subgrupo   = _re(r'GRUPO\s+[AB]\s+(A\d+|B\d+)\b', text) or ""
    classe     = _re(r'\b(COMERCIAL|INDUSTRIAL|RESIDENCIAL|RURAL)\b', text) or ""
    modalidade = _re(r'(HOROSAZONAL VERDE|HOROSAZONAL AZUL|CONVENCIONAL|BRANCA)', text, re.IGNORECASE) or ""
    medidor    = _re(r'NORMAL\s+(\d{7,9})', text) or _re(r'(\d{7,9})\s+NORMAL', text)
    tensao_v   = _float(_re(r'Tensao Contratada\s*[-\u2013]\s*([\d.]+)\s*V', text))

    # ── Datas de leitura ─────────────────────────────────────────────────
    all_dates = re.findall(r'\b(\d{2}/\d{2}/\d{4})\b', text)
    leit_ant  = all_dates[1] if len(all_dates) > 1 else None
    leit_atual = all_dates[2] if len(all_dates) > 2 else None
    dias = None
    m = re.search(r'\b(\d{2})\s*\n.*?Emissão', text, re.DOTALL)
    if not m:
        m = re.search(r'Dias de consumo\s*\n?\s*(\d+)', text)
    if m:
        try:
            dias = int(m.group(1))
            if dias > 60:
                dias = None
        except (ValueError, IndexError):
            dias = None

    # ── Itens faturados ───────────────────────────────────────────────────
    itens = _parse_itens(text)

    cons_pta = next((i["quantidade"] for i in itens if i["descricao"] == "Consumo Ponta"), None)
    cons_fp  = sum(i["quantidade"] or 0 for i in itens
                   if "Consumo F/Ponta" in i["descricao"] and (i["valor"] or 0) > 0)
    cosip    = next((i["valor"] for i in itens if "COSIP" in i["descricao"]), None)
    cred_ger = next((i["valor"] for i in itens if "Credito" in i["descricao"]), None)

    tar_pta = next((i["tarifa"] for i in itens if i["descricao"] == "Consumo Ponta"), None)
    tar_fp  = next((i["tarifa"] for i in itens if "Consumo F/Ponta" in i["descricao"]
                    and (i["valor"] or 0) > 0), None)
    tar_dem = next((i["tarifa"] for i in itens if "Demanda" in i["descricao"]), None)

    # ── Total a pagar ─────────────────────────────────────────────────────
    total = _float(_re(r'Total a pagar\s+R\$\s+([\d.,]+)', text))

    # ── Demanda contratada ────────────────────────────────────────────────
    dem_ctda_pta = _float(_re(r'D\.\s*Ctda\s*Pta:\s*([\d.]+)', text))
    dem_ctda_fp  = _float(_re(r'D\.\s*Ctda\s*F\.?Pta:\s*([\d.]+)', text))

    # ── Tabela de leituras ────────────────────────────────────────────────
    leituras     = _parse_leituras(text)
    dem_med_pta  = _grandeza(leituras, "Dem Acum Pta")
    dem_med_fp   = _grandeza(leituras, "Dem Acum F-Pta")
    dmcr_pta     = _grandeza(leituras, "Dmcr Acum Pta")
    dmcr_fp      = _grandeza(leituras, "Dmcr Acum F-Pta")
    ufer_pta     = _grandeza(leituras, "Ufer Pta")
    ufer_fp      = _grandeza(leituras, "Ufer F-Pta")
    en_reversa   = _grandeza(leituras, "En Reversa")

    # ── Media e historico ─────────────────────────────────────────────────
    media_12 = _float(_re(r'M[eé]dia1?\s*2\s*meses:\s*([\d.,]+)\s*kWh', text))

    hist_m = re.search(
        r'Hist[oó]rico de Medi[cç][aã]o \(kWh\)[^\n]*\n\s*'
        r'((?:\d+\s+){5,}\d+)',
        text
    )
    historico = []
    if hist_m:
        historico = [float(x) for x in hist_m.group(1).split()
                     if x.isdigit() and int(x) > 1000]

    # ── Tributos ──────────────────────────────────────────────────────────
    pis_al    = _float(_re(r'PIS\s+([\d.,]+)\s+[\d.,]+', text))
    cofins_al = _float(_re(r'Cofins\s+([\d.,]+)\s+[\d.,]+', text))
    icms_st   = bool(re.search(r'Substitui[cç][aã]o Tribut[aá]ria', text, re.IGNORECASE))

    # ── Bandeira ──────────────────────────────────────────────────────────
    band_m = re.search(r'\d{2}/\d{4}\s+(Verde|Amarela|Vermelha \d+)\s+([\d.,]+)', text)
    bandeira     = band_m.group(1) if band_m else None
    bandeira_val = _float(band_m.group(2)) if band_m else None

    return {
        "uc":               uc,
        "mes_referencia":   mes_ref,
        "vencimento":       vencimento,
        "cliente_nome":     cliente.strip(),
        "nota_fiscal":      nota_fiscal,
        "data_emissao":     data_emissao,
        "grupo":            grupo,
        "subgrupo":         subgrupo,
        "classe":           classe,
        "modalidade":       modalidade,
        "numero_medidor":   medidor,
        "tensao_contratada_v": tensao_v,
        "data_leitura_anterior": leit_ant,
        "data_leitura_atual":    leit_atual,
        "dias_consumo":          dias,
        "consumo_ponta_kwh":          cons_pta,
        "consumo_fora_ponta_kwh":     cons_fp or None,
        "consumo_total_kwh":          (cons_pta or 0) + cons_fp,
        "energia_reversa_kwh":        en_reversa,
        "media_12_meses_kwh":         media_12,
        "historico_kwh":              historico,
        "demanda_contratada_ponta_kw":      dem_ctda_pta,
        "demanda_contratada_fora_ponta_kw": dem_ctda_fp,
        "demanda_medida_ponta_kw":          dem_med_pta,
        "demanda_medida_fora_ponta_kw":     dem_med_fp,
        "demanda_reativa_ponta_kw":         dmcr_pta,
        "demanda_reativa_fora_ponta_kw":    dmcr_fp,
        "ufer_ponta_kvarh":                 ufer_pta,
        "ufer_fora_ponta_kvarh":            ufer_fp,
        "tarifa_consumo_ponta":      tar_pta,
        "tarifa_consumo_fora_ponta": tar_fp,
        "tarifa_demanda":            tar_dem,
        "bandeira_tarifaria":        bandeira,
        "bandeira_valor_kwh":        bandeira_val,
        "icms_st":         icms_st,
        "pis_aliquota":    pis_al,
        "cofins_aliquota": cofins_al,
        "cosip_valor":     cosip,
        "credito_geracao": cred_ger,
        "total_a_pagar":   total,
        "itens_faturados": itens,
        "dados_leitura":   leituras,
    }
