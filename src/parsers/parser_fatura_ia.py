"""
Parser de Faturas via Claude Vision API.
Envia imagem do PDF → Claude extrai TODOS os campos em JSON estruturado.
Fallback: parser regex se a API falhar.
"""
from __future__ import annotations
import io, os, json, logging, base64, tempfile
from pathlib import Path
from typing import Optional
import httpx

logger = logging.getLogger(__name__)

ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = os.getenv("AI_PARSER_MODEL", "claude-haiku-4-5-20251001")

PROMPT_PARSE = """Analise esta fatura de energia elétrica da Amazonas Energia e extraia TODOS os campos em JSON.

Retorne APENAS o JSON válido, sem markdown, sem explicação. Formato exato:

{
  "uc": "0087346-2",
  "nota_fiscal": "112612898",
  "data_emissao": "12/02/2026",
  "vencimento": "02/03/2026",
  "mes_referencia": "02/2026",
  "cliente_nome": "NOME COMPLETO DO CLIENTE",
  "cliente_endereco": "Endereço completo",
  "grupo": "A",
  "subgrupo": "A4",
  "classe": "COMERCIAL",
  "modalidade": "HOROSAZONAL VERDE",
  "medidor": "11429009",
  "tensao_contratada_v": 13800,
  "periodo_consumo_inicio": "15/01/2026",
  "periodo_consumo_fim": "13/02/2026",
  "dias_consumo": 30,
  "leitura_anterior": 72373,
  "leitura_atual": 73024,
  "constante_faturamento": 1.0,
  "npl": 5,
  "consumo_medido": 651,
  "consumo_faturado": 651,
  "demanda_contratada_ponta_kw": 225,
  "demanda_contratada_fora_ponta_kw": 225,
  "demanda_medida_ponta_kw": 98,
  "demanda_medida_fora_ponta_kw": 142,
  "consumo_ponta_kwh": 2564,
  "consumo_fora_ponta_kwh": 29128,
  "consumo_total_kwh": 31692,
  "ufer_ponta_kvarh": 0,
  "ufer_fora_ponta_kvarh": 2,
  "dmcr_ponta_kw": 0,
  "dmcr_fora_ponta_kw": 0,
  "bandeira_tarifaria": "Verde",
  "bandeira_valor": 0,
  "itens_faturados": [
    {"descricao": "Consumo Ponta", "quantidade": 2564, "unidade": "kWh", "tarifa": 1.73009, "tarifa_com_impostos": 1.73009, "valor": 4435.95},
    {"descricao": "Demanda 142 kW", "quantidade": 142, "unidade": "kW", "tarifa": 22.96, "tarifa_com_impostos": 22.96, "valor": 3260.32},
    {"descricao": "COSIP", "quantidade": null, "unidade": null, "tarifa": null, "tarifa_com_impostos": null, "valor": 2666.44},
    {"descricao": "Credito Geracao F/Ponta", "quantidade": null, "unidade": null, "tarifa": null, "tarifa_com_impostos": null, "valor": -1092.51}
  ],
  "leituras_grandeza": [
    {"grandeza": "En Ativa Pta", "leitura_atual": 114.21, "leitura_anterior": 107.45, "constante": 1400, "registrado": 9464},
    {"grandeza": "En Ativa F-Pta", "leitura_atual": 1765.94, "leitura_anterior": 1691.98, "constante": 1400, "registrado": 103544}
  ],
  "icms_base_calculo": null,
  "icms_aliquota": null,
  "icms_valor": null,
  "tributacao_diferimento": null,
  "total_encargo_uso": null,
  "percentual_desconto_demanda": null,
  "total_a_pagar": 25621.71,
  "cosip_valor": 2666.44,
  "credito_geracao": -1092.51,
  "tarifa_consumo_ponta": 1.73009,
  "tarifa_consumo_fora_ponta": 0.49592,
  "tarifa_demanda": 22.96,
  "media_12_meses_kwh": null,
  "historico_consumo": [
    {"mes": "02/2025", "kwh": 28500},
    {"mes": "03/2025", "kwh": 30200}
  ]
}

REGRAS:
- Extraia TODOS os itens faturados, incluindo multas, correções monetárias, juros
- Na tabela "Descrição da Grandeza", extraia TODAS as linhas (En Ativa, Dem Acum, Ufer, Dmcr, En Reversa)
- Se um campo não existir no PDF, use null
- Valores monetários como números (não strings): 25621.71, não "25.621,71"
- Quantidades como números: 2564, não "2.564"
- Tarifas com todas as casas decimais: 1.730090
- Identifique ICMS: base de cálculo, alíquota, valor, diferimento/substituição
- Total Encargo de Uso e Percentual Desconto Para Demanda se existirem
- Histórico de consumo se visível no PDF
- Bandeira tarifária e seu valor adicional por kWh
"""


def _render_pdf_to_images(pdf_path: str, max_pages: int = 2) -> list[bytes]:
    """Renderiza páginas do PDF como PNG em memória."""
    import fitz
    doc = fitz.open(pdf_path)
    images = []
    for i in range(min(len(doc), max_pages)):
        page = doc[i]
        # DPI ~200 para boa qualidade sem ser gigante
        mat = fitz.Matrix(2.0, 2.0)
        pix = page.get_pixmap(matrix=mat)
        img_bytes = pix.tobytes("png")
        images.append(img_bytes)
    doc.close()
    return images


def _render_pdf_screenshot(pdf_path: str, page_num: int = 0, dpi: float = 2.5) -> bytes:
    """Renderiza uma página específica do PDF como PNG de alta qualidade."""
    import fitz
    doc = fitz.open(pdf_path)
    if page_num >= len(doc):
        page_num = 0
    page = doc[page_num]
    mat = fitz.Matrix(dpi, dpi)
    pix = page.get_pixmap(matrix=mat)
    img_bytes = pix.tobytes("png")
    doc.close()
    return img_bytes


async def parse_pdf_ia(pdf_path: str) -> dict:
    """Parse completo via Claude Vision. Retorna dict estruturado."""
    if not ANTHROPIC_KEY:
        logger.warning("ANTHROPIC_API_KEY não configurada, usando parser regex")
        from src.parsers.parser_fatura import parse_pdf
        return parse_pdf(pdf_path)

    try:
        images = _render_pdf_to_images(pdf_path, max_pages=2)
        logger.info(f"[ParserIA] Renderizou {len(images)} páginas do PDF")

        # Monta conteúdo com imagens
        content = []
        for i, img_bytes in enumerate(images):
            b64 = base64.b64encode(img_bytes).decode("utf-8")
            content.append({
                "type": "image",
                "source": {"type": "base64", "media_type": "image/png", "data": b64}
            })
        content.append({"type": "text", "text": PROMPT_PARSE})

        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": ANTHROPIC_KEY,
                    "anthropic-version": "2023-06-01",
                    "Content-Type": "application/json",
                },
                json={
                    "model": ANTHROPIC_MODEL,
                    "max_tokens": 4096,
                    "messages": [{"role": "user", "content": content}],
                },
            )

        if resp.status_code != 200:
            logger.error(f"[ParserIA] Claude API erro {resp.status_code}: {resp.text[:300]}")
            raise Exception(f"Claude API {resp.status_code}")

        data = resp.json()
        text_response = ""
        for block in data.get("content", []):
            if block.get("type") == "text":
                text_response += block["text"]

        # Limpa markdown se Claude envolveu em ```json
        text_response = text_response.strip()
        if text_response.startswith("```"):
            text_response = text_response.split("\n", 1)[1] if "\n" in text_response else text_response[3:]
        if text_response.endswith("```"):
            text_response = text_response[:-3]
        text_response = text_response.strip()

        parsed = json.loads(text_response)
        logger.info(f"[ParserIA] ✓ UC {parsed.get('uc','?')} {parsed.get('mes_referencia','?')} — {len(parsed.get('itens_faturados',[]))} itens")

        # Garante campos obrigatórios
        parsed.setdefault("source_parser", "claude_vision")
        return parsed

    except json.JSONDecodeError as e:
        logger.error(f"[ParserIA] JSON inválido do Claude: {e}")
        logger.debug(f"[ParserIA] Resposta: {text_response[:500]}")
    except Exception as e:
        logger.error(f"[ParserIA] Erro: {e}", exc_info=True)

    # Fallback para regex
    logger.info("[ParserIA] Fallback para parser regex")
    from src.parsers.parser_fatura import parse_pdf
    result = parse_pdf(pdf_path)
    result["source_parser"] = "regex_fallback"
    return result


def parse_pdf_ia_sync(pdf_path: str) -> dict:
    """Versão síncrona para compatibilidade."""
    import asyncio
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(asyncio.run, parse_pdf_ia(pdf_path))
                return future.result(timeout=90)
        return loop.run_until_complete(parse_pdf_ia(pdf_path))
    except RuntimeError:
        return asyncio.run(parse_pdf_ia(pdf_path))
