"""
AI Provider — Fase 3
Gera texto enriquecido de análise em linguagem natural.
Suporta OpenAI (padrão) e Claude via variável AI_PROVIDER.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

AI_PROVIDER    = os.getenv("AI_PROVIDER", "openai").lower()
OPENAI_KEY     = os.getenv("OPENAI_API_KEY", "")
ANTHROPIC_KEY  = os.getenv("ANTHROPIC_API_KEY", "")

OPENAI_MODEL    = os.getenv("AI_MODEL", "gpt-4o-mini")
ANTHROPIC_MODEL = os.getenv("AI_MODEL", "claude-haiku-4-5-20251001")


def _f(v, default=0):
    """Retorna float seguro — converte None para default."""
    if v is None:
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _prompt_analise(fatura: dict, analise: dict) -> str:
    alertas = analise.get("alertas", [])
    criticos = [a for a in alertas if a.get("severidade") == "critico"]
    atencao  = [a for a in alertas if a.get("severidade") == "atencao"]

    alertas_txt = ""
    for a in criticos + atencao:
        alertas_txt += f"- {a.get('titulo','')}: {a.get('descricao','')}"
        if a.get("acao_recomendada"):
            alertas_txt += f" Ação recomendada: {a['acao_recomendada']}"
        alertas_txt += "\n"

    return f"""Você é um especialista em eficiência energética para indústrias e comércios do Grupo A (alta tensão) no Brasil.

Com base nos dados abaixo, escreva um parágrafo executivo claro e direto para o cliente final.
Use linguagem simples, sem jargão técnico. Destaque a economia em reais. Seja objetivo.
Máximo 5 linhas.

DADOS DA FATURA:
- Cliente: {fatura.get("cliente_nome", "N/D")}
- UC: {fatura.get("uc")} | Mês: {fatura.get("mes_referencia")}
- Modalidade: {fatura.get("modalidade")} | Subgrupo: {fatura.get("subgrupo")}
- Total da fatura: R$ {_f(analise.get("total_fatura")):,.2f}
- Consumo total: {_f(fatura.get("consumo_total_kwh")):,.0f} kWh
- Demanda contratada: {_f(fatura.get("demanda_contratada_fora_ponta_kw")):.0f} kW
- Demanda medida: {_f(fatura.get("demanda_medida_fora_ponta_kw")):.0f} kW
- Score de eficiência: {analise.get("score_eficiencia")}/100
- Economia potencial: R$ {_f(analise.get("potencial_economia_mensal")):,.2f}/mês · R$ {_f(analise.get("potencial_economia_anual")):,.2f}/ano

PROBLEMAS IDENTIFICADOS:
{alertas_txt if alertas_txt else "Nenhuma anomalia significativa encontrada."}

Escreva o parágrafo executivo agora:"""


async def gerar_analise_textual(
    fatura: dict,
    analise: dict,
) -> Optional[str]:
    if AI_PROVIDER == "openai":
        return await _openai(fatura, analise)
    elif AI_PROVIDER == "claude":
        return await _claude(fatura, analise)
    else:
        logger.warning(f"AI_PROVIDER '{AI_PROVIDER}' desconhecido. Use 'openai' ou 'claude'.")
        return None


async def _openai(fatura: dict, analise: dict) -> Optional[str]:
    if not OPENAI_KEY:
        logger.warning("OPENAI_API_KEY não configurada.")
        return None

    prompt = _prompt_analise(fatura, analise)

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {OPENAI_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": OPENAI_MODEL,
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 300,
                    "temperature": 0.4,
                },
            )
            resp.raise_for_status()
            data  = resp.json()
            texto = data["choices"][0]["message"]["content"].strip()
            logger.info(f"[AI/OpenAI] Texto gerado para UC {fatura.get('uc')} ({len(texto)} chars)")
            return texto

    except Exception as exc:
        logger.error(f"[AI/OpenAI] Erro: {exc}")
        return None


async def _claude(fatura: dict, analise: dict) -> Optional[str]:
    if not ANTHROPIC_KEY:
        logger.warning("ANTHROPIC_API_KEY não configurada.")
        return None

    prompt = _prompt_analise(fatura, analise)

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": ANTHROPIC_KEY,
                    "anthropic-version": "2023-06-01",
                    "Content-Type": "application/json",
                },
                json={
                    "model": ANTHROPIC_MODEL,
                    "max_tokens": 300,
                    "messages": [{"role": "user", "content": prompt}],
                },
            )
            resp.raise_for_status()
            data  = resp.json()
            texto = data["content"][0]["text"].strip()
            logger.info(f"[AI/Claude] Texto gerado para UC {fatura.get('uc')} ({len(texto)} chars)")
            return texto

    except Exception as exc:
        logger.error(f"[AI/Claude] Erro: {exc}")
        return None
