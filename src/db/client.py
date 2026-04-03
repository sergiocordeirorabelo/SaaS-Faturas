"""
Cliente Supabase — gerencia acesso ao banco e ao Storage.
Todas as operações são assíncronas via httpx.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx
from supabase import create_client, Client

from src.config import settings

logger = logging.getLogger(__name__)

TABLE_REQUESTS  = "extraction_requests"
TABLE_PARSED    = "faturas_parsed"
TABLE_ANALISE   = "faturas_analise"


class SupabaseClient:
    """Wrapper sobre o SDK oficial do Supabase com helpers de domínio."""

    def __init__(self):
        self._client: Client = create_client(
            settings.SUPABASE_URL,
            settings.SUPABASE_SERVICE_KEY,
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Fila / Tarefas
    # ─────────────────────────────────────────────────────────────────────────

    async def fetch_pending_tasks(self, limit: int = 5) -> list[dict]:
        loop = asyncio.get_event_loop()

        def _query():
            result = (
                self._client.table(TABLE_REQUESTS)
                .select("*")
                .eq("status", "pendente")
                .order("created_at")
                .limit(limit)
                .execute()
            )
            return result.data or []

        rows = await loop.run_in_executor(None, _query)
        for row in rows:
            await self.update_task_status(row["id"], "em_progresso")
        return rows

    async def update_task_status(
        self,
        task_id: str,
        status: str,
        detail: Optional[str] = None,
        pdf_links: Optional[list[dict]] = None,
    ) -> None:
        loop = asyncio.get_event_loop()
        payload: dict = {
            "status": status,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        if detail:
            payload["status_detail"] = detail
        if pdf_links:
            payload["pdf_links"] = pdf_links

        def _update():
            self._client.table(TABLE_REQUESTS).update(payload).eq("id", task_id).execute()

        await loop.run_in_executor(None, _update)
        logger.debug(f"[Task {task_id}] Status → {status}")

    # ─────────────────────────────────────────────────────────────────────────
    # Storage
    # ─────────────────────────────────────────────────────────────────────────

    async def upload_pdf(
        self,
        local_path: Path,
        storage_path: str,
        task_id: str,
    ) -> str:
        loop = asyncio.get_event_loop()

        def _upload():
            with open(local_path, "rb") as f:
                data = f.read()

            self._client.storage.from_(settings.SUPABASE_BUCKET).upload(
                path=storage_path,
                file=data,
                file_options={"content-type": "application/pdf", "upsert": "true"},
            )
            signed = self._client.storage.from_(settings.SUPABASE_BUCKET).create_signed_url(
                storage_path, expires_in=60 * 60 * 24 * 365
            )
            return signed["signedURL"]

        url = await loop.run_in_executor(None, _upload)
        logger.info(f"[Task {task_id}] PDF enviado → {storage_path}")
        return url

    async def upload_screenshot(self, local_path: Path, storage_path: str) -> str:
        loop = asyncio.get_event_loop()

        def _upload():
            with open(local_path, "rb") as f:
                data = f.read()
            self._client.storage.from_(settings.SUPABASE_BUCKET).upload(
                path=storage_path,
                file=data,
                file_options={"content-type": "image/png", "upsert": "true"},
            )
            signed = self._client.storage.from_(settings.SUPABASE_BUCKET).create_signed_url(
                storage_path, expires_in=60 * 60 * 24 * 30
            )
            return signed["signedURL"]

        return await loop.run_in_executor(None, _upload)

    # ─────────────────────────────────────────────────────────────────────────
    # Fase 2 — Parsing e Análise
    # ─────────────────────────────────────────────────────────────────────────

    async def save_fatura_parsed(
        self,
        parsed: dict,
        extraction_id: Optional[str] = None,
        source_pdf_path: Optional[str] = None,
    ) -> Optional[str]:
        """
        Grava os dados parseados do PDF na tabela faturas_parsed.
        Usa upsert em (uc, mes_referencia) para idempotência.
        Retorna o UUID do registro inserido/atualizado.
        """
        loop = asyncio.get_event_loop()

        # Remove campos não mapeados na tabela (detalhes ficam em JSON)
        payload = {
            "uc":               parsed.get("uc"),
            "mes_referencia":   parsed.get("mes_referencia"),
            "vencimento":       parsed.get("vencimento"),
            "nota_fiscal":      parsed.get("nota_fiscal"),
            "data_emissao":     parsed.get("data_emissao"),
            "cliente_nome":     parsed.get("cliente_nome"),
            "grupo":            parsed.get("grupo"),
            "subgrupo":         parsed.get("subgrupo"),
            "classe":           parsed.get("classe"),
            "modalidade":       parsed.get("modalidade"),
            "numero_medidor":   parsed.get("numero_medidor"),
            "tensao_contratada_v": parsed.get("tensao_contratada_v"),
            "data_leitura_anterior": parsed.get("data_leitura_anterior"),
            "data_leitura_atual":    parsed.get("data_leitura_atual"),
            "dias_consumo":          parsed.get("dias_consumo"),
            "consumo_ponta_kwh":           parsed.get("consumo_ponta_kwh"),
            "consumo_fora_ponta_kwh":      parsed.get("consumo_fora_ponta_kwh"),
            "consumo_total_kwh":           parsed.get("consumo_total_kwh"),
            "energia_reversa_kwh":         parsed.get("energia_reversa_kwh"),
            "media_12_meses_kwh":          parsed.get("media_12_meses_kwh"),
            "historico_kwh":               parsed.get("historico_kwh") or [],
            "demanda_contratada_ponta_kw":      parsed.get("demanda_contratada_ponta_kw"),
            "demanda_contratada_fora_ponta_kw": parsed.get("demanda_contratada_fora_ponta_kw"),
            "demanda_medida_ponta_kw":          parsed.get("demanda_medida_ponta_kw"),
            "demanda_medida_fora_ponta_kw":     parsed.get("demanda_medida_fora_ponta_kw"),
            "demanda_reativa_ponta_kw":         parsed.get("demanda_reativa_ponta_kw"),
            "demanda_reativa_fora_ponta_kw":    parsed.get("demanda_reativa_fora_ponta_kw"),
            "ufer_ponta_kvarh":                 parsed.get("ufer_ponta_kvarh"),
            "ufer_fora_ponta_kvarh":            parsed.get("ufer_fora_ponta_kvarh"),
            "tarifa_consumo_ponta":      parsed.get("tarifa_consumo_ponta"),
            "tarifa_consumo_fora_ponta": parsed.get("tarifa_consumo_fora_ponta"),
            "tarifa_demanda":            parsed.get("tarifa_demanda"),
            "bandeira_tarifaria":        parsed.get("bandeira_tarifaria"),
            "bandeira_valor_kwh":        parsed.get("bandeira_valor_kwh"),
            "icms_st":         parsed.get("icms_st", False),
            "pis_aliquota":    parsed.get("pis_aliquota"),
            "cofins_aliquota": parsed.get("cofins_aliquota"),
            "cosip_valor":     parsed.get("cosip_valor"),
            "credito_geracao": parsed.get("credito_geracao"),
            "total_a_pagar":   parsed.get("total_a_pagar"),
            "itens_faturados": parsed.get("itens_faturados") or [],
            "dados_leitura":   parsed.get("dados_leitura") or [],
        }

        if extraction_id:
            payload["extraction_id"] = extraction_id
        if source_pdf_path:
            payload["source_pdf_path"] = source_pdf_path

        # Remove None para evitar erros de tipo no Postgres
        payload = {k: v for k, v in payload.items() if v is not None}

        def _upsert():
            result = (
                self._client.table(TABLE_PARSED)
                .upsert(payload, on_conflict="uc,mes_referencia")
                .execute()
            )
            rows = result.data or []
            return rows[0]["id"] if rows else None

        try:
            fatura_id = await loop.run_in_executor(None, _upsert)
            logger.info(
                f"[Parsed] UC {parsed.get('uc')} {parsed.get('mes_referencia')} "
                f"→ faturas_parsed id={fatura_id}"
            )
            return fatura_id
        except Exception as exc:
            logger.error(f"[Parsed] Erro ao salvar faturas_parsed: {exc}")
            return None

    async def save_fatura_analise(
        self,
        fatura_id: str,
        analise: dict,
    ) -> None:
        """
        Grava o resultado da análise na tabela faturas_analise.
        Usa upsert em fatura_id para idempotência.
        """
        loop = asyncio.get_event_loop()

        payload = {
            "fatura_id":       fatura_id,
            "uc":              analise.get("uc"),
            "mes_referencia":  analise.get("mes_referencia"),
            "total_fatura":    analise.get("total_fatura"),
            "score_eficiencia":           analise.get("score_eficiencia"),
            "potencial_economia_mensal":  analise.get("potencial_economia_mensal"),
            "potencial_economia_anual":   analise.get("potencial_economia_anual"),
            "resumo_executivo":           analise.get("resumo_executivo"),
            "alertas":                    analise.get("alertas") or [],
        }
        payload = {k: v for k, v in payload.items() if v is not None}

        def _upsert():
            self._client.table(TABLE_ANALISE).upsert(
                payload, on_conflict="fatura_id"
            ).execute()

        try:
            await loop.run_in_executor(None, _upsert)
            logger.info(
                f"[Analise] UC {analise.get('uc')} {analise.get('mes_referencia')} "
                f"score={analise.get('score_eficiencia')} "
                f"economia=R${analise.get('potencial_economia_anual', 0):,.0f}/ano"
            )
        except Exception as exc:
            logger.error(f"[Analise] Erro ao salvar faturas_analise: {exc}")
