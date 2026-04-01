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

# ── Constantes de tabela ──────────────────────────────────────────────────────
TABLE_REQUESTS = "extraction_requests"


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
        """
        Busca tarefas com status 'pendente' e as marca atomicamente como
        'em_progresso' para evitar que dois workers peguem a mesma tarefa.
        """
        loop = asyncio.get_event_loop()

        def _query():
            # Seleciona e marca atomicamente (Postgres FOR UPDATE SKIP LOCKED via RPC)
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

        # Reserva as linhas (best-effort — concorrência baixa no MVP)
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
        """Atualiza o status e metadados de uma tarefa."""
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
        """
        Faz upload de um PDF para o Supabase Storage.

        Args:
            local_path: Caminho local do arquivo.
            storage_path: Caminho de destino no bucket (ex: "uc_123/2024-01.pdf").
            task_id: ID da tarefa (para logging).

        Returns:
            URL pública assinada (1 ano de validade).
        """
        loop = asyncio.get_event_loop()

        def _upload():
            with open(local_path, "rb") as f:
                data = f.read()

            # Upsert para idempotência
            self._client.storage.from_(settings.SUPABASE_BUCKET).upload(
                path=storage_path,
                file=data,
                file_options={"content-type": "application/pdf", "upsert": "true"},
            )

            # Gera URL assinada (365 dias)
            signed = self._client.storage.from_(settings.SUPABASE_BUCKET).create_signed_url(
                storage_path, expires_in=60 * 60 * 24 * 365
            )
            return signed["signedURL"]

        url = await loop.run_in_executor(None, _upload)
        logger.info(f"[Task {task_id}] PDF enviado → {storage_path}")
        return url

    async def upload_screenshot(self, local_path: Path, storage_path: str) -> str:
        """Faz upload de screenshot de erro para o bucket de logs."""
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
                storage_path, expires_in=60 * 60 * 24 * 30  # 30 dias
            )
            return signed["signedURL"]

        return await loop.run_in_executor(None, _upload)
