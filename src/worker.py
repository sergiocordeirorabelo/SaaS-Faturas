"""
Worker Principal — Invoice Extraction Worker (HTTP + Cron)
Faz polling de tarefas + verificação diária automática de novos clientes.
Sem browser, sem captcha — login via API mobile.
"""

import asyncio
import logging
import random
import signal
import sys
from datetime import datetime, timezone, timedelta

from src.db.client import SupabaseClient
from src.extractors.amazonas_energia import AmazonasEnergiaHTTPExtractor
from src.utils.logger import setup_logger
from src.config import settings

logger = setup_logger(__name__)

EXTRACTOR_MAP = {
    "amazonas_energia": AmazonasEnergiaHTTPExtractor,
}

_shutdown_event = asyncio.Event()


def _handle_signal(sig, frame):
    logger.info(f"Sinal {sig} recebido. Encerrando...")
    _shutdown_event.set()


async def process_task(db: SupabaseClient, task: dict) -> None:
    """Processa uma única tarefa de extração."""
    task_id = task["id"]
    concessionaria = task.get("concessionaria", "amazonas_energia")
    logger.info(f"[Task {task_id}] Iniciando extração | {concessionaria}")

    ExtractorClass = EXTRACTOR_MAP.get(concessionaria)
    if not ExtractorClass:
        await db.update_task_status(task_id, "erro_extracao", "Concessionária não suportada.")
        return

    extractor = ExtractorClass(db=db, task=task)

    try:
        await db.update_task_status(task_id, "em_progresso")
        pdfs = await extractor.run()

        if pdfs:
            await db.update_task_status(
                task_id, "concluido",
                detail=f"{len(pdfs)} faturas extraídas com sucesso.",
                pdf_links=pdfs,
            )
            logger.info(f"[Task {task_id}] ✓ Concluído com {len(pdfs)} faturas.")
        else:
            await db.update_task_status(task_id, "erro_extracao", "Nenhuma fatura encontrada.")

    except Exception as exc:
        error_msg = str(exc)
        if "inválid" in error_msg.lower() or "invalida" in error_msg.lower():
            await db.update_task_status(task_id, "credenciais_invalidas", "CPF ou senha inválidos.")
            logger.warning(f"[Task {task_id}] Credenciais inválidas.")
        elif "401" in error_msg or "expirado" in error_msg.lower():
            await db.update_task_status(task_id, "credenciais_invalidas", "Sessão expirada.")
            logger.warning(f"[Task {task_id}] JWT expirado.")
        else:
            logger.exception(f"[Task {task_id}] Erro: {exc}")
            await db.update_task_status(task_id, "erro_extracao", error_msg[:500])


async def auto_reextract(db: SupabaseClient) -> None:
    """
    Cron automático: verifica clientes que precisam de re-extração.
    Cria novas tarefas para clientes cuja última extração foi há mais de 24h.
    """
    logger.info("Cron: verificando clientes para re-extração automática...")

    try:
        loop = asyncio.get_event_loop()

        def _query():
            # Busca todas as tarefas concluídas
            result = (
                db._client.table("extraction_requests")
                .select("id,credentials,created_at,concessionaria")
                .eq("status", "concluido")
                .order("created_at", desc=True)
                .execute()
            )
            return result.data or []

        tasks = await loop.run_in_executor(None, _query)

        # Agrupa por CPF — pega só a mais recente de cada
        latest_by_cpf = {}
        for t in tasks:
            cpf = t.get("credentials", {}).get("cpf_cnpj", "")
            if cpf and cpf not in latest_by_cpf:
                latest_by_cpf[cpf] = t

        now = datetime.now(timezone.utc)
        created_count = 0

        for cpf, t in latest_by_cpf.items():
            created_at = datetime.fromisoformat(t["created_at"].replace("Z", "+00:00"))
            hours_ago = (now - created_at).total_seconds() / 3600

            # Se última extração foi há mais de 24h, cria nova tarefa
            if hours_ago > 24:
                senha = t.get("credentials", {}).get("senha", "")
                if not senha:
                    continue

                def _insert(cpf=cpf, senha=senha, conc=t["concessionaria"]):
                    db._client.table("extraction_requests").insert({
                        "concessionaria": conc,
                        "credentials": {"cpf_cnpj": cpf, "senha": senha},
                        "status": "pendente",
                        "status_detail": "Re-extração automática diária.",
                    }).execute()

                await loop.run_in_executor(None, _insert)
                created_count += 1

                # Jitter entre criações para não sobrecarregar
                await asyncio.sleep(random.uniform(0.5, 2.0))

        if created_count > 0:
            logger.info(f"Cron: {created_count} tarefa(s) de re-extração criada(s).")
        else:
            logger.info("Cron: todos os clientes estão atualizados.")

    except Exception as exc:
        logger.error(f"Cron: erro na verificação: {exc}", exc_info=True)


async def run_worker() -> None:
    """Loop principal: polling de tarefas + cron diário."""
    db = SupabaseClient()
    logger.info(
        f"Worker HTTP iniciado | Polling: {settings.POLL_INTERVAL_SECONDS}s | "
        f"Concorrência: {settings.MAX_CONCURRENT_TASKS}"
    )

    semaphore = asyncio.Semaphore(settings.MAX_CONCURRENT_TASKS)
    last_cron = datetime.min  # Força cron na primeira execução

    async def bounded_process(task):
        async with semaphore:
            await process_task(db, task)

    while not _shutdown_event.is_set():
        try:
            # ── Cron: re-extração automática a cada 6 horas ──────────────
            now = datetime.now()
            if (now - last_cron).total_seconds() > 6 * 3600:
                await auto_reextract(db)
                last_cron = now

            # ── Polling: processa tarefas pendentes ──────────────────────
            tasks = await db.fetch_pending_tasks(limit=settings.MAX_CONCURRENT_TASKS)

            if tasks:
                logger.info(f"{len(tasks)} tarefa(s) encontrada(s). Processando...")
                await asyncio.gather(*[bounded_process(t) for t in tasks])
            else:
                logger.debug("Nenhuma tarefa pendente.")

        except Exception as exc:
            logger.error(f"Erro no ciclo: {exc}", exc_info=True)

        try:
            await asyncio.wait_for(
                _shutdown_event.wait(),
                timeout=settings.POLL_INTERVAL_SECONDS,
            )
        except asyncio.TimeoutError:
            pass

    logger.info("Worker encerrado.")


def main():
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    try:
        asyncio.run(run_worker())
    except KeyboardInterrupt:
        logger.info("Interrompido.")
        sys.exit(0)


if __name__ == "__main__":
    main()
