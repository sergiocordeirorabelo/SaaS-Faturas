"""
Worker Principal — Invoice Extraction Worker
Faz polling na fila do Supabase e despacha tarefas de extração de faturas.
"""

import asyncio
import logging
import signal
import sys
from datetime import datetime

from src.db.client import SupabaseClient
from src.extractors.amazonas_energia import AmazonasEnergiaExtractor
from src.utils.logger import setup_logger
from src.config import settings

logger = setup_logger(__name__)

# Mapa de concessionárias disponíveis
EXTRACTOR_MAP = {
    "amazonas_energia": AmazonasEnergiaExtractor,
}

# Flag de controle de shutdown gracioso
_shutdown_event = asyncio.Event()


def _handle_signal(sig, frame):
    """Captura SIGTERM/SIGINT para encerrar o worker com segurança."""
    logger.info(f"Sinal {sig} recebido. Aguardando tarefa atual finalizar...")
    _shutdown_event.set()


async def process_task(db: SupabaseClient, task: dict) -> None:
    """
    Processa uma única tarefa de extração.

    Args:
        db: Cliente Supabase.
        task: Registro da tabela extraction_requests.
    """
    task_id = task["id"]
    concessionaria = task.get("concessionaria", "amazonas_energia")
    logger.info(f"[Task {task_id}] Iniciando extração | Concessionária: {concessionaria}")

    ExtractorClass = EXTRACTOR_MAP.get(concessionaria)
    if not ExtractorClass:
        logger.error(f"[Task {task_id}] Concessionária '{concessionaria}' não suportada.")
        await db.update_task_status(task_id, "erro_extracao", "Concessionária não suportada.")
        return

    extractor = ExtractorClass(db=db, task=task)

    try:
        await db.update_task_status(task_id, "em_progresso")
        pdfs = await extractor.run()

        if pdfs:
            await db.update_task_status(
                task_id,
                "concluido",
                detail=f"{len(pdfs)} faturas extraídas com sucesso.",
                pdf_links=pdfs,
            )
            logger.info(f"[Task {task_id}] Concluído com {len(pdfs)} faturas.")
        else:
            await db.update_task_status(task_id, "erro_extracao", "Nenhuma fatura encontrada.")

    except Exception as exc:
        logger.exception(f"[Task {task_id}] Erro inesperado: {exc}")
        await db.update_task_status(task_id, "erro_extracao", str(exc))


async def run_worker() -> None:
    """Loop principal do worker com polling na fila do banco de dados."""
    db = SupabaseClient()
    logger.info(
        f"Worker iniciado | Polling a cada {settings.POLL_INTERVAL_SECONDS}s | "
        f"Max concorrência: {settings.MAX_CONCURRENT_TASKS}"
    )

    semaphore = asyncio.Semaphore(settings.MAX_CONCURRENT_TASKS)

    async def bounded_process(task):
        async with semaphore:
            await process_task(db, task)

    while not _shutdown_event.is_set():
        try:
            tasks = await db.fetch_pending_tasks(limit=settings.MAX_CONCURRENT_TASKS)

            if tasks:
                logger.info(f"{len(tasks)} tarefa(s) encontrada(s). Processando...")
                await asyncio.gather(*[bounded_process(t) for t in tasks])
            else:
                logger.debug("Nenhuma tarefa pendente. Aguardando próximo ciclo...")

        except Exception as exc:
            logger.error(f"Erro no ciclo de polling: {exc}", exc_info=True)

        # Aguarda o intervalo OU até que o shutdown seja sinalizado
        try:
            await asyncio.wait_for(
                _shutdown_event.wait(),
                timeout=settings.POLL_INTERVAL_SECONDS,
            )
        except asyncio.TimeoutError:
            pass  # Timeout esperado — apenas continua o loop

    logger.info("Worker encerrado com segurança.")


def main():
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    try:
        asyncio.run(run_worker())
    except KeyboardInterrupt:
        logger.info("Interrompido pelo usuário.")
        sys.exit(0)


if __name__ == "__main__":
    main()
