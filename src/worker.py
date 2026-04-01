"""
Worker Principal — Invoice Extraction Worker (HTTP Version)
Faz polling na fila do Supabase e despacha tarefas de extração via API REST.
Sem browser, sem captcha — usa JWT para autenticação.
"""

import asyncio
import logging
import signal
import sys

from src.db.client import SupabaseClient
from src.extractors.amazonas_energia import AmazonasEnergiaHTTPExtractor
from src.utils.logger import setup_logger
from src.config import settings

logger = setup_logger(__name__)

# Mapa de concessionárias disponíveis
EXTRACTOR_MAP = {
    "amazonas_energia": AmazonasEnergiaHTTPExtractor,
}

# Flag de controle de shutdown gracioso
_shutdown_event = asyncio.Event()


def _handle_signal(sig, frame):
    """Captura SIGTERM/SIGINT para encerrar o worker com segurança."""
    logger.info(f"Sinal {sig} recebido. Aguardando tarefa atual finalizar...")
    _shutdown_event.set()


async def process_task(db: SupabaseClient, task: dict) -> None:
    """Processa uma única tarefa de extração."""
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
            logger.info(f"[Task {task_id}] ✓ Concluído com {len(pdfs)} faturas.")
        else:
            await db.update_task_status(task_id, "erro_extracao", "Nenhuma fatura encontrada.")

    except Exception as exc:
        error_msg = str(exc)

        # Detecta JWT expirado
        if "401" in error_msg or "expirado" in error_msg.lower():
            await db.update_task_status(
                task_id, "credenciais_invalidas",
                "JWT expirado. Necessário re-autenticar no portal."
            )
            logger.warning(f"[Task {task_id}] JWT expirado — marcado para re-autenticação.")
        else:
            logger.exception(f"[Task {task_id}] Erro inesperado: {exc}")
            await db.update_task_status(task_id, "erro_extracao", error_msg[:500])


async def run_worker() -> None:
    """Loop principal do worker com polling."""
    db = SupabaseClient()
    logger.info(
        f"Worker HTTP iniciado | Polling a cada {settings.POLL_INTERVAL_SECONDS}s | "
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

        try:
            await asyncio.wait_for(
                _shutdown_event.wait(),
                timeout=settings.POLL_INTERVAL_SECONDS,
            )
        except asyncio.TimeoutError:
            pass

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
