"""
Worker Principal — Invoice Extraction Worker (HTTP)
Faz polling de tarefas pendentes criadas pelo usuário.
Sem browser, sem captcha — login via API mobile.
"""

import asyncio
import logging
import signal
import sys
from datetime import datetime, timezone

import os

from src.db.client import SupabaseClient
from src.extractors.amazonas_energia import AmazonasEnergiaHTTPExtractor
from src.utils.logger import setup_logger
from src.config import settings
from src.api import start_api_server

logger = setup_logger(__name__)

EXTRACTOR_MAP = {
    "amazonas_energia": AmazonasEnergiaHTTPExtractor,
}

_shutdown_event = asyncio.Event()


def _handle_signal(sig, frame):
    logger.info(f"Sinal {sig} recebido. Encerrando...")
    _shutdown_event.set()


async def _parse_and_analyze(db: SupabaseClient, task_id: str, pdfs: list, task: dict) -> None:
    """Faz parse e análise IA de cada PDF extraído, salvando em faturas_parsed e faturas_analise."""
    import asyncio
    from pathlib import Path
    from src.parsers.parser_fatura import parse_pdf
    from src.parsers.analyzer_fatura import analisar_fatura
    from src.ai.ai_provider import gerar_analise_textual

    loop = asyncio.get_event_loop()
    credentials = task.get("credentials", {})
    cliente_nome = None

    for pdf_info in pdfs:
        try:
            storage_url = pdf_info.get("storage_url", "")
            uc = str(pdf_info.get("uc", ""))
            mes_ref = pdf_info.get("mes_referencia", "")
            tipo = pdf_info.get("tipo", "")

            if tipo != "detalhada":
                continue  # Só processa faturas detalhadas

            # Baixa PDF do Supabase Storage
            storage_path = f"faturas/{uc}/{mes_ref.replace('/', '-')}_detalhada.pdf"
            tmp_path = Path(f"/tmp/parse_{task_id}_{uc}_{mes_ref.replace('/', '-')}.pdf")

            def _download():
                data = db._client.storage.from_(settings.SUPABASE_BUCKET).download(storage_path)
                return data

            try:
                pdf_bytes = await loop.run_in_executor(None, _download)
                tmp_path.write_bytes(pdf_bytes)
            except Exception as dl_exc:
                logger.warning(f"[Parse] Erro ao baixar do storage {storage_path}: {dl_exc}")
                continue

            # Parse — tenta IA (Claude Vision) primeiro, fallback regex
            try:
                from src.parsers.parser_fatura_ia import parse_pdf_ia
                dados_fatura = await parse_pdf_ia(str(tmp_path))
                logger.info(f"[Parse] Parser usado: {dados_fatura.get('source_parser','ia')}")
            except Exception as parse_err:
                logger.warning(f"[Parse] Parser IA falhou ({parse_err}), usando regex")
                def _parse():
                    return parse_pdf(str(tmp_path))
                dados_fatura = await loop.run_in_executor(None, _parse)
            tmp_path.unlink(missing_ok=True)

            if not dados_fatura.get("uc"):
                dados_fatura["uc"] = uc
            if not dados_fatura.get("mes_referencia"):
                dados_fatura["mes_referencia"] = mes_ref

            dados_fatura["extraction_id"] = task_id
            if cliente_nome is None and dados_fatura.get("cliente_nome"):
                cliente_nome = dados_fatura["cliente_nome"]

            # Salva fatura parseada
            fatura_id = await db.save_fatura_parsed(dados_fatura)
            if not fatura_id:
                logger.warning(f"[Parse] Falha ao salvar fatura UC {uc} {mes_ref}")
                continue

            logger.info(f"[Parse] ✓ UC {uc} {mes_ref} salvo (id={fatura_id})")

            # Análise
            def _analisar():
                return analisar_fatura(dados_fatura)

            analise_dict = await loop.run_in_executor(None, _analisar)
            analise_dict["fatura_id"] = fatura_id
            analise_dict["uc"] = uc

            # Gera texto IA
            try:
                texto_ia = await gerar_analise_textual(dados_fatura, analise_dict)
                if texto_ia:
                    analise_dict["analise_claude"] = texto_ia
            except Exception as e:
                logger.warning(f"[IA] Texto não gerado para UC {uc}: {e}")

            await db.save_fatura_analise(fatura_id, analise_dict)
            logger.info(f"[Análise] ✓ UC {uc} {mes_ref} analisado (score={analise_dict.get('score_eficiencia')})")

            # Gerar alertas automaticamente
            try:
                alertas_list = analise_dict.get("alertas") or []
                for alerta in alertas_list[:5]:
                    if isinstance(alerta, dict) and alerta.get("titulo"):
                        alert_payload = {
                            "uc": uc,
                            
                            "tipo": alerta.get("tipo", "info"),
                            "severidade": alerta.get("severidade", "medio"),
                            "titulo": alerta.get("titulo", ""),
                            "descricao": alerta.get("descricao", ""),
                            "resolvido": False,
                        }
                        def _save_alert(p=alert_payload):
                            try:
                                db._client.table("alertas_de_fatura").upsert(
                                    p, on_conflict="uc,titulo"
                                ).execute()
                            except:
                                pass
                        await loop.run_in_executor(None, _save_alert)
            except Exception as ae:
                logger.warning(f"[Alertas] Falha ao gerar alertas UC {uc}: {ae}")

        except Exception as exc:
            logger.error(f"[Parse] Erro UC {pdf_info.get('uc')} {pdf_info.get('mes_referencia')}: {exc}", exc_info=True)

    if cliente_nome:
        logger.info(f"[Parse] Cliente identificado: {cliente_nome}")


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

            # ── Parse + Análise de cada PDF ───────────────────────────────
            await _parse_and_analyze(db, task_id, pdfs, task)

        else:
            await db.update_task_status(task_id, "erro_extracao", "Nenhuma fatura encontrada.")

    except Exception as exc:
        error_msg = str(exc)
        lower = error_msg.lower()
        if "limite de tentativas" in lower or "tente novamente daqui" in lower:
            msg = "Rate limit da Amazonas Energia. Aguarde 10-30 minutos antes de tentar novamente."
            await db.update_task_status(task_id, "erro_extracao", msg)
            logger.warning(f"[Task {task_id}] Rate limit — cooldown acionado.")
        elif "inválid" in lower or "invalida" in lower:
            await db.update_task_status(task_id, "credenciais_invalidas", "CPF ou senha inválidos.")
            logger.warning(f"[Task {task_id}] Credenciais inválidas.")
        elif "401" in error_msg or "expirado" in lower:
            await db.update_task_status(task_id, "credenciais_invalidas", "Sessão expirada.")
            logger.warning(f"[Task {task_id}] JWT expirado.")
        elif "ReadTimeout" in error_msg or "Timeout" in error_msg:
            msg = "Limite diário atingido ou portal da Amazonas Energia instável. Tente novamente amanhã ou em alguns minutos."
            await db.update_task_status(task_id, "erro_extracao", msg)
            logger.warning(f"[Task {task_id}] Timeout — limite diário ou instabilidade.")
        else:
            logger.exception(f"[Task {task_id}] Erro: {exc}")
            await db.update_task_status(task_id, "erro_extracao", error_msg[:500])


async def cleanup_stale_tasks(db: SupabaseClient) -> None:
    """
    One-shot na subida do worker: cancela tarefas órfãs.

    - 'em_progresso': nenhum worker retoma extração num restart.
    - 'pendente' de 'Re-extração automática': eram criadas pelo cron antigo
      (removido). Hoje, re-extração é sempre manual pelo usuário.
    """
    loop = asyncio.get_event_loop()

    def _cleanup():
        stuck = (
            db._client.table("extraction_requests")
            .select("id")
            .eq("status", "em_progresso")
            .execute()
        )
        stuck_ids = [r["id"] for r in (stuck.data or [])]
        if stuck_ids:
            db._client.table("extraction_requests").update({
                "status": "erro_extracao",
                "status_detail": "Cancelada — worker reiniciou antes de concluir.",
            }).in_("id", stuck_ids).execute()

        auto = (
            db._client.table("extraction_requests")
            .select("id")
            .eq("status", "pendente")
            .like("status_detail", "Re-extração automática%")
            .execute()
        )
        auto_ids = [r["id"] for r in (auto.data or [])]
        if auto_ids:
            db._client.table("extraction_requests").update({
                "status": "erro_extracao",
                "status_detail": "Cancelada — re-extração automática foi removida. Refaça pelo painel.",
            }).in_("id", auto_ids).execute()

        return len(stuck_ids), len(auto_ids)

    try:
        stuck_n, auto_n = await loop.run_in_executor(None, _cleanup)
        if stuck_n or auto_n:
            logger.info(
                f"Cleanup: {stuck_n} travada(s) em em_progresso + "
                f"{auto_n} re-extração automática pendente(s) canceladas."
            )
        else:
            logger.info("Cleanup: nada a limpar.")
    except Exception as exc:
        logger.warning(f"Cleanup: erro (continuando): {exc}")


async def run_worker() -> None:
    """Loop principal: polling de tarefas pendentes criadas pelo usuário."""
    db = SupabaseClient()

    # ── Inicia servidor HTTP na porta do Railway ──────────────────────────────
    port = int(os.environ.get("PORT", 8080))
    await start_api_server(port=port)

    # ── Limpeza de tarefas órfãs deixadas por deploys anteriores ──────────────
    await cleanup_stale_tasks(db)

    logger.info(
        f"Worker HTTP iniciado | Polling: {settings.POLL_INTERVAL_SECONDS}s | "
        f"Concorrência: {settings.MAX_CONCURRENT_TASKS}"
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
