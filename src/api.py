"""
API HTTP — Servidor leve para geração de PDFs.
Roda em paralelo com o loop de polling do worker.

Endpoints:
  GET /health                    → status do worker
  GET /relatorio/{fatura_id}     → gera e retorna PDF da fatura
  GET /relatorio/uc/{uc}         → gera PDF da última fatura de uma UC
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime

from aiohttp import web

from src.db.client import SupabaseClient
from src.utils.logger import setup_logger

try:
    from src.reports.gerar_relatorio import gerar_relatorio
    PDF_DISPONIVEL = True
except ImportError:
    PDF_DISPONIVEL = False

logger = setup_logger(__name__)


async def handle_health(request: web.Request) -> web.Response:
    return web.json_response({
        "status": "ok",
        "worker": "online",
        "pdf": PDF_DISPONIVEL,
        "timestamp": datetime.now().isoformat(),
    })


async def handle_relatorio_fatura(request: web.Request) -> web.Response:
    """Gera PDF a partir do fatura_id (UUID da tabela faturas_parsed)."""
    fatura_id = request.match_info.get("fatura_id", "")
    if not fatura_id:
        return web.json_response({"error": "fatura_id obrigatório"}, status=400)

    if not PDF_DISPONIVEL:
        return web.json_response({"error": "reportlab não instalado"}, status=500)

    try:
        db = SupabaseClient()
        loop = asyncio.get_event_loop()

        def _buscar():
            # Tenta primeiro buscar por id em faturas_parsed
            r = db._client.table("faturas_parsed").select(
                "*, faturas_analise(score_eficiencia, potencial_economia_mensal, "
                "potencial_economia_anual, resumo_executivo, alertas, analise_claude)"
            ).eq("id", fatura_id).execute()

            if r.data:
                return r.data[0]

            # Se não encontrar, tenta buscar via faturas_analise.fatura_id
            r2 = db._client.table("faturas_parsed").select(
                "*, faturas_analise(score_eficiencia, potencial_economia_mensal, "
                "potencial_economia_anual, resumo_executivo, alertas, analise_claude)"
            ).eq("faturas_analise.id", fatura_id).execute()

            return r2.data[0] if r2.data else None

        dados_raw = await loop.run_in_executor(None, _buscar)

        if not dados_raw:
            return web.json_response({"error": "Fatura não encontrada"}, status=404)

        # Monta dict para o gerador
        analise = dados_raw.get("faturas_analise") or {}
        if isinstance(analise, list):
            analise = analise[0] if analise else {}

        dados = {
            **dados_raw,
            "score_eficiencia":           analise.get("score_eficiencia"),
            "potencial_economia_mensal":  analise.get("potencial_economia_mensal"),
            "potencial_economia_anual":   analise.get("potencial_economia_anual"),
            "resumo_executivo":           analise.get("resumo_executivo"),
            "alertas":                    analise.get("alertas") or [],
            "analise_claude":             analise.get("analise_claude"),
            "modelo_recomendado":         _inferir_modelo(analise.get("alertas") or []),
        }

        pdf_bytes = await loop.run_in_executor(None, gerar_relatorio, dados)

        uc  = dados_raw.get("uc", "uc")
        mes = (dados_raw.get("mes_referencia") or "").replace("/", "-")
        filename = f"relatorio_{uc}_{mes}.pdf"

        logger.info(f"[API] PDF gerado: {filename} ({len(pdf_bytes)//1024} KB)")

        return web.Response(
            body=pdf_bytes,
            content_type="application/pdf",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    except Exception as exc:
        logger.error(f"[API] Erro ao gerar PDF: {exc}", exc_info=True)
        return web.json_response({"error": str(exc)}, status=500)


async def handle_relatorio_uc(request: web.Request) -> web.Response:
    """Gera PDF da fatura mais recente de uma UC."""
    uc = request.match_info.get("uc", "")
    if not uc:
        return web.json_response({"error": "uc obrigatória"}, status=400)

    if not PDF_DISPONIVEL:
        return web.json_response({"error": "reportlab não instalado"}, status=500)

    try:
        db = SupabaseClient()
        loop = asyncio.get_event_loop()

        def _buscar():
            r = db._client.table("faturas_parsed").select(
                "*, faturas_analise(score_eficiencia, potencial_economia_mensal, "
                "potencial_economia_anual, resumo_executivo, alertas, analise_claude)"
            ).eq("uc", uc).order("parsed_at", desc=True).limit(1).execute()
            return r.data[0] if r.data else None

        dados_raw = await loop.run_in_executor(None, _buscar)

        if not dados_raw:
            return web.json_response({"error": f"Nenhuma fatura encontrada para UC {uc}"}, status=404)

        analise = dados_raw.get("faturas_analise") or {}
        if isinstance(analise, list):
            analise = analise[0] if analise else {}

        dados = {
            **dados_raw,
            "score_eficiencia":          analise.get("score_eficiencia"),
            "potencial_economia_mensal": analise.get("potencial_economia_mensal"),
            "potencial_economia_anual":  analise.get("potencial_economia_anual"),
            "resumo_executivo":          analise.get("resumo_executivo"),
            "alertas":                   analise.get("alertas") or [],
            "analise_claude":            analise.get("analise_claude"),
            "modelo_recomendado":        _inferir_modelo(analise.get("alertas") or []),
        }

        pdf_bytes = await loop.run_in_executor(None, gerar_relatorio, dados)

        mes      = (dados_raw.get("mes_referencia") or "").replace("/", "-")
        filename = f"relatorio_{uc}_{mes}.pdf"

        logger.info(f"[API] PDF gerado: {filename} ({len(pdf_bytes)//1024} KB)")

        return web.Response(
            body=pdf_bytes,
            content_type="application/pdf",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    except Exception as exc:
        logger.error(f"[API] Erro ao gerar PDF para UC {uc}: {exc}", exc_info=True)
        return web.json_response({"error": str(exc)}, status=500)


def _inferir_modelo(alertas: list) -> str:
    codigos = [a.get("codigo", "") for a in alertas]
    if "MERCADO_LIVRE_ELEGIVEL" in codigos:
        return "mercado"
    if "DEMANDA_SUPERDIMENSIONADA" in codigos:
        return "consultoria"
    return "assinatura"


async def criar_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/health",                   handle_health)
    app.router.add_get("/relatorio/{fatura_id}",    handle_relatorio_fatura)
    app.router.add_get("/relatorio/uc/{uc}",        handle_relatorio_uc)

    # CORS para o dashboard Vercel
    async def cors_middleware(app, handler):
        async def middleware(request):
            if request.method == "OPTIONS":
                return web.Response(headers={
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Methods": "GET, OPTIONS",
                    "Access-Control-Allow-Headers": "Content-Type",
                })
            response = await handler(request)
            response.headers["Access-Control-Allow-Origin"] = "*"
            return response
        return middleware

    app.middlewares.append(cors_middleware)
    return app


async def start_api_server(port: int = 8080) -> None:
    """Inicia o servidor HTTP em background."""
    app   = await criar_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site  = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"[API] Servidor HTTP iniciado na porta {port}")
    logger.info(f"[API] Endpoints: /health · /relatorio/{{fatura_id}} · /relatorio/uc/{{uc}}")
