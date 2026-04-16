"""
API HTTP — Servidor leve para geração de PDFs e análise de UCs.
Roda em paralelo com o loop de polling do worker.

Endpoints:
  GET /health                    → status do worker
  GET /relatorio/{fatura_id}     → gera e retorna PDF da fatura
  GET /relatorio/uc/{uc}         → gera PDF da última fatura de uma UC
  GET /analise/uc/{uc}           → diagnóstico executivo IA de uma UC
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
            r = db._client.table("faturas_parsed").select(
                "*, faturas_analise(score_eficiencia, potencial_economia_mensal, "
                "potencial_economia_anual, resumo_executivo, alertas, analise_claude)"
            ).eq("id", fatura_id).execute()

            if r.data:
                return r.data[0]

            r2 = db._client.table("faturas_parsed").select(
                "*, faturas_analise(score_eficiencia, potencial_economia_mensal, "
                "potencial_economia_anual, resumo_executivo, alertas, analise_claude)"
            ).eq("faturas_analise.id", fatura_id).execute()

            return r2.data[0] if r2.data else None

        dados_raw = await loop.run_in_executor(None, _buscar)

        if not dados_raw:
            return web.json_response({"error": "Fatura não encontrada"}, status=404)

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


async def handle_analise_uc(request: web.Request) -> web.Response:
    """Gera diagnóstico executivo completo de uma UC usando IA (GPT-4o-mini)."""
    uc = request.match_info.get("uc", "")
    if not uc:
        return web.json_response({"error": "uc obrigatória"}, status=400)

    try:
        import os, json
        import httpx
        from src.db.client import SupabaseClient

        db = SupabaseClient()
        loop = asyncio.get_event_loop()

        def _buscar():
            faturas = db._client.table("faturas_parsed").select("*").eq("uc", uc)\
                .order("mes_referencia", desc=True).limit(12).execute().data or []
            analises = db._client.table("faturas_analise").select("*").eq("uc", uc)\
                .order("analyzed_at", desc=True).limit(3).execute().data or []
            alertas = db._client.table("alertas_de_fatura").select("*").eq("uc", uc)\
                .eq("resolvido", False).execute().data or []
            return faturas, analises, alertas

        faturas, analises, alertas = await loop.run_in_executor(None, _buscar)

        if not faturas:
            return web.json_response({"error": "UC não encontrada"}, status=404)

        f0 = faturas[0]
        a0 = analises[0] if analises else {}
        nome = f0.get("cliente_nome") or uc
        n = len(faturas)
        custo_medio   = sum(float(f.get("total_a_pagar") or 0) for f in faturas) / n
        consumo_medio = sum(float(f.get("consumo_total_kwh") or 0) for f in faturas) / n
        custo_total   = sum(float(f.get("total_a_pagar") or 0) for f in faturas)
        dem_ctda = float(f0.get("demanda_contratada_fora_ponta_kw") or 0)
        dem_medi = float(f0.get("demanda_medida_fora_ponta_kw") or 0)
        has_gd      = any("GD" in (al.get("tipo") or "") for al in alertas)
        has_reativo = any("reativo" in (al.get("titulo") or "").lower() for al in alertas)
        eleg_ml     = (f0.get("subgrupo") or "").startswith("A") or dem_ctda >= 300
        utilizacao  = f"{round(dem_medi/dem_ctda*100)}%" if dem_ctda > 0 else "—"
        alertas_txt = ", ".join(filter(None, [al.get("titulo") for al in alertas[:5]])) or "Nenhum"
        analise_ant = " | ".join(filter(None, [a.get("analise_claude","") for a in analises[:2]]))[:400]

        prompt = (
            f"Você é especialista em eficiência energética no Brasil, "
            f"focado em clientes da Amazonas Energia em Manaus.\n\n"
            f"Gere um DIAGNÓSTICO EXECUTIVO COMPLETO da UC abaixo. "
            f"Profissional, direto, orientado a negócios.\n\n"
            f"DADOS DA UC:\n"
            f"- Cliente: {nome}\n"
            f"- UC: {uc}\n"
            f"- Subgrupo: {f0.get('subgrupo','?')} | Modalidade: {f0.get('modalidade','?')}\n"
            f"- Demanda Contratada: {f'{dem_ctda} kW' if dem_ctda else 'não disponível'}\n"
            f"- Demanda Medida: {f'{dem_medi} kW' if dem_medi else 'não disponível'}\n"
            f"- Utilização da Demanda: {utilizacao}\n"
            f"- Consumo Médio: {round(consumo_medio):,} kWh/mês\n"
            f"- Custo Médio: R$ {custo_medio:,.2f}/mês\n"
            f"- Gasto 12 meses: R$ {custo_total:,.2f}\n"
            f"- Score Eficiência: {a0.get('score_eficiencia','?')}/100\n"
            f"- Geração Distribuída: {'SIM' if has_gd else 'Não detectada'}\n"
            f"- Energia Reativa: {'SIM — cobranças detectadas' if has_reativo else 'Normal'}\n"
            f"- Elegível Mercado Livre: {'SIM' if eleg_ml else 'Verificar'}\n"
            f"- Alertas: {alertas_txt}\n"
            f"- Análise anterior: {analise_ant}\n\n"
            f"ESTRUTURA OBRIGATÓRIA (use markdown ## para seções):\n"
            f"## 🔍 Diagnóstico Atual\n"
            f"## ⚡ Oportunidades Identificadas\n"
            f"## 🚨 Ações Urgentes\n"
            f"## 🏭 Adequação ao Mercado Livre\n"
            f"## 📋 Escopo de Serviços Recomendado\n"
            f"## 💰 Estimativa de Resultado\n"
            f"## 📅 Próximos Passos\n\n"
            f"Use os números reais. Convença o cliente a fechar contrato com a Trianon Gestão de Energia."
        )

        openai_key = os.environ.get("OPENAI_API_KEY", "")
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {openai_key}", "Content-Type": "application/json"},
                json={
                    "model": "gpt-4o-mini",
                    "max_tokens": 2000,
                    "messages": [{"role": "user", "content": prompt}]
                }
            )
            data = resp.json()
            text = data["choices"][0]["message"]["content"]

        logger.info(f"[API] Análise gerada para UC {uc} — {len(text)} chars")

        return web.json_response(
            {"analise": text, "uc": uc, "cliente": nome},
            dumps=lambda v, **kw: json.dumps(v, ensure_ascii=False)
        )

    except Exception as exc:
        logger.error(f"[API] Erro análise UC {uc}: {exc}", exc_info=True)
        return web.json_response({"error": str(exc)}, status=500)


def _inferir_modelo(alertas: list) -> str:
    codigos = [a.get("codigo", "") for a in alertas]
    if "MERCADO_LIVRE_ELEGIVEL" in codigos:
        return "mercado"
    if "DEMANDA_SUPERDIMENSIONADA" in codigos:
        return "consultoria"
    return "assinatura"


async def handle_db_patch(request: web.Request) -> web.Response:
    """
    PATCH /db/{table}?{filtro}
    Body: JSON com campos a atualizar.
    Usa service_role key — bypassa RLS e allowlist do Supabase.
    """
    import aiohttp as aiohttp_lib
    table = request.match_info.get("table", "")
    query_string = request.query_string  # ex: id=eq.xxx

    if not table or not query_string:
        return web.json_response({"error": "table e filtro obrigatórios"}, status=400)

    # Tabelas permitidas (segurança mínima)
    TABELAS_PERMITIDAS = {"clientes", "faturas_parsed", "faturas_analise"}
    if table not in TABELAS_PERMITIDAS:
        return web.json_response({"error": f"Tabela não permitida: {table}"}, status=403)

    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "Body JSON inválido"}, status=400)

    from src.config import settings
    url = f"{settings.SUPABASE_URL}/rest/v1/{table}?{query_string}"
    headers = {
        "apikey": settings.SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {settings.SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }

    try:
        async with aiohttp_lib.ClientSession() as session:
            async with session.patch(url, json=body, headers=headers) as resp:
                if resp.status in (200, 204):
                    return web.json_response({"ok": True})
                else:
                    text = await resp.text()
                    logger.error(f"[DB PATCH] Erro {resp.status}: {text}")
                    return web.json_response({"error": text}, status=resp.status)
    except Exception as exc:
        logger.error(f"[DB PATCH] Exceção: {exc}")
        return web.json_response({"error": str(exc)}, status=500)


async def criar_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/health",                   handle_health)
    app.router.add_get("/relatorio/{fatura_id}",    handle_relatorio_fatura)
    app.router.add_get("/relatorio/uc/{uc}",        handle_relatorio_uc)
    app.router.add_get("/analise/uc/{uc}",          handle_analise_uc)
    app.router.add_patch("/db/{table}",             handle_db_patch)

    # CORS para o dashboard Vercel
    async def cors_middleware(app, handler):
        async def middleware(request):
            if request.method == "OPTIONS":
                return web.Response(headers={
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Methods": "GET, POST, PATCH, OPTIONS",
                    "Access-Control-Allow-Headers": "Content-Type, Authorization",
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
    logger.info(f"[API] Endpoints: /health · /relatorio/{{fatura_id}} · /relatorio/uc/{{uc}} · /analise/uc/{{uc}}")
