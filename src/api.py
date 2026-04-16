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

from src.config import settings
from src.db.client import SupabaseClient
from src.utils.logger import setup_logger

try:
    from src.reports.gerar_relatorio import gerar_relatorio
    PDF_DISPONIVEL = True
except ImportError:
    PDF_DISPONIVEL = False

try:
    from src.reports.gerar_estudo import gerar_estudo_pdf as gerar_estudo_pptx
    from src.parsers.analyzer_historico import AnalisadorHistorico
    PPTX_DISPONIVEL = True
except ImportError:
    PPTX_DISPONIVEL = False

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


async def handle_estudo_uc(request: web.Request) -> web.Response:
    """Gera Estudo Técnico + Proposta Comercial em PDF para uma UC."""
    uc = request.match_info.get("uc", "")
    if not uc:
        return web.json_response({"error": "uc obrigatória"}, status=400)

    limit = min(int(request.query.get("limit", "12")), 12)
    limit = max(limit, 1)

    if not PPTX_DISPONIVEL:
        return web.json_response({"error": "gerar_estudo não disponível no servidor"}, status=500)

    try:
        import os
        from src.db.client import SupabaseClient

        db = SupabaseClient()
        loop = asyncio.get_event_loop()

        def _buscar():
            faturas = db._client.table("faturas_parsed").select("*").eq("uc", uc)\
                .order("mes_referencia", desc=True).limit(limit).execute().data or []
            alertas = db._client.table("alertas_de_fatura").select("*").eq("uc", uc)\
                .eq("resolvido", False).execute().data or []
            # CORRIGIDO: tabela é "empresas", não "clientes"
            empresas = db._client.table("empresas").select("cnpj")\
                .contains("ucs", [uc]).limit(1).execute().data or []
            cnpj = empresas[0].get("cnpj", "") if empresas else ""
            return faturas, alertas, cnpj

        faturas, alertas, cnpj = await loop.run_in_executor(None, _buscar)

        if not faturas:
            return web.json_response({"error": f"UC {uc} não encontrada"}, status=404)

        # Monta ResultadoHistorico via AnalisadorHistorico (fonte única de verdade)
        resultado = AnalisadorHistorico().analisar(faturas, alertas, cnpj=cnpj)

        # Tenta screenshot da fatura mais recente
        pdf_screenshot = None
        try:
            source_path = faturas[0].get("source_pdf_path", "")
            if source_path:
                def _render_screenshot():
                    import tempfile
                    pdf_bytes = db._client.storage.from_("Faturas").download(source_path)
                    tmp = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
                    tmp.write(pdf_bytes)
                    tmp.close()
                    try:
                        from src.parsers.parser_fatura_ia import _render_pdf_screenshot
                        return _render_pdf_screenshot(tmp.name, page_num=0, dpi=2.5)
                    finally:
                        os.unlink(tmp.name)
                pdf_screenshot = await loop.run_in_executor(None, _render_screenshot)
                logger.info(f"[API] Screenshot renderizado: {len(pdf_screenshot)} bytes")
        except Exception as ss_err:
            logger.warning(f"[API] Screenshot falhou: {ss_err}")

        buf = gerar_estudo_pptx(resultado, pdf_screenshot_bytes=pdf_screenshot)

        nome = resultado.nome.replace(" ", "_")[:30] or uc
        filename = f"Estudo_Tecnico_{nome}.pdf"
        logger.info(f"[API] Estudo PDF gerado para UC {uc} — {resultado.n_faturas} faturas")

        return web.Response(
            body=buf.read(),
            content_type="application/pdf",
            headers={
                "Content-Disposition": f'inline; filename="{filename}"',
                "Access-Control-Allow-Origin": "*",
            }
        )

    except Exception as exc:
        logger.error(f"[API] Erro estudo UC {uc}: {exc}", exc_info=True)
        return web.json_response({"error": str(exc)}, status=500)


async def handle_diagnostico(request: web.Request) -> web.Response:
    """Recebe PDF de fatura via upload, faz parse IA + análise + salva no Supabase + cadastra cliente."""
    try:
        import tempfile, os, json, base64
        from src.parsers.parser_fatura_ia import parse_pdf_ia, _render_pdf_screenshot
        from src.db.client import SupabaseClient

        reader = await request.multipart()
        pdf_bytes = None
        pdf_filename = "fatura.pdf"
        while True:
            part = await reader.next()
            if part is None:
                break
            if part.name == 'pdf' or (part.filename and part.filename.endswith('.pdf')):
                pdf_bytes = await part.read()
                pdf_filename = part.filename or "fatura.pdf"
                break

        if not pdf_bytes:
            return web.json_response({"error": "Envie um arquivo PDF"}, status=400)

        # Salva em temp
        tmp = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
        tmp.write(pdf_bytes)
        tmp.close()
        tmp_path = tmp.name

        try:
            # Parse com IA
            dados = await parse_pdf_ia(tmp_path)

            # Screenshot
            screenshot_b64 = None
            try:
                ss = _render_pdf_screenshot(tmp_path, page_num=0, dpi=2.0)
                screenshot_b64 = base64.b64encode(ss).decode("utf-8")
                dados["screenshot_b64"] = screenshot_b64
            except:
                dados["screenshot_b64"] = None

            # Análise de regras
            try:
                from src.parsers.analyzer_fatura import analisar_fatura
                analise = analisar_fatura(dados)
                dados["analise"] = analise
            except Exception as ae:
                logger.warning(f"[Diag] Análise falhou: {ae}")
                dados["analise"] = {}

            # Análise textual IA
            try:
                from src.ai.ai_provider import gerar_analise_textual
                texto_ia = await gerar_analise_textual(dados, dados.get("analise", {}))
                if texto_ia:
                    dados["analise_textual"] = texto_ia
            except:
                pass

            uc = dados.get("uc", "")
            mes_ref = dados.get("mes_referencia", "")
            nome = dados.get("cliente_nome", "")
            logger.info(f"[Diag] ✓ UC {uc} {mes_ref} — parser: {dados.get('source_parser','?')}")

            # ══════════════════════════════════════════════════════════════
            # SALVAR NO SUPABASE (fatura + cliente)
            # ══════════════════════════════════════════════════════════════
            db = SupabaseClient()
            loop = asyncio.get_event_loop()
            fatura_id = None
            cliente_id = None
            storage_path = ""

            def _salvar():
                nonlocal fatura_id, cliente_id, storage_path

                # 1. Upload PDF pro Storage
                storage_path = ""
                if uc and mes_ref:
                    uc_clean = uc.replace("-", "")
                    mes_clean = mes_ref.replace("/", "-")
                    storage_path = f"faturas/{uc_clean}/{mes_clean}_detalhada.pdf"
                    try:
                        db._client.storage.from_("Faturas").upload(
                            storage_path, pdf_bytes,
                            {"content-type": "application/pdf", "upsert": "true"}
                        )
                    except:
                        try:
                            db._client.storage.from_("Faturas").update(
                                storage_path, pdf_bytes,
                                {"content-type": "application/pdf"}
                            )
                        except Exception as ue:
                            logger.warning(f"[Diag] Upload storage falhou: {ue}")
                            storage_path = ""

                # 2. Salva fatura parseada
                fatura_data = {
                    "uc": uc,
                    "mes_referencia": mes_ref,
                    "cliente_nome": nome,
                    "subgrupo": dados.get("subgrupo", ""),
                    "modalidade": dados.get("modalidade", ""),
                    "grupo": dados.get("grupo", ""),
                    "total_a_pagar": float(dados.get("total_a_pagar") or 0),
                    "consumo_total_kwh": float(dados.get("consumo_total_kwh") or 0),
                    "consumo_ponta_kwh": float(dados.get("consumo_ponta_kwh") or 0),
                    "consumo_fora_ponta_kwh": float(dados.get("consumo_fora_ponta_kwh") or 0),
                    "demanda_contratada_ponta_kw": float(dados.get("demanda_contratada_ponta_kw") or 0),
                    "demanda_contratada_fora_ponta_kw": float(dados.get("demanda_contratada_fora_ponta_kw") or 0),
                    "demanda_medida_ponta_kw": float(dados.get("demanda_medida_ponta_kw") or 0),
                    "demanda_medida_fora_ponta_kw": float(dados.get("demanda_medida_fora_ponta_kw") or 0),
                    "ufer_fora_ponta_kvarh": float(dados.get("ufer_fora_ponta_kvarh") or 0),
                    "cosip_valor": float(dados.get("cosip_valor") or 0),
                    "bandeira_tarifaria": dados.get("bandeira_tarifaria", ""),
                    "tarifa_demanda": float(dados.get("tarifa_demanda") or 0),
                    "itens_faturados": dados.get("itens_faturados", []),
                    "source_pdf_path": storage_path,
                    "source_parser": dados.get("source_parser", "upload"),
                }
                # Remove chaves com valor None
                fatura_data = {k: v for k, v in fatura_data.items() if v is not None}

                try:
                    # Upsert por uc+mes_referencia
                    existing = db._client.table("faturas_parsed").select("id")\
                        .eq("uc", uc).eq("mes_referencia", mes_ref).limit(1).execute().data
                    if existing:
                        fatura_id = existing[0]["id"]
                        db._client.table("faturas_parsed").update(fatura_data)\
                            .eq("id", fatura_id).execute()
                    else:
                        r = db._client.table("faturas_parsed").insert(fatura_data).execute()
                        fatura_id = r.data[0]["id"] if r.data else None
                except Exception as fe:
                    logger.warning(f"[Diag] Salvar fatura: {fe}")

                # 3. Cadastra/atualiza cliente
                if uc and nome:
                    try:
                        cnpj = dados.get("cnpj", "") or ""
                        # Busca por UC ou CNPJ
                        q = db._client.table("clientes").select("id,ucs")\
                            .contains("ucs", [uc]).limit(1).execute().data
                        if not q and cnpj:
                            q = db._client.table("clientes").select("id,ucs")\
                                .eq("cnpj", cnpj).limit(1).execute().data

                        if q:
                            # Atualiza
                            cl = q[0]
                            ucs = cl.get("ucs", []) or []
                            if uc not in ucs:
                                ucs.append(uc)
                            db._client.table("clientes").update({
                                "nome": nome, "ucs": ucs,
                                "subgrupo": dados.get("subgrupo", ""),
                                "modalidade": dados.get("modalidade", ""),
                                "demanda_kw": float(dados.get("demanda_contratada_fora_ponta_kw") or 0),
                                "custo_medio": float(dados.get("total_a_pagar") or 0),
                            }).eq("id", cl["id"]).execute()
                            cliente_id = cl["id"]
                        else:
                            # Cria novo
                            r = db._client.table("clientes").insert({
                                "nome": nome, "cnpj": cnpj, "ucs": [uc],
                                "subgrupo": dados.get("subgrupo", ""),
                                "modalidade": dados.get("modalidade", ""),
                                "demanda_kw": float(dados.get("demanda_contratada_fora_ponta_kw") or 0),
                                "custo_medio": float(dados.get("total_a_pagar") or 0),
                                "status": "prospecto",
                            }).execute()
                            cliente_id = r.data[0]["id"] if r.data else None
                    except Exception as ce:
                        logger.warning(f"[Diag] Cadastrar cliente: {ce}")

                # 4. Salva análise
                if fatura_id and dados.get("analise"):
                    try:
                        an = dados["analise"]
                        analise_data = {
                            "fatura_id": fatura_id, "uc": uc, "mes_referencia": mes_ref,
                            "score_eficiencia": an.get("score_eficiencia"),
                            "potencial_economia_mensal": an.get("potencial_economia_mensal"),
                            "potencial_economia_anual": an.get("potencial_economia_anual"),
                            "resumo_executivo": an.get("resumo_executivo", ""),
                            "alertas": json.dumps(an.get("alertas", [])),
                            "analise_claude": dados.get("analise_textual", ""),
                        }
                        db._client.table("faturas_analise").upsert(
                            analise_data, on_conflict="fatura_id"
                        ).execute()
                    except Exception as ae2:
                        logger.warning(f"[Diag] Salvar análise: {ae2}")

                # 5. Salva alertas
                if fatura_id and dados.get("analise", {}).get("alertas"):
                    try:
                        for al in dados["analise"]["alertas"]:
                            al_data = {
                                "fatura_id": fatura_id, "uc": uc,
                                "titulo": al.get("titulo", ""),
                                "descricao": al.get("descricao", ""),
                                "severidade": al.get("severidade", "info"),
                                "codigo": al.get("codigo", ""),
                                "economia_mensal_r": al.get("economia_mensal_r"),
                                "economia_anual_r": al.get("economia_anual_r"),
                                "acao_recomendada": al.get("acao_recomendada", ""),
                            }
                            db._client.table("alertas_de_fatura").insert(al_data).execute()
                    except Exception as ale:
                        logger.warning(f"[Diag] Salvar alertas: {ale}")

            await loop.run_in_executor(None, _salvar)

            dados["_saved"] = {
                "fatura_id": str(fatura_id) if fatura_id else None,
                "cliente_id": str(cliente_id) if cliente_id else None,
                "storage_path": storage_path if uc else None,
            }
            logger.info(f"[Diag] Salvo: fatura={fatura_id}, cliente={cliente_id}")

            return web.json_response(dados, headers={"Access-Control-Allow-Origin": "*"})

        finally:
            os.unlink(tmp_path)

    except Exception as exc:
        logger.error(f"[Diag] Erro: {exc}", exc_info=True)
        return web.json_response({"error": str(exc)}, status=500)


async def handle_db_patch(request: web.Request) -> web.Response:
    """
    PATCH /db/{table}?{filtro}
    Proxy para updates no Supabase usando service_role key.
    Necessário porque o Supabase allowlist bloqueia writes diretos do browser.
    """
    import aiohttp as _aiohttp
    table = request.match_info.get("table", "")
    qs = request.query_string

    if not table or not qs:
        return web.json_response({"error": "tabela e filtro obrigatórios"}, status=400)

    TABELAS_PERMITIDAS = {"clientes", "faturas_parsed", "faturas_analise"}
    if table not in TABELAS_PERMITIDAS:
        return web.json_response({"error": f"tabela não permitida: {table}"}, status=403)

    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "body JSON inválido"}, status=400)

    url = f"{settings.SUPABASE_URL}/rest/v1/{table}?{qs}"
    headers = {
        "apikey": settings.SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {settings.SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }

    try:
        async with _aiohttp.ClientSession() as session:
            async with session.patch(url, json=body, headers=headers) as resp:
                if resp.status in (200, 204):
                    return web.json_response({"ok": True})
                text = await resp.text()
                logger.error(f"[DB PATCH] {resp.status}: {text}")
                return web.json_response({"error": text}, status=resp.status)
    except Exception as exc:
        logger.error(f"[DB PATCH] Exceção: {exc}")
        return web.json_response({"error": str(exc)}, status=500)


async def criar_app() -> web.Application:
    app = web.Application(client_max_size=20*1024*1024)  # 20MB max upload
    app.router.add_get("/health",                   handle_health)
    app.router.add_get("/relatorio/{fatura_id}",    handle_relatorio_fatura)
    app.router.add_get("/relatorio/uc/{uc}",        handle_relatorio_uc)
    app.router.add_get("/analise/uc/{uc}",          handle_analise_uc)
    app.router.add_get("/estudo/uc/{uc}",           handle_estudo_uc)
    app.router.add_post("/diagnostico",             handle_diagnostico)
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
