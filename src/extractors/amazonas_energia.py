"""
Extrator Amazonas Energia — com parse + análise integrados (Fase 2).
Fluxo por fatura: download → desbloqueia → upload Storage → parse → analisa → grava tabelas.
"""

from __future__ import annotations

import asyncio
import email
import imaplib
import logging
import random
import tempfile
import time
from email import policy
from pathlib import Path
from typing import Optional

import httpx

from src.config import settings
from src.db.client import SupabaseClient
from src.utils.logger import setup_logger

# ── Importa Fase 2 ────────────────────────────────────────────────────────────
try:
    from src.parsers.parser_fatura import parse_pdf
    from src.parsers.analyzer_fatura import analisar_fatura
    FASE2_DISPONIVEL = True
except ImportError:
    FASE2_DISPONIVEL = False
    logging.getLogger(__name__).warning(
        "parser_fatura / analyzer_fatura não encontrados em src/parsers/. "
        "Parse e análise desativados."
    )

# ── Importa Fase 3 ────────────────────────────────────────────────────────────
try:
    from src.ai.ai_provider import gerar_analise_textual
    FASE3_DISPONIVEL = True
except ImportError:
    FASE3_DISPONIVEL = False
    logging.getLogger(__name__).warning(
        "ai_provider não encontrado em src/ai/. "
        "Geração de texto desativada."
    )

logger = setup_logger(__name__)

API_URL = "https://api-agencia.amazonasenergia.com"

MOBILE_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Accept-Encoding": "gzip",
    "Connection": "Keep-Alive",
    "User-Agent": "okhttp/4.9.0",
}

SAAS_EMAIL          = "saasfaturasam@gmail.com"
SAAS_EMAIL_PASSWORD = "yfwe fsav ooyc qhje"
IMAP_SERVER         = "imap.gmail.com"


class AmazonasEnergiaHTTPExtractor:
    """Extrator 100% automático — login + download via API REST + parse + análise por fatura."""

    def __init__(self, db: SupabaseClient, task: dict):
        self.db          = db
        self.task        = task
        self.task_id: str = task["id"]
        self.credentials: dict = task.get("credentials", {})
        self._tmp_dir    = Path(tempfile.mkdtemp(prefix=f"task_{self.task_id}_"))

    async def run(self) -> list[dict]:
        cpf   = self.credentials.get("cpf_cnpj", "")
        senha = self.credentials.get("senha", "")
        if not cpf or not senha:
            raise Exception("CPF/CNPJ e senha são obrigatórios.")

        pdf_password = cpf[:5]

        async with httpx.AsyncClient(base_url=API_URL, timeout=30) as client:
            jwt, clientes = await self._login(client, cpf, senha)

            clientes_ucs = []
            for cli in clientes:
                id_cli = cli.get("ID_CLIENTE")
                for uc in cli.get("UNIDADES_CONSUMIDORAS", []):
                    clientes_ucs.append({"id_cliente": id_cli, "id_uc": uc["ID_UC"]})

            selected_ucs = self.credentials.get("selected_ucs", [])
            if selected_ucs:
                selected_set = set(str(u) for u in selected_ucs)
                clientes_ucs = [c for c in clientes_ucs if str(c["id_uc"]) in selected_set]

            meses_limit = int(self.credentials.get("meses", settings.MAX_INVOICES_MONTHS))

            logger.info(
                f"[Task {self.task_id}] Login OK! "
                f"{len(clientes_ucs)} UC(s) | {meses_limit} meses | "
                f"fase2={'ON' if FASE2_DISPONIVEL else 'OFF'}"
            )

            auth_headers = {
                **MOBILE_HEADERS,
                "Authorization": f"Bearer {jwt}",
                "Origin": "https://agencia.amazonasenergia.com",
                "Referer": "https://agencia.amazonasenergia.com/",
            }

            all_pdfs: list[dict] = []

            for item in clientes_ucs:
                id_cliente = item["id_cliente"]
                id_uc      = item["id_uc"]
                logger.info(f"[Task {self.task_id}] Processando UC {id_uc} (Cliente {id_cliente})")

                uc_headers = {
                    **auth_headers,
                    "X-Client-Id":     str(id_cliente),
                    "X-Consumer-Unit": str(id_uc),
                }

                abertas = await self._fetch_faturas(client, uc_headers, "abertas")
                pagas   = await self._fetch_faturas(client, uc_headers, "pagas")

                logger.info(
                    f"[Task {self.task_id}] UC {id_uc}: "
                    f"{len(pagas)} pagas + {len(abertas)} abertas"
                )

                faturas = abertas + pagas[:meses_limit]

                for fatura in faturas:
                    pdf = await self._download_fatura_detalhada(
                        client, uc_headers, id_uc, fatura, cpf, pdf_password
                    )
                    if pdf:
                        all_pdfs.append(pdf)
                    else:
                        pdf = await self._download_fatura_simples(
                            client, uc_headers, id_uc, fatura
                        )
                        if pdf:
                            all_pdfs.append(pdf)

                    await asyncio.sleep(random.uniform(2.0, 5.0))

            return all_pdfs

    # ── Login ─────────────────────────────────────────────────────────────────

    async def _login(self, client: httpx.AsyncClient, cpf: str, senha: str) -> tuple:
        logger.info(f"[Task {self.task_id}] Fazendo login via API mobile...")
        resp = await client.post(
            "/api/autenticacao/login",
            headers=MOBILE_HEADERS,
            json={"CPF_CNPJ": cpf, "SENHA": senha},
        )
        if resp.status_code == 400:
            body = resp.text
            if "invalida" in body.lower() or "incorret" in body.lower():
                raise Exception("Credenciais inválidas. Verifique CPF e senha.")
            raise Exception(f"Erro no login: {body[:200]}")
        if resp.status_code != 200:
            raise Exception(f"Erro no login: status {resp.status_code}")

        data  = resp.json()
        token = data.get("TOKEN", "")
        if not token:
            raise Exception("Login retornou sem TOKEN.")

        clientes_raw = data.get("CLIENTES", [])
        if not clientes_raw:
            clientes_raw = await self._fetch_clientes(client, token)

        return token, clientes_raw

    async def _fetch_clientes(self, client: httpx.AsyncClient, jwt: str) -> list:
        try:
            resp = await client.get(
                "/api/atualizacao-cadastral",
                headers={**MOBILE_HEADERS, "Authorization": f"Bearer {jwt}"},
            )
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list):
                    return data
                if "CLIENTES" in data:
                    return data["CLIENTES"]
        except Exception:
            pass

        import json, base64
        try:
            payload = json.loads(base64.b64decode(jwt.split('.')[1] + '=='))
            ucs     = payload.get("UCS", [])
            id_user = payload.get("ID", 0)
            return [{"ID_CLIENTE": id_user, "UNIDADES_CONSUMIDORAS": [{"ID_UC": uc} for uc in ucs]}]
        except Exception:
            raise Exception("Não foi possível obter lista de UCs.")

    async def _fetch_faturas(self, client, uc_headers, tipo):
        try:
            resp = await client.get(f"/api/faturas/{tipo}", headers=uc_headers)
            if resp.status_code == 401:
                raise Exception("JWT expirado.")
            if resp.status_code != 200:
                return []
            data = resp.json()
            return data if isinstance(data, list) else data.get("data", [])
        except httpx.HTTPError as exc:
            logger.error(f"[Task {self.task_id}] Erro faturas {tipo}: {exc}")
            return []

    # ── Download detalhada ────────────────────────────────────────────────────

    async def _download_fatura_detalhada(
        self, client, uc_headers, id_uc, fatura, cpf, pdf_password
    ) -> Optional[dict]:
        """Baixa fatura detalhada via email, desbloqueia, faz upload, parseia e analisa."""
        mes_ano       = fatura.get("MES_ANO_REFERENCIA", "")
        fatura_diversa = fatura.get("FATURA_DIVERSA", 0)
        if not mes_ano:
            return None

        parts = mes_ano.split("/")
        if len(parts) != 2:
            return None

        mes_ano_api   = f"{parts[1]}-{parts[0]}-01"
        tag           = f"UC{id_uc}_{parts[1]}{parts[0]}"
        email_destino = f"saasfaturasam+{tag}@gmail.com"

        try:
            # 1. Dispara envio
            resp = await client.post(
                "/api/faturas/baixar-completa",
                headers=uc_headers,
                json={"MES_ANO": mes_ano_api, "FATURA_DIVERSA": fatura_diversa, "EMAIL": email_destino},
            )
            if resp.status_code != 200:
                logger.warning(
                    f"[Task {self.task_id}] Detalhada {mes_ano} UC {id_uc}: "
                    f"status {resp.status_code} - {resp.text[:100]}"
                )
                return None

            logger.info(f"[Task {self.task_id}] Detalhada {mes_ano} UC {id_uc}: email enviado. Aguardando...")

            # 2. Aguarda email
            loop      = asyncio.get_event_loop()
            pdf_bytes = await loop.run_in_executor(None, self._wait_for_email, tag, 180)
            if not pdf_bytes:
                logger.warning(f"[Task {self.task_id}] Email não chegou para {mes_ano} UC {id_uc}")
                return None

            # 3. Desbloqueia PDF
            pdf_bytes = await loop.run_in_executor(None, self._unlock_pdf, pdf_bytes, pdf_password)

            # 4. Salva localmente
            mes_str    = mes_ano.replace("/", "-")
            filename   = f"detalhada_{id_uc}_{mes_str}.pdf"
            local_path = self._tmp_dir / filename
            local_path.write_bytes(pdf_bytes)
            size_kb = len(pdf_bytes) // 1024
            logger.info(f"[Task {self.task_id}] ✓ {filename} ({size_kb} KB) [DETALHADA]")

            # 5. Upload Supabase Storage
            storage_path = f"faturas/{id_uc}/{mes_str}_detalhada.pdf"
            storage_url  = await self.db.upload_pdf(
                local_path=local_path, storage_path=storage_path, task_id=self.task_id,
            )

            # 6. Parse + Análise (Fase 2) — não bloqueia o fluxo se falhar
            await self._parse_and_analyze(local_path, storage_path)

            return {
                "mes_referencia": mes_ano,
                "uc":             id_uc,
                "storage_url":    storage_url,
                "filename":       filename,
                "size_kb":        size_kb,
                "situacao":       fatura.get("SITUACAO", ""),
                "valor":          fatura.get("VALOR_TOTAL", 0),
                "tipo":           "detalhada",
            }

        except Exception as exc:
            logger.error(f"[Task {self.task_id}] Erro detalhada {mes_ano} UC {id_uc}: {exc}")
            return None

    # ── Download simples (fallback) ───────────────────────────────────────────

    async def _download_fatura_simples(self, client, uc_headers, id_uc, fatura) -> Optional[dict]:
        """Fallback: baixa via de pagamento (boleto). Sem parse detalhado — PDF incompleto."""
        mes_ano        = fatura.get("MES_ANO_REFERENCIA", "")
        fatura_diversa = fatura.get("FATURA_DIVERSA", 0)
        if not mes_ano:
            return None

        mes_ano_param = mes_ano.replace("/", "")

        try:
            resp = await client.post(
                "/api/faturas/baixar",
                headers=uc_headers,
                json={"MES_ANO": mes_ano_param, "FATURA_DIVERSA": fatura_diversa},
                timeout=60,
            )
            if resp.status_code != 200 or len(resp.content) < 500:
                return None

            mes_str    = mes_ano.replace("/", "-")
            filename   = f"fatura_{id_uc}_{mes_str}.pdf"
            local_path = self._tmp_dir / filename
            local_path.write_bytes(resp.content)
            size_kb = len(resp.content) // 1024
            logger.info(f"[Task {self.task_id}] ✓ {filename} ({size_kb} KB) [SIMPLES]")

            storage_path = f"faturas/{id_uc}/{mes_str}.pdf"
            storage_url  = await self.db.upload_pdf(
                local_path=local_path, storage_path=storage_path, task_id=self.task_id,
            )

            # Tenta parsear mesmo a fatura simples (pode ter dados suficientes)
            await self._parse_and_analyze(local_path, storage_path)

            return {
                "mes_referencia": mes_ano,
                "uc":             id_uc,
                "storage_url":    storage_url,
                "filename":       filename,
                "size_kb":        size_kb,
                "situacao":       fatura.get("SITUACAO", ""),
                "valor":          fatura.get("VALOR_TOTAL", 0),
                "tipo":           "simples",
            }

        except Exception as exc:
            logger.error(f"[Task {self.task_id}] Erro download {mes_ano}: {exc}")
            return None

    # ── Parse + Análise (Fase 2) ──────────────────────────────────────────────

    async def _parse_and_analyze(
        self,
        local_path: Path,
        storage_path: str,
    ) -> None:
        """
        Parseia o PDF local e grava os dados + análise nas tabelas da Fase 2.
        Chamado individualmente para cada fatura baixada.
        Erros são logados mas NÃO propagados — não interrompem o download das demais.
        """
        if not FASE2_DISPONIVEL:
            return

        try:
            loop = asyncio.get_event_loop()

            # Parse em thread separada (pdfplumber é CPU-bound)
            parsed = await loop.run_in_executor(None, parse_pdf, local_path)

            uc  = parsed.get("uc", "?")
            mes = parsed.get("mes_referencia", "?")

            if not uc or not mes:
                logger.warning(f"[Fase2] Parse incompleto para {local_path.name} — UC ou mês não identificados")
                return

            # Salva dados estruturados
            fatura_id = await self.db.save_fatura_parsed(
                parsed=parsed,
                extraction_id=self.task_id,
                source_pdf_path=storage_path,
            )

            if not fatura_id:
                logger.warning(f"[Fase2] Não foi possível salvar faturas_parsed para UC {uc} {mes}")
                return

            # Análise de regras de negócio (síncrono mas rápido — < 5ms)
            analise = analisar_fatura(parsed)

            await self.db.save_fatura_analise(
                fatura_id=fatura_id,
                analise=analise,
            )

            economia = analise.get("potencial_economia_anual", 0)
            score    = analise.get("score_eficiencia", 100)
            logger.info(
                f"[Fase2] ✓ UC {uc} {mes} | score={score} | "
                f"economia potencial=R${economia:,.0f}/ano"
            )

            # Fase 3 — texto executivo em linguagem natural via IA
            if FASE3_DISPONIVEL:
                texto = await gerar_analise_textual(parsed, analise)
                if texto:
                    await self.db.save_analise_textual(fatura_id, texto)
                    logger.info(f"[Fase3] ✓ Texto gerado para UC {uc} {mes}")

        except Exception as exc:
            # Nunca interrompe o fluxo de download
            logger.error(f"[Fase2] Erro no parse/análise de {local_path.name}: {exc}", exc_info=True)

    # ── IMAP ─────────────────────────────────────────────────────────────────

    def _wait_for_email(self, tag: str, timeout: int = 180) -> Optional[bytes]:
        """Aguarda email com a tag chegar no Gmail via IMAP."""
        start         = time.time()
        wait_interval = 15

        time.sleep(30)  # Espera inicial

        while (time.time() - start) < timeout:
            try:
                mail = imaplib.IMAP4_SSL(IMAP_SERVER)
                mail.login(SAAS_EMAIL, SAAS_EMAIL_PASSWORD)
                mail.select("INBOX")

                status, messages = mail.search(None, f'(TO "saasfaturasam+{tag}@gmail.com")')

                if status == "OK" and messages[0]:
                    msg_ids   = messages[0].split()
                    latest_id = msg_ids[-1]
                    status, msg_data = mail.fetch(latest_id, "(RFC822)")

                    if status == "OK":
                        msg = email.message_from_bytes(msg_data[0][1], policy=policy.default)

                        for part in msg.walk():
                            if part.get_content_type() == "application/pdf":
                                pdf_data = part.get_payload(decode=True)
                                if pdf_data:
                                    mail.store(latest_id, "+FLAGS", "\\Seen")
                                    mail.logout()
                                    return pdf_data

                            filename = part.get_filename() or ""
                            if filename.lower().endswith(".pdf"):
                                pdf_data = part.get_payload(decode=True)
                                if pdf_data:
                                    mail.store(latest_id, "+FLAGS", "\\Seen")
                                    mail.logout()
                                    return pdf_data

                mail.logout()
            except Exception as exc:
                logger.debug(f"IMAP check failed: {exc}")

            time.sleep(wait_interval)

        return None

    def _unlock_pdf(self, pdf_bytes: bytes, password: str) -> bytes:
        """Desbloqueia PDF protegido por senha."""
        try:
            import pikepdf
            import io

            pdf    = pikepdf.open(io.BytesIO(pdf_bytes), password=password)
            output = io.BytesIO()
            pdf.save(output)
            pdf.close()
            return output.getvalue()
        except Exception as exc:
            logger.warning(f"PDF unlock failed: {exc} — retornando original")
            return pdf_bytes
