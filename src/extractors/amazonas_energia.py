"""
Extrator Amazonas Energia — versão HTTP pura (sem Playwright/browser).

Usa a API REST diretamente com JWT para extrair faturas.
Sem captcha, sem browser, sem stealth.

API mapeada via DevTools:
  POST /api/autenticacao/login          → JWT (captcha necessário, feito externamente)
  GET  /api/faturas/pagas               → lista faturas pagas
  GET  /api/faturas/abertas             → lista faturas abertas
  POST /api/faturas/baixar              → download do PDF da fatura

Headers obrigatórios em cada request:
  Authorization: Bearer {jwt}
  X-Client-Id: {id_cliente}
  X-Consumer-Unit: {id_uc}
"""

from __future__ import annotations

import asyncio
import logging
import tempfile
from pathlib import Path
from typing import Optional

import httpx

from src.config import settings
from src.db.client import SupabaseClient
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

API_URL = "https://api-agencia.amazonasenergia.com"


class AmazonasEnergiaHTTPExtractor:
    """Extrator de faturas via API REST — sem browser, sem captcha."""

    def __init__(self, db: SupabaseClient, task: dict):
        self.db = db
        self.task = task
        self.task_id: str = task["id"]
        self.credentials: dict = task.get("credentials", {})
        self._tmp_dir = Path(tempfile.mkdtemp(prefix=f"task_{self.task_id}_"))

    async def run(self) -> list[dict]:
        """
        Fluxo principal:
        1. Obtém JWT (das credentials ou via login automático)
        2. Descobre clientes e UCs
        3. Para cada UC: busca faturas abertas + pagas
        4. Baixa cada PDF e faz upload ao Supabase Storage
        """
        jwt = self.credentials.get("jwt", "")

        # Se não tem JWT mas tem CPF+senha, tenta login via API
        if not jwt:
            cpf = self.credentials.get("cpf_cnpj", "")
            senha = self.credentials.get("senha", "")
            if not cpf or not senha:
                raise Exception(
                    "Credenciais insuficientes. Forneça 'jwt' ou 'cpf_cnpj' + 'senha'."
                )
            login_data = await self._do_login_via_playwright(cpf, senha)
            jwt = login_data["TOKEN"]

            # Salva JWT + dados completos na tarefa para reuso
            all_ucs = []
            id_cliente = ""
            for cli in login_data.get("CLIENTES", []):
                if not id_cliente:
                    id_cliente = str(cli["ID_CLIENTE"])
                for uc in cli.get("UNIDADES_CONSUMIDORAS", []):
                    all_ucs.append(str(uc["ID_UC"]))

            self.credentials = {
                **self.credentials,
                "jwt": jwt,
                "clientes": login_data.get("CLIENTES", []),
                "id_cliente": id_cliente,
                "ucs": all_ucs,
            }
            logger.info(
                f"[Task {self.task_id}] Login OK! JWT salvo. "
                f"{len(all_ucs)} UC(s) encontrada(s)."
            )

        # Headers base (compartilhados em todas as requests)
        base_headers = {
            "Authorization": f"Bearer {jwt}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Origin": "https://agencia.amazonasenergia.com",
            "Referer": "https://agencia.amazonasenergia.com/",
        }

        async with httpx.AsyncClient(
            base_url=API_URL,
            headers=base_headers,
            timeout=30,
        ) as client:

            # 1. Descobre clientes e UCs
            clientes_ucs = self._parse_clientes_from_credentials()
            logger.info(
                f"[Task {self.task_id}] "
                f"{len(clientes_ucs)} par(es) cliente/UC para processar."
            )

            all_pdfs: list[dict] = []

            # 2. Para cada cliente+UC, busca e baixa faturas
            for item in clientes_ucs:
                id_cliente = item["id_cliente"]
                id_uc = item["id_uc"]
                logger.info(
                    f"[Task {self.task_id}] Processando UC {id_uc} "
                    f"(Cliente {id_cliente})"
                )

                uc_headers = {
                    "X-Client-Id": str(id_cliente),
                    "X-Consumer-Unit": str(id_uc),
                }

                # Busca faturas abertas PRIMEIRO (são as mais importantes)
                faturas_abertas = await self._fetch_faturas(client, uc_headers, "abertas")
                faturas_pagas = await self._fetch_faturas(client, uc_headers, "pagas")

                logger.info(
                    f"[Task {self.task_id}] UC {id_uc}: "
                    f"{len(faturas_pagas)} pagas + {len(faturas_abertas)} abertas"
                )

                # Abertas TODAS (sem limite) + pagas até o limite
                faturas_para_baixar = faturas_abertas + faturas_pagas[:settings.MAX_INVOICES_MONTHS]

                # 3. Baixa cada fatura
                for fatura in faturas_para_baixar:
                    pdf = await self._download_fatura(
                        client, uc_headers, id_uc, fatura
                    )
                    if pdf:
                        all_pdfs.append(pdf)
                    await asyncio.sleep(0.5)

            return all_pdfs

    def _parse_clientes_from_credentials(self) -> list[dict]:
        """
        Extrai pares (id_cliente, id_uc) das credentials.

        Suporta dois formatos:
        1. Simples: {"id_cliente": "123", "unidade_consumidora": "456"}
        2. Múltiplas UCs: {"id_cliente": "123", "ucs": ["456", "789"]}
        3. Login response salva: {"clientes": [{...}]}
        """
        # Formato 3: resposta completa do login salva
        if "clientes" in self.credentials:
            result = []
            for cli in self.credentials["clientes"]:
                id_cli = cli.get("ID_CLIENTE", cli.get("id_cliente"))
                ucs = cli.get("UNIDADES_CONSUMIDORAS", cli.get("ucs", []))
                for uc in ucs:
                    if isinstance(uc, dict):
                        result.append({"id_cliente": id_cli, "id_uc": uc["ID_UC"]})
                    else:
                        result.append({"id_cliente": id_cli, "id_uc": int(uc)})
            return result

        id_cliente = self.credentials.get("id_cliente", "")
        if not id_cliente:
            raise Exception(
                "id_cliente não fornecido nas credentials. "
                "Salve os dados do login (ID_CLIENTE e UCs) no campo credentials."
            )

        # Formato 2: múltiplas UCs
        if "ucs" in self.credentials:
            return [
                {"id_cliente": int(id_cliente), "id_uc": int(uc)}
                for uc in self.credentials["ucs"]
            ]

        # Formato 1: UC única
        uc = self.credentials.get("unidade_consumidora", "")
        if not uc:
            raise Exception("unidade_consumidora não fornecido nas credentials.")

        return [{"id_cliente": int(id_cliente), "id_uc": int(uc)}]

    async def _fetch_faturas(
        self,
        client: httpx.AsyncClient,
        uc_headers: dict,
        tipo: str,
    ) -> list[dict]:
        """Busca faturas pagas ou abertas de uma UC."""
        try:
            resp = await client.get(
                f"/api/faturas/{tipo}",
                headers=uc_headers,
            )

            if resp.status_code == 401:
                raise Exception(
                    "JWT expirado (401). Necessário re-autenticar. "
                    "Faça login novamente e atualize o JWT na tarefa."
                )

            if resp.status_code != 200:
                logger.warning(
                    f"[Task {self.task_id}] Erro faturas {tipo}: "
                    f"status={resp.status_code}"
                )
                return []

            data = resp.json()

            # A API pode retornar lista direta ou {"data": [...]}
            if isinstance(data, list):
                return data
            return data.get("data", [])

        except httpx.HTTPError as exc:
            logger.error(f"[Task {self.task_id}] Erro HTTP faturas {tipo}: {exc}")
            return []

    async def _download_fatura(
        self,
        client: httpx.AsyncClient,
        uc_headers: dict,
        id_uc: int,
        fatura: dict,
    ) -> Optional[dict]:
        """Baixa o PDF de uma fatura e faz upload ao Supabase Storage."""
        mes_ano = fatura.get("MES_ANO_REFERENCIA", "")
        fatura_diversa = fatura.get("FATURA_DIVERSA", 0)

        if not mes_ano:
            return None

        # Converte "02/2026" → "022026"
        mes_ano_param = mes_ano.replace("/", "")

        try:
            resp = await client.post(
                "/api/faturas/baixar",
                headers=uc_headers,
                json={
                    "MES_ANO": mes_ano_param,
                    "FATURA_DIVERSA": fatura_diversa,
                },
                timeout=60,
            )

            if resp.status_code != 200:
                logger.warning(
                    f"[Task {self.task_id}] Download falhou {mes_ano}: "
                    f"status={resp.status_code}"
                )
                return None

            # Valida se é realmente um PDF
            if len(resp.content) < 500:
                logger.warning(
                    f"[Task {self.task_id}] Resposta muito pequena para {mes_ano}: "
                    f"{len(resp.content)} bytes — provavelmente não é PDF"
                )
                return None

            # Salva localmente
            mes_str = mes_ano.replace("/", "-")
            filename = f"fatura_{id_uc}_{mes_str}.pdf"
            local_path = self._tmp_dir / filename
            local_path.write_bytes(resp.content)

            size_kb = len(resp.content) // 1024
            logger.info(f"[Task {self.task_id}] ✓ {filename} ({size_kb} KB)")

            # Upload para Supabase Storage
            storage_path = f"faturas/{id_uc}/{mes_str}.pdf"
            storage_url = await self.db.upload_pdf(
                local_path=local_path,
                storage_path=storage_path,
                task_id=self.task_id,
            )

            return {
                "mes_referencia": mes_ano,
                "uc": id_uc,
                "storage_url": storage_url,
                "filename": filename,
                "size_kb": size_kb,
                "situacao": fatura.get("SITUACAO", ""),
                "valor": fatura.get("VALOR_TOTAL", 0),
            }

        except Exception as exc:
            logger.error(f"[Task {self.task_id}] Erro download {mes_ano}: {exc}")
            return None

    async def _do_login_via_playwright(self, cpf: str, senha: str) -> dict:
        """
        Faz login via Playwright (Chrome real) para capturar o JWT.
        
        O captcha é resolvido pelo browser nativamente.
        Com headless=false, um operador pode intervir se necessário.
        Uma vez obtido o JWT, tudo roda via httpx.
        """
        from playwright.async_api import async_playwright

        logger.info(f"[Task {self.task_id}] Iniciando login via Playwright...")

        login_result = {}

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                channel="chrome",
                headless=settings.BROWSER_HEADLESS,
                args=["--disable-blink-features=AutomationControlled"],
            )

            context = await browser.new_context(
                viewport={"width": 1366, "height": 768},
                locale="pt-BR",
                timezone_id="America/Manaus",
                geolocation={"latitude": -3.1190, "longitude": -60.0217},
                permissions=["geolocation"],
            )

            # Aplica stealth
            try:
                from playwright_stealth import Stealth
                stealth = Stealth(init_scripts_only=True)
                await stealth.apply_stealth_async(context)
            except ImportError:
                pass

            page = await context.new_page()

            # Intercepta resposta do login para capturar JWT
            async def capture_login_response(response):
                if "autenticacao/login" in response.url and response.status == 200:
                    try:
                        data = await response.json()
                        login_result["data"] = data
                        logger.info(f"[Task {self.task_id}] JWT capturado do login!")
                    except Exception:
                        pass

            page.on("response", capture_login_response)

            try:
                # Acessa portal
                await page.goto("https://agencia.amazonasenergia.com", wait_until="networkidle")

                # Preenche credenciais
                cpf_input = page.locator("input#CPF_CNPJ")
                await cpf_input.click()
                await cpf_input.fill("")
                await cpf_input.type(cpf, delay=100)

                senha_input = page.locator("input#SENHA")
                await senha_input.click()
                await senha_input.fill("")
                await senha_input.type(senha, delay=100)

                # Clica no checkbox "Não sou um robô"
                checkbox = page.locator("text=Não sou um robô").first
                await checkbox.click()

                logger.info(
                    f"[Task {self.task_id}] Credenciais preenchidas. "
                    "Aguardando login (captcha + Entrar)..."
                )

                # Aguarda até 120s pelo login (dá tempo pro operador intervir)
                for _ in range(120):
                    if login_result.get("data"):
                        break
                    await page.wait_for_timeout(1000)

                if not login_result.get("data"):
                    # Tenta clicar em Entrar automaticamente
                    try:
                        await page.locator("button[type='submit']").click()
                        for _ in range(30):
                            if login_result.get("data"):
                                break
                            await page.wait_for_timeout(1000)
                    except Exception:
                        pass

                if not login_result.get("data"):
                    raise Exception(
                        "Login não completado em 150s. "
                        "Com BROWSER_HEADLESS=false, resolva o captcha manualmente."
                    )

            finally:
                await browser.close()

        return login_result["data"]
