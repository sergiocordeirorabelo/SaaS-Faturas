"""
Extrator Amazonas Energia — versão FINAL.
Login direto via API mobile (sem captcha, sem browser, sem extensão).
100% httpx, 100% automático.
"""

from __future__ import annotations

import asyncio
import logging
import tempfile
from pathlib import Path
from typing import Optional

import random

import httpx

from src.config import settings
from src.db.client import SupabaseClient
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

API_URL = "https://api-agencia.amazonasenergia.com"

# Headers que simulam o app mobile Android (blindados conforme Dev Senior)
MOBILE_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Accept-Encoding": "gzip",
    "Connection": "Keep-Alive",
    "User-Agent": "okhttp/4.9.0",
}


class AmazonasEnergiaHTTPExtractor:
    """Extrator 100% automático — login + download via API REST."""

    def __init__(self, db: SupabaseClient, task: dict):
        self.db = db
        self.task = task
        self.task_id: str = task["id"]
        self.credentials: dict = task.get("credentials", {})
        self._tmp_dir = Path(tempfile.mkdtemp(prefix=f"task_{self.task_id}_"))

    async def run(self) -> list[dict]:
        """
        Fluxo completo:
        1. Login via API mobile (sem captcha)
        2. Descobre todas as UCs
        3. Baixa faturas abertas + pagas de cada UC
        4. Upload pro Supabase Storage
        """
        cpf = self.credentials.get("cpf_cnpj", "")
        senha = self.credentials.get("senha", "")

        if not cpf or not senha:
            raise Exception("CPF/CNPJ e senha são obrigatórios.")

        async with httpx.AsyncClient(base_url=API_URL, timeout=30) as client:

            # 1. Login direto (sem captcha!)
            jwt, clientes = await self._login(client, cpf, senha)

            # 2. Monta lista de cliente/UC
            clientes_ucs = []
            for cli in clientes:
                id_cli = cli.get("ID_CLIENTE")
                for uc in cli.get("UNIDADES_CONSUMIDORAS", []):
                    clientes_ucs.append({
                        "id_cliente": id_cli,
                        "id_uc": uc["ID_UC"],
                    })

            logger.info(
                f"[Task {self.task_id}] Login OK! "
                f"{len(clientes_ucs)} UC(s) encontrada(s)."
            )

            # Headers autenticados
            auth_headers = {
                **MOBILE_HEADERS,
                "Authorization": f"Bearer {jwt}",
                "Origin": "https://agencia.amazonasenergia.com",
                "Referer": "https://agencia.amazonasenergia.com/",
            }

            all_pdfs: list[dict] = []

            # 3. Para cada UC, baixa faturas
            for item in clientes_ucs:
                id_cliente = item["id_cliente"]
                id_uc = item["id_uc"]
                logger.info(
                    f"[Task {self.task_id}] Processando UC {id_uc} "
                    f"(Cliente {id_cliente})"
                )

                uc_headers = {
                    **auth_headers,
                    "X-Client-Id": str(id_cliente),
                    "X-Consumer-Unit": str(id_uc),
                }

                # Abertas primeiro (mais importantes)
                abertas = await self._fetch_faturas(client, uc_headers, "abertas")
                pagas = await self._fetch_faturas(client, uc_headers, "pagas")

                logger.info(
                    f"[Task {self.task_id}] UC {id_uc}: "
                    f"{len(pagas)} pagas + {len(abertas)} abertas"
                )

                faturas = abertas + pagas[:settings.MAX_INVOICES_MONTHS]

                for fatura in faturas:
                    pdf = await self._download_fatura(
                        client, uc_headers, id_uc, fatura
                    )
                    if pdf:
                        all_pdfs.append(pdf)
                    await asyncio.sleep(random.uniform(1.0, 3.0))  # Jitter natural

            return all_pdfs

    async def _login(self, client: httpx.AsyncClient, cpf: str, senha: str) -> tuple:
        """Login via API mobile — sem captcha, sem browser."""
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

        data = resp.json()
        token = data.get("TOKEN", "")
        if not token:
            raise Exception("Login retornou sem TOKEN.")

        # Extrai dados dos clientes
        usuario = data.get("USUARIO", {})
        clientes_raw = data.get("CLIENTES", [])

        # Se CLIENTES não veio na resposta, busca via endpoint
        if not clientes_raw:
            clientes_raw = await self._fetch_clientes(client, token)

        return token, clientes_raw

    async def _fetch_clientes(self, client: httpx.AsyncClient, jwt: str) -> list:
        """Busca dados dos clientes/UCs via API."""
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
        except Exception as exc:
            logger.warning(f"[Task {self.task_id}] Erro ao buscar clientes: {exc}")

        # Fallback: usa UCs do JWT
        import json, base64
        try:
            payload = json.loads(base64.b64decode(jwt.split('.')[1] + '=='))
            ucs = payload.get("UCS", [])
            id_user = payload.get("ID", 0)
            return [{
                "ID_CLIENTE": id_user,
                "UNIDADES_CONSUMIDORAS": [{"ID_UC": uc} for uc in ucs],
            }]
        except Exception:
            raise Exception("Não foi possível obter lista de UCs.")

    async def _fetch_faturas(
        self, client: httpx.AsyncClient, uc_headers: dict, tipo: str,
    ) -> list[dict]:
        """Busca faturas pagas ou abertas de uma UC."""
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

    async def _download_fatura(
        self, client: httpx.AsyncClient, uc_headers: dict,
        id_uc: int, fatura: dict,
    ) -> Optional[dict]:
        """Baixa PDF de uma fatura e faz upload ao Supabase."""
        mes_ano = fatura.get("MES_ANO_REFERENCIA", "")
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

            mes_str = mes_ano.replace("/", "-")
            filename = f"fatura_{id_uc}_{mes_str}.pdf"
            local_path = self._tmp_dir / filename
            local_path.write_bytes(resp.content)

            size_kb = len(resp.content) // 1024
            logger.info(f"[Task {self.task_id}] ✓ {filename} ({size_kb} KB)")

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
