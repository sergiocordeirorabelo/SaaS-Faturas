"""
Módulo de resolução de CAPTCHA.
Suporta: 2Captcha, Anti-Captcha, CapSolver.
Implementa retry automático e timeout configurável.
"""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Optional

import httpx

from src.config import settings

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Classe base
# ─────────────────────────────────────────────────────────────────────────────

class CaptchaSolverBase(ABC):
    """Interface comum para todos os solvers de captcha."""

    @abstractmethod
    async def solve_recaptcha_v2(self, site_key: str, page_url: str) -> str:
        """
        Resolve um reCAPTCHA v2.

        Args:
            site_key: Chave pública do reCAPTCHA (data-sitekey).
            page_url: URL da página onde o captcha está presente.

        Returns:
            Token g-recaptcha-response resolvido.

        Raises:
            CaptchaTimeoutError: Se não resolver dentro do timeout.
            CaptchaError: Para qualquer outro erro da API.
        """


class CaptchaError(Exception):
    """Erro genérico de resolução de captcha."""


class CaptchaTimeoutError(CaptchaError):
    """Timeout aguardando a resolução."""


# ─────────────────────────────────────────────────────────────────────────────
# 2Captcha
# ─────────────────────────────────────────────────────────────────────────────

class TwoCaptchaSolver(CaptchaSolverBase):
    """Integração com a API do 2Captcha."""

    BASE_URL = "https://2captcha.com"

    def __init__(self, api_key: str):
        self.api_key = api_key

    async def solve_recaptcha_v2(self, site_key: str, page_url: str) -> str:
        async with httpx.AsyncClient(timeout=30) as client:
            # 1. Envia o captcha para resolução
            resp = await client.post(
                f"{self.BASE_URL}/in.php",
                data={
                    "key": self.api_key,
                    "method": "userrecaptcha",
                    "googlekey": site_key,
                    "pageurl": page_url,
                    "json": 1,
                },
            )
            resp.raise_for_status()
            data = resp.json()

            if data.get("status") != 1:
                raise CaptchaError(f"2Captcha erro ao submeter: {data}")

            captcha_id = data["request"]
            logger.debug(f"2Captcha — ID de tarefa: {captcha_id}")

            # 2. Polling para obter o resultado
            elapsed = 0
            await asyncio.sleep(15)  # Aguarda antes do primeiro poll

            while elapsed < settings.CAPTCHA_TIMEOUT_SECONDS:
                result = await client.get(
                    f"{self.BASE_URL}/res.php",
                    params={"key": self.api_key, "action": "get", "id": captcha_id, "json": 1},
                )
                result.raise_for_status()
                result_data = result.json()

                if result_data.get("status") == 1:
                    logger.info("2Captcha — Captcha resolvido com sucesso.")
                    return result_data["request"]
                elif result_data.get("request") != "CAPCHA_NOT_READY":
                    raise CaptchaError(f"2Captcha erro: {result_data}")

                await asyncio.sleep(settings.CAPTCHA_POLL_INTERVAL_SECONDS)
                elapsed += settings.CAPTCHA_POLL_INTERVAL_SECONDS

            raise CaptchaTimeoutError(
                f"2Captcha não resolveu em {settings.CAPTCHA_TIMEOUT_SECONDS}s"
            )


# ─────────────────────────────────────────────────────────────────────────────
# Anti-Captcha
# ─────────────────────────────────────────────────────────────────────────────

class AntiCaptchaSolver(CaptchaSolverBase):
    """Integração com a API do Anti-Captcha."""

    BASE_URL = "https://api.anti-captcha.com"

    def __init__(self, api_key: str):
        self.api_key = api_key

    async def solve_recaptcha_v2(self, site_key: str, page_url: str) -> str:
        async with httpx.AsyncClient(timeout=30) as client:
            # 1. Cria a tarefa
            resp = await client.post(
                f"{self.BASE_URL}/createTask",
                json={
                    "clientKey": self.api_key,
                    "task": {
                        "type": "NoCaptchaTaskProxyless",
                        "websiteURL": page_url,
                        "websiteKey": site_key,
                    },
                },
            )
            resp.raise_for_status()
            data = resp.json()

            if data.get("errorId") != 0:
                raise CaptchaError(f"Anti-Captcha erro: {data}")

            task_id = data["taskId"]
            logger.debug(f"Anti-Captcha — Task ID: {task_id}")

            # 2. Polling
            elapsed = 0
            await asyncio.sleep(10)

            while elapsed < settings.CAPTCHA_TIMEOUT_SECONDS:
                result = await client.post(
                    f"{self.BASE_URL}/getTaskResult",
                    json={"clientKey": self.api_key, "taskId": task_id},
                )
                result.raise_for_status()
                result_data = result.json()

                if result_data.get("status") == "ready":
                    logger.info("Anti-Captcha — Resolvido.")
                    return result_data["solution"]["gRecaptchaResponse"]
                elif result_data.get("errorId") != 0:
                    raise CaptchaError(f"Anti-Captcha erro na solução: {result_data}")

                await asyncio.sleep(settings.CAPTCHA_POLL_INTERVAL_SECONDS)
                elapsed += settings.CAPTCHA_POLL_INTERVAL_SECONDS

            raise CaptchaTimeoutError(f"Anti-Captcha timeout após {settings.CAPTCHA_TIMEOUT_SECONDS}s")


# ─────────────────────────────────────────────────────────────────────────────
# CapSolver
# ─────────────────────────────────────────────────────────────────────────────

class CapSolverSolver(CaptchaSolverBase):
    """Integração com a API do CapSolver."""

    BASE_URL = "https://api.capsolver.com"

    def __init__(self, api_key: str):
        self.api_key = api_key

    async def solve_recaptcha_v2(self, site_key: str, page_url: str) -> str:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{self.BASE_URL}/createTask",
                json={
                    "clientKey": self.api_key,
                    "task": {
                        "type": "ReCaptchaV2TaskProxyLess",
                        "websiteURL": page_url,
                        "websiteKey": site_key,
                    },
                },
            )
            resp.raise_for_status()
            data = resp.json()

            if data.get("errorId") != 0:
                raise CaptchaError(f"CapSolver erro: {data}")

            task_id = data["taskId"]
            elapsed = 0
            await asyncio.sleep(5)

            while elapsed < settings.CAPTCHA_TIMEOUT_SECONDS:
                result = await client.post(
                    f"{self.BASE_URL}/getTaskResult",
                    json={"clientKey": self.api_key, "taskId": task_id},
                )
                result.raise_for_status()
                result_data = result.json()

                if result_data.get("status") == "ready":
                    logger.info("CapSolver — Resolvido.")
                    return result_data["solution"]["gRecaptchaResponse"]
                elif result_data.get("errorId") != 0:
                    raise CaptchaError(f"CapSolver erro na solução: {result_data}")

                await asyncio.sleep(settings.CAPTCHA_POLL_INTERVAL_SECONDS)
                elapsed += settings.CAPTCHA_POLL_INTERVAL_SECONDS

            raise CaptchaTimeoutError(f"CapSolver timeout após {settings.CAPTCHA_TIMEOUT_SECONDS}s")


# ─────────────────────────────────────────────────────────────────────────────
# Factory com retry
# ─────────────────────────────────────────────────────────────────────────────

def get_solver() -> CaptchaSolverBase:
    """Retorna o solver configurado via variável de ambiente CAPTCHA_SERVICE."""
    solvers = {
        "2captcha": TwoCaptchaSolver,
        "anticaptcha": AntiCaptchaSolver,
        "capsolver": CapSolverSolver,
    }
    cls = solvers.get(settings.CAPTCHA_SERVICE.lower())
    if not cls:
        raise ValueError(f"Serviço de captcha desconhecido: '{settings.CAPTCHA_SERVICE}'")
    return cls(api_key=settings.CAPTCHA_API_KEY)


async def solve_with_retry(site_key: str, page_url: str) -> str:
    """
    Tenta resolver o captcha até CAPTCHA_MAX_RETRIES vezes.

    Raises:
        CaptchaError: Se todas as tentativas falharem.
    """
    solver = get_solver()
    last_error: Optional[Exception] = None

    for attempt in range(1, settings.CAPTCHA_MAX_RETRIES + 1):
        try:
            logger.info(f"Tentativa {attempt}/{settings.CAPTCHA_MAX_RETRIES} de resolver captcha...")
            token = await solver.solve_recaptcha_v2(site_key, page_url)
            return token
        except CaptchaTimeoutError as exc:
            logger.warning(f"Tentativa {attempt} — Timeout: {exc}")
            last_error = exc
        except CaptchaError as exc:
            logger.error(f"Tentativa {attempt} — Erro: {exc}")
            last_error = exc
            break  # Erros da API não se recuperam com retry

    raise CaptchaError(
        f"Captcha não resolvido após {settings.CAPTCHA_MAX_RETRIES} tentativas. Último erro: {last_error}"
    )
