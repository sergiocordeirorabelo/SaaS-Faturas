"""
Módulo de resolução de CAPTCHA.
Suporta: 2Captcha, Anti-Captcha, CapSolver.
"""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Optional

import httpx

from src.config import settings

logger = logging.getLogger(__name__)


class CaptchaSolverBase(ABC):
    @abstractmethod
    async def solve_recaptcha_v2(self, site_key: str, page_url: str) -> str:
        pass


class CaptchaError(Exception):
    pass


class CaptchaTimeoutError(CaptchaError):
    pass


# ── 2Captcha ──────────────────────────────────────────────────────────────────

class TwoCaptchaSolver(CaptchaSolverBase):
    BASE_URL = "https://2captcha.com"

    def __init__(self, api_key: str):
        self.api_key = api_key

    async def solve_recaptcha_v2(self, site_key: str, page_url: str) -> str:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{self.BASE_URL}/in.php",
                data={
                    "key": self.api_key,
                    "method": "userrecaptcha",
                    "googlekey": site_key,
                    "pageurl": page_url,
                    "enterprise": 1,
                    "invisible": 1,
                    "json": 1,
                },
            )
            resp.raise_for_status()
            data = resp.json()

            if data.get("status") != 1:
                raise CaptchaError(f"2Captcha erro ao submeter: {data}")

            captcha_id = data["request"]
            elapsed = 0
            await asyncio.sleep(15)

            while elapsed < settings.CAPTCHA_TIMEOUT_SECONDS:
                result = await client.get(
                    f"{self.BASE_URL}/res.php",
                    params={"key": self.api_key, "action": "get", "id": captcha_id, "json": 1},
                )
                result.raise_for_status()
                result_data = result.json()

                if result_data.get("status") == 1:
                    return result_data["request"]
                elif result_data.get("request") != "CAPCHA_NOT_READY":
                    raise CaptchaError(f"2Captcha erro: {result_data}")

                await asyncio.sleep(settings.CAPTCHA_POLL_INTERVAL_SECONDS)
                elapsed += settings.CAPTCHA_POLL_INTERVAL_SECONDS

            raise CaptchaTimeoutError(f"2Captcha timeout após {settings.CAPTCHA_TIMEOUT_SECONDS}s")


# ── Anti-Captcha ──────────────────────────────────────────────────────────────

class AntiCaptchaSolver(CaptchaSolverBase):
    BASE_URL = "https://api.anti-captcha.com"

    def __init__(self, api_key: str):
        self.api_key = api_key

    async def solve_recaptcha_v2(self, site_key: str, page_url: str) -> str:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{self.BASE_URL}/createTask",
                json={
                    "clientKey": self.api_key,
                    "task": {
                        "type": "RecaptchaV2EnterpriseTaskProxyless",
                        "websiteURL": page_url,
                        "websiteKey": site_key,
                        "isInvisible": True,
                        "enterprisePayload": {"action": "login"},
                    },
                },
            )
            resp.raise_for_status()
            data = resp.json()

            if data.get("errorId") != 0:
                raise CaptchaError(f"Anti-Captcha erro: {data}")

            task_id = data["taskId"]
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
                    return result_data["solution"]["gRecaptchaResponse"]
                elif result_data.get("errorId") != 0:
                    raise CaptchaError(f"Anti-Captcha erro: {result_data}")

                await asyncio.sleep(settings.CAPTCHA_POLL_INTERVAL_SECONDS)
                elapsed += settings.CAPTCHA_POLL_INTERVAL_SECONDS

            raise CaptchaTimeoutError(f"Anti-Captcha timeout após {settings.CAPTCHA_TIMEOUT_SECONDS}s")


# ── CapSolver ─────────────────────────────────────────────────────────────────

class CapSolverSolver(CaptchaSolverBase):
    """
    Integração com CapSolver — reCAPTCHA V2 Enterprise.
    
    Estratégia:
    - COM proxy residencial BR → ReCaptchaV2EnterpriseTask (melhor score)
    - SEM proxy → ReCaptchaV2EnterpriseTaskProxyless (fallback)
    
    Nota: o portal usa reCAPTCHA Enterprise *Invisível* (V2, não V3).
    O pageAction='login' vai no enterprisePayload.
    """
    BASE_URL = "https://api.capsolver.com"

    def __init__(self, api_key: str):
        self.api_key = api_key

    def _has_proxy(self) -> bool:
        return bool(settings.CAPTCHA_PROXY_ADDRESS and settings.CAPTCHA_PROXY_PORT)

    def _build_proxy_string(self) -> str:
        """Monta proxy no formato URL: http://user:pass@host:port"""
        login = settings.CAPTCHA_PROXY_LOGIN or ""
        password = settings.CAPTCHA_PROXY_PASSWORD or ""
        host = settings.CAPTCHA_PROXY_ADDRESS
        port = settings.CAPTCHA_PROXY_PORT
        if login:
            return f"http://{login}:{password}@{host}:{port}"
        return f"http://{host}:{port}"

    def _get_task_types(self) -> list[dict]:
        """
        Retorna lista de tasks para tentar em ordem.
        Tenta V3 Enterprise primeiro, depois V2 Enterprise como fallback.
        """
        tasks = []
        proxy_str = self._build_proxy_string() if self._has_proxy() else None

        if proxy_str:
            logger.info(f"CapSolver — Proxy: {settings.CAPTCHA_PROXY_ADDRESS}:{settings.CAPTCHA_PROXY_PORT}")
            # Tentativa 1: V3 Enterprise com proxy
            tasks.append({
                "type": "ReCaptchaV3EnterpriseTask",
                "proxy": proxy_str,
                "pageAction": "login",
            })
            # Tentativa 2: V2 Enterprise com proxy
            tasks.append({
                "type": "ReCaptchaV2EnterpriseTask",
                "proxy": proxy_str,
                "pageAction": "login",
                "isInvisible": True,
            })
        else:
            tasks.append({
                "type": "ReCaptchaV3EnterpriseTaskProxyLess",
                "pageAction": "login",
            })
            tasks.append({
                "type": "ReCaptchaV2EnterpriseTaskProxyless",
                "pageAction": "login",
                "isInvisible": True,
            })

        return tasks

    async def solve_recaptcha_v2(self, site_key: str, page_url: str) -> str:
        task_variants = self._get_task_types()
        last_error: Exception | None = None

        for variant in task_variants:
            task = {
                **variant,
                "websiteURL": page_url,
                "websiteKey": site_key,
            }
            task_type = task["type"]
            logger.info(f"CapSolver — Tentando: {task_type}")
            logger.debug(f"CapSolver — Payload: {task}")

            try:
                token = await self._solve_task(task)
                logger.info(f"CapSolver — Resolvido com {task_type}! Token length: {len(token)}")
                return token
            except CaptchaError as exc:
                logger.warning(f"CapSolver — {task_type} falhou: {exc}")
                last_error = exc
                continue

        raise CaptchaError(f"Todos os tipos falharam. Último erro: {last_error}")

    async def _solve_task(self, task: dict) -> str:
        """Envia uma task para o CapSolver e aguarda o resultado."""
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{self.BASE_URL}/createTask",
                json={
                    "clientKey": self.api_key,
                    "task": task,
                },
            )
            resp.raise_for_status()
            data = resp.json()

            if data.get("errorId") != 0:
                raise CaptchaError(f"CapSolver erro ao criar tarefa: {data}")

            task_id = data["taskId"]
            logger.debug(f"CapSolver — Task ID: {task_id}")

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
                    token = result_data["solution"]["gRecaptchaResponse"]
                    logger.info(f"CapSolver — Resolvido! Token length: {len(token)}")
                    return token
                elif result_data.get("errorId") != 0:
                    raise CaptchaError(f"CapSolver erro na solução: {result_data}")

                await asyncio.sleep(settings.CAPTCHA_POLL_INTERVAL_SECONDS)
                elapsed += settings.CAPTCHA_POLL_INTERVAL_SECONDS

            raise CaptchaTimeoutError(f"CapSolver timeout após {settings.CAPTCHA_TIMEOUT_SECONDS}s")


# ── Factory + Retry ───────────────────────────────────────────────────────────

def get_solver() -> CaptchaSolverBase:
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
            if attempt < settings.CAPTCHA_MAX_RETRIES:
                await asyncio.sleep(3)  # Espera antes de retry

    raise CaptchaError(
        f"Captcha não resolvido após {settings.CAPTCHA_MAX_RETRIES} tentativas. Último erro: {last_error}"
    )
