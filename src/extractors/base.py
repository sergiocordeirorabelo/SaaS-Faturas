"""
Extractor Base — gerencia o ciclo de vida do Playwright e define a interface
que todos os extratores de concessionárias devem implementar.
"""

from __future__ import annotations

import logging
import tempfile
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Optional

from playwright.async_api import (
    async_playwright,
    Browser,
    BrowserContext,
    Page,
    PlaywrightContextManager,
    TimeoutError as PlaywrightTimeout,
)

from src.config import settings
from src.db.client import SupabaseClient

logger = logging.getLogger(__name__)


class LoginError(Exception):
    """Credenciais inválidas ou falha de autenticação."""


class ExtractionError(Exception):
    """Falha durante a extração das faturas."""


class BaseExtractor(ABC):
    """
    Classe base para extratores de faturas.

    Gerencia o ciclo de vida do Playwright (abertura e fechamento garantido)
    e expõe métodos utilitários comuns para as subclasses.
    """

    def __init__(self, db: SupabaseClient, task: dict):
        self.db = db
        self.task = task
        self.task_id: str = task["id"]
        self.credentials: dict = task.get("credentials", {})

        self._playwright: Optional[PlaywrightContextManager] = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._page: Optional[Page] = None

        # Diretório temporário para downloads desta tarefa
        self._tmp_dir = Path(tempfile.mkdtemp(prefix=f"task_{self.task_id}_"))

    # ─────────────────────────────────────────────────────────────────────────
    # Interface pública
    # ─────────────────────────────────────────────────────────────────────────

    async def run(self) -> list[dict]:
        """
        Ponto de entrada principal.
        Garante que o navegador seja sempre fechado após a execução.

        Returns:
            Lista de dicts com metadados de cada PDF extraído:
            [{"mes_referencia": "2024-01", "storage_url": "https://..."}]
        """
        try:
            await self._setup_browser()
            return await self._extract()
        except LoginError:
            raise
        except Exception as exc:
            await self._capture_error_screenshot(str(exc))
            raise ExtractionError(str(exc)) from exc
        finally:
            await self._teardown_browser()

    # ─────────────────────────────────────────────────────────────────────────
    # Interface para subclasses
    # ─────────────────────────────────────────────────────────────────────────

    @abstractmethod
    async def _extract(self) -> list[dict]:
        """Lógica específica de extração a ser implementada por cada concessionária."""

    # ─────────────────────────────────────────────────────────────────────────
    # Ciclo de vida do Playwright
    # ─────────────────────────────────────────────────────────────────────────

    async def _setup_browser(self) -> None:
        """Inicializa o Playwright com configurações stealth e proxy opcional."""
        self._playwright = await async_playwright().start()

        launch_kwargs = {
            "headless": settings.BROWSER_HEADLESS,
            "slow_mo": settings.BROWSER_SLOW_MO_MS,
            "args": [
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
            ],
        }

        if settings.PROXY_SERVER:
            launch_kwargs["proxy"] = {"server": settings.PROXY_SERVER}

        self._browser = await self._playwright.chromium.launch(**launch_kwargs)

        context_kwargs = {
            "viewport": {"width": 1366, "height": 768},
            "user_agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "locale": "pt-BR",
            "timezone_id": "America/Manaus",
            "accept_downloads": True,
        }

        self._context = await self._browser.new_context(**context_kwargs)
        self._context.set_default_timeout(settings.BROWSER_TIMEOUT_MS)

        # Injeta script stealth antes de cada página (remove marcadores de automação)
        await self._context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
            Object.defineProperty(navigator, 'languages', { get: () => ['pt-BR', 'pt', 'en-US'] });
            window.chrome = { runtime: {} };
        """)

        self._page = await self._context.new_page()
        logger.debug(f"[Task {self.task_id}] Navegador iniciado.")

    async def _teardown_browser(self) -> None:
        """Fecha o navegador e libera todos os recursos, sempre."""
        try:
            if self._context:
                await self._context.close()
            if self._browser:
                await self._browser.close()
            if self._playwright:
                await self._playwright.stop()
        except Exception as exc:
            logger.warning(f"[Task {self.task_id}] Erro ao fechar navegador: {exc}")
        finally:
            self._page = None
            self._context = None
            self._browser = None
            self._playwright = None
            logger.debug(f"[Task {self.task_id}] Navegador encerrado.")

    # ─────────────────────────────────────────────────────────────────────────
    # Utilitários comuns
    # ─────────────────────────────────────────────────────────────────────────

    async def _capture_error_screenshot(self, error_msg: str) -> Optional[str]:
        """Captura screenshot e faz upload para o Storage em caso de erro."""
        if not self._page:
            return None
        try:
            ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            local_path = self._tmp_dir / f"error_{ts}.png"
            await self._page.screenshot(path=str(local_path), full_page=True)

            storage_path = f"errors/{self.task_id}/error_{ts}.png"
            url = await self.db.upload_screenshot(local_path, storage_path)
            logger.info(f"[Task {self.task_id}] Screenshot de erro salvo: {url}")
            return url
        except Exception as exc:
            logger.warning(f"[Task {self.task_id}] Falha ao capturar screenshot: {exc}")
            return None

    async def _safe_click(self, selector: str, timeout: int = 10_000) -> None:
        """Clica em um elemento com timeout customizado."""
        await self._page.locator(selector).click(timeout=timeout)

    async def _safe_fill(self, selector: str, value: str) -> None:
        """Preenche um campo de formulário simulando digitação humana."""
        locator = self._page.locator(selector)
        await locator.click()
        await locator.fill("")
        await locator.type(value, delay=50)

    async def _inject_recaptcha_token(self, token: str) -> None:
        """Injeta o token resolvido pelo serviço de captcha no DOM da página."""
        await self._page.evaluate(f"""
            (() => {{
                // Tenta o campo padrão do reCAPTCHA
                const textarea = document.getElementById('g-recaptcha-response');
                if (textarea) {{
                    textarea.style.display = 'block';
                    textarea.value = '{token}';
                }}
                // Fallback para campos alternativos
                const alt = document.querySelector('[name="g-recaptcha-response"]');
                if (alt) {{ alt.value = '{token}'; }}
            }})();
        """)
        logger.debug(f"[Task {self.task_id}] Token de captcha injetado no DOM.")
