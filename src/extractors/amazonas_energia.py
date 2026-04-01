"""
Extrator Amazonas Energia — implementação específica para o portal da concessionária.

Portal: https://www.amazonasenergia.gov.br (ou portal de atendimento online)
Fluxo:
  1. Acessa a página de login
  2. Preenche CPF/CNPJ + Senha
  3. Resolve reCAPTCHA v2
  4. Navega para "Segunda Via / Histórico de Faturas"
  5. Faz download dos PDFs dos últimos N meses
  6. Faz upload para o Supabase Storage

NOTA: Os seletores CSS/XPath abaixo são baseados na estrutura conhecida do portal.
      Caso o portal sofra atualização de layout, o worker captura screenshot e 
      marca a tarefa como 'erro_extracao' para análise manual.
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from pathlib import Path
from typing import Optional

from playwright.async_api import TimeoutError as PlaywrightTimeout, Download

from src.captcha.solver import solve_with_retry, CaptchaError
from src.config import settings
from src.extractors.base import BaseExtractor, LoginError, ExtractionError

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Constantes do Portal
# ─────────────────────────────────────────────────────────────────────────────

PORTAL_URL = "https://servicos.amazonasenergia.gov.br"
LOGIN_URL = f"{PORTAL_URL}/login"
HISTORICO_URL = f"{PORTAL_URL}/segunda-via"

# Seletores — atualizar se o portal mudar de layout
SEL_CPF_INPUT = "input[name='cpf_cnpj'], input[id*='cpf'], input[placeholder*='CPF']"
SEL_SENHA_INPUT = "input[type='password']"
SEL_SUBMIT_BTN = "button[type='submit'], input[type='submit']"
SEL_RECAPTCHA_IFRAME = "iframe[src*='recaptcha']"
SEL_RECAPTCHA_SITEKEY = ".g-recaptcha, [data-sitekey]"
SEL_FATURA_ROWS = "table.faturas tbody tr, .lista-faturas .item-fatura"
SEL_DOWNLOAD_BTN = "a[href*='.pdf'], button[data-action='download'], a.btn-download"
SEL_MES_REF = "td.mes-referencia, .mes-fatura, td:first-child"
SEL_ERROR_MSG = ".alert-danger, .error-message, [class*='erro']"


class AmazonasEnergiaExtractor(BaseExtractor):
    """
    Extrator de faturas para o portal da Amazonas Energia.
    Herda ciclo de vida do Playwright de BaseExtractor.
    """

    async def _extract(self) -> list[dict]:
        """
        Orquestra todo o fluxo de extração:
        login → navegação → download → upload.

        Returns:
            Lista de metadados dos PDFs enviados ao Storage.
        """
        await self._do_login()
        await self._navigate_to_historico()
        pdf_records = await self._download_faturas()
        return pdf_records

    # ─────────────────────────────────────────────────────────────────────────
    # Etapa 1 — Login
    # ─────────────────────────────────────────────────────────────────────────

    async def _do_login(self) -> None:
        """Realiza o login com tratamento de captcha e credenciais inválidas."""
        cpf_cnpj = self.credentials.get("cpf_cnpj", "")
        senha = self.credentials.get("senha", "")
        unidade_consumidora = self.credentials.get("unidade_consumidora", "")

        if not cpf_cnpj and not unidade_consumidora:
            raise LoginError("Credenciais insuficientes: CPF/CNPJ ou UC obrigatório.")

        logger.info(f"[Task {self.task_id}] Acessando portal de login: {LOGIN_URL}")
        await self._page.goto(LOGIN_URL, wait_until="networkidle")

        # Preenche CPF/CNPJ (ou número da UC se disponível)
        identifier = cpf_cnpj or unidade_consumidora
        await self._safe_fill(SEL_CPF_INPUT, identifier)

        if senha:
            await self._safe_fill(SEL_SENHA_INPUT, senha)

        # ── Captcha ──────────────────────────────────────────────────────────
        site_key = await self._get_recaptcha_sitekey()
        if site_key:
            logger.info(f"[Task {self.task_id}] reCAPTCHA detectado. Site key: {site_key[:20]}...")
            try:
                token = await solve_with_retry(site_key=site_key, page_url=LOGIN_URL)
                await self._inject_recaptcha_token(token)
            except CaptchaError as exc:
                raise ExtractionError(f"Falha na resolução do captcha: {exc}") from exc

        # Submete o formulário
        await self._safe_click(SEL_SUBMIT_BTN)

        # Aguarda redirecionamento ou mensagem de erro
        try:
            await self._page.wait_for_load_state("networkidle", timeout=20_000)
        except PlaywrightTimeout:
            pass  # Pode ser que o portal usa SPA (sem navegação full)

        await self._assert_login_success()

    async def _get_recaptcha_sitekey(self) -> Optional[str]:
        """Extrai o data-sitekey do reCAPTCHA na página, se existir."""
        try:
            element = await self._page.query_selector(SEL_RECAPTCHA_SITEKEY)
            if element:
                return await element.get_attribute("data-sitekey")
        except Exception:
            pass
        return None

    async def _assert_login_success(self) -> None:
        """Verifica se o login foi bem-sucedido ou lança exceções adequadas."""
        current_url = self._page.url

        # Ainda na página de login = falhou
        if "login" in current_url.lower():
            # Verifica mensagem de erro específica
            error_el = await self._page.query_selector(SEL_ERROR_MSG)
            error_text = ""
            if error_el:
                error_text = (await error_el.inner_text()).strip()

            if any(kw in error_text.lower() for kw in ["inválid", "incorret", "senha", "usuário"]):
                raise LoginError(f"Credenciais inválidas: {error_text}")

            raise ExtractionError(f"Login não concluído. URL atual: {current_url}. Erro: {error_text}")

        logger.info(f"[Task {self.task_id}] Login realizado com sucesso. URL: {current_url}")

    # ─────────────────────────────────────────────────────────────────────────
    # Etapa 2 — Navegação para histórico
    # ─────────────────────────────────────────────────────────────────────────

    async def _navigate_to_historico(self) -> None:
        """Navega para a seção de segunda via / histórico de faturas."""
        logger.info(f"[Task {self.task_id}] Navegando para histórico de faturas...")

        try:
            await self._page.goto(HISTORICO_URL, wait_until="networkidle")
        except PlaywrightTimeout:
            # Tenta localizar o link de menu como fallback
            menu_link = await self._page.query_selector(
                "a[href*='segunda-via'], a[href*='historico'], a:has-text('Segunda Via')"
            )
            if menu_link:
                await menu_link.click()
                await self._page.wait_for_load_state("networkidle")
            else:
                raise ExtractionError("Não foi possível navegar para o histórico de faturas.")

        # Aguarda a tabela de faturas carregar
        try:
            await self._page.wait_for_selector(SEL_FATURA_ROWS, timeout=15_000)
        except PlaywrightTimeout:
            raise ExtractionError("Tabela de faturas não encontrada após navegação.")

        logger.info(f"[Task {self.task_id}] Página de histórico carregada.")

    # ─────────────────────────────────────────────────────────────────────────
    # Etapa 3 — Download das faturas
    # ─────────────────────────────────────────────────────────────────────────

    async def _download_faturas(self) -> list[dict]:
        """
        Identifica e baixa as faturas dos últimos N meses.

        Returns:
            Lista de dicts com mes_referencia e storage_url.
        """
        months_limit = settings.MAX_INVOICES_MONTHS
        cutoff_date = date.today() - relativedelta(months=months_limit)

        rows = await self._page.query_selector_all(SEL_FATURA_ROWS)
        logger.info(f"[Task {self.task_id}] {len(rows)} fatura(s) encontrada(s) na tabela.")

        if not rows:
            raise ExtractionError("Nenhuma fatura encontrada na tabela.")

        pdf_records: list[dict] = []

        for i, row in enumerate(rows):
            # Extrai o mês de referência da linha
            mes_ref = await self._parse_mes_referencia(row)
            if mes_ref and mes_ref < cutoff_date:
                logger.debug(f"[Task {self.task_id}] Fatura {mes_ref} fora do período. Ignorando.")
                continue

            # Localiza o botão de download dentro da linha
            download_btn = await row.query_selector(SEL_DOWNLOAD_BTN)
            if not download_btn:
                logger.warning(f"[Task {self.task_id}] Linha {i+1}: botão de download não encontrado.")
                continue

            # Executa o download
            local_path = await self._download_pdf(download_btn, mes_ref, index=i)
            if not local_path:
                continue

            # Upload para o Supabase Storage
            mes_str = mes_ref.strftime("%Y-%m") if mes_ref else f"fatura_{i+1}"
            uc = self.credentials.get("unidade_consumidora") or self.credentials.get("cpf_cnpj", "unknown")
            storage_path = f"faturas/{uc}/{mes_str}.pdf"

            storage_url = await self.db.upload_pdf(
                local_path=local_path,
                storage_path=storage_path,
                task_id=self.task_id,
            )

            pdf_records.append({
                "mes_referencia": mes_str,
                "storage_url": storage_url,
                "filename": local_path.name,
            })

            logger.info(f"[Task {self.task_id}] ✓ Fatura {mes_str} processada.")

            # Pequena pausa entre downloads para não sobrecarregar o servidor
            await asyncio.sleep(1.5)

        return pdf_records

    async def _parse_mes_referencia(self, row) -> Optional[date]:
        """
        Tenta extrair a data de referência de uma linha da tabela.
        Suporta formatos: MM/YYYY, YYYY-MM, Jan/2024, etc.
        """
        try:
            cell = await row.query_selector(SEL_MES_REF)
            if not cell:
                return None

            text = (await cell.inner_text()).strip()

            # Tenta múltiplos formatos
            for fmt in ("%m/%Y", "%Y-%m", "%b/%Y", "%B/%Y", "%m-%Y"):
                try:
                    return datetime.strptime(text, fmt).date().replace(day=1)
                except ValueError:
                    continue

            # Fallback: extrai números e tenta construir a data
            nums = re.findall(r'\d+', text)
            if len(nums) >= 2:
                month, year = int(nums[0]), int(nums[1])
                if 1 <= month <= 12 and 2000 <= year <= 2100:
                    return date(year, month, 1)

        except Exception as exc:
            logger.debug(f"Não foi possível parsear mês de referência: {exc}")

        return None

    async def _download_pdf(self, btn_element, mes_ref: Optional[date], index: int) -> Optional[Path]:
        """
        Clica no botão de download e aguarda o arquivo ser salvo.

        Returns:
            Path local do arquivo baixado, ou None em caso de falha.
        """
        mes_str = mes_ref.strftime("%Y-%m") if mes_ref else f"fatura_{index+1}"
        filename = f"{mes_str}.pdf"
        local_path = self._tmp_dir / filename

        try:
            async with self._page.expect_download(timeout=30_000) as dl_info:
                await btn_element.click()

            download: Download = await dl_info.value

            if download.failure():
                logger.error(f"[Task {self.task_id}] Download falhou: {download.failure()}")
                return None

            await download.save_as(str(local_path))
            size_kb = local_path.stat().st_size // 1024
            logger.info(f"[Task {self.task_id}] Downloaded: {filename} ({size_kb} KB)")
            return local_path

        except PlaywrightTimeout:
            logger.error(f"[Task {self.task_id}] Timeout aguardando download de {mes_str}.")
            await self._capture_error_screenshot(f"timeout_download_{mes_str}")
            return None
        except Exception as exc:
            logger.error(f"[Task {self.task_id}] Erro no download de {mes_str}: {exc}")
            return None
