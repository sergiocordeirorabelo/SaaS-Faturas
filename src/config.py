"""
Configurações centralizadas — carregadas via variáveis de ambiente (.env).
"""

from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    # ── Supabase ──────────────────────────────────────────────────────────────
    SUPABASE_URL: str = Field(..., description="URL do projeto Supabase")
    SUPABASE_SERVICE_KEY: str = Field(..., description="Chave de serviço do Supabase (service_role)")
    SUPABASE_BUCKET: str = Field(default="faturas", description="Nome do bucket no Supabase Storage")

    # ── Captcha ───────────────────────────────────────────────────────────────
    CAPTCHA_SERVICE: str = Field(
        default="2captcha",
        description="Serviço de resolução: '2captcha' | 'anticaptcha' | 'capsolver'",
    )
    CAPTCHA_API_KEY: str = Field(..., description="Chave da API do serviço de captcha")
    CAPTCHA_TIMEOUT_SECONDS: int = Field(default=120, description="Timeout total para resolução do captcha")
    CAPTCHA_POLL_INTERVAL_SECONDS: int = Field(default=5, description="Intervalo de polling da solução")
    CAPTCHA_MAX_RETRIES: int = Field(default=3, description="Número máximo de tentativas de resolução")

    # ── Playwright ────────────────────────────────────────────────────────────
    BROWSER_HEADLESS: bool = Field(default=True, description="Rodar o navegador sem interface gráfica")
    BROWSER_TIMEOUT_MS: int = Field(default=30_000, description="Timeout padrão de navegação em ms")
    BROWSER_SLOW_MO_MS: int = Field(default=0, description="Desaceleração entre ações (debug)")

    # ── Proxy (opcional) ──────────────────────────────────────────────────────
    PROXY_SERVER: str | None = Field(default=None, description="URL do proxy: http://user:pass@host:port")

    # ── Proxy para Captcha (residencial BR — eleva o score) ──────────────────
    CAPTCHA_PROXY_TYPE: str = Field(default="http", description="Tipo do proxy: http | https | socks5")
    CAPTCHA_PROXY_ADDRESS: str | None = Field(default=None, description="Host do proxy residencial")
    CAPTCHA_PROXY_PORT: int = Field(default=0, description="Porta do proxy")
    CAPTCHA_PROXY_LOGIN: str | None = Field(default=None, description="Usuário do proxy")
    CAPTCHA_PROXY_PASSWORD: str | None = Field(default=None, description="Senha do proxy")

    # ── Worker ────────────────────────────────────────────────────────────────
    POLL_INTERVAL_SECONDS: int = Field(default=15, description="Intervalo entre ciclos de polling (s)")
    MAX_CONCURRENT_TASKS: int = Field(default=2, description="Máximo de tarefas simultâneas")
    MAX_INVOICES_MONTHS: int = Field(default=12, description="Quantidade de meses de faturas a extrair")

    # ── Logging ───────────────────────────────────────────────────────────────
    LOG_LEVEL: str = Field(default="INFO", description="Nível de log: DEBUG | INFO | WARNING | ERROR")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True


settings = Settings()
