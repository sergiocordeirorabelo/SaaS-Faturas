# ==============================================================================
# Dockerfile — Invoice Worker (Playwright + Python 3.12)
# Otimizado para deploy no Railway como worker de background.
# ==============================================================================

# Estágio 1: Builder — instala dependências Python isoladas
FROM python:3.12-slim AS builder

WORKDIR /build
COPY requirements.txt .

RUN pip install --upgrade pip \
 && pip install --no-cache-dir --prefix=/install -r requirements.txt


# ==============================================================================
# Estágio 2: Runtime — imagem final enxuta
# ==============================================================================
FROM python:3.12-slim AS runtime

# Dependências de sistema para o Chromium (Playwright)
# Lista completa: https://playwright.dev/python/docs/intro
RUN apt-get update && apt-get install -y --no-install-recommends \
    libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 \
    libcups2 libdrm2 libdbus-1-3 libxkbcommon0 \
    libxcomposite1 libxdamage1 libxfixes3 libxrandr2 \
    libgbm1 libasound2 libpango-1.0-0 libcairo2 \
    fonts-liberation fonts-noto-cjk wget ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Copia pacotes Python do builder
COPY --from=builder /install /usr/local

# Cria usuário não-root
RUN groupadd --gid 1001 worker \
 && useradd --uid 1001 --gid worker --shell /bin/bash --create-home worker

WORKDIR /app
COPY --chown=worker:worker . .

# Instala browser Chromium (como root para deps de sistema)
RUN playwright install chromium \
 && playwright install-deps chromium

USER worker
ENV PLAYWRIGHT_BROWSERS_PATH=/home/worker/.cache/ms-playwright
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

RUN playwright install chromium

CMD ["python", "-m", "app.worker"]
