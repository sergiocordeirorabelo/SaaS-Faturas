# 🤖 Invoice Extraction Worker
**Worker de extração automática de faturas de energia elétrica**  
Foco inicial: Amazonas Energia | Stack: Python + Playwright + Supabase + Railway

---

## 📋 Índice
- [Visão Geral](#visão-geral)
- [Arquitetura](#arquitetura)
- [Pré-requisitos](#pré-requisitos)
- [Configuração Local](#configuração-local)
- [Variáveis de Ambiente](#variáveis-de-ambiente)
- [Banco de Dados (Supabase)](#banco-de-dados-supabase)
- [Deploy no Railway](#deploy-no-railway)
- [Testes](#testes)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Adicionando Novas Concessionárias](#adicionando-novas-concessionárias)
- [Troubleshooting](#troubleshooting)

---

## Visão Geral

O worker roda como um **serviço de background** que:

1. Faz **polling** na tabela `extraction_requests` do Supabase buscando tarefas `pendente`
2. Abre uma instância headless do **Chromium via Playwright** com configurações stealth
3. Faz **login** no portal da concessionária e resolve o **reCAPTCHA** via API terceira
4. Navega até o histórico de faturas e faz **download dos PDFs** dos últimos 12 meses
5. Faz **upload** dos PDFs para o Supabase Storage e atualiza o banco com os links
6. Marca a tarefa como `concluido` ou `erro_extracao` com detalhes do problema

```
┌─────────────────┐     poll     ┌──────────────────────┐
│   Supabase DB   │◄─────────────│                      │
│  (fila/status)  │─────────────►│   Invoice Worker     │
└─────────────────┘   atualiza   │   (Python/Playwright)│
                                 │                      │
┌─────────────────┐   upload     │  ┌────────────────┐  │
│ Supabase Storage│◄─────────────│  │ Captcha Solver │  │
│    (PDFs)       │              │  │ (2Captcha API) │  │
└─────────────────┘              └──┴────────────────┘──┘
```

---

## Pré-requisitos

- Python **3.11+**
- Docker e Docker Compose (para rodar localmente em container)
- Conta no **Supabase** (plano gratuito é suficiente para MVP)
- Conta num serviço de captcha: **2Captcha**, **Anti-Captcha** ou **CapSolver**

---

## Configuração Local

### 1. Clone o repositório e crie o ambiente virtual

```bash
git clone https://github.com/sua-org/invoice-worker.git
cd invoice-worker

python -m venv .venv
source .venv/bin/activate        # Linux/macOS
# .venv\Scripts\activate         # Windows

pip install -r requirements.txt
```

### 2. Instale os navegadores do Playwright

```bash
playwright install chromium --with-deps
```

> ⚠️ O `--with-deps` instala automaticamente as bibliotecas do sistema necessárias (libnss3, libatk, etc.)

### 3. Configure as variáveis de ambiente

```bash
cp .env.example .env
# Edite o .env com seus valores reais
```

### 4. Execute a migração no Supabase

Acesse **Supabase Dashboard → SQL Editor** e execute o conteúdo de:
```
migrations/001_initial_schema.sql
```

### 5. Rode o worker

```bash
python -m src.worker
```

Ou via Docker:

```bash
docker compose up --build
```

---

## Variáveis de Ambiente

| Variável | Obrigatório | Padrão | Descrição |
|---|---|---|---|
| `SUPABASE_URL` | ✅ | — | URL do projeto Supabase |
| `SUPABASE_SERVICE_KEY` | ✅ | — | Chave `service_role` do Supabase |
| `SUPABASE_BUCKET` | ❌ | `faturas` | Nome do bucket de storage |
| `CAPTCHA_SERVICE` | ✅ | — | `2captcha` \| `anticaptcha` \| `capsolver` |
| `CAPTCHA_API_KEY` | ✅ | — | Chave da API do serviço de captcha |
| `CAPTCHA_TIMEOUT_SECONDS` | ❌ | `120` | Timeout total para resolver captcha |
| `CAPTCHA_MAX_RETRIES` | ❌ | `3` | Tentativas de resolução do captcha |
| `BROWSER_HEADLESS` | ❌ | `true` | `false` para ver o navegador (debug) |
| `PROXY_SERVER` | ❌ | — | `http://user:pass@host:porta` |
| `POLL_INTERVAL_SECONDS` | ❌ | `15` | Intervalo entre ciclos de polling |
| `MAX_CONCURRENT_TASKS` | ❌ | `2` | Tarefas simultâneas |
| `MAX_INVOICES_MONTHS` | ❌ | `12` | Meses de histórico a extrair |
| `LOG_LEVEL` | ❌ | `INFO` | `DEBUG` \| `INFO` \| `WARNING` \| `ERROR` |

---

## Banco de Dados (Supabase)

### Estrutura da tabela `extraction_requests`

| Campo | Tipo | Descrição |
|---|---|---|
| `id` | UUID | Chave primária |
| `concessionaria` | TEXT | Ex: `amazonas_energia` |
| `unidade_consumidora` | TEXT | Número da UC do cliente |
| `credentials` | JSONB | `{"cpf_cnpj": "...", "senha": "..."}` |
| `status` | TEXT | `pendente` → `em_progresso` → `concluido` ou `erro_*` |
| `status_detail` | TEXT | Mensagem de erro ou log da execução |
| `pdf_links` | JSONB | Array com `{mes_referencia, storage_url, filename}` |
| `created_at` | TIMESTAMPTZ | Criação da tarefa |
| `updated_at` | TIMESTAMPTZ | Última atualização (auto) |

### Ciclo de vida de uma tarefa

```
pendente → em_progresso → concluido
                       ↘ erro_extracao
                       ↘ credenciais_invalidas
```

### Como enfileirar uma tarefa (via API/código externo)

```python
from supabase import create_client

client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

client.table("extraction_requests").insert({
    "concessionaria": "amazonas_energia",
    "unidade_consumidora": "123456-7",
    "credentials": {
        "cpf_cnpj": "000.000.000-00",
        "senha": "senha_do_cliente",
        "unidade_consumidora": "123456-7"
    }
}).execute()
```

> 🔐 **Segurança:** Considere criptografar o campo `credentials` com `pgcrypto` ou AES antes de salvar. O campo é JSONB por flexibilidade, mas **não deve ser exposto via API pública**.

---

## Deploy no Railway

### 1. Crie um novo projeto no Railway

```bash
# Instale o CLI do Railway
npm install -g @railway/cli

railway login
railway init
```

### 2. Configure as variáveis de ambiente

No Dashboard do Railway: **Variables → Add Variable** para cada item do `.env`.

Ou via CLI:
```bash
railway variables set SUPABASE_URL=https://xxx.supabase.co
railway variables set SUPABASE_SERVICE_KEY=eyJ...
railway variables set CAPTCHA_API_KEY=sua_chave
# ... demais variáveis
```

### 3. Deploy

```bash
railway up
```

O Railway detecta automaticamente o `Dockerfile` e faz o build.

### Recursos recomendados no Railway

| Plano | RAM | CPU | Adequado para |
|---|---|---|---|
| Hobby ($5/mês) | 512 MB | 0.5 vCPU | Até ~2 tarefas/hora |
| Pro ($20/mês) | 1 GB | 1 vCPU | Produção leve |
| Custom | 2 GB+ | 2+ vCPU | Alto volume |

> ⚠️ O Chromium consome ~300-500 MB por instância. Com `MAX_CONCURRENT_TASKS=2`, recomenda-se **mínimo 1.5 GB de RAM**.

---

## Testes

```bash
# Todos os testes
pytest tests/ -v

# Com cobertura
pytest tests/ -v --cov=src --cov-report=term-missing

# Apenas testes do captcha
pytest tests/test_worker.py::TestTwoCaptchaSolver -v
```

---

## Estrutura do Projeto

```
invoice-worker/
├── src/
│   ├── worker.py              # Entry point — loop de polling
│   ├── config.py              # Configurações via .env (Pydantic)
│   ├── captcha/
│   │   └── solver.py          # Integração 2Captcha / AntiCaptcha / CapSolver
│   ├── db/
│   │   └── client.py          # Wrapper Supabase (DB + Storage)
│   ├── extractors/
│   │   ├── base.py            # Classe base com ciclo de vida do Playwright
│   │   └── amazonas_energia.py # Lógica específica da Amazonas Energia
│   └── utils/
│       └── logger.py          # Logger estruturado
├── tests/
│   └── test_worker.py         # Testes unitários
├── migrations/
│   └── 001_initial_schema.sql # Schema do banco de dados
├── Dockerfile                 # Imagem de produção (multi-stage)
├── docker-compose.yml         # Dev local
├── railway.toml               # Configuração de deploy
├── requirements.txt
├── .env.example
└── README.md
```

---

## Adicionando Novas Concessionárias

1. Crie o arquivo `src/extractors/nome_concessionaria.py`
2. Herde de `BaseExtractor` e implemente `async def _extract(self) -> list[dict]`
3. Registre no mapa em `src/worker.py`:

```python
EXTRACTOR_MAP = {
    "amazonas_energia": AmazonasEnergiaExtractor,
    "nome_concessionaria": NomeConcessionariaExtractor,  # ← adiciona aqui
}
```

4. Insira tarefas com `"concessionaria": "nome_concessionaria"` no banco.

---

## Troubleshooting

### `playwright install` falha no Docker
Certifique-se de usar `playwright install chromium --with-deps` no Dockerfile e que o stage base é `python:3.11-slim` (baseado em Debian).

### Captcha sempre falha com timeout
- Verifique o saldo da conta no serviço de captcha
- Aumente `CAPTCHA_TIMEOUT_SECONDS` para `180`
- Teste com `BROWSER_HEADLESS=false` e inspecione a página manualmente

### Worker processa a mesma tarefa duas vezes
Com `MAX_CONCURRENT_TASKS > 1` pode haver race condition no polling. Solução: implemente uma RPC no Supabase usando `SELECT ... FOR UPDATE SKIP LOCKED` para lock atômico das linhas.

### Erro `credenciais_invalidas` inesperado
O seletor `SEL_ERROR_MSG` pode estar pegando mensagens que não são de erro de credenciais. Verifique os logs com `LOG_LEVEL=DEBUG` e ajuste as keywords em `_assert_login_success()`.

### Alto uso de memória no Railway
- Reduza `MAX_CONCURRENT_TASKS` para `1`
- Verifique se `browser.close()` está sendo chamado (o `finally` em `_teardown_browser` garante isso)
- Reinicie o worker periodicamente via cron se houver memory leak acumulado

---

## Licença

MIT — uso livre para projetos comerciais e open source.
