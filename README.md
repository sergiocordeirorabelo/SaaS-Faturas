# Trianon — Plataforma de Gestão de Energia Elétrica

Monorepo unificado do SaaS Faturas + SaaS Análise.

## Estrutura

```
/
├── src/                        # Backend — roda no Railway
│   ├── extractors/             # Extração de faturas (Amazonas Energia)
│   ├── parsers/
│   │   ├── parser_fatura.py    # PDF → dados estruturados
│   │   ├── parser_fatura_ia.py # Parse via IA
│   │   ├── analyzer_fatura.py  # Regras de negócio por fatura
│   │   └── analyzer_historico.py # Agrega N faturas → ResultadoHistorico
│   ├── reports/
│   │   ├── gerar_relatorio.py  # Relatório mensal (ReportLab)
│   │   └── gerar_estudo.py     # Estudo técnico (PPTX → PDF)
│   ├── api.py                  # Endpoints HTTP (aiohttp)
│   └── worker.py               # Worker de polling/cron
├── frontend/                   # Dashboard — deploy no Vercel
│   ├── index.html              # SPA (vanilla JS + Supabase)
│   └── vercel.json             # Config de deploy
├── Dockerfile
└── requirements.txt
```

## Motor de análise

| Classe | Arquivo | Entrada | Saída |
|---|---|---|---|
| `AnalisadorFatura` | `analyzer_fatura.py` | 1 fatura | `ResultadoAnalise` |
| `AnalisadorHistorico` | `analyzer_historico.py` | N faturas | `ResultadoHistorico` |

`AnalisadorHistorico` usa `AnalisadorFatura` internamente — sem duplicação de lógica.

## Deploy

**Backend (Railway):** root directory `/`, usa Dockerfile na raiz.

**Frontend (Vercel):** root directory `frontend/`, framework Other (static).
