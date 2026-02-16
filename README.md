# Cardapioweb Integrator

Sistema de integração e inteligência operacional para delivery, conectado à plataforma Cardapioweb.

[![Python](https://img.shields.io/badge/python-3.11-blue.svg)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-green.svg)](https://fastapi.tiangolo.com)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14+-blue.svg)](https://postgresql.org)
[![TimescaleDB](https://img.shields.io/badge/TimescaleDB-latest-orange.svg)](https://www.timescale.com)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

## 🎯 Visão

Centralizar, enriquecer e gerenciar a operação de delivery recebida via Cardapioweb, atuando como hub de inteligência (ETL) que captura eventos simples, busca dados complexos e armazena histórico para análise e tomada de decisão.

### Funcionalidades Core

- **Ingestão em tempo real**: Webhooks Cardapioweb com buffer de proteção (Inbox Pattern)
- **Enriquecimento inteligente**: Dados da API pública + API interna (dashboard)
- **Cálculo geoespacial**: Distância e zona de entrega (Haversine)
- **Séries temporais**: Snapshots operacionais para predição de tempos
- **Analytics**: Métricas por expediente, performance de entregadores, canais de venda

## 🏗️ Arquitetura

### Stack Tecnológico

| Camada | Tecnologia | Motivação |
|--------|-----------|-----------|
| **API** | FastAPI + Python 3.11 | Async nativo, alta performance para webhooks |
| **Banco** | PostgreSQL 14 + TimescaleDB | Dados temporais, compressão automática, hypertables |
| **Cache** | Redis | Idempotência, fila de processamento, sessões |
| **Deploy** | Docker Compose | Simplicidade operacional, VPS única |
| **ORM** | SQLAlchemy Core | Queries explícitas, controle total |

### Padrões Arquiteturais

- **Inbox Pattern**: Buffer de proteção contra perda de eventos
- **Event Sourcing Híbrido**: Webhooks detectam, APIs enriquecem
- **Operation Day**: Expediente lógico (18:00-02:00) ≠ dia temporal
- **Separação de Responsabilidades**: Dados de negócio vs logs técnicos

## 📁 Estrutura do Projeto

.
├── docker/                          # Infraestrutura Docker
│   ├── docker-compose.yml          # Orquestração completa
│   ├── Dockerfile                  # Multi-stage Python
│   └── postgres/
│       └── initdb/                 # Scripts SQL (00-08)
│
├── src/                             # Código-fonte principal
│   ├── config.py                   # Configurações Pydantic
│   ├── main.py                     # Entry point FastAPI
│   ├── api/                        # Camada HTTP
│   │   ├── routes/                 # Endpoints (webhooks, health, admin)
│   │   ├── dependencies.py         # Injeção de dependências
│   │   └── middleware.py           # Logging, correlation ID
│   ├── core/                       # Regras de negócio
│   │   ├── models/                 # Pydantic models
│   │   └── services/               # Inbox, enriquecimento, geo, operation day
│   ├── infrastructure/             # Adaptadores externos
│   │   ├── db/                     # SQLAlchemy, conexões
│   │   ├── cache/                  # Redis client
│   │   └── external/               # Clients Cardapioweb (pública + dashboard)
│   └── tasks/                      # Background processing
│       ├── worker.py               # Loop de processamento
│       └── snapshot_generator.py   # Cron de métricas
│
├── tests/                           # Testes
│   ├── integration/                # Testes de integração
│   └── unit/                       # Testes unitários
│
├── scripts/                         # Utilitários
│   ├── init-db.sh                  # Setup inicial
│   ├── backup.sh                   # Backup para S3
│   └── load_test.py                # Teste de carga
│
└── docs/                            # Documentação
├── architecture.md             # ADRs (Architecture Decision Records)
├── deployment.md               # Guia de deploy
└── runbooks/                   # Playbooks de incidentes


## 🚀 Quick Start

### Pré-requisitos

- Docker 20.10+
- Docker Compose 2.0+
- Git

### 1. Clone e Configure

```bash
git clone https://github.com/seu-usuario/cardapioweb-integrator.git
cd cardapioweb-integrator

# Copie e edite as variáveis de ambiente
cp .env.example .env
# Edite .env com suas credenciais Cardapioweb
```

### 2. Suba a Infraestrutura

```bash
cd docker
docker-compose up -d

# Verifique se tudo está saudável
docker-compose ps
docker-compose logs -f app
```

### 3. Verifique o Setup

```bash
# Health check
curl http://localhost:8000/health

# Ready check (inclui DB e Redis)
curl http://localhost:8000/ready
```

### 4. Teste o Webhook

```bash
curl -X POST http://localhost:8000/webhook/orders \
  -H "X-Webhook-Token: seu_token_aqui" \
  -H "Content-Type: application/json" \
  -d '{
    "event_id": "test_001",
    "event_type": "ORDER_CREATED",
    "merchant_id": 6758,
    "order_id": 182564627,
    "order_status": "waiting_confirmation",
    "created_at": "2024-02-09T18:30:41-03:00"
  }'
```