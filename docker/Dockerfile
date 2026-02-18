# ============================================
# CARDAPIOWEB INTEGRATOR - Dockerfile
# Multi-stage: build → production
# ============================================

# --------------------------------------------
# STAGE 1: Builder
# --------------------------------------------
FROM python:3.11-slim as builder

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY pyproject.toml .
COPY src/ ./src/

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -e .

# --------------------------------------------
# STAGE 2: Production
# --------------------------------------------
FROM python:3.11-slim as production

WORKDIR /app

RUN groupadd -r appgroup && useradd -r -g appgroup appuser

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Copiar virtual environment COMPLETO (com todos os binários)
COPY --from=builder /opt/venv /opt/venv

# Ativar PATH do venv
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1 \
    PYTHONHASHSEED=random \
    APP_HOME=/app \
    PYTHONPATH=/app

# Copiar código fonte (necessário para imports)
COPY pyproject.toml .
COPY src/ ./src/

# Garantir que o pacote está registrado (symlinks)
RUN pip install -e . --no-deps

# Permissões
RUN mkdir -p /app/logs && chown -R appuser:appgroup /app /opt/venv

USER appuser

EXPOSE 8000

# Usar python -m para garantir que encontra o módulo
CMD ["python", "-m", "uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]