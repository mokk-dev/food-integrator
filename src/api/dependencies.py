# ============================================
# DEPENDÊNCIAS FASTAPI - AUTH E VALIDAÇÃO
# ============================================

import secrets

from fastapi import HTTPException, Request, status

from src.config import settings
from src.infrastructure.cache.redis_client import redis_client


async def verify_payload_size(request: Request):
    """Bloqueia payloads maiores que 500KB para evitar Memory Exhaustion (DDoS)."""
    content_length = request.headers.get("content-length")
    if content_length and int(content_length) > 512_000:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail="Payload too large",
        )


async def rate_limiter(request: Request):
    """Limita a quantidade de requisições por IP usando Redis."""
    await redis_client.connect()

    # Usa o IP do cliente como chave. Em produção com proxy (Nginx/Cloudflare), use X-Forwarded-For
    client_ip = request.client.host if request.client else "unknown"
    key = f"rate_limit:webhook:{client_ip}"

    # Limite de 100 requisições a cada 10 segundos por IP
    allowed, remaining = await redis_client.check_rate_limit(
        key, max_requests=100, window_seconds=10
    )

    if not allowed:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many requests. Please slow down.",
        )


async def verify_webhook_token(request: Request) -> bool:
    """
    Verifica o token de webhook com timing-safe comparison.

    Header esperado: X-Webhook-Token: <token>
    """
    token_header = request.headers.get("X-Webhook-Token")

    if not token_header:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-Webhook-Token header",
        )

    # Timing-safe comparison
    if not secrets.compare_digest(token_header, settings.webhook_secret_token):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Invalid token"
        )

    return True


async def get_correlation_id(request: Request) -> str:
    """Recupera ou gera correlation ID."""
    return getattr(request.state, "correlation_id", "unknown")


async def check_idempotency(event_id: str) -> bool:
    """
    Verifica se evento já foi processado (deduplicação).

    Retorna:
        True: Evento já existe (duplicado)
        False: Evento novo
    """
    await redis_client.connect()
    return await redis_client.is_event_processed(event_id)


async def acquire_processing_lock(event_id: str) -> bool:
    """
    Tenta adquirir lock para processar evento.

    Retorna:
        True: Lock adquirido (podemos processar)
        False: Outro worker está processando
    """
    await redis_client.connect()
    return await redis_client.mark_event_processing(event_id, ttl_seconds=60)
