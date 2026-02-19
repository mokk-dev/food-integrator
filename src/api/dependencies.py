# ============================================
# DEPENDÊNCIAS FASTAPI - AUTH E VALIDAÇÃO
# ============================================

import hmac
import secrets
import time
from typing import Optional

from fastapi import Header, HTTPException, Request, status

from src.config import settings
from src.infrastructure.cache.redis_client import redis_client


async def verify_webhook_token(request: Request) -> bool:
    """
    Verifica o token de webhook com timing-safe comparison.
    
    Header esperado: X-Webhook-Token: <token>
    """
    token_header = request.headers.get("X-Webhook-Token")
    
    if not token_header:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-Webhook-Token header"
        )
    
    # Timing-safe comparison
    if not secrets.compare_digest(token_header, settings.webhook_secret_token):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid token"
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