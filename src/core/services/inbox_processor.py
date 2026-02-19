# ============================================
# INBOX PROCESSOR - INGESTÃO DE WEBHOOKS
# ============================================

import json
from datetime import datetime
from typing import Dict, Any, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.core.models.webhook import WebhookPayload
from src.infrastructure.cache.redis_client import redis_client
from src.infrastructure.db.connection import get_db_session


class InboxProcessor:
    """
    Processa ingestão de webhooks no inbox.
    
    Responsabilidades:
    1. Validar payload
    2. Verificar duplicatas (Redis)
    3. Inserir no webhook_inbox
    4. Marcar como "processado" no Redis (para deduplicação)
    """
    
    def __init__(self):
        self.merchant_id = settings.default_merchant_id
    
    async def process_webhook(
        self,
        payload: WebhookPayload,
        correlation_id: Optional[str] = None
    ) -> Tuple[str, Optional[str]]:
        """
        Processa webhook recebido.
        
        Retorna:
            (status, error_message)
            status: "accepted", "duplicate", "error"
        """
        try:
            # 1. Verificar duplicata no Redis (rápido)
            is_duplicate = await redis_client.is_event_processed(payload.event_id)
            if is_duplicate:
                return "duplicate", None
            
            # 2. Tentar adquirir lock (evita race condition)
            has_lock = await redis_client.mark_event_processing(payload.event_id)
            if not has_lock:
                # Outro processo está tratando, considerar duplicado
                return "duplicate", None
            
            # 3. Inserir no inbox
            await self._insert_to_inbox(payload)
            
            # 4. Marcar como processado no Redis (TTL longo)
            await redis_client.mark_event_processed(
                payload.event_id,
                ttl_seconds=86400  # 24 horas
            )
            
            # 5. Liberar lock
            await redis_client.release_event_lock(payload.event_id)
            
            return "accepted", None
            
        except Exception as e:
            # Tentar liberar lock em caso de erro
            try:
                await redis_client.release_event_lock(payload.event_id)
            except:
                pass
            
            return "error", str(e)
    
    async def _insert_to_inbox(self, payload: WebhookPayload) -> None:
        """Insere evento no webhook_inbox."""
        
        async with get_db_session() as session:
            # Preparar payload JSONB
            raw_payload = payload.raw_payload if payload.raw_payload else payload.model_dump()
            
            query = text("""
                INSERT INTO webhook_inbox (
                    event_id, order_id, event_type, order_status,
                    payload, status, received_at
                ) VALUES (
                    :event_id, :order_id, :event_type, :order_status,
                    :payload, 'pending', NOW()
                )
                ON CONFLICT (event_id) DO NOTHING
                RETURNING event_id
            """)
            
            result = await session.execute(
                query,
                {
                    "event_id": payload.event_id,
                    "order_id": payload.order_id,
                    "event_type": payload.event_type,
                    "order_status": payload.order_status,
                    "payload": json.dumps(raw_payload, default=str)
                }
            )
            
            # Verificar se inseriu (não foi conflito)
            row = result.fetchone()
            if not row:
                # Conflito de PK - já existe, considerar duplicado
                raise ValueError(f"Event {payload.event_id} already exists in inbox")