# ============================================
# WORKER - PROCESSAMENTO BACKGROUND
# ============================================

import asyncio
import json
import signal
import sys
from datetime import datetime
from typing import Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.core.services.order_enrichment import OrderEnrichmentService
from src.infrastructure.cache.redis_client import redis_client
from src.infrastructure.db.connection import get_db_session


class WebhookWorker:
    """
    Worker assíncrono para processamento de webhooks.
    """
    
    def __init__(self):
        self.running = False
        self.poll_interval = settings.worker_poll_interval
        self.batch_size = settings.worker_batch_size
        self.max_retries = settings.worker_max_retries
        self.merchant_id = settings.default_merchant_id
    
    async def start(self):
        """Inicia loop principal."""
        self.running = True
        
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.stop)
        
        await redis_client.connect()
        
        print(f"🔄 Worker iniciado (intervalo: {self.poll_interval}s)")
        
        while self.running:
            try:
                processed = await self._process_batch()
                
                if processed > 0:
                    print(f"✅ Processados {processed} eventos")
                else:
                    await asyncio.sleep(self.poll_interval)
                    
            except Exception as e:
                print(f"❌ Erro no worker: {e}")
                await asyncio.sleep(self.poll_interval)
        
        print("🛑 Worker encerrado")
    
    def stop(self):
        """Sinaliza parada."""
        print("⚠️  Recebido sinal de parada...")
        self.running = False
    
    async def _process_batch(self) -> int:
        """Processa lote de eventos pendentes."""
        processed_count = 0
        
        async with get_db_session() as session:
            events = await self._fetch_pending(session)
            
            for event in events:
                if not self.running:
                    break
                
                success = await self._process_event(session, event)
                if success:
                    processed_count += 1
        
        return processed_count
    
    async def _fetch_pending(self, session: AsyncSession) -> list:
        """Busca eventos pendentes."""
        query = text("""
            SELECT event_id, order_id, event_type, order_status,
                   payload, received_at, processing_attempts
            FROM webhook_inbox
            WHERE status = 'pending'
              AND processing_attempts < :max_retries
            ORDER BY received_at ASC
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
        """)
        
        result = await session.execute(
            query,
            {"max_retries": self.max_retries, "limit": self.batch_size}
        )
        
        return result.fetchall()
    
    async def _process_event(self, session: AsyncSession, event: tuple) -> bool:
        """Processa evento individual."""
        (
            event_id, order_id, event_type, order_status,
            payload, received_at, attempts
        ) = event
        
        try:
            print(f"📝 {event_id}: {event_type} (order: {order_id})")
            
            payload_dict = json.loads(payload) if payload else {}
            
            # ETAPA 4: Enriquecimento
            if event_type == "ORDER_CREATED":
                enrichment = OrderEnrichmentService()
                success, error = await enrichment.enrich_order(
                    order_id=order_id,
                    event_type=event_type,
                    order_status=order_status or "pending",
                    raw_payload=payload_dict
                )
                
                if not success:
                    raise Exception(f"Enrichment failed: {error}")
                
                print(f"✅ Order {order_id} enriquecido")
            
            elif event_type == "ORDER_STATUS_UPDATED":
                # TODO: Atualizar status em orders existente
                print(f"⚠️  Status update não implementado: {order_id}")
                pass
            
            else:
                print(f"ℹ️  Evento não tratado: {event_type}")
            
            # Marcar como processado
            await self._mark_processed(session, event_id)
            return True
            
        except Exception as e:
            print(f"❌ Falha em {event_id}: {e}")
            await self._mark_failed(session, event_id, str(e))
            return False
    
    async def _mark_processed(self, session: AsyncSession, event_id: str):
        """Marca evento como processado."""
        await session.execute(
            text("""
                UPDATE webhook_inbox
                SET status = 'processed',
                    processed_at = NOW(),
                    processing_attempts = processing_attempts + 1
                WHERE event_id = :event_id
            """),
            {"event_id": event_id}
        )
    
    async def _mark_failed(
        self,
        session: AsyncSession,
        event_id: str,
        error: str
    ):
        """Marca evento como falho."""
        await session.execute(
            text("""
                UPDATE webhook_inbox
                SET status = 'failed',
                    last_error = :error,
                    processing_attempts = processing_attempts + 1,
                    processed_at = NOW()
                WHERE event_id = :event_id
            """),
            {"event_id": event_id, "error": error[:500]}  # Limitar tamanho
        )


async def main():
    """Entry point."""
    worker = WebhookWorker()
    await worker.start()


if __name__ == "__main__":
    asyncio.run(main())