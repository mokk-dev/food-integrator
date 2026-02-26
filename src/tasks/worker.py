# ============================================
# WORKER - PROCESSAMENTO BACKGROUND
# ============================================

import asyncio
import json
import signal
import sys
import structlog
from datetime import datetime
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text


from src.core.logger import logger
from src.config import settings
from src.core.services.order_enrichment import OrderEnrichmentService
from src.infrastructure.cache.redis_client import redis_client
from src.infrastructure.db.connection import get_db_session

from src.infrastructure.external.cardapioweb_dashboard import CardapiowebDashboardAPI

from src.infrastructure.external.cardapioweb_auth import CardapiowebAuthManager


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
        import time
        self.running = True
        
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.stop)
        
        await redis_client.connect()

        try:
            print("🔐 Validando token de acesso da API Cardapioweb...")
            await CardapiowebAuthManager().get_valid_access_token()
        except Exception as e:
            logger.warning("worker.initial_auth_warning", error=str(e), msg="Falha no warm-up do token.")

        print(f"🔄 Worker iniciado (intervalo: {self.poll_interval}s | batch: {self.batch_size})")

        logger.info("worker.started", interval=self.poll_interval, batch_size=self.batch_size)
        
        while self.running:
            try:
                start_time = time.time()
                processed = await self._process_batch()
                duration = time.time() - start_time
                
                if processed > 0:
                    logger.info("worker.batch_processed", 
                                processed_count=processed, 
                                max_batch=self.batch_size, 
                                duration_seconds=round(duration, 2))
                    
                    if processed == self.batch_size:
                        logger.warning("worker.queue_saturated", msg="Lote cheio processado. Fila pode estar atrasada.")
                else:
                    await asyncio.sleep(self.poll_interval)
                    
            except Exception as e:
                logger.error("worker.batch_error", error=str(e), exc_info=True)
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

            if events:
                await session.commit()
                
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

        log = logger.bind(event_id=event_id, order_id=order_id, event_type=event_type)
        
        try:
            log.info("event.processing_started")
            if isinstance(payload, dict):
                payload_dict = payload
            elif isinstance(payload, str):
                payload_dict = json.loads(payload)
            else:
                payload_dict = {}

            merchant_id = payload_dict.get("merchant_id", self.merchant_id)
            
            if event_type == "ORDER_CREATED":
                enrichment = OrderEnrichmentService()
                success, error = await enrichment.enrich_order(
                    order_id=order_id,
                    event_type=event_type,
                    order_status=order_status or "pending",
                    raw_payload=payload_dict,
                    merchant_id=merchant_id
                )
                
                if not success:
                    log.error("event.enrichment_failed", error=error)
                    raise Exception(f"Enrichment failed: {error}")
                
                log.info("event.order_enriched")
            
            elif event_type == "ORDER_STATUS_UPDATED":
                new_status = payload_dict.get("order_status") or payload_dict.get("new_status")
                
                if new_status:
                    is_cancelled = new_status.lower() in ["cancelled", "canceled", "cancelado"]
                    cancel_query_part = ", cancelled_at = NOW()" if is_cancelled else ""

                    await session.execute(
                        text(f"""
                            UPDATE orders 
                            SET status = :status, 
                                updated_at = NOW(),
                                status_changed_at = NOW()
                                {cancel_query_part}
                            WHERE id = :order_id
                        """),
                        {"status": new_status, "order_id": order_id}
                    )
                    if is_cancelled:
                        log.info("event.order_cancelled", new_status=new_status)
                    else:
                        log.info("event.status_updated", new_status=new_status)
                else:
                    log.warning("event.missing_new_status", payload=payload_dict)
            
            else:
                log.info("event.ignored", msg="Evento não tratado")
            
            await self._mark_processed(session, event_id)
            return True
            
        except Exception as e:
            log.error("event.processing_failed", error=str(e), exc_info=True)
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