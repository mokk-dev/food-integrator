# src/tasks/worker.py
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
from src.tasks.scheduler import start_scheduler


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
    
    async def _register_order_event(
        self,
        session: AsyncSession,
        event_id: str,
        order_id: int,
        event_type: str,
        status: str,
        payload_dict: dict,
        inbox_received_at: datetime
    ):
        """Helper para registrar a linha do tempo (Event Sourcing) na tabela order_events."""
        result = await session.execute(
            text("SELECT operation_day_id FROM orders WHERE id = :order_id"),
            {"order_id": order_id}
        )
        row = result.fetchone()
        if not row:
            logger.warning("worker.order_missing_for_event", order_id=order_id, event_id=event_id, msg="Pedido não encontrado para atrelar evento.")
            return
            
        operation_day_id = row[0]
        
        raw_event_at = payload_dict.get("created_at") or payload_dict.get("timestamp")
        if raw_event_at:
            try:
                event_at = datetime.fromisoformat(str(raw_event_at).replace("Z", "+00:00"))
            except ValueError:
                event_at = datetime.now()
        else:
            event_at = datetime.now()
            
        query = text("""
            INSERT INTO order_events (
                event_id, order_id, operation_day_id, event_type, status,
                event_at, received_at, inbox_received_at
            ) VALUES (
                :event_id, :order_id, :operation_day_id, :event_type, :status,
                :event_at, NOW(), :inbox_received_at
            ) ON CONFLICT (event_id, event_at) DO NOTHING
        """)
        
        await session.execute(query, {
            "event_id": str(event_id),
            "order_id": int(order_id),
            "operation_day_id": int(operation_day_id),
            "event_type": str(event_type),
            "status": str(status) if status else "unknown",
            "event_at": event_at,
            "inbox_received_at": inbox_received_at
        })
    
    async def _process_event(self, session: AsyncSession, event: tuple) -> bool:
        """Processa evento individual injetando a sessão (Unit of Work)."""
        (event_id, order_id, event_type, order_status, payload, received_at, attempts) = event
        log = logger.bind(event_id=event_id, order_id=order_id, event_type=event_type)
        
        try:
            log.info("event.processing_started")
            payload_dict = payload if isinstance(payload, dict) else (json.loads(payload) if isinstance(payload, str) else {})
            merchant_id = payload_dict.get("merchant_id", self.merchant_id)
            
            if event_type == "ORDER_CREATED":
                enrichment = OrderEnrichmentService()
                success, error = await enrichment.enrich_order(session=session, order_id=order_id, merchant_id=merchant_id)
                if not success:
                    log.error("event.enrichment_failed", error=error)
                    raise Exception(f"Enrichment failed: {error}")
                log.info("event.order_enriched")
            
            elif event_type == "ORDER_STATUS_UPDATED":
                new_status = payload_dict.get("order_status") or payload_dict.get("new_status")
                
                if new_status:
                    new_status = new_status.lower().strip()
                    
                    # 1. Extração Dinâmica de Datas e Motivos
                    raw_event_at = payload_dict.get("created_at") or payload_dict.get("timestamp")
                    event_dt = datetime.fromisoformat(str(raw_event_at).replace("Z", "+00:00")) if raw_event_at else datetime.now()
                    
                    cancellation_reason = payload_dict.get("cancellation_reason")

                    # 2. Mapeamento de Status para Colunas da Tabela 'orders'
                    status_columns = {
                        "confirmed": "confirmed_at", "ready": "ready_at", "released": "released_at",
                        "waiting_to_catch": "waiting_to_catch_at", "canceling": "canceling_at",
                        "canceled": "cancelled_at", "closed": "closed_at", "delivered": "delivered_at"
                    }

                    timestamp_update_query = f", {status_columns[new_status]} = :event_dt" if new_status in status_columns else ""
                    cancel_update_query = ", cancellation_reason = COALESCE(:cancel_reason, cancellation_reason)" if new_status in ["canceled", "canceling"] else ""

                    # 3. Update Rápido e Limpo
                    await session.execute(
                        text(f"""
                            UPDATE orders 
                            SET status = :status, 
                                updated_at = NOW(),
                                status_changed_at = :event_dt
                                {timestamp_update_query}
                                {cancel_update_query}
                            WHERE id = :order_id
                        """),
                        {"status": new_status, "order_id": order_id, "event_dt": event_dt, "cancel_reason": cancellation_reason}
                    )
                    
                    # 4. Gatilho de Motoboy (API Dashboard)
                    if new_status == "released":
                        result = await session.execute(text("SELECT order_type FROM orders WHERE id = :order_id"), {"order_id": order_id})
                        row = result.fetchone()
                        
                        if row and row[0] == "delivery":
                            try:
                                log.info("event.fetching_delivery_man", msg="Buscando motoboy na API de Dashboard...")
                                enrichment = OrderEnrichmentService()
                                async with CardapiowebDashboardAPI() as api_dash:
                                    dashboard_data = await api_dash.get_order_details(order_id)
                                    if dashboard_data and not dashboard_data.get("_api_error"):
                                        await enrichment._update_with_dashboard_data(session, order_id, dashboard_data)
                                        log.info("event.delivery_man_updated", msg="Motoboy registrado com sucesso.")
                            except Exception as dash_err:
                                log.warning("event.delivery_man_fetch_failed", error=str(dash_err))

                    log.info("event.status_updated", new_status=new_status, event_time=str(event_dt))
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
    
    async def _mark_failed(self, session: AsyncSession, event_id: str, error: str):
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
            {"event_id": event_id, "error": error[:500]}
        )


async def main():
    """Entry point."""
    scheduler = start_scheduler()
    
    worker = WebhookWorker()
    await worker.start()
    
    scheduler.shutdown()

if __name__ == "__main__":
    asyncio.run(main())