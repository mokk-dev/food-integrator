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
from src.infrastructure.cache.redis_client import redis_client
from src.infrastructure.db.connection import get_db_session


class WebhookWorker:
    """
    Worker assíncrono para processamento de webhooks pendentes.
    
    Etapa 3 (atual): Apenas polling e logging.
    Etapa 4 (futuro): Enriquecimento de pedidos, chamadas API externas.
    """
    
    def __init__(self):
        self.running = False
        self.poll_interval = settings.worker_poll_interval
        self.batch_size = settings.worker_batch_size
        self.max_retries = settings.worker_max_retries
    
    async def start(self):
        """Inicia loop principal do worker."""
        self.running = True
        
        # Setup signal handlers para graceful shutdown
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.stop)
        
        await redis_client.connect()
        
        print(f"🔄 Worker iniciado (intervalo: {self.poll_interval}s, batch: {self.batch_size})")
        
        while self.running:
            try:
                processed = await self._process_batch()
                
                if processed > 0:
                    print(f"✅ Processados {processed} eventos")
                else:
                    # Sem eventos, aguardar
                    await asyncio.sleep(self.poll_interval)
                    
            except Exception as e:
                print(f"❌ Erro no worker: {e}")
                await asyncio.sleep(self.poll_interval)
        
        print("🛑 Worker encerrado")
    
    def stop(self):
        """Sinaliza parada graceful."""
        print("⚠️  Recebido sinal de parada...")
        self.running = False
    
    async def _process_batch(self) -> int:
        """
        Processa um lote de eventos pendentes.
        
        Retorna: número de eventos processados
        """
        processed_count = 0
        
        async with get_db_session() as session:
            # Buscar eventos pendentes
            events = await self._fetch_pending_events(session)
            
            for event in events:
                if not self.running:
                    break
                
                success = await self._process_single_event(session, event)
                if success:
                    processed_count += 1
        
        return processed_count
    
    async def _fetch_pending_events(self, session: AsyncSession) -> list:
        """Busca eventos pendentes do inbox."""
        query = text("""
            SELECT 
                event_id, order_id, event_type, order_status,
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
    
    async def _process_single_event(
        self,
        session: AsyncSession,
        event: tuple
    ) -> bool:
        """
        Processa evento individual.
        
        Etapa 3: Apenas marca como 'processed' e loga.
        Etapa 4: Aqui virá o enriquecimento de pedidos.
        """
        (
            event_id, order_id, event_type, order_status,
            payload, received_at, attempts
        ) = event
        
        try:
            print(f"📝 Processando: {event_id} (order: {order_id}, type: {event_type})")
            
            # TODO Etapa 4: 
            # - Chamar API pública Cardapioweb
            # - Enriquecer dados do pedido
            # - Inserir em 'orders'
            # - Chamar API dashboard (se necessário)
            
            # Por enquanto, apenas marcar como processado
            await self._mark_processed(session, event_id)
            
            print(f"✅ Concluído: {event_id}")
            return True
            
        except Exception as e:
            print(f"❌ Falha em {event_id}: {e}")
            await self._mark_failed(session, event_id, str(e))
            return False
    
    async def _mark_processed(self, session: AsyncSession, event_id: str):
        """Marca evento como processado."""
        query = text("""
            UPDATE webhook_inbox
            SET status = 'processed',
                processed_at = NOW(),
                processing_attempts = processing_attempts + 1
            WHERE event_id = :event_id
        """)
        await session.execute(query, {"event_id": event_id})
    
    async def _mark_failed(
        self,
        session: AsyncSession,
        event_id: str,
        error: str
    ):
        """Marca evento como falho."""
        query = text("""
            UPDATE webhook_inbox
            SET status = 'failed',
                last_error = :error,
                processing_attempts = processing_attempts + 1,
                processed_at = NOW()
            WHERE event_id = :event_id
        """)
        await session.execute(query, {"event_id": event_id, "error": error})


# Entry point para execução standalone
async def main():
    """Função principal para rodar worker."""
    worker = WebhookWorker()
    await worker.start()


if __name__ == "__main__":
    asyncio.run(main())