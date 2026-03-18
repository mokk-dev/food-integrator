import asyncio
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from tenacity import retry, wait_exponential, stop_after_attempt

from src.core.logger import logger
from src.infrastructure.db.connection import get_db_session
from src.infrastructure.external.cardapioweb_public import CardapiowebPublicAPI
from src.infrastructure.external.cardapioweb_dashboard import CardapiowebDashboardAPI
from src.core.services.order_enrichment import OrderEnrichmentService
from src.core.services.reconciliation_service import ReconciliationService
from src.infrastructure.cache.redis_client import redis_client

class HistoricalSyncService:
    def __init__(self):
        self.public_api = CardapiowebPublicAPI()
        self.enrichment_service = OrderEnrichmentService()
        self.reconciliation_service = ReconciliationService()

    @retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(5))
    async def _safe_get_orders_history(self, opened_at, closed_at, page):
        return await self.public_api.get_orders_history_page(opened_at, closed_at, page)

    async def _update_job_status(self, session: AsyncSession, job_id: int, status: str, total: int = None, processed: int = None, error: str = None):
        """Atualiza a barra de progresso no banco."""
        updates = ["status = :status", "updated_at = NOW()"]
        params = {"status": status, "job_id": job_id}
        
        if total is not None:
            updates.append("total_shifts = :total")
            params["total"] = total
        if processed is not None:
            updates.append("processed_shifts = :processed")
            params["processed"] = processed
        if error is not None:
            updates.append("error_message = :error")
            params["error"] = error
            
        query = text(f"UPDATE sync_jobs SET {', '.join(updates)} WHERE id = :job_id")
        await session.execute(query, params)
        await session.commit()

    async def run_job(self, job_id: int, merchant_id: str, start_date: datetime, end_date: datetime):
        """Executa a sincronização vinculada a um Job ID e gerencia o Redis Lock."""
        lock_key = f"backfill_lock:{merchant_id}"
        logger.info("historical_sync.job_started", job_id=job_id, merchant_id=merchant_id)

        try:
            async with get_db_session() as session:
                await self._update_job_status(session, job_id, "processing")

            # 1. Busca os caixas
            async with CardapiowebDashboardAPI() as dashboard_api:
                cash_flows = await dashboard_api.get_cash_flows_by_period(merchant_id, start_date, end_date)

            total_shifts = len(cash_flows) if cash_flows else 0

            async with get_db_session() as session:
                if total_shifts == 0:
                    await self._update_job_status(session, job_id, "completed", total=0, processed=0)
                    return
                await self._update_job_status(session, job_id, "processing", total=total_shifts, processed=0)

            # 2. Processa cada expediente
            for index, cf in enumerate(cash_flows, start=1):
                await redis_client.client.expire(lock_key, 600)
                
                await self._process_single_shift(merchant_id, cf)
                
                async with get_db_session() as session:
                    await self._update_job_status(session, job_id, "processing", processed=index)
                
                await asyncio.sleep(7)

            async with get_db_session() as session:
                await self._update_job_status(session, job_id, "completed")
            logger.info("historical_sync.job_completed", job_id=job_id)

        except Exception as e:
            logger.error("historical_sync.fatal_error", job_id=job_id, error=str(e), exc_info=True)
            async with get_db_session() as session:
                await self._update_job_status(session, job_id, "failed", error=str(e))
        finally:
            await redis_client.delete(lock_key)

    async def _process_single_shift(self, merchant_id: str, cash_flow_data: dict):
        cf_id = cash_flow_data.get("id")
        opened_at = datetime.fromisoformat(cash_flow_data["open_at"].replace("Z", "+00:00"))
        closed_at = datetime.fromisoformat(cash_flow_data["closed_at"].replace("Z", "+00:00"))
        operation_day_date = opened_at.date()

        async with get_db_session() as session:
            query_shift = text("""
                INSERT INTO operation_days (
                    merchant_id, operation_day, opened_at, closed_at, cash_flow_id,
                    start_time, end_time, delivery_capacity
                )
                SELECT 
                    merchant_id, :op_day, :opened, :closed, :cf_id,
                    default_start_time, default_end_time, default_delivery_capacity
                FROM merchants
                WHERE merchant_id = :mid
                ON CONFLICT (merchant_id, operation_day, opened_at) DO UPDATE SET 
                    closed_at = EXCLUDED.closed_at, 
                    cash_flow_id = EXCLUDED.cash_flow_id
                RETURNING id
            """)
            
            result = await session.execute(query_shift, {
                "mid": merchant_id, 
                "op_day": operation_day_date, 
                "opened": opened_at, 
                "closed": closed_at, 
                "cf_id": cf_id
            })
            shift_internal_id = result.scalar()
            await session.commit()

            page = 1
            has_more = True
            while has_more:
                response = await self._safe_get_orders_history(opened_at, closed_at, page)
                orders = response if isinstance(response, list) else response.get("data", [])
                
                if not orders: break

                for order_data in orders:
                    order_id = order_data.get("id")
                    current_status = order_data.get("status")

                    try:
                        async with session.begin_nested():
                            await self.enrichment_service.enrich_order(session=session, order_id=order_id, merchant_id=merchant_id)
                            await session.execute(
                                text("UPDATE orders SET status = :status, updated_at = NOW() WHERE id = :order_id"),
                                {"status": current_status, "order_id": order_id}
                            )
                    except Exception as e:
                        logger.error("historical_sync.order_failed", order_id=order_id, error=str(e))

                await session.commit()

                if isinstance(response, dict):
                    has_more = page < response.get("meta", {}).get("last_page", response.get("lastPage", 1))
                else:
                    has_more = len(orders) >= 100
                
                page += 1
                await asyncio.sleep(1)

        await self.reconciliation_service.run_reconciliation_for_shift(merchant_id, opened_at, closed_at, shift_internal_id)