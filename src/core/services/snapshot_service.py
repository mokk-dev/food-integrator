# src/core/services/snapshot_service.py
# ============================================
# SNAPSHOT SERVICE - MÉTRICAS DA OPERAÇÃO
# ============================================

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.logger import logger
from src.infrastructure.db.connection import get_db_session

class SnapshotService:
    """
    Serviço responsável por calcular e registrar o estado da operação (WIP).
    """

    async def take_snapshots(self):
        """Busca todos os expedientes abertos e gera um snapshot para cada."""
        logger.info("snapshot.started", msg="Iniciando geração de snapshots operacionais...")
        
        try:
            async with get_db_session() as session:
                result = await session.execute(
                    text("SELECT id, merchant_id, delivery_capacity FROM operation_days WHERE closed_at IS NULL")
                )
                open_days = result.fetchall()

                if not open_days:
                    logger.info("snapshot.skipped", msg="Nenhum expediente aberto no momento.")
                    return

                for op_day in open_days:
                    op_id, merchant_id, capacity = op_day
                    await self._generate_snapshot_for_day(session, op_id, merchant_id, capacity)

                await session.commit()
                logger.info("snapshot.completed", generated_count=len(open_days))
                
        except Exception as e:
            logger.error("snapshot.failed", error=str(e), exc_info=True)

    async def _generate_snapshot_for_day(
        self, 
        session: AsyncSession, 
        op_id: int, 
        merchant_id: str, 
        capacity: int
    ):
        """Calcula as métricas e insere na tabela operation_snapshots."""

        metrics_query = text("""
            SELECT
                COUNT(*) FILTER (WHERE status IN ('pending', 'confirmed', 'preparing')) AS in_queue,
                COUNT(*) FILTER (WHERE status = 'ready') AS ready_waiting,
                COUNT(*) FILTER (WHERE status IN ('dispatched', 'in_transit')) AS in_delivery,
                COUNT(*) FILTER (WHERE status NOT IN ('delivered', 'cancelled', 'closed')) AS total_active,
                COUNT(DISTINCT delivery_man_name) FILTER (WHERE status IN ('dispatched', 'in_transit')) AS delivery_men_busy
            FROM orders
            WHERE operation_day_id = :op_id
        """)
        metrics_result = await session.execute(metrics_query, {"op_id": op_id})
        metrics = metrics_result.fetchone()

        throughput_query = text("""
            SELECT COUNT(*)
            FROM orders
            WHERE operation_day_id = :op_id
              AND status = 'delivered'
              AND delivered_at >= NOW() - INTERVAL '1 hour'
        """)
        throughput_result = await session.execute(throughput_query, {"op_id": op_id})
        throughput = throughput_result.scalar() or 0

        avgs_query = text("SELECT avg_prep_time, avg_delivery_time FROM calculate_recent_averages(:op_id)")
        avgs_result = await session.execute(avgs_query, {"op_id": op_id})
        avgs = avgs_result.fetchone()
        
        avg_prep = avgs[0] if avgs and avgs[0] else 0
        avg_del = avgs[1] if avgs and avgs[1] else 0

        insert_query = text("""
            INSERT INTO operation_snapshots (
                operation_day_id, merchant_id, snapshot_at,
                orders_in_queue, orders_ready_waiting, orders_in_delivery, orders_total_active,
                delivery_men_active, delivery_men_busy, delivery_capacity_total,
                throughput_per_hour, avg_preparation_time_last_5, avg_delivery_time_last_5
            ) VALUES (
                :op_id, :merchant_id, NOW(),
                :in_queue, :ready_waiting, :in_delivery, :total_active,
                :delivery_men_active, :delivery_men_busy, :capacity,
                :throughput, :avg_prep, :avg_del
            )
        """)

        await session.execute(insert_query, {
            "op_id": op_id,
            "merchant_id": merchant_id,
            "in_queue": metrics.in_queue or 0,
            "ready_waiting": metrics.ready_waiting or 0,
            "in_delivery": metrics.in_delivery or 0,
            "total_active": metrics.total_active or 0,
            "delivery_men_active": capacity,
            "delivery_men_busy": metrics.delivery_men_busy or 0,
            "capacity": capacity or 0,
            "throughput": throughput,
            "avg_prep": avg_prep,
            "avg_del": avg_del
        })