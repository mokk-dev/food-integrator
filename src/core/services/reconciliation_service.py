import json
import asyncio
from datetime import datetime
from sqlalchemy import text

from src.infrastructure.db.connection import get_db_session
from src.infrastructure.external.cardapioweb_public import CardapiowebPublicAPI
from src.infrastructure.external.cardapioweb_dashboard import CardapiowebDashboardAPI
from src.core.logger import logger
from src.config import settings

class ReconciliationService:
    def __init__(self):
        self.public_api = CardapiowebPublicAPI()
        self.dashboard_api = CardapiowebDashboardAPI()

        # Calcula o tempo de espera (em segundos) entre as requisições com base na .env
        self.history_sleep_time = (60.0 / settings.cardapioweb_history_rate_limit) + 0.5
        self.details_sleep_time = (60.0 / settings.cardapioweb_details_rate_limit) + 0.1

    async def run_reconciliation_for_shift(self, merchant_id: str, opened_at: datetime, closed_at: datetime):
        """
        Rotina de Conformidade Completa projetada para rodar após o fechamento de caixa.
        Executa 2 etapas de auditoria:
        1. Resgate de pedidos inteiros não registrados.
        2. Resgate de informações de entregadores (motoboys) ausentes.
        """
        logger.info("reconciliation.started", merchant=merchant_id, opened=opened_at.isoformat(), closed=closed_at.isoformat())

        try:
            # ==========================================
            # ETAPA 1: AUDITORIA DE PEDIDOS PERDIDOS
            # ==========================================
            api_orders = await self._fetch_history_with_rate_limit(opened_at, closed_at)
            api_order_ids = {str(order["id"]) for order in api_orders}
            
            if not api_order_ids:
                logger.info("reconciliation.no_orders_api")
            else:
                async with get_db_session() as session:
                    # Nota: Alinhado com a nomenclatura 'id' que o OrderEnrichmentService usa
                    query = text("SELECT id FROM orders WHERE id = ANY(:ids)")
                    result = await session.execute(query, {"ids": [int(i) for i in api_order_ids]})
                    db_order_ids = {str(row[0]) for row in result.fetchall()}
                    
                missing_ids = api_order_ids - db_order_ids
                
                if not missing_ids:
                    logger.info("reconciliation.orders_success", msg="100% de conformidade de pedidos. Nenhum pedido perdido no turno.")
                else:
                    logger.warning("reconciliation.missing_orders_found", count=len(missing_ids), missing_ids=list(missing_ids))
                    for missing_id in missing_ids:
                        await self._recover_and_save_order(missing_id)
                        await asyncio.sleep(self.details_sleep_time)

            # ==========================================
            # ETAPA 2: AUDITORIA DE ENTREGADORES (MOTOBOYS)
            # ==========================================
            logger.info("reconciliation.delivery_info_started", msg="Iniciando verificação de entregadores não registrados...")
            await self._recover_missing_delivery_info(merchant_id, opened_at, closed_at)

            logger.info("reconciliation.completed", msg="Rotina completa de reconciliação de turno finalizada.")

        except Exception as e:
            logger.error("reconciliation.failed", error=str(e))

    async def _fetch_history_with_rate_limit(self, start_date: datetime, end_date: datetime):
        all_orders = []
        page = 1

        while True:
            if page > 1:
                logger.debug("reconciliation.rate_limit", msg=f"Aguardando {self.history_sleep_time}s (Rate Limit)...")
                await asyncio.sleep(self.history_sleep_time)

            response = await self.public_api.get_orders_history_page(start_date, end_date, page)
            
            orders = response.get("orders", [])
            all_orders.extend(orders)
            
            pagination = response.get("pagination", {})
            current_page = pagination.get("current_page", 1)
            total_pages = pagination.get("total_pages", 1)
            
            if current_page >= total_pages or not orders:
                break
                
            page += 1
            
        return all_orders

    async def _recover_and_save_order(self, order_id: str):
        """Busca os detalhes completos na rota unitária e regista no banco."""
        try:
            details = await self.public_api.get_order(int(order_id))
            customer = details.get("customer", {})
            
            async with get_db_session() as session:
                insert_query = text("""
                    INSERT INTO orders (
                        id, merchant_id, display_id, status, order_type, 
                        customer_name, customer_phone, total_value, delivery_fee, 
                        created_at, updated_at
                    ) VALUES (
                        :id, :merchant_id, :display_id, :status, :order_type,
                        :customer_name, :customer_phone, :total, :delivery_fee,
                        COALESCE(CAST(:created_at AS TIMESTAMPTZ), NOW()), NOW()
                    ) ON CONFLICT (id) DO NOTHING
                """)
                
                await session.execute(insert_query, {
                    "id": int(order_id),
                    "merchant_id": str(settings.default_merchant_id),
                    "display_id": details.get("shortId", str(order_id)[-4:]),
                    "status": details.get("status", "closed"),
                    "order_type": details.get("type", "delivery"),
                    "customer_name": customer.get("name"),
                    "customer_phone": customer.get("phone"),
                    "total": float(details.get("total", 0.0)),
                    "delivery_fee": float(details.get("deliveryFee", 0.0)),
                    "created_at": details.get("createdAt")
                })
                await session.commit()
                
            logger.info("reconciliation.order_recovered", order_id=order_id)
            
        except Exception as e:
            logger.error("reconciliation.recover_failed", order_id=order_id, error=str(e))

    async def _recover_missing_delivery_info(self, merchant_id: str, opened_at: datetime, closed_at: datetime):
        """
        Busca pedidos de 'delivery' no turno atual que não possuem informações de entregador.
        Mapeia ID, Nome, Telefone e Rota baseando-se na mesma lógica do OrderEnrichmentService.
        """
        try:
            async with get_db_session() as session:
                # Localiza pedidos do turno, que são delivery, e estão sem motoboy
                query = text("""
                    SELECT id 
                    FROM orders 
                    WHERE merchant_id = :mid 
                      AND order_type = 'delivery'
                      AND created_at >= :opened_at
                      AND created_at <= :closed_at
                """)
                result = await session.execute(query, {
                    "mid": merchant_id,
                    "opened_at": opened_at,
                    "closed_at": closed_at
                })
                missing_delivery_orders = [row[0] for row in result.fetchall()]

            if not missing_delivery_orders:
                logger.info("reconciliation.delivery_info_complete", msg="Todos os pedidos de delivery do turno possuem entregador registrado.")
                return

            logger.info("reconciliation.missing_delivery_found", count=len(missing_delivery_orders))

            for order_id in missing_delivery_orders:
                try:
                    # Busca detalhes de entrega na API Interna (Dashboard)
                    dashboard_data = await self.dashboard_api.get_delivery_info(int(order_id))
                    
                    if dashboard_data and not dashboard_data.get("_api_error"):
                        # Extração exata como feita em OrderEnrichmentService._extract_from_dashboard
                        core_data = dashboard_data.get("data") or dashboard_data.get("order") or dashboard_data
                        delivery_man = core_data.get("delivery_man") or {}
                        delivery_route = core_data.get("delivery_route") or {}

                        delivery_man_id = delivery_man.get("id")
                        delivery_man_name = delivery_man.get("name")
                        delivery_man_phone = delivery_man.get("phone_number") or delivery_man.get("phone")
                        delivery_route_id = str(delivery_route.get("id")) if delivery_route.get("id") else None

                        if delivery_man_name:
                            async with get_db_session() as session:
                                update_query = text("""
                                    UPDATE orders SET 
                                        delivery_man_id = COALESCE(:delivery_man_id, delivery_man_id),
                                        delivery_man_name = COALESCE(:delivery_man_name, delivery_man_name),
                                        delivery_man_phone = COALESCE(:delivery_man_phone, delivery_man_phone),
                                        delivery_route = COALESCE(:delivery_route_id, delivery_route),
                                        api_dashboard_response = :api_response,
                                        updated_at = NOW()
                                    WHERE id = :order_id AND merchant_id = :mid
                                """)
                                await session.execute(update_query, {
                                    "order_id": int(order_id),
                                    "mid": merchant_id,
                                    "delivery_man_id": delivery_man_id,
                                    "delivery_man_name": delivery_man_name,
                                    "delivery_man_phone": delivery_man_phone,
                                    "delivery_route_id": delivery_route_id,
                                    "api_response": json.dumps(dashboard_data, default=str)
                                })
                                await session.commit()
                            logger.debug("reconciliation.delivery_info_recovered", order_id=order_id, driver_name=delivery_man_name)
                        else:
                            logger.debug("reconciliation.no_driver_in_api", order_id=order_id, msg="API da Cardapioweb também não possui entregador para este pedido.")

                except Exception as e:
                    logger.error("reconciliation.delivery_info_recover_failed", order_id=order_id, error=str(e))
                
                # Respeita o Rate Limit 
                await asyncio.sleep(self.details_sleep_time)

        except Exception as e:
            logger.error("reconciliation.delivery_info_batch_failed", error=str(e))