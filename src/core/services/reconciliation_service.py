import json
import asyncio
from datetime import datetime
from sqlalchemy import text
from typing import Optional

from src.infrastructure.db.connection import get_db_session
from src.infrastructure.external.cardapioweb_public import CardapiowebPublicAPI
from src.infrastructure.external.cardapioweb_dashboard import CardapiowebDashboardAPI
from src.core.logger import logger
from src.config import settings

class ReconciliationService:
    def __init__(self):
        self.public_api = CardapiowebPublicAPI()
        self.dashboard_api = CardapiowebDashboardAPI()

        # Calcula o tempo de espera dinâmico baseado no Rate Limit (.env)
        self.history_sleep_time = (60.0 / settings.cardapioweb_history_rate_limit) + 0.5
        self.details_sleep_time = (60.0 / settings.cardapioweb_details_rate_limit) + 0.1

    async def run_reconciliation_for_shift(self, merchant_id: str, opened_at: datetime, closed_at: datetime, shift_id: Optional[int] = None):
        """
        Rotina de Conformidade Completa projetada para rodar após o fechamento de caixa.
        Executa 3 etapas de auditoria:
        1. Resgate de pedidos inteiros não registrados.
        2. Resgate de informações de entregadores (motoboys) em lote (Heurística de Divergência).
        3. Auditoria Financeira e Fechamento de Caixa.
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
            await self._recover_missing_delivery_info(merchant_id, opened_at, closed_at)

            # ==========================================
            # ETAPA 3: AUDITORIA FINANCEIRA (CASH FLOW)
            # ==========================================
            if shift_id:
                logger.info("reconciliation.cash_flow_started", msg="Iniciando resgate de dados financeiros do caixa...")
                await self._recover_cash_flow_data(shift_id, opened_at)
            else:
                logger.warning("reconciliation.cash_flow_skipped", msg="ID do expediente não fornecido. Pulando auditoria financeira.")

            logger.info("reconciliation.completed", msg="Rotina completa de reconciliação de turno finalizada.")

        except Exception as e:
            logger.error("reconciliation.failed", error=str(e))

    async def _fetch_history_with_rate_limit(self, start_date: datetime, end_date: datetime):
        """Varre as páginas do histórico implementando throttling dinâmico."""
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
        """Busca os detalhes completos na rota unitária e registra no banco."""
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
        Consulta em Lote (Bulk) com Heurística de Divergência.
        Compara o total de pedidos do Banco com a API e só busca os detalhes em caso de diferença.
        """
        try:
            logger.info("reconciliation.delivery_info_started", msg="Comparando contagem de motoboys (API vs Banco de Dados)...")
            
            summary = await self.dashboard_api.get_delivery_men_summary(opened_at, closed_at)
            
            if not summary:
                logger.info("reconciliation.no_delivery_men", msg="Nenhum motoboy com entregas no resumo da API.")
                return

            async with get_db_session() as session:
                count_query = text("""
                    SELECT delivery_man_id, COUNT(id) as total_deliveries
                    FROM orders 
                    WHERE merchant_id = :mid 
                      AND order_type = 'delivery'
                      AND created_at >= :opened_at
                      AND created_at <= :closed_at
                      AND delivery_man_id IS NOT NULL
                    GROUP BY delivery_man_id
                """)
                result = await session.execute(count_query, {
                    "mid": merchant_id,
                    "opened_at": opened_at,
                    "closed_at": closed_at
                })
                db_driver_counts = {str(row[0]): row[1] for row in result.fetchall()}

            order_updates = []
            
            for driver in summary:
                driver_id_str = str(driver.get("id"))
                driver_name = driver.get("name")
                driver_phone = driver.get("phone_number")
                
                status_summary = driver.get("summary_by_order_status", [])
                api_total = sum(s.get("quantity", 0) for s in status_summary)
                
                db_total = db_driver_counts.get(driver_id_str, 0)
                
                if api_total > 0 and api_total != db_total:
                    logger.info(
                        "reconciliation.driver_divergence_found", 
                        driver=driver_name,
                        api_count=api_total,
                        db_count=db_total,
                        msg="Divergência detectada! Buscando lista de pedidos deste motoboy."
                    )
                    
                    orders = await self.dashboard_api.get_orders_by_delivery_man(driver.get("id"), opened_at, closed_at)
                    
                    for order in orders:
                        order_updates.append({
                            "order_id": order.get("id"),
                            "mid": merchant_id,
                            "driver_id": int(driver_id_str) if driver_id_str and driver_id_str.isdigit() else None,
                            "driver_name": driver_name,
                            "driver_phone": driver_phone
                        })
                    await asyncio.sleep(self.details_sleep_time)

            if order_updates:
                async with get_db_session() as session:
                    for update in order_updates:
                        update_query = text("""
                            UPDATE orders SET 
                                delivery_man_id = :driver_id,
                                delivery_man_name = :driver_name,
                                delivery_man_phone = COALESCE(:driver_phone, delivery_man_phone),
                                updated_at = NOW()
                            WHERE id = :order_id AND merchant_id = :mid
                        """)
                        await session.execute(update_query, update)
                    
                    await session.commit()
                    
                logger.info(
                    "reconciliation.delivery_info_bulk_success", 
                    updated_orders=len(order_updates), 
                    msg="Pedidos divergentes foram sincronizados com os motoboys definitivos."
                )
            else:
                logger.info("reconciliation.delivery_info_synced", msg="100% de conformidade! A contagem de todos os motoboys bateu perfeitamente.")

        except Exception as e:
            logger.error("reconciliation.delivery_info_batch_failed", error=str(e))

    async def _recover_cash_flow_data(self, shift_id: int, opened_at: datetime):
        """
        Encontra o caixa correspondente na API, puxa o Sumário e as Operações para o BI.
        Filtra vendas puras para não duplicar dados com a tabela 'orders'.
        """
        try:
            recent_cash_flows = await self.dashboard_api.get_cash_flows(page=1, per_page=10)
            
            target_cash_flow = None
            for cf in recent_cash_flows:
                # Cruza a data de abertura do caixa da API com o nosso opened_at (Margem de 2 horas)
                cf_open_at = datetime.fromisoformat(cf.get("open_at").replace("Z", "+00:00"))
                diff_hours = abs((cf_open_at - opened_at).total_seconds()) / 3600
                
                if diff_hours <= 2.0:
                    target_cash_flow = cf
                    break
                    
            if not target_cash_flow:
                logger.warning("reconciliation.cash_flow_not_found", shift_id=shift_id, msg="Nenhum caixa da API corresponde a este expediente.")
                return

            cash_flow_id = target_cash_flow.get("id")
            logger.info("reconciliation.cash_flow_matched", cash_flow_id=cash_flow_id)

            await asyncio.sleep(self.details_sleep_time)
            summary = await self.dashboard_api.get_cash_flow_summary(cash_flow_id)
            
            await asyncio.sleep(self.details_sleep_time)
            operations = await self.dashboard_api.get_cash_flow_operations(cash_flow_id)

            async with get_db_session() as session:
                update_query = text("""
                    UPDATE operation_days SET 
                        cash_flow_id = :cf_id,
                        cash_flow_initial_value = :initial,
                        cash_flow_expected_value = :expected,
                        cash_flow_final_value = :final,
                        cash_flow_summary = :summary_json
                    WHERE id = :shift_id
                """)
                await session.execute(update_query, {
                    "cf_id": cash_flow_id,
                    "initial": summary.get("initial_value", 0.0),
                    "expected": summary.get("expected_value", 0.0),
                    "final": summary.get("final_value", 0.0),
                    "summary_json": json.dumps(summary, default=str),
                    "shift_id": shift_id
                })

                if operations:
                    op_query = text("""
                        INSERT INTO cash_flow_operations (
                            id, operation_day_id, cash_flow_id, kind, description, 
                            value, payment_method_id, payment_method_name, 
                            created_at, order_id, user_name
                        ) VALUES (
                            :id, :shift_id, :cf_id, :kind, :description, 
                            :value, :payment_id, :payment_name, 
                            :created_at, :order_id, :user_name
                        ) ON CONFLICT (id) DO NOTHING
                    """)
                    
                    for op in operations:
                        # Filtro Inteligente: Ignora vendas brutas para não duplicar faturamento no BI
                        if op.get("kind") in ["sale", "ifood_sale"]:
                            continue
                            
                        val = op.get("value")
                        if isinstance(val, dict):
                            val = val.get("parsedValue", 0.0)
                            
                        user_info = op.get("user") or {}
                        
                        # Conversão da string ISO para datetime
                        raw_created_at = op.get("created_at")
                        parsed_created_at = None
                        if raw_created_at:
                            # Trata a string ISO se vier do formato JSON da API
                            parsed_created_at = datetime.fromisoformat(str(raw_created_at).replace("Z", "+00:00"))
                        
                        await session.execute(op_query, {
                            "id": op.get("id"),
                            "shift_id": shift_id,
                            "cf_id": cash_flow_id,
                            "kind": op.get("kind"),
                            "description": op.get("description"),
                            "value": float(val) if val is not None else 0.0,
                            "payment_id": op.get("payment_method_id"),
                            "payment_name": op.get("payment_method_name"),
                            "created_at": parsed_created_at, # Usando o datetime convertido
                            "order_id": op.get("order_id"),
                            "user_name": user_info.get("name", "Sistema")
                        })
                
                await session.commit()
                logger.info("reconciliation.cash_flow_success", msg="Dados financeiros do caixa gravados com sucesso.")

        except Exception as e:
            logger.error("reconciliation.cash_flow_failed", error=str(e))