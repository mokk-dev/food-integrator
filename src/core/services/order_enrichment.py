# src/core/services/order_enrichment.py
# ============================================
# ORDER ENRICHMENT SERVICE
# ============================================

import json
import zoneinfo
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.services.geo_service import GeoService
from src.infrastructure.external.cardapioweb_dashboard import CardapiowebDashboardAPI
from src.infrastructure.external.cardapioweb_public import CardapiowebPublicAPI

class OrderEnrichmentService:
    
    def __init__(self):
        self.geo = GeoService()
    
    async def enrich_order(
        self,
        session: AsyncSession,
        order_id: int,
        merchant_id: str
    ) -> Tuple[bool, Optional[str]]:
        try:
            async with CardapiowebPublicAPI() as api_public:
                partner_data = await api_public.get_order(order_id)
            
            if not partner_data or partner_data.get("_api_error"):
                return False, f"API Partner falhou para order {order_id}"
            
            order_data = self._extract_from_partner(partner_data)
            
            distance_km, distance_zone = await self._calculate_distance(
                session, order_data.get("delivery_address", {}), merchant_id
            )
            
            operation_day_id = await self._get_or_create_operation_day(session, merchant_id)
                
            if not operation_day_id:
                return False, f"Não foi possível obter/criar operation_day."
            
            await self._insert_order(
                session=session,
                order_id=order_id,
                merchant_id=merchant_id,
                operation_day_id=operation_day_id,
                order_data=order_data,
                distance_km=distance_km,
                distance_zone=distance_zone
            )
            
            if self._should_call_dashboard(order_data):
                async with CardapiowebDashboardAPI() as api_dash:
                    dashboard_data = await api_dash.get_order_details(order_id)
                    
                    if dashboard_data and not dashboard_data.get("_api_error"):
                        await self._update_with_dashboard_data(
                            session, order_id, dashboard_data
                        )
            
            return True, None
            
        except Exception as e:
            return False, str(e)
            
    async def _get_or_create_operation_day(self, session: AsyncSession, merchant_id: str) -> Optional[int]:
        result = await session.execute(
            text("SELECT default_start_time, default_end_time, default_delivery_capacity FROM merchants WHERE merchant_id = :id"),
            {"id": str(merchant_id)}
        )
        merchant = result.fetchone()
        if not merchant: return None
            
        start_time, end_time, capacity = merchant
        tz = zoneinfo.ZoneInfo('America/Sao_Paulo')
        local_now = datetime.now(tz)
        logical_date = local_now.date()
        
        if start_time > end_time:
            if local_now.time() <= end_time:
                logical_date = logical_date - timedelta(days=1)
                
        await session.execute(
            text("UPDATE operation_days SET closed_at = NOW() WHERE merchant_id = :id AND operation_day < :logical_date AND closed_at IS NULL"),
            {"id": str(merchant_id), "logical_date": logical_date}
        )
        
        result = await session.execute(
            text("SELECT id FROM operation_days WHERE merchant_id = :id AND operation_day = :logical_date LIMIT 1"),
            {"id": str(merchant_id), "logical_date": logical_date}
        )
        row = result.fetchone()
        if row: return row[0]
            
        result = await session.execute(text("""
            INSERT INTO operation_days (merchant_id, operation_day, start_time, end_time, opened_at, delivery_capacity) 
            VALUES (:merchant_id, :operation_day, :start_time, :end_time, NOW(), :capacity) RETURNING id;
        """), {
            "merchant_id": str(merchant_id), "operation_day": logical_date,
            "start_time": start_time, "end_time": end_time, "capacity": capacity
        })
        new_row = result.fetchone()
        return new_row[0] if new_row else None
    
    def _extract_from_partner(self, data: Dict) -> Dict:
        """Extração poderosa mapeando as novas dimensões de BI."""
        address = data.get("delivery_address") or data.get("deliveryAddress") or {}
        client = data.get("client") or data.get("customer") or {}

        raw_uid = data.get("id")
        raw_display = data.get("display_id")
        
        # CORREÇÃO AQUI: Em vez de converter para int(), tratamos como string pura
        if raw_display is None and raw_uid:
            raw_display = str(raw_uid)[-4:]
        elif raw_display is not None:
            raw_display = str(raw_display)

        created_dt = None
        raw_created = data.get("created_at")
        if raw_created:
            try: created_dt = datetime.fromisoformat(str(raw_created))
            except ValueError: pass
        
        return {
            "uid": str(raw_uid) if raw_uid else None,
            "display_id": raw_display, # Agora vai enviar sempre como Texto (str)
            "order_type": data.get("order_type", "delivery"),
            "sales_channel": data.get("sales_channel") or "app_proprio",
            "status": self._normalize_status(data.get("status")),
            "created_at": created_dt,
            "cancellation_reason": data.get("cancellation_reason"),
            "total_value": data.get("total", data.get("final_value", 0.0)),
            "delivery_fee": data.get("delivery_fee", 0.0),
            
            # Dados de BI - Cliente
            "customer_id": client.get("id"),
            "customer_name": client.get("name") or client.get("fullName"),
            "customer_phone": client.get("telephone") or client.get("phone") or client.get("cellphone"),
            "customer_orders_count": client.get("orders_count"),
            
            # Dados de BI - Geografia
            "delivery_address": address,
            "delivery_neighborhood": address.get("neighborhood"),
            "delivery_city": address.get("city"),
            
            # Listas para tabelas relacionais
            "items": data.get("order_items") or data.get("items") or [],
            "payments": data.get("payment_values") or data.get("payments") or [],
        }
    
    def _extract_from_dashboard(self, data: Dict) -> Dict:
        core_data = data.get("data") or data.get("order") or data
        delivery_man = core_data.get("delivery_man") or {}
        delivery_route = core_data.get("delivery_route") or {}
        
        return {
            "delivery_man_id": delivery_man.get("id"),
            "delivery_man_name": delivery_man.get("name"),
            "delivery_man_phone": delivery_man.get("phone_number") or delivery_man.get("phone"),
            "delivery_route_id": str(delivery_route.get("id")) if delivery_route.get("id") else None,
        }
    
    def _normalize_status(self, status: Optional[str]) -> str:
        if not status: return "pending"
        return status.lower().replace(" ", "_")
    
    async def _calculate_distance(self, session: AsyncSession, address: Dict, merchant_id: str) -> Tuple[Optional[float], Optional[str]]:
        result = await session.execute(
            text("SELECT address_lat, address_lng, distance_threshold_near, distance_threshold_medium FROM merchants WHERE merchant_id = :id"),
            {"id": str(merchant_id)}
        )
        merchant = result.fetchone()
        if not merchant: return None, None
        
        m_lat, m_lng, thresh_near, thresh_med = merchant
        cust_lat, cust_lng = self.geo.extract_coordinates_from_address(address)
        
        if cust_lat is None or cust_lng is None: return None, None
        distance = self.geo.haversine(m_lat, m_lng, cust_lat, cust_lng)
        zone = self.geo.classify_distance_zone(distance, thresh_near or 2.0, thresh_med or 5.0)
        
        return distance, zone
    
    def _should_call_dashboard(self, order_data: Dict) -> bool:
        if order_data.get("order_type") != "delivery": return False
        return order_data.get("status", "") == "released"

    async def _insert_order(
        self, session: AsyncSession, order_id: int, merchant_id: str,
        operation_day_id: int, order_data: Dict, distance_km: Optional[float], distance_zone: Optional[str]
    ):
        """Insere a ordem principal E propaga itens e pagamentos."""
        
        # 1. Inserir Ordem Principal
        query_order = text("""
            INSERT INTO orders (
                id, uid, display_id, merchant_id, operation_day_id, source_event_id, 
                created_at, order_type, sales_channel, status, cancellation_reason,
                customer_id, customer_name, customer_phone, customer_orders_count,
                delivery_address, delivery_neighborhood, delivery_city,
                total_value, delivery_fee, distance_km, distance_zone
            ) VALUES (
                :id, :uid, :display_id, :merchant_id, :operation_day_id, :source_event_id, 
                :created_at, :order_type, :sales_channel, :status, :cancellation_reason,
                :customer_id, :customer_name, :customer_phone, :customer_orders_count,
                :delivery_address, :delivery_neighborhood, :delivery_city,
                :total_value, :delivery_fee, :distance_km, :distance_zone
            )
            ON CONFLICT (id) DO UPDATE SET
                updated_at = NOW(),
                status = EXCLUDED.status,
                cancellation_reason = COALESCE(EXCLUDED.cancellation_reason, orders.cancellation_reason),
                distance_km = EXCLUDED.distance_km, distance_zone = EXCLUDED.distance_zone
        """)
        
        await session.execute(query_order, {
            "id": int(order_id), "uid": order_data.get("uid"), "display_id": order_data.get("display_id"),
            "merchant_id": str(merchant_id), "operation_day_id": int(operation_day_id),
            "source_event_id": f"api_partner_{order_id}", "created_at": order_data.get("created_at") or datetime.now(),
            "order_type": order_data.get("order_type"), "sales_channel": order_data.get("sales_channel"),
            "status": order_data.get("status", "pending"), "cancellation_reason": order_data.get("cancellation_reason"),
            "customer_id": order_data.get("customer_id"), "customer_name": order_data.get("customer_name"),
            "customer_phone": order_data.get("customer_phone"), "customer_orders_count": order_data.get("customer_orders_count"),
            "delivery_address": json.dumps(order_data.get("delivery_address", {})),
            "delivery_neighborhood": order_data.get("delivery_neighborhood"), "delivery_city": order_data.get("delivery_city"),
            "total_value": order_data.get("total_value"), "delivery_fee": order_data.get("delivery_fee"),
            "distance_km": distance_km, "distance_zone": distance_zone
        })
        
        # 2. Idempotência: Limpar itens e pagamentos antigos (útil para retries seguros)
        await session.execute(text("DELETE FROM order_items WHERE order_id = :id"), {"id": int(order_id)})
        await session.execute(text("DELETE FROM order_payments WHERE order_id = :id"), {"id": int(order_id)})
        
        # 3. Inserir Itens
        items = order_data.get("items", [])
        if items:
            query_items = text("""
                INSERT INTO order_items (order_id, item_id, name, quantity, unit_price, total_price, category_name)
                VALUES (:order_id, :item_id, :name, :quantity, :unit_price, :total_price, :category_name)
            """)
            for item in items:
                await session.execute(query_items, {
                    "order_id": int(order_id),
                    "item_id": item.get("item_id"),
                    "name": item.get("name"),
                    "quantity": item.get("quantity", 1),
                    "unit_price": item.get("unit_price", item.get("price", 0.0)),
                    "total_price": item.get("total_price", item.get("price", 0.0)),
                    "category_name": item.get("category_name")
                })

        # 4. Inserir Pagamentos
        payments = order_data.get("payments", [])
        if payments:
            query_payments = text("""
                INSERT INTO order_payments (order_id, payment_method, payment_type, total_value)
                VALUES (:order_id, :payment_method, :payment_type, :total_value)
            """)
            for pay in payments:
                await session.execute(query_payments, {
                    "order_id": int(order_id),
                    "payment_method": pay.get("payment_method"),
                    "payment_type": pay.get("payment_type"),
                    "total_value": pay.get("total", pay.get("total_value", 0.0))
                })
    
    async def _update_with_dashboard_data(self, session: AsyncSession, order_id: int, dashboard_data: Dict):
        delivery_info = self._extract_from_dashboard(dashboard_data)
        
        await session.execute(text("""
            UPDATE orders SET 
                delivery_man_id = COALESCE(:delivery_man_id, delivery_man_id),
                delivery_man_name = COALESCE(:delivery_man_name, delivery_man_name),
                delivery_man_phone = COALESCE(:delivery_man_phone, delivery_man_phone),
                delivery_route = COALESCE(:delivery_route_id, delivery_route),
                api_dashboard_response = :api_response,
                updated_at = NOW()
            WHERE id = :order_id
        """), {
            "order_id": int(order_id),
            "delivery_man_id": delivery_info.get("delivery_man_id"),
            "delivery_man_name": delivery_info.get("delivery_man_name"),
            "delivery_man_phone": delivery_info.get("delivery_man_phone"),
            "delivery_route_id": delivery_info.get("delivery_route_id"),
            "api_response": json.dumps(dashboard_data, default=str)
        })