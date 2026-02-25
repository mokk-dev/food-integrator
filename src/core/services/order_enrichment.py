# ============================================
# ORDER ENRICHMENT SERVICE
# ============================================

import json
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.services.geo_service import GeoService
from src.infrastructure.db.connection import get_db_session
from src.infrastructure.external.cardapioweb_dashboard import CardapiowebDashboardAPI
from src.infrastructure.external.cardapioweb_public import CardapiowebPublicAPI


class OrderEnrichmentService:
    """
    Orquestra o enriquecimento completo de pedidos.
    """
    
    def __init__(self):
        self.geo = GeoService()
        # Removido o self.merchant_id fixo para suportar multi-tenant
    
    async def enrich_order(
        self,
        order_id: int,
        event_type: str,
        order_status: str,
        raw_payload: Dict[str, Any],
        merchant_id: str
    ) -> Tuple[bool, Optional[str]]:
        """
        Enriquece pedido chamando APIs do Cardapioweb.
        
        Fluxo:
        1. API Partner (pública) - dados básicos
        2. Calcular distância (se delivery)
        3. Obter ou Criar Expediente (operation_day)
        4. Inserir em 'orders'
        5. API Plataforma (dashboard) - dados enriquecidos
        6. Atualizar com delivery info
        """
        try:
            # 1. API PARTNER (Fase 1)
            async with CardapiowebPublicAPI() as api_public:
                partner_data = await api_public.get_order(order_id)
            
            if not partner_data or partner_data.get("_api_error"):
                return False, f"API Partner falhou para order {order_id}"
            
            # 2. Extrair dados
            order_data = self._extract_from_partner(partner_data)
            
            # 3. Calcular distância
            distance_km, distance_zone = await self._calculate_distance(
                order_data.get("delivery_address", {}),
                merchant_id
            )
            
            # 4. Obter/criar operation_day
            operation_day_id = await self._find_operation_day(merchant_id)
            
            if not operation_day_id:
                operation_day_id = await self._create_operation_day(merchant_id)
                
            if not operation_day_id:
                return False, f"Não foi possível obter ou criar operation_day para o merchant {merchant_id}"
            
            # 5. Inserir em 'orders' (Fase 1)
            async with get_db_session() as session:
                await self._insert_order(
                    session=session,
                    order_id=order_id,
                    merchant_id=merchant_id,
                    operation_day_id=operation_day_id,
                    order_data=order_data,
                    distance_km=distance_km,
                    distance_zone=distance_zone,
                    api_response=partner_data
                )
                
                # 6. API PLATAFORMA (Fase 2) - se necessário
                if self._should_call_dashboard(order_data):
                    async with CardapiowebDashboardAPI() as api_dash:
                        dashboard_data = await api_dash.get_order_details(order_id)
                        
                        if dashboard_data and not dashboard_data.get("_api_error"):
                            await self._update_with_dashboard_data(
                                session,
                                order_id,
                                dashboard_data
                            )
                
                await session.commit()
            
            return True, None
            
        except Exception as e:
            return False, str(e)
    
    def _extract_from_partner(self, data: Dict) -> Dict:
        """Extrai dados normalizados da API Partner."""
        address = data.get("deliveryAddress") or data.get("delivery_address") or {}

        raw_uid = data.get("uid") or data.get("uuid") or data.get("id")
        raw_display = data.get("shortId") or data.get("displayId")

        display_id_val = int(raw_display) if raw_display else None

        raw_created = data.get("createdAt") or data.get("created_at")
        
        created_dt = None
        if raw_created:
            try:
                # Substituímos o "Z" (Zulu/UTC) pelo offset padrão que o Python compreende perfeitamente
                created_dt = datetime.fromisoformat(str(raw_created).replace("Z", "+00:00"))
            except ValueError:
                pass
        
        return {
            "uid": str(raw_uid) if raw_uid else None,
            "display_id": display_id_val,
            "order_type": self._normalize_order_type(data.get("type")),
            "sales_channel": data.get("salesChannel") or data.get("platform"),
            "customer_name": self._extract_customer_name(data),
            "customer_phone": self._extract_customer_phone(data),
            "total_value": data.get("total") or data.get("totalPrice"),
            "delivery_fee": data.get("deliveryFee") or data.get("deliveryPrice"),
            "status": self._normalize_status(data.get("status")),
            "created_at": created_dt,
            "items": data.get("items") or [],
            "payments": data.get("payments") or [],
            "delivery_address": address,
        }
    
    def _extract_from_dashboard(self, data: Dict) -> Dict:
        """Extrai dados de entrega da API Plataforma."""
        delivery = data.get("delivery") or {}
        driver = delivery.get("driver") or delivery.get("deliveryMan") or {}
        
        return {
            "delivery_man_name": driver.get("name"),
            "delivery_man_phone": driver.get("phone") or driver.get("cellphone"),
            "delivery_route": delivery.get("route") or delivery.get("routeName"),
            "dispatched_at": delivery.get("dispatchedAt"),
            "delivered_at": delivery.get("deliveredAt"),
        }
    
    def _normalize_order_type(self, type_str: Optional[str]) -> str:
        """Normaliza tipo de pedido."""
        if not type_str:
            return "delivery"
        
        type_lower = type_str.lower()
        if type_lower in ["delivery", "entrega"]:
            return "delivery"
        elif type_lower in ["takeout", "pickup", "retirada"]:
            return "takeout"
        elif type_lower in ["onsite", "local", "mesa"]:
            return "onsite"
        return "delivery"
    
    def _normalize_status(self, status: Optional[str]) -> str:
        """Normaliza status do pedido."""
        if not status:
            return "pending"
        return status.lower().replace(" ", "_")
    
    def _extract_customer_name(self, data: Dict) -> Optional[str]:
        """Extrai nome do cliente de várias possíveis estruturas."""
        customer = data.get("customer") or data.get("client") or {}
        return customer.get("name") or customer.get("fullName")
    
    def _extract_customer_phone(self, data: Dict) -> Optional[str]:
        """Extrai telefone do cliente."""
        customer = data.get("customer") or data.get("client") or {}
        return customer.get("phone") or customer.get("cellphone") or customer.get("whatsapp")
    
    async def _calculate_distance(
        self,
        address: Dict,
        merchant_id: str
    ) -> Tuple[Optional[float], Optional[str]]:
        """Calcula distância do pedido baseada no merchant."""
        async with get_db_session() as session:
            # Cast explícito para string
            safe_merchant_id = str(merchant_id)
            
            result = await session.execute(
                text("""
                    SELECT address_lat, address_lng, 
                           distance_threshold_near, 
                           distance_threshold_medium 
                    FROM merchants 
                    WHERE merchant_id = :id
                """),
                {"id": safe_merchant_id}
            )
            merchant = result.fetchone()
            
            if not merchant:
                return None, None
            
            m_lat, m_lng, thresh_near, thresh_med = merchant
        
        cust_lat, cust_lng = self.geo.extract_coordinates_from_address(address)
        
        if cust_lat is None or cust_lng is None:
            return None, None
        
        distance = self.geo.haversine(m_lat, m_lng, cust_lat, cust_lng)
        zone = self.geo.classify_distance_zone(
            distance,
            thresh_near or 2.0,
            thresh_med or 5.0
        )
        
        return distance, zone
    
    async def _find_operation_day(self, merchant_id: str) -> Optional[int]:
        """Busca o ID do operation_day atual (aberto) para o merchant."""
        async with get_db_session() as session:
            # Cast explícito para string
            safe_merchant_id = str(merchant_id)
            
            result = await session.execute(
                text("SELECT operation_day_id FROM get_open_operation_day(:merchant_id)"),
                {"merchant_id": safe_merchant_id}
            )
            row = result.fetchone()
            
            if row and row[0]:
                return row[0]
            return None

    async def _create_operation_day(self, merchant_id: str) -> Optional[int]:
        """Cria um novo operation_day usando os horários padrão do merchant."""
        async with get_db_session() as session:
            # Cast explícito para string
            safe_merchant_id = str(merchant_id)
            
            query = text("""
                INSERT INTO operation_days (
                    merchant_id, operation_day, start_time, end_time, 
                    opened_at, delivery_capacity
                )
                SELECT 
                    CAST(:merchant_id AS VARCHAR), CURRENT_DATE, default_start_time, default_end_time, 
                    NOW(), default_delivery_capacity
                FROM merchants 
                WHERE merchant_id = CAST(:merchant_id AS VARCHAR)
                RETURNING id;
            """)
            result = await session.execute(query, {"merchant_id": safe_merchant_id})
            new_row = result.fetchone()
            
            if new_row:
                await session.commit()
                print(f"ℹ️ Novo operation_day criado automaticamente para merchant {merchant_id}")
                return new_row[0]
                
            return None
    
    def _should_call_dashboard(self, order_data: Dict) -> bool:
        """Determina se deve chamar API de plataforma."""
        if order_data.get("order_type") != "delivery":
            return False
        
        status = order_data.get("status", "")
        return status in ["released", "dispatched", "in_transit", "delivered", "ready", "confirmed"]
    
    async def _insert_order(
        self,
        session: AsyncSession,
        order_id: int,
        merchant_id: str,
        operation_day_id: int,
        order_data: Dict,
        distance_km: Optional[float],
        distance_zone: Optional[str],
        api_response: Dict
    ):
        """Insere pedido na tabela orders."""
        
        # Garante que a data seja serializada corretamente pelo asyncpg
        created_at_val = order_data.get("created_at")
        if not created_at_val:
            created_at_val = datetime.now()
        
        query = text("""
            INSERT INTO orders (
                id, uid, display_id, merchant_id, operation_day_id,
                source_event_id, created_at, order_type, sales_channel,
                customer_name, customer_phone, delivery_address,
                items, payments, total_value, delivery_fee,
                status, distance_km, distance_zone,
                api_public_response
            ) VALUES (
                :id, :uid, :display_id, :merchant_id, :operation_day_id,
                :source_event_id, :created_at, :order_type, :sales_channel,
                :customer_name, :customer_phone, :delivery_address,
                :items, :payments, :total_value, :delivery_fee,
                :status, :distance_km, :distance_zone,
                :api_response
            )
            ON CONFLICT (id) DO UPDATE SET
                updated_at = NOW(),
                status = EXCLUDED.status,
                distance_km = EXCLUDED.distance_km,
                distance_zone = EXCLUDED.distance_zone,
                api_public_response = EXCLUDED.api_public_response
        """)
        
        await session.execute(
            query,
            {
                "id": int(order_id),
                "uid": order_data.get("uid"),
                "display_id": order_data.get("display_id"),
                "merchant_id": str(merchant_id),
                "operation_day_id": int(operation_day_id),
                "source_event_id": f"api_partner_{order_id}",
                "created_at": created_at_val,
                "order_type": order_data.get("order_type", "delivery"),
                "sales_channel": order_data.get("sales_channel"),
                "customer_name": order_data.get("customer_name"),
                "customer_phone": order_data.get("customer_phone"),
                "delivery_address": json.dumps(order_data.get("delivery_address", {})),
                "items": json.dumps(order_data.get("items", [])),
                "payments": json.dumps(order_data.get("payments", [])),
                "total_value": order_data.get("total_value"),
                "delivery_fee": order_data.get("delivery_fee"),
                "status": order_data.get("status", "pending"),
                "distance_km": distance_km,
                "distance_zone": distance_zone,
                "api_response": json.dumps(api_response, default=str)
            }
        )
    
    async def _update_with_dashboard_data(
        self,
        session: AsyncSession,
        order_id: int,
        dashboard_data: Dict
    ):
        """Atualiza pedido com dados da API Plataforma."""
        delivery_info = self._extract_from_dashboard(dashboard_data)
        
        if not any(delivery_info.values()):
            return
        
        query = text("""
            UPDATE orders
            SET delivery_man_name = :delivery_man_name,
                delivery_man_phone = :delivery_man_phone,
                delivery_route = :delivery_route,
                dispatched_at = COALESCE(:dispatched_at, dispatched_at),
                delivered_at = COALESCE(:delivered_at, delivered_at),
                api_dashboard_response = :api_response,
                updated_at = NOW()
            WHERE id = :order_id
        """)
        
        await session.execute(
            query,
            {
                "order_id": int(order_id),
                "delivery_man_name": delivery_info.get("delivery_man_name"),
                "delivery_man_phone": delivery_info.get("delivery_man_phone"),
                "delivery_route": delivery_info.get("delivery_route"),
                "dispatched_at": delivery_info.get("dispatched_at"),
                "delivered_at": delivery_info.get("delivered_at"),
                "api_response": json.dumps(dashboard_data, default=str)
            }
        )