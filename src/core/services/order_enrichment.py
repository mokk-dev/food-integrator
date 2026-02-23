# ============================================
# ORDER ENRICHMENT SERVICE
# ============================================

import json
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
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
        self.merchant_id = settings.default_merchant_id
    
    async def enrich_order(
        self,
        order_id: int,
        event_type: str,
        order_status: str,
        raw_payload: Dict[str, Any]
    ) -> Tuple[bool, Optional[str]]:
        """
        Enriquece pedido chamando APIs do Cardapioweb.
        
        Fluxo:
        1. API Partner (pública) - dados básicos
        2. Calcular distância (se delivery)
        3. Inserir em 'orders'
        4. API Plataforma (dashboard) - dados enriquecidos
        5. Atualizar com delivery info
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
                order_data.get("delivery_address", {})
            )
            
            # 4. Obter/criar operation_day
            operation_day_id = await self._get_operation_day_id()
            if not operation_day_id:
                return False, "Não foi possível obter operation_day"
            
            # 5. Inserir em 'orders' (Fase 1)
            async with get_db_session() as session:
                await self._insert_order(
                    session,
                    order_id=order_id,
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
        
        return {
            "uid": data.get("uid") or data.get("uuid") or data.get("id"),
            "display_id": data.get("shortId") or data.get("displayId"),
            "order_type": self._normalize_order_type(data.get("type")),
            "sales_channel": data.get("salesChannel") or data.get("platform"),
            "customer_name": self._extract_customer_name(data),
            "customer_phone": self._extract_customer_phone(data),
            "total_value": data.get("total") or data.get("totalPrice"),
            "delivery_fee": data.get("deliveryFee") or data.get("deliveryPrice"),
            "status": self._normalize_status(data.get("status")),
            "created_at": data.get("createdAt") or data.get("created_at"),
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
        address: Dict
    ) -> Tuple[Optional[float], Optional[str]]:
        """Calcula distância do pedido."""
        # Buscar coordenadas do merchant
        async with get_db_session() as session:
            result = await session.execute(
                text("""
                    SELECT address_lat, address_lng, 
                           distance_threshold_near, 
                           distance_threshold_medium 
                    FROM merchants 
                    WHERE merchant_id = :id
                """),
                {"id": self.merchant_id}
            )
            merchant = result.fetchone()
            
            if not merchant:
                return None, None
            
            m_lat, m_lng, thresh_near, thresh_med = merchant
        
        # Extrair coordenadas do cliente
        cust_lat, cust_lng = self.geo.extract_coordinates_from_address(address)
        
        if cust_lat is None or cust_lng is None:
            return None, None
        
        # Calcular
        distance = self.geo.haversine(m_lat, m_lng, cust_lat, cust_lng)
        zone = self.geo.classify_distance_zone(
            distance,
            thresh_near or 2.0,
            thresh_med or 5.0
        )
        
        return distance, zone
    
    async def _get_operation_day_id(self) -> Optional[int]:
        """Obtém ID do operation_day atual ou cria novo."""
        async with get_db_session() as session:
            # Buscar aberto
            result = await session.execute(
                text("SELECT id FROM get_open_operation_day(:merchant_id)"),
                {"merchant_id": self.merchant_id}
            )
            row = result.fetchone()
            
            if row:
                return row[0] if isinstance(row, tuple) else getattr(row, 'id', None)
            
            # Criar novo (simplificado - em produção teria lógica mais robusta)
            # Aqui retornamos None para indicar que precisa criar
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
        operation_day_id: int,
        order_data: Dict,
        distance_km: Optional[float],
        distance_zone: Optional[str],
        api_response: Dict
    ):
        """Insere pedido na tabela orders."""
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
                "id": order_id,
                "uid": order_data.get("uid"),
                "display_id": order_data.get("display_id"),
                "merchant_id": self.merchant_id,
                "operation_day_id": operation_day_id,
                "source_event_id": f"api_partner_{order_id}",
                "created_at": order_data.get("created_at") or datetime.now(),
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
        
        # Se não tem delivery info, não atualiza
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
                "order_id": order_id,
                "delivery_man_name": delivery_info.get("delivery_man_name"),
                "delivery_man_phone": delivery_info.get("delivery_man_phone"),
                "delivery_route": delivery_info.get("delivery_route"),
                "dispatched_at": delivery_info.get("dispatched_at"),
                "delivered_at": delivery_info.get("delivered_at"),
                "api_response": json.dumps(dashboard_data, default=str)
            }
        )