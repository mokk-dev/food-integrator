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
    """
    Orquestra o enriquecimento completo de pedidos.
    """
    
    def __init__(self):
        self.geo = GeoService()
    
    async def enrich_order(
        self,
        session: AsyncSession,
        order_id: int,
        merchant_id: str
    ) -> Tuple[bool, Optional[str]]:
        """
        Enriquece pedido chamando APIs do Cardapioweb utilizando Unit of Work.
        """
        try:
            # 1. API PARTNER (Fase 1)
            async with CardapiowebPublicAPI() as api_public:
                partner_data = await api_public.get_order(order_id)
            
            if not partner_data or partner_data.get("_api_error"):
                return False, f"API Partner falhou para order {order_id}"
            
            # 2. Extrair dados normalizados usando novo payload estruturado
            order_data = self._extract_from_partner(partner_data)
            
            # 3. Calcular distância 
            distance_km, distance_zone = await self._calculate_distance(
                session,
                order_data.get("delivery_address", {}),
                merchant_id
            )
            
            # 4. Obter/criacao automatica e inteligente de operation_day
            operation_day_id = await self._get_or_create_operation_day(session, merchant_id)
                
            if not operation_day_id:
                return False, f"Não foi possível obter ou criar operation_day para o merchant {merchant_id}"
            
            # 5. Inserir em 'orders' na sessão ativa
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
                
            # 6. API PLATAFORMA (Fase 2) - Enriquecimento de entrega (fallback/segurança extra)
            if self._should_call_dashboard(order_data):
                async with CardapiowebDashboardAPI() as api_dash:
                    dashboard_data = await api_dash.get_order_details(order_id)
                    
                    if dashboard_data and not dashboard_data.get("_api_error"):
                        await self._update_with_dashboard_data(
                            session,
                            order_id,
                            dashboard_data
                        )
            
            # Atenção: Sem session.commit() aqui. O commit é feito pelo Worker.
            return True, None
            
        except Exception as e:
            # Propaga o erro para garantir o rollback no context manager
            return False, str(e)
            
    async def _get_or_create_operation_day(self, session: AsyncSession, merchant_id: str) -> Optional[int]:
        """
        Gera a data lógica do expediente baseada no start_time e end_time da loja.
        Se passar da meia-noite mas for antes do fechamento, a venda cai no caixa do dia anterior.
        """
        # 1. Configs da loja
        result = await session.execute(
            text("SELECT default_start_time, default_end_time, default_delivery_capacity FROM merchants WHERE merchant_id = :id"),
            {"id": str(merchant_id)}
        )
        merchant = result.fetchone()
        if not merchant:
            return None
            
        start_time, end_time, capacity = merchant
        
        # 2. Calcular Data Lógica
        tz = zoneinfo.ZoneInfo('America/Sao_Paulo')
        local_now = datetime.now(tz)
        logical_date = local_now.date()
        
        # Avalia se a jornada cruza a meia-noite
        turno_cruza_meia_noite = start_time > end_time
        
        if turno_cruza_meia_noite:
            # Se a hora atual for menor ou igual ao fechamento (ex: 01:00 <= 02:00), 
            # significa que a madrugada pertence ao expediente do dia anterior.
            if local_now.time() <= end_time:
                logical_date = logical_date - timedelta(days=1)
                
        # 3. Auto-Fechar expedientes antigos deixados abertos
        await session.execute(
            text("""
                UPDATE operation_days 
                SET closed_at = NOW() 
                WHERE merchant_id = :id 
                  AND operation_day < :logical_date 
                  AND closed_at IS NULL
            """),
            {"id": str(merchant_id), "logical_date": logical_date}
        )
        
        # 4. Tentar obter o expediente do dia lógico atual
        result = await session.execute(
            text("SELECT id FROM operation_days WHERE merchant_id = :id AND operation_day = :logical_date LIMIT 1"),
            {"id": str(merchant_id), "logical_date": logical_date}
        )
        row = result.fetchone()
        if row:
            return row[0]
            
        # 5. Criar caso não exista
        query = text("""
            INSERT INTO operation_days (
                merchant_id, operation_day, start_time, end_time, 
                opened_at, delivery_capacity
            ) VALUES (
                :merchant_id, :operation_day, :start_time, :end_time,
                NOW(), :capacity
            )
            RETURNING id;
        """)
        result = await session.execute(query, {
            "merchant_id": str(merchant_id),
            "operation_day": logical_date,
            "start_time": start_time,
            "end_time": end_time,
            "capacity": capacity
        })
        new_row = result.fetchone()
        return new_row[0] if new_row else None
    
    def _extract_from_partner(self, data: Dict) -> Dict:
        """Extrai dados normalizados do Payload Exato fornecido da API Partner."""
        address = data.get("delivery_address") or {}

        raw_uid = data.get("id")
        raw_display = data.get("display_id")
        
        # Fallback de segurança para display_id
        if raw_display is None and raw_uid:
            raw_display = int(str(raw_uid)[-4:])
            
        sales_channel = data.get("sales_channel") or "app_proprio"
        order_type = data.get("order_type")

        raw_created = data.get("created_at")
        created_dt = None
        if raw_created:
            try:
                # O python lida nativamente com o formato '2023-06-25T10:40:33.744-03:00'
                created_dt = datetime.fromisoformat(str(raw_created))
            except ValueError:
                pass
        
        return {
            "uid": str(raw_uid) if raw_uid else None,
            "display_id": raw_display,
            "order_type": order_type if order_type else "delivery",
            "sales_channel": sales_channel,
            "customer_name": self._extract_customer_name(data),
            "customer_phone": self._extract_customer_phone(data),
            "total_value": data.get("total", 0.0),
            "delivery_fee": data.get("delivery_fee", 0.0),
            "status": self._normalize_status(data.get("status")),
            "created_at": created_dt,
            "items": data.get("items") or [],
            "payments": data.get("payments") or [],
            "delivery_address": address,
        }
    
    def _extract_from_dashboard(self, data: Dict) -> Dict:
        """Extrai dados de entrega do Payload Real da API Dashboard."""
        core_data = data.get("data") or data.get("order") or data
        
        # Extração Direta do Motoboy (agora com ID)
        delivery_man = core_data.get("delivery_man") or {}
        driver_id = delivery_man.get("id")
        driver_name = delivery_man.get("name")
        driver_phone = delivery_man.get("phone_number") or delivery_man.get("phone")
        
        # Extração da Rota
        delivery_route = core_data.get("delivery_route") or {}
        route_name = delivery_route.get("name")
        
        # Extração dos Tempos Exatos via Histórico de Mudanças
        dispatched_at = None
        delivered_at = None
        
        status_changes = core_data.get("status_changes", [])
        for change in status_changes:
            status = change.get("status")
            if status == "released":
                dispatched_at = change.get("created_at")
            elif status == "delivered":
                delivered_at = change.get("created_at")
                
        # Fallback de tempo
        if not dispatched_at and core_data.get("status") == "released":
            dispatched_at = core_data.get("updated_at")
            
        return {
            "delivery_man_id": driver_id,
            "delivery_man_name": driver_name,
            "delivery_man_phone": driver_phone,
            "delivery_route": route_name,
            "dispatched_at": dispatched_at,
            "delivered_at": delivered_at,
        }
    
    def _normalize_status(self, status: Optional[str]) -> str:
        """Normaliza status do pedido."""
        if not status:
            return "pending"
        return status.lower().replace(" ", "_")
    
    def _extract_customer_name(self, data: Dict) -> Optional[str]:
        """Extrai nome do cliente."""
        customer = data.get("customer") or data.get("client") or {}
        return customer.get("name") or customer.get("fullName")
    
    def _extract_customer_phone(self, data: Dict) -> Optional[str]:
        """Extrai telefone do cliente."""
        customer = data.get("customer") or data.get("client") or {}
        return customer.get("phone") or customer.get("cellphone") or customer.get("whatsapp")
    
    async def _calculate_distance(
        self,
        session: AsyncSession,
        address: Dict,
        merchant_id: str
    ) -> Tuple[Optional[float], Optional[str]]:
        """Calcula distância do pedido baseada no merchant."""
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
    
    def _should_call_dashboard(self, order_data: Dict) -> bool:
        """Determina se deve chamar API de plataforma para dados de delivery."""
        if order_data.get("order_type") != "delivery":
            return False
        
        status = order_data.get("status", "")
        # Chama a API de Dashboard EXCLUSIVAMENTE quando o pedido sai para entrega
        return status == "released"
    
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
        """Insere pedido na tabela orders usando a sessão herdada."""
        
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
                display_id = COALESCE(EXCLUDED.display_id, orders.display_id),
                order_type = COALESCE(EXCLUDED.order_type, orders.order_type),
                sales_channel = COALESCE(EXCLUDED.sales_channel, orders.sales_channel),
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
                "order_type": order_data.get("order_type"),
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
        """Atualiza pedido com dados da API Plataforma usando a sessão herdada."""
        delivery_info = self._extract_from_dashboard(dashboard_data)
        
        query = text("""
            UPDATE orders
            SET delivery_man_id = COALESCE(:delivery_man_id, delivery_man_id),
                delivery_man_name = COALESCE(:delivery_man_name, delivery_man_name),
                delivery_man_phone = COALESCE(:delivery_man_phone, delivery_man_phone),
                delivery_route = COALESCE(:delivery_route, delivery_route),
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
                "delivery_man_id": delivery_info.get("delivery_man_id"),
                "delivery_man_name": delivery_info.get("delivery_man_name"),
                "delivery_man_phone": delivery_info.get("delivery_man_phone"),
                "delivery_route": delivery_info.get("delivery_route"),
                "dispatched_at": delivery_info.get("dispatched_at"),
                "delivered_at": delivery_info.get("delivered_at"),
                "api_response": json.dumps(dashboard_data, default=str)
            }
        )