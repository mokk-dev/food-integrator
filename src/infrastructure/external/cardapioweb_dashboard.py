# ============================================
# CLIENT API DASHBOARD (PLATAFORMA) - CARDAPIOWEB
# ============================================

from typing import Any, Dict

from src.config import settings
from src.infrastructure.external.base_client import BaseAPIClient, api_method


class CardapiowebDashboardAPI(BaseAPIClient):
    """
    Cliente para API da Plataforma/Dashboard do Cardapioweb.
    
    URL Base: https://api.cardapioweb.com/api
    Endpoint: GET /v1/company/orders/{orderId}
    """
    
    def __init__(self):
        super().__init__(
            base_url=settings.cardapioweb_dashboard_base_url,
            api_key=settings.cardapioweb_dashboard_api_key,
            timeout=settings.cardapioweb_api_timeout
        )
    
    @api_method
    async def get_order_details(self, order_id: int) -> Dict[str, Any]:
        """
        Busca detalhes completos do pedido na plataforma.
        Endpoint: /v1/company/orders/{orderId}
        """
        return await self.get(f"/v1/company/orders/{order_id}")
    
    @api_method
    async def get_delivery_info(self, order_id: int) -> Dict[str, Any]:
        """
        Busca informações de entrega (delivery man, route).
        Tenta endpoint específico de delivery ou extrai do order details.
        """
        # Primeiro tenta endpoint específico de delivery
        delivery = await self.get(f"/v1/company/orders/{order_id}/delivery")
        if delivery and not delivery.get("_api_error"):
            return delivery
        
        # Fallback: retornar order details (contém delivery info)
        return await self.get_order_details(order_id)
    
    def should_enrich(self, order_status: str, order_type: str) -> bool:
        """Determina se deve chamar API de enriquecimento."""
        if order_type != 'delivery':
            return False
        return order_status in ['released', 'dispatched', 'in_transit', 'delivered', 'ready']