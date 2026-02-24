# ============================================
# CLIENT API PÚBLICA (PARTNER) - CARDAPIOWEB
# ============================================

from typing import Any, Dict

from src.config import settings
from src.infrastructure.external.base_client import BaseAPIClient, api_method


class CardapiowebPublicAPI(BaseAPIClient):
    """
    Cliente para API Partner do Cardapioweb.
    
    URL Base: https://integracao.cardapioweb.com/api/partner/v1
    Endpoint: GET /orders/{orderId}
    """
    
    def __init__(self):
        super().__init__(
            base_url=settings.cardapioweb_public_base_url
        )
        self.client.headers.update({
            "X-API-KEY": settings.cardapioweb_public_api_key,
            "Accept": "application/json"
        })
    
    @api_method
    async def get_order(self, order_id: int) -> Dict[str, Any]:
        """Busca dados completos de um pedido."""

        if settings.app_env != "production" and order_id in [555777, 999001, 999999]:
            print(f"🛠️ [MOCK MODE] Retornando dados para a order {order_id}")
            return {
                "id": order_id,
                "shortId": str(order_id)[-4:],
                "type": "delivery",
                "customer": {"name": "Cliente Teste Local", "phone": "44999999999"},
                "total": 50.00,
                "deliveryFee": 5.00,
                "status": "pending",
                "createdAt": "2026-02-23T20:00:00Z",
                "deliveryAddress": {"lat": -23.425, "lng": -51.915}
            }
        
        return await self.get(f"/orders/{order_id}")
    
    @api_method
    async def get_order_by_display_id(self, display_id: str) -> Dict[str, Any]:
        """Busca pedido por display ID (UID curto)."""
        return await self.get(f"/orders/by-display-id/{display_id}")