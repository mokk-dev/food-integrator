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
            base_url=settings.cardapioweb_public_base_url,
            api_key=settings.cardapioweb_public_api_key,
            timeout=settings.cardapioweb_api_timeout
        )
    
    @api_method
    async def get_order(self, order_id: int) -> Dict[str, Any]:
        """Busca dados completos de um pedido."""
        return await self.get(f"/orders/{order_id}")
    
    @api_method
    async def get_order_by_display_id(self, display_id: str) -> Dict[str, Any]:
        """Busca pedido por display ID (UID curto)."""
        return await self.get(f"/orders/by-display-id/{display_id}")