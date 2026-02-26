# ============================================
# CLIENT API DASHBOARD (PLATAFORMA) - CARDAPIOWEB
# ============================================

from typing import Any, Dict

from src.config import settings
from src.infrastructure.external.base_client import BaseAPIClient, api_method
from src.core.logger import logger

# Importa o gerenciador de autenticação isolado
from src.infrastructure.external.cardapioweb_auth import CardapiowebAuthManager


class CardapiowebDashboardAPI(BaseAPIClient):
    """
    Cliente para API da Plataforma/Dashboard do Cardapioweb.
    
    URL Base: https://api.cardapioweb.com/api
    Endpoint: GET /v1/company/orders/{orderId}
    """
    
    def __init__(self):
        super().__init__(
            base_url=settings.cardapioweb_dashboard_base_url,
            timeout=settings.cardapioweb_api_timeout
        )
        self.client.headers.update({
            "CompanyId": str(settings.default_merchant_id),
            "Accept": "application/json"
        })
        
        self.auth_manager = CardapiowebAuthManager()

    async def _ensure_auth(self, force_refresh: bool = False):
        """Busca o token válido no AuthManager e injeta nos headers."""
        token = await self.auth_manager.get_valid_access_token(force_refresh=force_refresh)
        if token:
            self.client.headers.update({"Authorization": token})

    async def _execute_with_auth(self, method_name: str, endpoint: str, **kwargs) -> Any:
        """
        Wrapper que injeta a autorização antes da requisição, captura erros 401 
        e aciona o refresh_token automaticamente caso o token atual tenha sido revogado.
        """
        await self._ensure_auth()
        
        method = getattr(self, method_name)
        try:
            return await method(endpoint, **kwargs)
        except Exception as e:
            error_str = str(e).lower()
            if "401" in error_str or "unauthorized" in error_str:
                logger.warning("api.unauthorized", endpoint=endpoint, msg="Token expirado ou inválido. Forçando refresh...")
                await self._ensure_auth(force_refresh=True)
                return await method(endpoint, **kwargs)
            raise e
            
    @api_method
    async def get_order_details(self, order_id: int) -> Dict[str, Any]:
        """
        Busca detalhes completos do pedido na plataforma.
        Endpoint: /v1/company/orders/{orderId}
        """

        if settings.app_env != "production" and order_id in [555777, 999001, 999999]:
            print(f"🛠️ [MOCK MODE] Retornando dados para a order {order_id}")
            return {
                "delivery": {
                    "driver": {"name": "Motoboy Mock", "phone": "44888888888"},
                    "route": "Rota 1"
                }
            }
            
        return await self._execute_with_auth("get", f"/v1/company/orders/{order_id}")
    
    @api_method
    async def get_delivery_info(self, order_id: int) -> Dict[str, Any]:
        """
        Busca informações de entrega (delivery man, route).
        Tenta endpoint específico de delivery ou extrai do order details.
        """
        try:
            delivery = await self._execute_with_auth("get", f"/v1/company/orders/{order_id}/delivery")
            if delivery and not delivery.get("_api_error"):
                return delivery
        except Exception:
            pass

        return await self.get_order_details(order_id)
    
    def should_enrich(self, order_status: str, order_type: str) -> bool:
        """Determina se deve chamar API de enriquecimento."""
        if order_type != 'delivery':
            return False
        return order_status in ['released', 'dispatched', 'in_transit', 'delivered', 'ready']