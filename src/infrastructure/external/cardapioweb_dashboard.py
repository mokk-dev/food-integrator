# ============================================
# CLIENT API DASHBOARD (PLATAFORMA) - CARDAPIOWEB
# ============================================

from typing import Any, Dict
from datetime import datetime

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
        Busca informações de entrega.
        A rota específica /delivery retorna 302 (redirect) na Cardapioweb,
        então vamos direto para a rota de detalhes que já contém essa informação.
        """
        return await self.get_order_details(order_id)
    
    @api_method
    async def get_delivery_men_summary(self, start_date: datetime, end_date: datetime) -> list:
        """
        Busca o resumo (quantidade de pedidos) de todos os motoboys no período.
        """
        start_str = start_date.strftime("%Y-%m-%dT%H:%M:%S.000-03:00")
        end_str = end_date.strftime("%Y-%m-%dT%H:%M:%S.000-03:00")
        
        params = {
            "q[order_filters][created_at_gteq]": start_str,
            "q[order_filters][created_at_lteq]": end_str,
            "q[delivery_man_filters][active_eq]": "true"
        }
        
        return await self._execute_with_auth("get", "/v2/company/delivery_men/orders_summary", params=params)

    @api_method
    async def get_orders_by_delivery_man(self, delivery_man_id: int, start_date: datetime, end_date: datetime) -> list:
        """
        Busca a lista completa de pedidos entregues por um motoboy específico (com paginação).
        """
        start_str = start_date.strftime("%Y-%m-%dT%H:%M:%S.000-03:00")
        end_str = end_date.strftime("%Y-%m-%dT%H:%M:%S.000-03:00")
        
        all_orders = []
        page = 1
        
        while True:
            params = {
                "page": page,
                "per_page": 100,
                # Utilizando os filtros de data de despacho conforme o payload mapeado
                "q[order_filters][order_dispatch_date_gteq]": start_str,
                "q[order_filters][order_dispatch_date_lteq]": end_str,
                "q[delivery_man_filters][id_eq]": delivery_man_id
            }
            
            response = await self._execute_with_auth("get", "/v2/company/delivery_men/orders", params=params)
            
            if not response or not isinstance(response, list):
                break
                
            all_orders.extend(response)
            
            if len(response) < 100:
                break
                
            page += 1
            
        return all_orders
    
    @api_method
    async def get_cash_flows(self, page: int = 1, per_page: int = 10) -> list:
        """Busca a lista dos caixas abertos e fechados."""
        params = {"page": page, "per_page": per_page, "order_by": "id", "order": "desc"}
        return await self._execute_with_auth("get", "/v1/company/cash_flows", params=params)

    @api_method
    async def get_cash_flow_summary(self, cash_flow_id: int) -> Dict[str, Any]:
        """Busca o super-resumo financeiro de um caixa específico."""
        return await self._execute_with_auth("get", f"/v1/company/cash_flow/{cash_flow_id}/summary")

    @api_method
    async def get_cash_flow_operations(self, cash_flow_id: int) -> list:
        """Busca todas as movimentações granulares (sangrias, vendas) do caixa."""
        return await self._execute_with_auth("get", f"/v1/company/cash_flow/{cash_flow_id}/operations")
    
    def should_enrich(self, order_status: str, order_type: str) -> bool:
        """Determina se deve chamar API de enriquecimento."""
        if order_type != 'delivery':
            return False
        return order_status in ['released', 'dispatched', 'in_transit', 'delivered', 'ready']
    
    @api_method
    async def get_cash_flows_by_period(self, merchant_id: str, start_date: datetime, end_date: datetime) -> list:
        """
        Busca os caixas fechados de um período específico.
        """
        page = 1
        per_page = 75
        target_cash_flows = []
        keep_fetching = True

        print(f"🔍 Iniciando rastreio de caixas entre {start_date.strftime('%d/%m/%Y')} e {end_date.strftime('%d/%m/%Y')}...")

        while keep_fetching:
            params = {
                "page": page,
                "per_page": per_page,
                "order_by": "id",
                "order": "desc"
            }
            
            # 1. httpx mescla o CompanyId com o Token JWT do BaseClient
            headers = {"CompanyId": str(merchant_id)}
            
            # 2. Force URL absoluta
            response = await self.get(
                "https://api.cardapioweb.com/api/v1/company/cash_flows", 
                params=params,
                headers=headers
            )
            
            items = response if isinstance(response, list) else response.get("data", [])
            
            if not items:
                print("🏁 Fim absoluto do histórico da loja atingido.")
                break

            for item in items:
                if item.get("status") != "close":
                    continue

                open_at_str = item.get("open_at")
                if not open_at_str:
                    continue
                
                open_at_dt = datetime.fromisoformat(open_at_str.replace("Z", "+00:00"))
                
                if open_at_dt > end_date:
                    continue
                    
                if open_at_dt < start_date:
                    keep_fetching = False
                    break
                    
                target_cash_flows.append(item)

            print(f"📄 Página {page} processada. Caixas encontrados no alvo até agora: {len(target_cash_flows)}")
            page += 1

        target_cash_flows.reverse()
        return target_cash_flows