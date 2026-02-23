# ============================================
# BASE HTTP CLIENT - Padrão para todas as APIs
# ============================================

from functools import wraps
from typing import Any, Callable, Dict, Optional, TypeVar

import httpx

T = TypeVar('T')


class BaseAPIClient:
    """
    Client HTTP base com retry, auth e error handling.
    """
    
    def __init__(
        self,
        base_url: str,
        api_key: Optional[str] = None,
        api_key_header: str = "X-API-Key",
        timeout: int = 10,
        retries: int = 3
    ):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.api_key_header = api_key_header
        self.timeout = timeout
        self.retries = retries
        
        headers = {
            "Accept": "application/json",
            "User-Agent": "Cardapioweb-Integrator/15.0"
        }
        
        if api_key:
            headers[api_key_header] = api_key
        
        self.client = httpx.AsyncClient(
            timeout=timeout,
            headers=headers,
            follow_redirects=True
        )
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()
    
    async def request(
        self,
        method: str,
        path: str,
        **kwargs
    ) -> Optional[Dict[str, Any]]:
        """
        Faz requisição HTTP com retry e error handling.
        """
        url = f"{self.base_url}{path}"
        
        for attempt in range(self.retries):
            try:
                response = await self.client.request(method, url, **kwargs)
                
                if response.status_code == 401:
                    print(f"❌ API {self.base_url}: Unauthorized")
                    return None
                
                if response.status_code == 404:
                    return None
                
                if response.status_code == 204:
                    return {}  # No content
                
                response.raise_for_status()
                return response.json()
                
            except httpx.HTTPStatusError as e:
                if attempt == self.retries - 1:
                    print(f"❌ API HTTP {e.response.status_code}: {e.response.text[:200]}")
                    return None
                continue
                
            except httpx.RequestError as e:
                if attempt == self.retries - 1:
                    print(f"❌ API Request error: {e}")
                    return None
                continue
        
        return None
    
    async def get(self, path: str, **kwargs) -> Optional[Dict[str, Any]]:
        return await self.request("GET", path, **kwargs)
    
    async def post(self, path: str, **kwargs) -> Optional[Dict[str, Any]]:
        return await self.request("POST", path, **kwargs)


def api_method(f: Callable[..., T]) -> Callable[..., T]:
    """
    Decorator para métodos de API com fallback automático.
    """
    @wraps(f)
    async def wrapper(self, *args, **kwargs):
        entity_id = args[0] if args else kwargs.get('order_id', 'unknown')
        
        print(f"🔍 API Call: {f.__name__}(id={entity_id})")
        
        result = await f(self, *args, **kwargs)
        
        if result is None:
            return {
                "_api_error": True,
                "_fallback": True,
                "_method": f.__name__,
                "id": entity_id
            }
        
        return result
    
    return wrapper