import asyncio
import httpx
from src.infrastructure.cache.redis_client import redis_client
from src.config import settings
from src.core.logger import logger

class CardapiowebAuthManager:
    """
    Gerencia o ciclo de vida dos tokens OAuth2 da Cardapioweb usando Redis
    com a estratégia de Sliding Window e segurança contra chamadas concorrentes.
    Implementado como Singleton para compartilhar o Lock entre instâncias.
    """
    _instance = None
    
    ACCESS_TOKEN_KEY = "cardapioweb:access_token"
    REFRESH_TOKEN_KEY = "cardapioweb:refresh_token"

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(CardapiowebAuthManager, cls).__new__(cls)
            cls._instance._auth_lock = asyncio.Lock()
            # Guarda o último token conhecido em memória para evitar chamadas redundantes ao Redis/API
            cls._instance._memory_access_token = settings.cardapioweb_dashboard_api_key
        return cls._instance

    @property
    def auth_url(self) -> str:
        return f"{settings.cardapioweb_auth_base_url}/v2/auth/token"

    async def get_valid_access_token(self, force_refresh: bool = False) -> str:
        """Retorna um token válido. Se não existir ou for forçado, renova antes."""
        if not force_refresh:
            access_token = await redis_client.client.get(self.ACCESS_TOKEN_KEY)
            if access_token:
                self._memory_access_token = access_token
                return self._memory_access_token
            
            if self._memory_access_token:
                return self._memory_access_token

        return await self.refresh_tokens()

    async def refresh_tokens(self) -> str:
        """Usa o Refresh Token para pegar novas credenciais e salva no Redis."""
        async with self._auth_lock:
            logger.info("auth.refresh_token_started", msg="Iniciando renovação do token de acesso")
            
            # --- DOUBLE CHECK ---
            # Verifica se outra thread/worker já atualizou o token enquanto esperávamos pelo Lock
            redis_access_token = await redis_client.client.get(self.ACCESS_TOKEN_KEY)
                
            if redis_access_token and redis_access_token != self._memory_access_token:
                logger.info("auth.token_already_refreshed", msg="Token renovado por outro processo. Atualizando estado local.")
                self._memory_access_token = redis_access_token
                return redis_access_token

            # --- FLUXO PRINCIPAL DE RENOVAÇÃO ---
            refresh_token = await redis_client.client.get(self.REFRESH_TOKEN_KEY)
            
            # Se não tiver no Redis, usa o da env
            if not refresh_token:
                refresh_token = settings.cardapioweb_refresh_token

            if not refresh_token:
                raise Exception("Nenhum Refresh Token disponível no Redis ou no .env para autenticar.")

            async with httpx.AsyncClient() as client:
                response = await client.post(
                    self.auth_url,
                    json={
                        "grant_type": "refresh_token",
                        "refresh_token": refresh_token
                    },
                    headers={
                        "Origin": "https://portal.cardapioweb.com",
                        "Referer": "https://portal.cardapioweb.com/",
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Mokk/1.0"
                    }
                )

                if response.status_code != 200:
                    logger.error("auth.refresh_failed", status=response.status_code, body=response.text)
                    raise Exception(f"Falha ao renovar tokens da Cardapioweb: {response.text}")

                data = response.json()
                new_access = data.get("access_token")
                new_refresh = data.get("refresh_token")

                if not new_access or not new_refresh:
                    raise ValueError("A resposta da API de autenticação não devolveu os tokens esperados.")

                # Margem de segurança no TTL
                access_exp = max(1, int(data.get("access_token_expires_in", 28800)) - 60)
                refresh_exp = int(data.get("refresh_token_expires_in", 432000))

                # Salva no Redis acessando a propriedade .client
                await redis_client.client.set(self.ACCESS_TOKEN_KEY, new_access, ex=access_exp)
                await redis_client.client.set(self.REFRESH_TOKEN_KEY, new_refresh, ex=refresh_exp)

                self._memory_access_token = new_access

                logger.info("auth.tokens_refreshed", msg="Tokens renovados e salvos no Redis com sucesso.")
                return new_access