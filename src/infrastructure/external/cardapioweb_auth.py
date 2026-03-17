import asyncio

import httpx
from sqlalchemy import text

from src.config import settings
from src.core.logger import logger
from src.infrastructure.cache.redis_client import redis_client
from src.infrastructure.db.connection import get_db_session


class CardapiowebAuthManager:
    """
    Gerencia o ciclo de vida dos tokens OAuth2 da Cardapioweb.
    Estratégia: Redis (Cache ultra-rápido) -> PostgreSQL (Persistência) -> Rotação Automática.
    """

    _instance = None

    ACCESS_TOKEN_KEY = "cardapioweb:access_token"
    REFRESH_TOKEN_KEY = "cardapioweb:refresh_token"

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._auth_lock = asyncio.Lock()
            # Guarda o último token conhecido em memória para evitar chamadas redundantes
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
        """Busca o Refresh Token no Banco/Env, faz a renovação e persiste os novos tokens."""
        async with self._auth_lock:
            logger.info(
                "auth.refresh_token_started",
                msg="Iniciando renovação do token de acesso",
            )

            # --- DOUBLE CHECK ---
            redis_access_token = await redis_client.client.get(self.ACCESS_TOKEN_KEY)

            if redis_access_token and redis_access_token != self._memory_access_token:
                logger.info(
                    "auth.token_already_refreshed",
                    msg="Token renovado por outro processo. Atualizando estado local.",
                )
                self._memory_access_token = redis_access_token
                return redis_access_token

            # --- BUSCA DO REFRESH TOKEN (CASCATA: Redis -> Postgres -> Env) ---
            refresh_token = await redis_client.client.get(self.REFRESH_TOKEN_KEY)

            if not refresh_token:
                async with get_db_session() as session:
                    result = await session.execute(
                        text(
                            "SELECT refresh_token FROM merchant_credentials WHERE merchant_id = :mid AND auth_status = 'ACTIVE'"
                        ),
                        {"mid": str(settings.default_merchant_id)},
                    )
                    row = result.fetchone()
                    refresh_token = row[0] if row else None

            # Último recurso (Seed inicial do .env)
            if not refresh_token:
                refresh_token = settings.cardapioweb_refresh_token

            if not refresh_token:
                raise Exception(
                    "Nenhum Refresh Token ativo no Redis, Banco ou .env. Necessária injeção manual."
                )

            # --- CHAMADA NA API PARA RENOVAÇÃO ---
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    self.auth_url,
                    json={
                        "grant_type": "refresh_token",
                        "refresh_token": refresh_token,
                    },
                    headers={
                        "Origin": "https://portal.cardapioweb.com",
                        "Referer": "https://portal.cardapioweb.com/",
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Mokk/1.0",
                    },
                )

                # Tratamento de erro grave (Token expirado/revogado pela API)
                if response.status_code != 200:
                    logger.error(
                        "auth.refresh_failed",
                        status=response.status_code,
                        body=response.text,
                    )

                    # Atualiza o banco para EXPIRED
                    async with get_db_session() as session:
                        await session.execute(
                            text(
                                "UPDATE merchant_credentials SET auth_status = 'EXPIRED', updated_at = NOW() WHERE merchant_id = :mid"
                            ),
                            {"mid": str(settings.default_merchant_id)},
                        )

                    raise Exception(
                        f"Falha fatal ao renovar tokens. Cadeia de Refresh expirou. É necessário injetar credenciais manualmente. Log: {response.text}"
                    )

                data = response.json()
                new_access = data.get("access_token")
                new_refresh = data.get("refresh_token")

                if not new_access or not new_refresh:
                    raise ValueError(
                        "A resposta da API de autenticação não devolveu os tokens esperados."
                    )

                access_exp = max(
                    1, int(data.get("access_token_expires_in", 28800)) - 60
                )
                refresh_exp = int(data.get("refresh_token_expires_in", 432000))

                # --- 1. PERSISTÊNCIA NO BANCO (Segurança contra reinicializações) ---
                async with get_db_session() as session:
                    await session.execute(
                        text("""
                            INSERT INTO merchant_credentials (merchant_id, access_token, refresh_token, expires_at, auth_status, updated_at)
                            VALUES (:mid, :access, :refresh, NOW() + INTERVAL '8 hours', 'ACTIVE', NOW())
                            ON CONFLICT (merchant_id) DO UPDATE SET
                                access_token = EXCLUDED.access_token,
                                refresh_token = EXCLUDED.refresh_token,
                                expires_at = EXCLUDED.expires_at,
                                auth_status = 'ACTIVE',
                                updated_at = NOW()
                        """),
                        {
                            "mid": str(settings.default_merchant_id),
                            "access": new_access,
                            "refresh": new_refresh,
                        },
                    )

                # --- 2. ATUALIZAÇÃO DO CACHE REDIS (Velocidade) ---
                await redis_client.client.set(
                    self.ACCESS_TOKEN_KEY, new_access, ex=access_exp
                )
                await redis_client.client.set(
                    self.REFRESH_TOKEN_KEY, new_refresh, ex=refresh_exp
                )

                self._memory_access_token = new_access

                logger.info(
                    "auth.tokens_refreshed",
                    msg="Tokens renovados no PostgreSQL e Redis com sucesso.",
                )
                return new_access
