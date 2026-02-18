import json
from typing import Any

import redis.asyncio as redis

from src.config import settings


class RedisClient:
    """
    Cliente Redis async para:
    - Idempotência de webhooks
    - Cache de tokens/sessões
    - Fila de processamento (alternativa ao polling DB)
    """

    def __init__(self):
        self._client: redis.Redis | None = None

    async def connect(self):
        if self._client is None:
            self._client = redis.from_url(
                str(settings.redis_url),
                socket_timeout=settings.redis_socket_timeout,
                socket_connect_timeout=settings.redis_socket_connect_timeout,
                retry_on_timeout=settings.redis_retry_on_timeout,
                decode_responses=True,
            )
            await self._client.ping()

    async def disconnect(self):
        if self._client:
            await self._client.close()
            self._client = None

    @property
    def client(self) -> redis.Redis:
        if self._client is None:
            raise RuntimeError("Redis não conectado. Chame connect() primeiro.")
        return self._client

    # Idempotência de Webhooks

    async def is_event_processed(self, event_id: str) -> bool:
        key = f"webhook:processed:{event_id}"
        exists = await self.client.exists(key)
        return bool(exists)

    async def mark_event_processed(
        self, event_id: str, ttl_seconds: int = 86400
    ) -> None:
        key = f"webhook:processed:{event_id}"
        await self.client.setex(key, ttl_seconds, "1")

    async def mark_event_processing(self, event_id: str, ttl_seconds: int = 60) -> bool:
        key = f"webhook:processing:{event_id}"
        result = await self.client.set(key, "1", nx=True, ex=ttl_seconds)
        return result is not None

    async def release_event_lock(self, event_id: str) -> None:
        key = f"webhook:processing:{event_id}"
        await self.client.delete(key)

    # Cache Genérico

    async def get_json(self, key: str) -> Any | None:
        data = await self.client.get(key)
        if data:
            return json.loads(data)
        return None

    async def set_json(self, key: str, value: Any, ttl_seconds: int = 300) -> None:
        await self.client.setex(key, ttl_seconds, json.dumps(value))

    async def delete(self, key: str) -> None:
        await self.client.delete(key)

    # TODO: Rate Limiting

    async def check_rate_limit(
        self, key: str, max_requests: int, window_seconds: int
    ) -> tuple[bool, int]:
        pipe = self.client.pipeline()
        now = await self.client.time()  # Redis server time

        # Implementação simplificada - pode ser melhorada
        current = await self.client.get(key)
        count = int(current) if current else 0

        if count >= max_requests:
            return False, 0

        pipe.incr(key)
        pipe.expire(key, window_seconds)
        await pipe.execute()

        return True, max_requests - count - 1


# Singleton global
redis_client = RedisClient()


# Helpers para injeção de dependência
async def get_redis() -> RedisClient:
    return redis_client
