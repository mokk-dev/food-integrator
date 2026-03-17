import asyncio
from datetime import datetime

from src.core.services.reconciliation_service import ReconciliationService
from src.infrastructure.cache.redis_client import redis_client


async def run_recovery():
    print("Iniciando script de recuperação de turno...")

    print("Conectando ao Redis...")
    await redis_client.connect()

    service = ReconciliationService()

    merchant_id = "6758"
    shift_id = 10
    opened_at = datetime.fromisoformat("2026-03-12T19:06:19.796882+00:00")
    closed_at = datetime.fromisoformat("2026-03-13T05:00:00.027102+00:00")

    print(f"Merchant: {merchant_id} | Shift ID: {shift_id}")
    print("ReconciliationService...")

    await service.run_reconciliation_for_shift(
        merchant_id=merchant_id,
        opened_at=opened_at,
        closed_at=closed_at,
        shift_id=shift_id,
    )

    print("Desconectando do Redis...")
    if hasattr(redis_client, "disconnect"):
        await redis_client.disconnect()
    elif hasattr(redis_client, "close"):
        await redis_client.close()

    print("Script de recuperação finalizado! Verifique o banco de dados.")


if __name__ == "__main__":
    asyncio.run(run_recovery())
