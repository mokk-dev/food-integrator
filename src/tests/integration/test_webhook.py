# ============================================
# TESTES DE INTEGRAÇÃO - WEBHOOK
# ============================================

import pytest
import pytest_asyncio
from httpx import AsyncClient


# Fixture para cliente HTTP
@pytest_asyncio.fixture
async def client():
    from src.main import app
    async with AsyncClient(app=app, base_url="http://test") as ac:
        yield ac


@pytest.mark.asyncio
async def test_webhook_health(client: AsyncClient):
    """Testa health check do webhook."""
    response = await client.get("/webhook/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_webhook_missing_token(client: AsyncClient):
    """Testa rejeição sem token."""
    response = await client.post("/webhook/orders", json={})
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_webhook_invalid_token(client: AsyncClient):
    """Testa rejeição com token inválido."""
    response = await client.post(
        "/webhook/orders",
        json={},
        headers={"X-Webhook-Token": "invalid_token"}
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_webhook_valid_payload(client: AsyncClient):
    """Testa aceitação de payload válido."""
    # TODO: Mock token ou usar token de teste
    payload = {
        "event_id": "test_evt_001",
        "order_id": 12345,
        "event_type": "ORDER_CREATED",
        "merchant_id": "6758",
        "timestamp": "2026-02-19T20:00:00Z",
        "order_status": "pending"
    }
    
    # Com token válido deveria retornar 202
    # Sem token ou com token inválido, testamos acima