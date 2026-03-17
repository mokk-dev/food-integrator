# ============================================
# PYDANTIC MODELS - WEBHOOK CARDAPIOWEB
# ============================================

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field, field_validator


class WebhookPayload(BaseModel):
    """
    Payload genérico de webhook do Cardapioweb.
    Campos obrigatórios para todos os eventos.
    """

    event_id: str = Field(..., description="ID único do evento (para idempotência)")

    order_id: int | str = Field(..., description="ID do pedido no Cardapioweb")
    event_type: str = Field(
        ..., description="Tipo do evento: ORDER_CREATED, ORDER_STATUS_UPDATED, etc."
    )

    merchant_id: int | str = Field(..., description="ID do estabelecimento")

    created_at: datetime | None = Field(
        None, description="Data e hora original do evento no Cardapioweb"
    )

    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="Timestamp do evento no nosso sistema",
    )

    order_status: str | None = Field(None, description="Status atual do pedido")
    previous_status: str | None = Field(
        None, description="Status anterior (para updates)"
    )

    raw_payload: dict[str, Any] = Field(
        default_factory=dict, description="Payload completo recebido"
    )

    @field_validator("event_type")
    @classmethod
    def validate_event_type(cls, v: str) -> str:
        allowed = {
            "ORDER_CREATED",
            "ORDER_STATUS_UPDATED",
            "ORDER_CANCELLED",
            "ORDER_DISPATCHED",
            "ORDER_DELIVERED",
            "PAYMENT_RECEIVED",
        }
        if v.upper() not in allowed:
            pass
        return v.upper()

    class Config:
        json_schema_extra = {
            "example": {
                "event_id": "1psu56ytdo8ztk53rir",
                "order_id": 182564627,
                "event_type": "ORDER_CREATED",
                "merchant_id": 6758,
                "created_at": "2026-02-09T18:30:41-03:00",
                "order_status": "waiting_confirmation",
            }
        }


class OrderCreatedPayload(WebhookPayload):
    """Payload específico para ORDER_CREATED."""

    event_type: str = "ORDER_CREATED"

    customer_name: str | None = None
    customer_phone: str | None = None
    total_value: Decimal | None = None
    delivery_fee: Decimal | None = None
    order_type: str | None = Field(
        None, description="delivery, takeout, onsite, closed_table"
    )
    sales_channel: str | None = Field(None, description="ifood, whatsapp, phone, etc.")

    delivery_address: dict[str, Any] | None = None

    items: list[dict[str, Any]] | None = None


class OrderStatusUpdatedPayload(WebhookPayload):
    """Payload específico para ORDER_STATUS_UPDATED."""

    event_type: str = "ORDER_STATUS_UPDATED"

    previous_status: str = Field(..., description="Status anterior")
    new_status: str = Field(..., description="Novo status")

    delivery_man_name: str | None = None
    delivery_man_phone: str | None = None
    delivery_route: str | None = None


class WebhookResponse(BaseModel):
    """Resposta padronizada do endpoint de webhook."""

    status: str = Field(..., description="accepted, duplicate, error")
    event_id: str
    message: str | None = None
    correlation_id: str | None = None

    class Config:
        json_schema_extra = {
            "example": {
                "status": "accepted",
                "event_id": "evt_123456789",
                "message": "Event queued for processing",
            }
        }
