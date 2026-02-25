# ============================================
# PYDANTIC MODELS - WEBHOOK CARDAPIOWEB
# ============================================

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, field_validator


class WebhookPayload(BaseModel):
    """
    Payload genérico de webhook do Cardapioweb.
    Campos obrigatórios para todos os eventos.
    """
    event_id: str = Field(..., description="ID único do evento (para idempotência)")
    
    # Atualizado: Cardapioweb pode mandar o ID como inteiro ou string
    order_id: Union[int, str] = Field(..., description="ID do pedido no Cardapioweb")
    event_type: str = Field(..., description="Tipo do evento: ORDER_CREATED, ORDER_STATUS_UPDATED, etc.")
    
    # Atualizado: Evita erro de tipagem caso o ID da loja venha como número
    merchant_id: Union[int, str] = Field(..., description="ID do estabelecimento")
    
    # Atualizado: Cardapioweb envia a data neste campo
    created_at: Optional[datetime] = Field(None, description="Data e hora original do evento no Cardapioweb")
    
    # Atualizado: Se o parceiro não enviar, geramos o nosso próprio timestamp interno para não quebrar
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc), 
        description="Timestamp do evento no nosso sistema"
    )
    
    # Campos opcionais presentes em alguns eventos
    order_status: Optional[str] = Field(None, description="Status atual do pedido")
    previous_status: Optional[str] = Field(None, description="Status anterior (para updates)")
    
    # Payload completo para debug/reprocessamento
    raw_payload: Dict[str, Any] = Field(default_factory=dict, description="Payload completo recebido")
    
    @field_validator('event_type')
    @classmethod
    def validate_event_type(cls, v: str) -> str:
        """Valida tipos de evento conhecidos."""
        allowed = {
            'ORDER_CREATED',
            'ORDER_STATUS_UPDATED',
            'ORDER_CANCELLED',
            'ORDER_DISPATCHED',
            'ORDER_DELIVERED',
            'PAYMENT_RECEIVED',
        }
        if v.upper() not in allowed:
            # Não falha, apenas loga (forward compatibility)
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
                "order_status": "waiting_confirmation"
            }
        }


class OrderCreatedPayload(WebhookPayload):
    """Payload específico para ORDER_CREATED."""
    event_type: str = "ORDER_CREATED"
    
    # Campos adicionais típicos de ORDER_CREATED
    customer_name: Optional[str] = None
    customer_phone: Optional[str] = None
    total_value: Optional[Decimal] = None
    delivery_fee: Optional[Decimal] = None
    order_type: Optional[str] = Field(None, description="delivery, takeout, onsite, closed_table")
    sales_channel: Optional[str] = Field(None, description="ifood, whatsapp, phone, etc.")
    
    # Endereço de entrega (quando delivery)
    delivery_address: Optional[Dict[str, Any]] = None
    
    # Itens do pedido
    items: Optional[List[Dict[str, Any]]] = None


class OrderStatusUpdatedPayload(WebhookPayload):
    """Payload específico para ORDER_STATUS_UPDATED."""
    event_type: str = "ORDER_STATUS_UPDATED"
    
    previous_status: str = Field(..., description="Status anterior")
    new_status: str = Field(..., description="Novo status")
    
    # Campos adicionais em certos status
    delivery_man_name: Optional[str] = None
    delivery_man_phone: Optional[str] = None
    delivery_route: Optional[str] = None


class WebhookResponse(BaseModel):
    """Resposta padronizada do endpoint de webhook."""
    status: str = Field(..., description="accepted, duplicate, error")
    event_id: str
    message: Optional[str] = None
    correlation_id: Optional[str] = None
    
    class Config:
        json_schema_extra = {
            "example": {
                "status": "accepted",
                "event_id": "evt_123456789",
                "message": "Event queued for processing"
            }
        }