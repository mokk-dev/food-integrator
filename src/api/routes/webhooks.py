# ============================================
# WEBHOOK ROUTES - ENDPOINTS CARDAPIOWEB
# ============================================

from typing import Any, Dict

from fastapi import APIRouter, Depends, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse

from src.api.dependencies import (
    check_idempotency,
    get_correlation_id,
    verify_webhook_token,
)
from src.config import settings
from src.core.models.webhook import WebhookPayload, WebhookResponse
from src.core.services.inbox_processor import InboxProcessor

router = APIRouter()


@router.post(
    "/orders",
    response_model=WebhookResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Recebe webhooks de pedidos do Cardapioweb",
    description="""
    Endpoint para receber eventos de pedidos do Cardapioweb.
    
    Valida o token, verifica duplicatas e armazena no inbox para processamento assíncrono.
    
    **Headers obrigatórios:**
    - `X-Webhook-Token`: Token de autenticação configurado no .env
    
    **Respostas:**
    - `202 Accepted`: Evento aceito e enfileirado
    - `200 OK`: Evento duplicado (já processado)
    - `401/403`: Token inválido
    """
)
async def receive_order_webhook(
    request: Request,
    payload: Dict[str, Any],  # Recebe como dict primeiro para flexibilidade
    correlation_id: str = Depends(get_correlation_id),
    _token_valid: bool = Depends(verify_webhook_token)
):
    """
    Recebe webhook de pedido do Cardapioweb.
    """
    # Construir modelo Pydantic (validação automática)
    try:
        webhook_payload = WebhookPayload(
            **payload,
            raw_payload=payload  # Preserva payload original
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid payload: {str(e)}"
        )
    
    # Processar ingestão
    processor = InboxProcessor()
    result_status, error = await processor.process_webhook(
        webhook_payload,
        correlation_id=correlation_id
    )
    
    # Responder conforme resultado
    if result_status == "duplicate":
        # Idempotente: 200 OK para duplicados
        return WebhookResponse(
            status="duplicate",
            event_id=webhook_payload.event_id,
            message="Event already processed",
            correlation_id=correlation_id
        )
    
    if result_status == "error":
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Processing error: {error}"
        )
    
    # Sucesso: 202 Accepted
    return WebhookResponse(
        status="accepted",
        event_id=webhook_payload.event_id,
        message="Event queued for processing",
        correlation_id=correlation_id
    )


@router.get(
    "/health",
    summary="Health check específico do webhook",
    include_in_schema=False
)
async def webhook_health():
    """Health check interno."""
    return {"status": "ok", "service": "webhook"}