# =================================
# ADMIN ROUTES - PORTAL DO CLIENTE
# =================================

from fastapi import APIRouter, Depends, status, BackgroundTasks
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from datetime import datetime
from src.infrastructure.db.connection import get_db
from src.infrastructure.cache.redis_client import redis_client
from src.core.services.reconciliation_service import ReconciliationService


router = APIRouter()

class InjectCredentialsPayload(BaseModel):
    refresh_token: str = Field(..., description="Refresh token extraído do navegador após passar pelo reCaptcha")
    access_token: str = Field("token_placeholder", description="Access token temporário (opcional, será renovado logo)")

@router.post(
    "/merchants/{merchant_id}/credentials", 
    status_code=status.HTTP_200_OK,
    summary="Injeta um novo refresh_token manualmente para um lojista"
)
async def inject_merchant_credentials(
    merchant_id: str,
    payload: InjectCredentialsPayload,
    session: AsyncSession = Depends(get_db)
):
    """
    Salva ou atualiza as credenciais do lojista no banco de dados e marca o status como ACTIVE.
    Isso é necessário quando o ciclo automático de renovação é quebrado (ex: servidor offline por > 5 dias).
    """
    
    query = text("""
        INSERT INTO merchant_credentials (merchant_id, access_token, refresh_token, expires_at, auth_status, updated_at)
        VALUES (:mid, :access, :refresh, NOW() + INTERVAL '8 hours', 'ACTIVE', NOW())
        ON CONFLICT (merchant_id) DO UPDATE SET
            access_token = EXCLUDED.access_token,
            refresh_token = EXCLUDED.refresh_token,
            expires_at = EXCLUDED.expires_at,
            auth_status = 'ACTIVE',
            updated_at = NOW()
    """)
    
    await session.execute(query, {
        "mid": merchant_id,
        "access": payload.access_token,
        "refresh": payload.refresh_token
    })
    
    # Limpa o cache atual no Redis para forçar o sistema a ler a nova credencial no próximo ciclo
    if redis_client._client is not None:
        await redis_client.client.delete("cardapioweb:access_token")
        await redis_client.client.delete("cardapioweb:refresh_token")

    return {
        "status": "success", 
        "message": f"Credenciais injetadas com sucesso para a loja {merchant_id}. Status atualizado para ACTIVE."
    }

@router.post(
    "/merchants/{merchant_id}/shifts/close", 
    status_code=status.HTTP_200_OK,
    summary="Fecha o caixa atual e dispara a auditoria (Reconciliação)"
)
async def close_merchant_shift(
    merchant_id: str,
    background_tasks: BackgroundTasks,
    session: AsyncSession = Depends(get_db)
):
    """Atualiza a tabela operation_days e agenda a reconciliação no background."""
    query = text("""
        SELECT id, opened_at 
        FROM operation_days 
        WHERE merchant_id = :mid AND closed_at IS NULL
        ORDER BY opened_at DESC LIMIT 1
    """)
    result = await session.execute(query, {"mid": merchant_id})
    shift = result.fetchone()

    if not shift:
        return {"status": "error", "message": "Nenhum caixa aberto encontrado para fechar."}

    shift_id, opened_at = shift
    closed_at = datetime.now()

    await session.execute(
        text("UPDATE operation_days SET closed_at = :closed_at WHERE id = :id"),
        {"closed_at": closed_at, "id": shift_id}
    )
    
    reconciliation_service = ReconciliationService()
    background_tasks.add_task(
        reconciliation_service.run_reconciliation_for_shift,
        merchant_id=str(merchant_id),
        opened_at=opened_at,
        closed_at=closed_at,
        shift_id=int(shift_id)
    )

    return {
        "status": "success", 
        "message": "Caixa fechado com sucesso. A auditoria de pedidos foi iniciada em background."
    }