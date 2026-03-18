# =================================
# ADMIN ROUTES - PORTAL DO CLIENTE
# =================================

from fastapi import APIRouter, Depends, status, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timedelta

from src.infrastructure.db.connection import get_db
from src.infrastructure.cache.redis_client import redis_client
from src.core.services.reconciliation_service import ReconciliationService
from src.core.services.historical_sync_service import HistoricalSyncService

router = APIRouter()

# -----------------------------------------------------------------------------
# SCHEMAS (Pydantic Models)
# -----------------------------------------------------------------------------

class InjectCredentialsPayload(BaseModel):
    refresh_token: str = Field(..., description="Refresh token extraído do navegador após passar pelo reCaptcha")
    access_token: str = Field("token_placeholder", description="Access token temporário (opcional, será renovado logo)")

class SyncHistoryRequest(BaseModel):
    start_date: datetime
    end_date: datetime


# -----------------------------------------------------------------------------
# ROUTES
# -----------------------------------------------------------------------------

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


@router.post(
    "/merchants/{merchant_id}/sync-history", 
    status_code=status.HTTP_202_ACCEPTED,
    summary="Dispara a sincronização histórica (Backfill) limitando a 31 dias e processos simultâneos."
)

@router.post("/merchants/{merchant_id}/sync-history", status_code=status.HTTP_202_ACCEPTED)
async def sync_merchant_history(
    merchant_id: str,
    payload: SyncHistoryRequest,
    session: AsyncSession = Depends(get_db)
):
    """Enfileira um Job de Backfill e trava concorrência."""
    if payload.start_date > payload.end_date:
        raise HTTPException(status_code=400, detail="Data início maior que data fim.")

    if (payload.end_date - payload.start_date).days > 31:
        raise HTTPException(status_code=400, detail="O período máximo é de 31 dias.")

    lock_key = f"backfill_lock:{merchant_id}"
    
    if not await redis_client.client.set(lock_key, "queued", nx=True, ex=600):
        raise HTTPException(status_code=429, detail="Sincronização em andamento para esta loja.")

    query = text("""
        INSERT INTO sync_jobs (merchant_id, start_date, end_date, status)
        VALUES (:mid, :start, :end, 'pending')
        RETURNING id
    """)
    result = await session.execute(query, {"mid": merchant_id, "start": payload.start_date, "end": payload.end_date})
    job_id = result.scalar()
    await session.commit()

    return {"message": "Sincronização enfileirada", "job_id": job_id, "status": "pending"}


@router.get("/merchants/{merchant_id}/sync-status", status_code=status.HTTP_200_OK)
async def get_sync_status(merchant_id: str, session: AsyncSession = Depends(get_db)):
    """Rota para o Front-end consultar e montar a Barra de Progresso."""
    query = text("""
        SELECT id, start_date, end_date, status, total_shifts, processed_shifts, error_message, updated_at
        FROM sync_jobs
        WHERE merchant_id = :mid
        ORDER BY created_at DESC LIMIT 1
    """)
    result = await session.execute(query, {"mid": merchant_id})
    job = result.fetchone()

    if not job:
        raise HTTPException(status_code=404, detail="Nenhum histórico de sincronização encontrado.")

    percentual = 0
    if job.total_shifts > 0:
        percentual = round((job.processed_shifts / job.total_shifts) * 100, 2)

    return {
        "job_id": job.id,
        "status": job.status,
        "progress_percentage": percentual,
        "shifts": {"processed": job.processed_shifts, "total": job.total_shifts},
        "error": job.error_message,
        "last_updated": job.updated_at
    }


@router.delete("/merchants/{merchant_id}/sync-lock", status_code=status.HTTP_200_OK)
async def unlock_merchant_sync(merchant_id: str):
    """ROTA DE EMERGÊNCIA: Remove a trava presa no Redis caso um Hard Crash ocorra."""
    lock_key = f"backfill_lock:{merchant_id}"
    await redis_client.delete(lock_key)
    return {"message": f"Lock de sincronização removido à força para {merchant_id}."}