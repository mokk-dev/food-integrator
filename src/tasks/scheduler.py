from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import text

from src.core.logger import logger
from src.core.services.snapshot_service import SnapshotService
from src.core.services.reconciliation_service import ReconciliationService
from src.infrastructure.db.connection import get_db_session
from src.infrastructure.external.cardapioweb_auth import CardapiowebAuthManager
from src.config import settings

async def _run_snapshot_job():
    """Wrapper para instanciar e rodar o serviço de snapshots (Fase 1/3)."""
    service = SnapshotService()
    await service.take_snapshots()

async def _run_proactive_token_rotation():
    """
    Rotação Preventiva (Fase 2):
    Roda toda hora no minuto 00.
    Lê a tabela `merchants` e verifica se a loja vai abrir na próxima 1 hora.
    Se sim, força a renovação preventiva dos tokens para o expediente.
    """
    try:
        async with get_db_session() as session:
            # Busca lojas ativas cujo horário de abertura é na próxima hora.
            query = text("""
                SELECT merchant_id, default_start_time 
                FROM merchants 
                WHERE is_active = TRUE 
                  AND merchant_id = :mid
                  AND EXTRACT(HOUR FROM default_start_time) = EXTRACT(HOUR FROM (NOW() AT TIME ZONE 'America/Sao_Paulo' + INTERVAL '1 hour'))
            """)
            
            result = await session.execute(query, {"mid": str(settings.default_merchant_id)})
            merchant_to_refresh = result.fetchone()

        if merchant_to_refresh:
            merchant_id, start_time = merchant_to_refresh
            logger.info(
                "scheduler.proactive_rotation_triggered", 
                merchant=merchant_id, 
                opening_time=str(start_time),
                msg="Loja abre em breve. Iniciando renovação preventiva de tokens."
            )
            
            auth_manager = CardapiowebAuthManager()
            await auth_manager.get_valid_access_token(force_refresh=True)
            
            logger.info(
                "scheduler.proactive_rotation_success", 
                merchant=merchant_id,
                msg="Tokens renovados preventivamente com sucesso para o expediente."
            )

    except Exception as e:
        logger.error(
            "scheduler.proactive_rotation_failed", 
            error=str(e),
            msg="ATENÇÃO: Falha na rotação preventiva. O sistema tentará novamente de forma reativa durante o uso."
        )

async def auto_close_shifts_and_reconcile():
    """
    Auditoria e Conformidade (Fase 2) - Gatilho Temporário:
    Verifica a cada hora se alguma loja atingiu o seu horário de fecho (default_end_time).
    Se sim, fecha o operation_days no banco e dispara a reconciliação com rate limits definidos na .env.
    """
    try:
        async with get_db_session() as session:
            # Busca lojas ativas cujo horário de encerramento acabou de passar
            query = text("""
                SELECT merchant_id 
                FROM merchants 
                WHERE is_active = TRUE 
                  AND EXTRACT(HOUR FROM default_end_time) = EXTRACT(HOUR FROM NOW() AT TIME ZONE 'America/Sao_Paulo')
            """)
            result = await session.execute(query)
            merchants_closing = result.fetchall()

            for (merchant_id,) in merchants_closing:
                # 1. Procura se a loja tem um caixa aberto
                shift_query = text("""
                    SELECT id, opened_at FROM operation_days 
                    WHERE merchant_id = :mid AND closed_at IS NULL
                    ORDER BY opened_at DESC LIMIT 1
                """)
                shift_result = await session.execute(shift_query, {"mid": merchant_id})
                shift = shift_result.fetchone()
                
                if shift:
                    shift_id, opened_at = shift
                    closed_at = datetime.now()
                    
                    # 2. Fecha o caixa no banco
                    await session.execute(
                        text("UPDATE operation_days SET closed_at = :closed_at WHERE id = :id"),
                        {"closed_at": closed_at, "id": shift_id}
                    )
                    await session.commit()

                    logger.info("scheduler.auto_close_shift", merchant=merchant_id, msg="Expediente encerrado automaticamente pelo horário padrão.")
                    
                    # 3. Dispara a Reconciliação
                    service = ReconciliationService()
                    await service.run_reconciliation_for_shift(
                        merchant_id=str(merchant_id), 
                        opened_at=opened_at, 
                        closed_at=closed_at,
                        shift_id=int(shift_id)
                    )

    except Exception as e:
        logger.error("scheduler.auto_close_shift_failed", error=str(e))


def start_scheduler() -> AsyncIOScheduler:
    """
    Inicializa o APScheduler e regista os cronjobs.
    """
    logger.info("scheduler.starting", msg="Iniciando Task Scheduler (APScheduler)...")
    
    scheduler = AsyncIOScheduler()

    # 1. Snapshots
    scheduler.add_job(
        _run_snapshot_job,
        trigger=IntervalTrigger(minutes=5),
        id="operation_snapshots_job",
        name="Geração de Snapshots Operacionais",
        replace_existing=True
    )

    # 2. Rotação Preventiva de Tokens (Minuto 00)
    scheduler.add_job(
        _run_proactive_token_rotation,
        trigger=CronTrigger(minute=0, timezone='America/Sao_Paulo'),
        id="proactive_token_rotation_job",
        name="Rotação Preventiva de Tokens",
        replace_existing=True
    )

    # 3. Fecho de Caixa e Auditoria de Conformidade (Minuto 00)
    scheduler.add_job(
        auto_close_shifts_and_reconcile,
        trigger=CronTrigger(minute=0, timezone='America/Sao_Paulo'),
        id="auto_close_shifts_job",
        name="Fecho Automático e Reconciliação",
        replace_existing=True
    )

    scheduler.start()
    return scheduler