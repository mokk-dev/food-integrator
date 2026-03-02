from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from src.core.logger import logger
from src.core.services.snapshot_service import SnapshotService

async def _run_snapshot_job():
    """Wrapper para instanciar e rodar o serviço."""
    service = SnapshotService()
    await service.take_snapshots()

def start_scheduler() -> AsyncIOScheduler:
    """
    Inicializa o APScheduler e registra os cronjobs.
    """
    logger.info("scheduler.starting", msg="Iniciando Task Scheduler (APScheduler)...")
    
    scheduler = AsyncIOScheduler()

    # Registra o SnapshotJob para rodar a cada 5 minutos
    scheduler.add_job(
        _run_snapshot_job,
        trigger=IntervalTrigger(minutes=5),
        id="operation_snapshots_job",
        name="Geração de Snapshots Operacionais",
        replace_existing=True
    )

    scheduler.start()
    return scheduler