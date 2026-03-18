import asyncio
from datetime import datetime
from sqlalchemy import text

# Importações do seu projeto
from src.core.services.reconciliation_service import ReconciliationService
from src.infrastructure.cache.redis_client import redis_client 
from src.infrastructure.db.connection import get_db_session

async def run_recovery():
    print("Iniciando script de Backfill (Recuperação em Lote)...")
    
    print("🔌 Conectando ao Redis...")
    await redis_client.connect()
    
    # Instancia o serviço
    service = ReconciliationService()
    
    shifts_to_process = []
    
    print("📦 Buscando expedientes no banco de dados desde 03/03/2026...")
    async with get_db_session() as session:
        query = text("""
            SELECT id, merchant_id, opened_at, closed_at 
            FROM operation_days 
            WHERE opened_at >= '2026-03-03 00:00:00'
              AND closed_at IS NOT NULL
            ORDER BY opened_at ASC
        """)
        result = await session.execute(query)
        shifts_to_process = result.fetchall()
        
    if not shifts_to_process:
        print("Nenhum expediente encontrado para o período solicitado.")
    else:
        total_shifts = len(shifts_to_process)
        print(f"📊 Encontrados {total_shifts} expedientes. Iniciando processamento...\n")
        
        for index, row in enumerate(shifts_to_process, start=1):
            shift_id, merchant_id, opened_at, closed_at = row
            
            data_formatada = opened_at.strftime('%d/%m/%Y')
            print(f"[{index}/{total_shifts}] 🚀 Reconciliando Shift ID: {shift_id} (Abertura: {data_formatada})...")
            
            try:
                await service.run_reconciliation_for_shift(
                    merchant_id=str(merchant_id),
                    opened_at=opened_at,
                    closed_at=closed_at,
                    shift_id=shift_id
                )
                print(f"✅ Shift {shift_id} concluído.")
                
            except Exception as e:
                print(f"❌ Erro ao processar Shift {shift_id}: {e}")
            
            if index < total_shifts:
                print("⏳ Aguardando 5 segundos antes do próximo expediente (Rate Limit)...\n")
                await asyncio.sleep(5)

    print("\nDesconectando do Redis...")
    if hasattr(redis_client, "disconnect"):
        await redis_client.disconnect()
    elif hasattr(redis_client, "close"):
        await redis_client.close()
        
    print("🎉 Script de Backfill finalizado com sucesso! Seu banco está 100% sincronizado.")

if __name__ == "__main__":
    asyncio.run(run_recovery())