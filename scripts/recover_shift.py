import asyncio
from datetime import datetime
from src.core.services.reconciliation_service import ReconciliationService

async def run_recovery():
    print("Iniciando script de recuperação de turno...")
    
    # Instancia o serviço que acabamos de corrigir
    service = ReconciliationService()
    
    # Dados exatos extraídos dos seus logs da madrugada
    merchant_id = "6758"
    shift_id = 10
    
    # Convertendo as datas do log para objetos datetime timezone-aware
    opened_at = datetime.fromisoformat("2026-03-12T19:06:19.796882+00:00")
    # Adicionando o +00:00 no closed_at para manter o UTC do seu servidor
    closed_at = datetime.fromisoformat("2026-03-13T05:00:00.027102+00:00") 

    print(f"Merchant: {merchant_id} | Shift ID: {shift_id}")
    print("Disparando ReconciliationService...")
    
    # Chama o serviço forçando a execução
    await service.run_reconciliation_for_shift(
        merchant_id=merchant_id,
        opened_at=opened_at,
        closed_at=closed_at,
        shift_id=shift_id
    )
    
    print("Script de recuperação finalizado! Verifique o banco de dados.")

if __name__ == "__main__":
    asyncio.run(run_recovery())