#!/bin/bash
# tests/manual/test_enrichment.sh

ORDER_ID="999001"
EVENT_ID="test_enrich_${ORDER_ID}"

echo "=== TESTE 3.1: Enriquecimento Completo ==="

# 1. Limpar
docker exec integrator-db psql -U postgres -d delivery -c \
  "DELETE FROM orders WHERE id = ${ORDER_ID}; DELETE FROM webhook_inbox WHERE event_id = '${EVENT_ID}';" > /dev/null

# 2. Inserir evento
curl -s -X POST http://localhost:8000/webhook/orders \
  -H "X-Webhook-Token: webhook" \
  -H "Content-Type: application/json" \
  -d "{
    \"event_id\": \"${EVENT_ID}\",
    \"order_id\": ${ORDER_ID},
    \"event_type\": \"ORDER_CREATED\",
    \"merchant_id\": \"6758\",
    \"timestamp\": \"2026-02-19T20:00:00Z\",
    \"order_status\": \"pending\"
  }" | jq .

# 3. Verificar inbox
echo "Inbox:"
docker exec integrator-db psql -U postgres -d delivery -c \
  "SELECT event_id, status FROM webhook_inbox WHERE event_id = '${EVENT_ID}';"

# 4. Rodar worker (em background ou manual)
echo "Rodar worker manualmente: docker exec -it integrator-app python -m src.tasks.worker"
echo "Aguardar processamento..."

# 5. Verificar orders (após rodar worker)
echo "Verificar com:"
echo "docker exec integrator-db psql -U postgres -d delivery -c \"SELECT * FROM orders WHERE id = ${ORDER_ID};\""