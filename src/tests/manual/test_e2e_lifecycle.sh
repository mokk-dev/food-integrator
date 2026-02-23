#!/bin/bash
# src/tests/manual/test_e2e_lifecycle.sh

URL="http://localhost:8000/webhook/orders"
TOKEN="webhook"
MERCHANT_ID="9999" # Simulando uma loja completamente nova
ORDER_ID="555777"

echo "=========================================="
echo "🔄 TESTE DE FLUXO E2E (CRIAÇÃO -> CANCELAMENTO)"
echo "=========================================="

echo -e "\n=== 1. Injetando Nova Loja no Banco (Setup Multi-Tenant) ==="
docker exec integrator-db psql -U postgres -d delivery -c \
  "INSERT INTO merchants (merchant_id, name, default_start_time, default_end_time, address_lat, address_lng) 
   VALUES ('$MERCHANT_ID', 'Loja Teste E2E', '10:00', '23:00', -23.4, -51.9) 
   ON CONFLICT DO NOTHING;" > /dev/null

echo "Loja $MERCHANT_ID pronta!"

echo -e "\n=== 2. Simulando Webhook: ORDER_CREATED ==="
curl -s -X POST "$URL" \
  -H "X-Webhook-Token: $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{
    \"event_id\": \"evt_create_${ORDER_ID}\",
    \"order_id\": ${ORDER_ID},
    \"event_type\": \"ORDER_CREATED\",
    \"merchant_id\": \"${MERCHANT_ID}\",
    \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",
    \"order_status\": \"pending\"
  }" | grep -q "accepted" && echo "Webhook recebido (202 Accepted)"

echo -e "\n=== 3. Aguardando o Worker processar (10 segundos) ==="
# Damos tempo para o worker.py ler do inbox, criar o operation_day e inserir o pedido
sleep 10

echo -e "\n=== 4. Verificando Banco: Expediente e Pedido ==="
docker exec integrator-db psql -U postgres -d delivery -c \
  "SELECT o.id, o.merchant_id, o.status, od.opened_at as operation_day_criado 
   FROM orders o 
   JOIN operation_days od ON o.operation_day_id = od.id 
   WHERE o.id = $ORDER_ID;"

echo -e "\n=== 5. Simulando Webhook: ORDER_STATUS_UPDATED (Cancelamento) ==="
curl -s -X POST "$URL" \
  -H "X-Webhook-Token: $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{
    \"event_id\": \"evt_cancel_${ORDER_ID}\",
    \"order_id\": ${ORDER_ID},
    \"event_type\": \"ORDER_STATUS_UPDATED\",
    \"merchant_id\": \"${MERCHANT_ID}\",
    \"new_status\": \"cancelled\",
    \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"
  }" | grep -q "accepted" && echo "Webhook de atualização recebido (202 Accepted)"

echo -e "\n=== 6. Aguardando o Worker processar a atualização (10 segundos) ==="
sleep 10

echo -e "\n=== 7. Verificando Banco: Status Cancelado ==="
docker exec integrator-db psql -U postgres -d delivery -c \
  "SELECT id, status, status_changed_at, cancelled_at 
   FROM orders 
   WHERE id = $ORDER_ID;"

echo -e "\nConcluído!"