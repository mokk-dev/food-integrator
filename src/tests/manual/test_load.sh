#!/bin/bash
# tests/manual/test_load.sh

TOKEN="webhook"
URL="http://localhost:8000/webhook/orders"

echo "Enviando 100 webhooks..."

for i in $(seq 1 100); do
  curl -s -X POST "$URL" \
    -H "X-Webhook-Token: $TOKEN" \
    -H "Content-Type: application/json" \
    -d "{
      \"event_id\": \"load_test_${i}_$(date +%s)\",
      \"order_id\": $((900000 + i)),
      \"event_type\": \"ORDER_CREATED\",
      \"merchant_id\": \"6758\",
      \"timestamp\": \"2026-02-19T20:00:00Z\"
    }" > /dev/null &
  
  # Limitar concorrência para não matar o servidor
  if (( i % 10 == 0 )); then
    wait
    echo "  Enviados $i..."
  fi
done

wait
echo "✅ 100 webhooks enviados"

# Verificar
docker exec integrator-db psql -U postgres -d delivery -c \
  "SELECT COUNT(*) FROM webhook_inbox WHERE event_id LIKE 'load_test_%';"