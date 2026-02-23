#!/bin/bash
# src/tests/manual/test_security.sh

URL="http://localhost:8000/webhook/orders"
TOKEN="webhook" # Substitua pelo WEBHOOK_SECRET_TOKEN do seu .env se não estiver usando o padrão

echo "=========================================="
echo "🛡️ TESTES DE SEGURANÇA E PROTEÇÃO"
echo "=========================================="

echo -e "\n=== 1. TESTE DE PAYLOAD GIGANTE (>500KB) ==="
# Gera um arquivo JSON temporário com mais de 500KB de lixo
PAYLOAD_FILE="/tmp/huge_payload.json"
echo -n '{"event_id": "huge_1", "order_id": 1, "event_type": "ORDER_CREATED", "merchant_id": "6758", "timestamp": "2026-02-23T20:00:00Z", "lixo": "' > $PAYLOAD_FILE
head -c 600000 < /dev/zero | tr '\0' 'A' >> $PAYLOAD_FILE
echo '"}' >> $PAYLOAD_FILE

HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$URL" \
  -H "X-Webhook-Token: $TOKEN" \
  -H "Content-Type: application/json" \
  -d @"$PAYLOAD_FILE")

if [ "$HTTP_STATUS" -eq 413 ]; then
    echo "✅ SUCESSO: Payload rejeitado corretamente com HTTP 413 (Payload Too Large)."
else
    echo "❌ FALHA: A API retornou HTTP $HTTP_STATUS em vez de 413."
fi
rm $PAYLOAD_FILE


echo -e "\n=== 2. TESTE DE RATE LIMIT (>100 req/10s) ==="
echo "Disparando 105 webhooks simultâneos para simular ataque/falha da Cardapioweb..."

RATE_LIMIT_HIT=false
for i in {1..105}; do
  # Executa em background para ser muito rápido
  HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$URL" \
    -H "X-Webhook-Token: $TOKEN" \
    -H "Content-Type: application/json" \
    -d "{\"event_id\": \"rl_test_${i}\", \"order_id\": ${i}, \"event_type\": \"ORDER_CREATED\", \"merchant_id\": \"6758\", \"timestamp\": \"2026-02-23T20:00:00Z\"}")
  
  if [ "$HTTP_STATUS" -eq 429 ]; then
      RATE_LIMIT_HIT=true
      echo "✅ SUCESSO: Rate Limit ativado no request $i! Retornou HTTP 429 (Too Many Requests)."
      break
  fi
done

if [ "$RATE_LIMIT_HIT" = false ]; then
    echo "❌ FALHA: O Rate Limit não bloqueou os requests excedentes."
fi

echo -e "\nConcluído!"