#!/bin/bash
# tests/manual/run_all_etapa4.sh

set -e

echo "=========================================="
echo "   TESTES ETAPA 4 - ENRIQUECIMENTO"
echo "=========================================="

# Cores
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass() { echo -e "${GREEN}✅ PASS${NC}: $1"; }
fail() { echo -e "${RED}❌ FAIL${NC}: $1"; exit 1; }
warn() { echo -e "${YELLOW}⚠️  WARN${NC}: $1"; }

# ============================================
# 1. TESTES DE SERVIÇO (Geo)
# ============================================
echo ""
echo "=== 1. GeoService Tests ==="

docker exec integrator-app python -c "
from src.core.services.geo_service import GeoService
g = GeoService()

# Testes básicos
assert g.haversine(-23.42, -51.91, -23.42, -51.91) == 0.0, 'Distância zero'
assert g.haversine(999, 999, -23.42, -51.91) is None, 'Coordenadas inválidas'
assert g.classify_distance_zone(1.5, 2.0, 5.0) == 'near', 'Zona near'
assert g.classify_distance_zone(6.0, 2.0, 5.0) == 'far', 'Zona far'

print('GeoService: OK')
" && pass "GeoService" || fail "GeoService"

# ============================================
# 2. TESTES DE CLIENTE (API)
# ============================================
echo ""
echo "=== 2. API Client Tests ==="

docker exec integrator-app python -c "
from src.infrastructure.external.base_client import BaseAPIClient, api_method

# Testar inicialização
client = BaseAPIClient('https://httpbin.org', api_key='test123')
assert client.base_url == 'https://httpbin.org'
assert client.api_key == 'test123'
assert 'X-API-Key' in client.client.headers
print('BaseAPIClient: OK')
" && pass "BaseAPIClient init" || warn "BaseAPIClient (pode precisar httpbin)"

# ============================================
# 3. TESTES DE INTEGRAÇÃO (Banco)
# ============================================
echo ""
echo "=== 3. Database Integration Tests ==="

# Verificar se operation_day existe
COUNT=$(docker exec integrator-db psql -U postgres -d delivery -t -c \
  "SELECT COUNT(*) FROM operation_days WHERE merchant_id='6758';" | tr -d ' ')

if [ "$COUNT" -gt "0" ]; then
  pass "Operation day existe ($COUNT encontrados)"
else
  warn "Nenhum operation_day - testes de orders serão afetados"
fi

# ============================================
# 4. TESTE DE FLUXO COMPLETO
# ============================================
echo ""
echo "=== 4. End-to-End Flow Test ==="

ORDER_ID="999999"
EVENT_ID="e2e_test_$(date +%s)"

# Limpar
docker exec integrator-db psql -U postgres -d delivery -c \
  "DELETE FROM orders WHERE id=$ORDER_ID; DELETE FROM webhook_inbox WHERE event_id='$EVENT_ID';" > /dev/null 2>&1

# Enviar webhook
RESPONSE=$(curl -s -X POST http://localhost:8000/webhook/orders \
  -H "X-Webhook-Token: webhook" \
  -H "Content-Type: application/json" \
  -d "{
    \"event_id\": \"$EVENT_ID\",
    \"order_id\": $ORDER_ID,
    \"event_type\": \"ORDER_CREATED\",
    \"merchant_id\": \"6758\",
    \"timestamp\": \"2026-02-19T20:00:00Z\",
    \"order_status\": \"pending\"
  }")

if echo "$RESPONSE" | grep -q "accepted"; then
  pass "Webhook aceito"
else
  fail "Webhook rejeitado: $RESPONSE"
fi

# Verificar inbox
INBOX=$(docker exec integrator-db psql -U postgres -d delivery -t -c \
  "SELECT status FROM webhook_inbox WHERE event_id='$EVENT_ID';" | tr -d ' ')

if [ "$INBOX" = "pending" ]; then
  pass "Evento em inbox (pending)"
else
  fail "Evento não encontrado em inbox"
fi

echo ""
echo "=========================================="
echo "   PRÓXIMOS PASSOS MANUAIS:"
echo "=========================================="
echo "1. Rodar worker: docker exec -it integrator-app python -m src.tasks.worker"
echo "2. Verificar orders: docker exec integrator-db psql -U postgres -d delivery -c \"SELECT * FROM orders WHERE id=$ORDER_ID;\""
echo "3. Verificar logs: docker compose logs app -f"
echo ""

pass "Suite de testes concluída!"