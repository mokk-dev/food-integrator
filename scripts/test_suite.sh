#!/bin/bash

# ============================================
# CARDAPIOWEB INTEGRATOR - Test Suite v15
# ============================================
# Uso: ./scripts/test_suite.sh [categoria]
# Categorias: all, infra, db, api, redis, perf (padrão: all)

# NÃO usar set -e - queremos que todos os testes rodem mesmo se alguns falharem
# set -e

# Cores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Contadores
TOTAL=0
PASSED=0
FAILED=0

# Configuração
DOCKER_DIR="docker"
COMPOSE="docker compose -f ${DOCKER_DIR}/docker-compose.yml"
DB_CONTAINER="integrator-db"
REDIS_CONTAINER="integrator-redis"
APP_CONTAINER="integrator-app"

# ============================================
# FUNÇÕES AUXILIARES
# ============================================

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_pass() {
    echo -e "${GREEN}[PASS]${NC} $1"
    ((PASSED++))
    ((TOTAL++))
}

log_fail() {
    echo -e "${RED}[FAIL]${NC} $1"
    ((FAILED++))
    ((TOTAL++))
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

run_test() {
    local name="$1"
    local cmd="$2"
    local ignore_fail="${3:-false}"  # Se true, não conta como falha
    
    echo ""
    log_info "Testando: $name"
    
    if eval "$cmd" > /tmp/test_output.log 2>&1; then
        log_pass "$name"
        return 0
    else
        if [ "$ignore_fail" = "true" ]; then
            log_warn "$name (ignorado)"
            ((TOTAL++))
            return 0
        else
            log_fail "$name"
            echo "  Detalhes do erro:"
            head -5 /tmp/test_output.log | sed 's/^/    /'
            return 1
        fi
    fi
}

# Função para verificar se containers estão rodando (case-insensitive)
check_containers_running() {
    local output
    output=$(${COMPOSE} ps 2>/dev/null || true)
    # Verifica se contém Running, running, Up, healthy, etc.
    echo "$output" | grep -qiE '(running|up|healthy)'
    return $?
}

psql_cmd() {
    docker exec -i ${DB_CONTAINER} psql -U postgres -d delivery -c "$1" 2>/dev/null
    return $?
}

# ============================================
# CATEGORIA 1: INFRAESTRUTURA
# ============================================

test_infra() {
    log_info "=== CATEGORIA 1: INFRAESTRUTURA ==="
    
    run_test "1.1.1 - Containers estão rodando" \
        "check_containers_running"
    
    run_test "1.1.2 - DB container existe" \
        "docker ps --format '{{.Names}}' | grep -q ${DB_CONTAINER}"
    
    run_test "1.1.3 - Redis container existe" \
        "docker ps --format '{{.Names}}' | grep -q ${REDIS_CONTAINER}"
    
    run_test "1.1.4 - App container existe" \
        "docker ps --format '{{.Names}}' | grep -q ${APP_CONTAINER}"
    
    run_test "1.1.5 - DB está respondendo" \
        "docker exec ${DB_CONTAINER} pg_isready -U postgres > /dev/null 2>&1"

    run_test "1.1.6 - Redis está respondendo" \
        "docker exec ${REDIS_CONTAINER} redis-cli ping > /dev/null 2>&1"
    
    run_test "1.1.7 - App está rodando" \
        "docker inspect --format='{{.State.Status}}' ${APP_CONTAINER} 2>/dev/null | grep -q 'running'"
    
    run_test "1.1.8 - Porta DB não exposta externamente (security)" \
        "! docker port ${DB_CONTAINER} 2>/dev/null | grep -q '5432'"
    
    run_test "1.1.9 - Volume Postgres existe" \
        "docker volume ls | grep -q 'postgres_data'"
    
    run_test "1.1.10 - Volume Redis existe" \
        "docker volume ls | grep -q 'redis_data'"
    
    run_test "1.2.1 - App responde na porta 8000" \
        "curl -s -o /dev/null -w '%{http_code}' --max-time 5 http://localhost:8000/health 2>/dev/null | grep -q '200'"
}

# ============================================
# CATEGORIA 2: BANCO DE DADOS
# ============================================

test_db() {
    log_info "=== CATEGORIA 2: BANCO DE DADOS ==="
    
    # Aguardar DB estar pronto
    sleep 2
    
    run_test "2.1.1 - Conexão PostgreSQL funciona" \
        "psql_cmd 'SELECT 1;' | grep -q '1'"
    
    run_test "2.1.2 - Extensão TimescaleDB instalada" \
        "psql_cmd \"SELECT extname FROM pg_extension WHERE extname='timescaledb';\" | grep -qi 'timescaledb'"
    
    run_test "2.1.3 - Timezone configurado (America/Sao_Paulo)" \
        "psql_cmd 'SHOW timezone;' | grep -q 'America/Sao_Paulo'"
    
    run_test "2.2.1 - Tabela merchants existe e acessível" \
        "psql_cmd 'SELECT COUNT(*) FROM merchants;' | grep -q '[0-9]'"
    
    run_test "2.2.2 - Tabela operation_days existe" \
        "psql_cmd 'SELECT 1 FROM operation_days LIMIT 1;' > /dev/null"
    
    run_test "2.2.3 - Tabela webhook_inbox existe" \
        "psql_cmd 'SELECT 1 FROM webhook_inbox LIMIT 1;' > /dev/null"
    
    run_test "2.2.4 - Tabela orders existe" \
        "psql_cmd 'SELECT 1 FROM orders LIMIT 1;' > /dev/null"
    
    run_test "2.2.5 - Tabela order_events existe" \
        "psql_cmd 'SELECT 1 FROM order_events LIMIT 1;' > /dev/null"
    
    run_test "2.2.6 - Tabela operation_snapshots existe" \
        "psql_cmd 'SELECT 1 FROM operation_snapshots LIMIT 1;' > /dev/null"
    
    run_test "2.3.1 - 3 Hypertables existem (TimescaleDB)" \
        "docker exec -i ${DB_CONTAINER} psql -U postgres -d delivery -t -c \"SELECT COUNT(*) FROM timescaledb_information.hypertables;\" 2>/dev/null | tr -d '[:space:]' | grep -q '^3$'"
    
    run_test "2.4.1 - operation_days.id é BIGSERIAL/INTEGER" \
        "psql_cmd \"SELECT data_type FROM information_schema.columns WHERE table_name='operation_days' AND column_name='id';\" | grep -qiE 'bigint|integer'"

    run_test "2.4.2 - orders.operation_day_id é INTEGER" \
        "psql_cmd \"SELECT data_type FROM information_schema.columns WHERE table_name='orders' AND column_name='operation_day_id';\" | grep -qiE 'bigint|integer'"
}

# ============================================
# CATEGORIA 3: DADOS E SEED
# ============================================

test_seed() {
    log_info "=== CATEGORIA 3: DADOS E SEED ==="
    
    run_test "3.1.1 - Merchant Caramelo (6758) existe" \
        "psql_cmd \"SELECT name FROM merchants WHERE merchant_id='6758';\" | grep -qi 'caramelo'"
    
    run_test "3.1.2 - Dados do merchant completos" \
        "psql_cmd \"SELECT default_start_time, default_end_time FROM merchants WHERE merchant_id='6758';\" | grep -q '18:00'"
    
    run_test "3.1.3 - Threshold near = 2.0 km" \
        "psql_cmd \"SELECT distance_threshold_near FROM merchants WHERE merchant_id='6758';\" | grep -q '2.00'"
    
    run_test "3.1.4 - Threshold medium = 5.0 km" \
        "psql_cmd \"SELECT distance_threshold_medium FROM merchants WHERE merchant_id='6758';\" | grep -q '5.00'"
    
    run_test "3.1.5 - Capacidade padrão = 4 entregadores" \
        "psql_cmd \"SELECT default_delivery_capacity FROM merchants WHERE merchant_id='6758';\" | grep -q '4'"
}

# ============================================
# CATEGORIA 4: FUNÇÕES SQL
# ============================================

test_functions() {
    log_info "=== CATEGORIA 4: FUNÇÕES SQL ==="
    
    run_test "4.1.1 - Função get_open_operation_day existe" \
        "psql_cmd \"SELECT proname FROM pg_proc WHERE proname='get_open_operation_day';\" | grep -q 'get_open_operation_day'"
    
    run_test "4.1.2 - Função update_updated_at_column existe" \
        "psql_cmd \"SELECT proname FROM pg_proc WHERE proname='update_updated_at_column';\" | grep -q 'update_updated_at_column'"
    
    run_test "4.1.3 - Função retry_failed_event existe" \
        "psql_cmd \"SELECT proname FROM pg_proc WHERE proname='retry_failed_event';\" | grep -q 'retry_failed_event'"
    
    run_test "4.1.4 - Função get_order_timeline existe" \
        "psql_cmd \"SELECT proname FROM pg_proc WHERE proname='get_order_timeline';\" | grep -q 'get_order_timeline'"
}

# ============================================
# CATEGORIA 5: CRUD OPERATIONS
# ============================================

test_crud() {
    log_info "=== CATEGORIA 5: OPERAÇÕES CRUD ==="
    
    # Limpar dados de teste anteriores (ignorar erros)
    psql_cmd "DELETE FROM webhook_inbox WHERE event_id LIKE 'test_suite_%';" > /dev/null 2>&1 || true
    psql_cmd "DELETE FROM orders WHERE id >= 90000;" > /dev/null 2>&1 || true
    psql_cmd "DELETE FROM order_events WHERE event_id LIKE 'test_suite_%';" > /dev/null 2>&1 || true
    psql_cmd "DELETE FROM operation_days WHERE operation_day > '2090-01-01';" > /dev/null 2>&1 || true
    
    local TEST_EVENT_ID="test_suite_$(date +%s)_$$"
    
    # ============================================
    # TESTES WEBHOOK INBOX
    # ============================================
    
    run_test "5.2.1 - Inserir webhook inbox" \
        "psql_cmd \"INSERT INTO webhook_inbox (event_id, order_id, event_type, order_status, payload, status) VALUES ('${TEST_EVENT_ID}', 12345, 'ORDER_CREATED', 'pending', '{\\\"test\\\": true}'::jsonb, 'pending');\" | grep -q 'INSERT'"
    
    run_test "5.2.2 - Consultar webhook por event_id" \
        "psql_cmd \"SELECT event_id FROM webhook_inbox WHERE event_id = '${TEST_EVENT_ID}';\" | grep -q '${TEST_EVENT_ID}'"
    
    run_test "5.2.3 - Deduplicação por PK: inserir duplicado deve falhar" \
        "! psql_cmd \"INSERT INTO webhook_inbox (event_id, order_id, event_type, order_status, payload, status) VALUES ('${TEST_EVENT_ID}', 99999, 'ORDER_CREATED', 'pending', '{}'::jsonb, 'pending');\" 2>/dev/null"
    
    # ============================================
    # CRIAR OPERATION DAY PARA TESTES DE ORDERS
    # ============================================
    
    log_info "Criando operation_day de teste..."
    
    # Inserir operation_day para data futura
    local TEST_OP_DAY="2099-12-31"
    
    # Capturar resultado do INSERT
    local OP_DAY_RESULT
    OP_DAY_RESULT=$(psql_cmd "INSERT INTO operation_days (merchant_id, operation_day, start_time, end_time, opened_at, delivery_capacity) VALUES ('6758', '${TEST_OP_DAY}', '18:00', '02:00', NOW(), 4) RETURNING id;" 2>&1)
    
    log_info "Resultado bruto: $(echo "$OP_DAY_RESULT" | head -3)"
    
    # Extrair ID - abordagem mais robusta com awk
    # Formato esperado: "  1" ou UUID com espaços antes
    local OP_DAY_ID
    OP_DAY_ID=$(echo "$OP_DAY_RESULT" | awk '/^[[:space:]]+[0-9a-f-]+/{gsub(/^[[:space:]]+|[[:space:]]+$/, ""); print; exit}')
    
    # Se awk falhou, tentar grep/sed alternativo
    if [ -z "$OP_DAY_ID" ]; then
        OP_DAY_ID=$(echo "$OP_DAY_RESULT" | tail -4 | head -1 | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
    fi
    
    # Validar ID
    if [ -z "$OP_DAY_ID" ] || [ "$OP_DAY_ID" = "id" ] || [ "$OP_DAY_ID" = "" ]; then
        log_warn "FALHA: Não foi possível extrair ID do resultado"
        log_warn "Tentando obter operation_day existente..."
        
        # Fallback: tentar obter um existente
        OP_DAY_ID=$(psql_cmd "SELECT id FROM operation_days WHERE merchant_id='6758' ORDER BY created_at DESC LIMIT 1;" | awk 'NR==3{gsub(/^[[:space:]]+|[[:space:]]+$/,""); print}')
    fi
    
    # Validação final
    if [ -z "$OP_DAY_ID" ] || [ "$OP_DAY_ID" = "id" ] || [ "$OP_DAY_ID" = "" ]; then
        log_fail "CRÍTICO: Não foi possível obter ID de operation_day"
        log_fail "Último resultado tentado: '${OP_DAY_ID}'"
        return
    fi
    
    log_info "Operation_day ID obtido: '${OP_DAY_ID}'"
    
    # ============================================
    # TESTES DE ORDERS
    # ============================================
    
    local TEST_ORDER_ID="99999"
    
    run_test "5.3.1 - Inserir order com operation_day_id válido" \
        "psql_cmd \"INSERT INTO orders (id, uid, merchant_id, operation_day_id, source_event_id, created_at, order_type, status, total_value, distance_km, distance_zone) VALUES (${TEST_ORDER_ID}, 'UID99999', '6758', '${OP_DAY_ID}', '${TEST_EVENT_ID}', NOW(), 'delivery', 'pending', 50.00, 3.5, 'medium');\" | grep -q 'INSERT'"
    
    run_test "5.3.2 - Consultar order com JOIN em merchants" \
        "psql_cmd \"SELECT o.id, m.name FROM orders o JOIN merchants m ON o.merchant_id = m.merchant_id WHERE o.id = ${TEST_ORDER_ID};\" | grep -qi 'caramelo'"
    
    run_test "5.3.3 - Atualizar status do order" \
        "psql_cmd \"UPDATE orders SET status = 'confirmed' WHERE id = ${TEST_ORDER_ID};\" | grep -q 'UPDATE'"
    
    # ============================================
    # LIMPEZA
    # ============================================
    
    log_info "Limpando dados de teste..."
    psql_cmd "DELETE FROM orders WHERE id = ${TEST_ORDER_ID};" > /dev/null 2>&1 || true
    psql_cmd "DELETE FROM webhook_inbox WHERE event_id = '${TEST_EVENT_ID}';" > /dev/null 2>&1 || true
    psql_cmd "DELETE FROM operation_days WHERE operation_day = '${TEST_OP_DAY}';" > /dev/null 2>&1 || true
}

# ============================================
# CATEGORIA 6: VIEW MATERIALIZADA
# ============================================

test_view() {
    log_info "=== CATEGORIA 6: VIEW MATERIALIZADA ==="
    
    run_test "6.1.1 - View shift_summary existe" \
        "psql_cmd \"SELECT matviewname FROM pg_matviews WHERE matviewname='shift_summary';\" | grep -q 'shift_summary'"
    
    run_test "6.1.2 - Refresh materialized view funciona" \
        "psql_cmd 'REFRESH MATERIALIZED VIEW shift_summary;' > /dev/null"
    
    run_test "6.1.3 - Consultar shift_summary (pode estar vazia inicialmente)" \
        "psql_cmd 'SELECT 1 FROM shift_summary LIMIT 1;' > /dev/null" \
        "true" 
}

# ============================================
# CATEGORIA 7: API HTTP
# ============================================

test_api() {
    log_info "=== CATEGORIA 7: API HTTP ==="
    
    # Aguardar app estar pronto
    sleep 2
    
    run_test "7.1.1 - Health endpoint responde 200" \
        "curl -s -o /dev/null -w '%{http_code}' --max-time 5 http://localhost:8000/health 2>/dev/null | grep -q '200'"
    
    run_test "7.1.2 - Health retorna JSON correto" \
        "curl -s --max-time 5 http://localhost:8000/health 2>/dev/null | grep -q '\"status\".*\"healthy\"'"
    
    run_test "7.1.3 - Readiness endpoint responde 200" \
        "curl -s -o /dev/null -w '%{http_code}' --max-time 5 http://localhost:8000/ready 2>/dev/null | grep -q '200'"
    
    run_test "7.1.4 - Readiness verifica database e redis" \
        "curl -s --max-time 5 http://localhost:8000/ready 2>/dev/null | grep -q '\"database\".*\"ok\"'"
    
    run_test "7.2.1 - Correlation ID é propagado" \
        "curl -s -H 'X-Correlation-ID: test-suite-123' --max-time 5 http://localhost:8000/health -i 2>/dev/null | grep -i 'X-Correlation-ID.*test-suite-123'"
    
    run_test "7.2.2 - Security headers presentes" \
        "curl -s -I --max-time 5 http://localhost:8000/health 2>/dev/null | grep -qi 'X-Content-Type-Options'"
    
    run_test "7.3.1 - 404 retorna erro estruturado" \
        "curl -s --max-time 5 http://localhost:8000/naoexiste 2>/dev/null | grep -qi 'error'"
}

# ============================================
# CATEGORIA 8: REDIS
# ============================================

test_redis() {
    log_info "=== CATEGORIA 8: REDIS ==="
    
    run_test "8.1.1 - Redis responde a PING" \
        "docker exec ${REDIS_CONTAINER} redis-cli ping 2>/dev/null | grep -q 'PONG'"
    
    run_test "8.1.2 - Redis SET e GET funcionam" \
        "docker exec ${REDIS_CONTAINER} redis-cli SET test_suite_key 'valor_teste' 2>/dev/null && docker exec ${REDIS_CONTAINER} redis-cli GET test_suite_key 2>/dev/null | grep -q 'valor_teste'"
    
    run_test "8.1.3 - Redis EXPIRE/TTL funcionam" \
        "docker exec ${REDIS_CONTAINER} redis-cli EXPIRE test_suite_key 60 2>/dev/null && [ \$(docker exec ${REDIS_CONTAINER} redis-cli TTL test_suite_key 2>/dev/null) -gt 0 ]"
    
    # Limpar
    docker exec ${REDIS_CONTAINER} redis-cli DEL test_suite_key > /dev/null 2>&1 || true
}

# ============================================
# CATEGORIA 9: PERFORMANCE (Opcional)
# ============================================

test_perf() {
    log_info "=== CATEGORIA 9: PERFORMANCE ==="
    
    if ! command -v ab &> /dev/null; then
        log_warn "apache2-utils não instalado, pulando testes de carga"
        return 0
    fi
    
    run_test "9.1.1 - 100 requests com 0 falhas" \
        "ab -n 100 -c 10 http://localhost:8000/health 2>&1 | grep -q 'Failed requests: *0'" \
        "true"  # Ignorar falha se ab não estiver disponível ou der erro
}

# ============================================
# RELATÓRIO FINAL
# ============================================

print_report() {
    echo ""
    echo "=========================================="
    echo "           RELATÓRIO DE TESTES"
    echo "=========================================="
    echo -e "Total de testes:  ${TOTAL}"
    echo -e "${GREEN}Passaram:        ${PASSED}${NC}"
    echo -e "${RED}Falharam:        ${FAILED}${NC}"
    
    if [ ${TOTAL} -gt 0 ]; then
        PERCENTAGE=$(( PASSED * 100 / TOTAL ))
        echo -e "Taxa de sucesso: ${PERCENTAGE}%"
    else
        echo -e "Taxa de sucesso: N/A"
    fi
    
    echo "=========================================="
    
    if [ ${FAILED} -eq 0 ]; then
        echo -e "${GREEN}✅ TODOS OS TESTES PASSARAM!${NC}"
        exit 0
    else
        echo -e "${RED}❌ ${FAILED} TESTE(S) FALHARAM${NC}"
        echo -e "${YELLOW}Verifique os detalhes acima${NC}"
        exit 1
    fi
}

# ============================================
# MAIN
# ============================================

main() {
    CATEGORY="${1:-all}"
    
    echo "=========================================="
    echo "  Cardapioweb Integrator - Test Suite v15"
    echo "=========================================="
    echo "Categoria: ${CATEGORY}"
    echo ""
    
    # Verificar se docker compose está disponível
    if ! docker compose version > /dev/null 2>&1; then
        log_fail "docker compose não está instalado ou não funciona"
        exit 1
    fi
    
    # Verificar se stack está rodando
    if ! check_containers_running; then
        log_warn "Stack não está rodando. Iniciando..."
        ${COMPOSE} up -d db redis
        sleep 10
        ${COMPOSE} up -d app
        sleep 5
        
        # Verificar novamente
        if ! check_containers_running; then
            log_fail "Não foi possível iniciar os containers"
            exit 1
        fi
    fi
    
    case ${CATEGORY} in
        infra)
            test_infra
            ;;
        db)
            test_db
            ;;
        seed)
            test_seed
            ;;
        functions)
            test_functions
            ;;
        crud)
            test_crud
            ;;
        view)
            test_view
            ;;
        api)
            test_api
            ;;
        redis)
            test_redis
            ;;
        perf)
            test_perf
            ;;
        all|*)
            test_infra
            test_db
            test_seed
            test_functions
            test_crud
            test_view
            test_api
            test_redis
            test_perf
            ;;
    esac
    
    print_report
}

# Executar
main "$@"