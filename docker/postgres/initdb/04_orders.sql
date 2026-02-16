-- ============================================
-- TABELA: PEDIDOS (Dados limpos e enriquecidos)
-- ============================================
-- NOTA ARQUITETURAL: Cálculo de distância geoespacial (Haversine) e 
-- classificação de zona são realizados em Python (GeoService), não no BD.
-- Motivo: Testabilidade, evitar carga no PostgreSQL, permitir lógica 
-- complexa de fallback quando coordenadas são inválidas.

CREATE TABLE orders (
    id BIGINT PRIMARY KEY,
    uid VARCHAR(20) UNIQUE,
    display_id INT,
    
    merchant_id VARCHAR(50) NOT NULL REFERENCES merchants(merchant_id),
    operation_day_id INT NOT NULL,
    
    -- Origem do processamento
    -- DECISÃO ADR-003: Sem FK física para webhook_inbox(event_id)
    -- Permite retention policy (limpeza) do inbox sem orphan records
    -- A rastreabilidade é mantida pelo valor lógico, não constraint física
    source_event_id VARCHAR(30) NOT NULL,
    
    -- Timeline do pedido
    created_at TIMESTAMPTZ NOT NULL,
    dispatched_at TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ,
    cancelled_at TIMESTAMPTZ,
    
    -- Tipo e canal
    order_type VARCHAR(20) NOT NULL CHECK (order_type IN ('delivery', 'takeout', 'onsite', 'closed_table')),
    sales_channel VARCHAR(50),
    
    -- Cliente
    customer_name VARCHAR(255),
    customer_phone VARCHAR(50),
    delivery_address JSONB,  -- Contém lat/lng para cálculo Python
    
    -- Conteúdo
    items JSONB,
    payments JSONB,
    
    -- Financeiro
    total_value DECIMAL(10, 2),
    delivery_fee DECIMAL(10, 2),
    
    -- Entrega (enriquecido via API dashboard - Fase 2)
    delivery_man_name VARCHAR(100),
    delivery_man_phone VARCHAR(20),
    delivery_route VARCHAR(50),
    
    -- Status atual (denormalizado para performance)
    status VARCHAR(30),
    status_changed_at TIMESTAMPTZ,
    
    -- Features geoespaciais (POPULADAS VIA PYTHON - GeoService)
    -- distance_km: Calculado por Haversine em Python durante enriquecimento
    -- distance_zone: Classificado em Python baseado nos thresholds do merchant
    distance_km DECIMAL(6, 2),
    distance_zone VARCHAR(10) CHECK (distance_zone IN ('near', 'medium', 'far', NULL)),
    
    -- Cache das APIs para debug e reprocessamento
    api_public_response JSONB,
    api_dashboard_response JSONB,
    
    -- ML
    used_for_training BOOLEAN DEFAULT FALSE,
    ml_features_version INT DEFAULT 1,
    
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Trigger para updated_at
CREATE TRIGGER trigger_orders_updated_at
    BEFORE UPDATE ON orders
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ============================================
-- ÍNDICES
-- ============================================

-- Rastreabilidade: source_event_id deve ser único (um webhook = um pedido)
CREATE UNIQUE INDEX idx_unique_source_event 
ON orders (source_event_id);

-- Queries operacionais por expediente
CREATE INDEX idx_day_status 
ON orders (operation_day_id, status);

-- Queries temporais por merchant
CREATE INDEX idx_merchant_temporal 
ON orders (merchant_id, created_at);

-- Performance: Entregadores por expediente
CREATE INDEX idx_driver_day 
ON orders (delivery_man_name, operation_day_id) 
WHERE delivery_man_name IS NOT NULL;

-- Análise geoespacial
CREATE INDEX idx_distance_zone 
ON orders (distance_zone) 
WHERE distance_zone IS NOT NULL;

CREATE INDEX idx_distance_km 
ON orders (distance_km) 
WHERE distance_km IS NOT NULL;

-- Canais de venda
CREATE INDEX idx_sales_channel 
ON orders (sales_channel) 
WHERE sales_channel IS NOT NULL;

-- Lookup por UID (interface Cardapioweb)
CREATE INDEX idx_uid_lookup 
ON orders (uid);

-- ML: Pedidos não usados para treinamento
CREATE INDEX idx_training 
ON orders (used_for_training, ml_features_version) 
WHERE used_for_training = FALSE;

-- Performance operacional: Pedidos ativos (exclui finalizados)
CREATE INDEX idx_active_orders 
ON orders (merchant_id, status, created_at) 
WHERE status NOT IN ('delivered', 'cancelled', 'closed');

-- ============================================
-- COMENTÁRIOS DOCUMENTAIS
-- ============================================

COMMENT ON COLUMN orders.distance_km IS 'Distância calculada em Python via GeoService.haversine(). NULL se coordenadas inválidas.';
COMMENT ON COLUMN orders.distance_zone IS 'Classificação calculada em Python: near/medium/far baseado nos thresholds do merchant.';
COMMENT ON COLUMN orders.source_event_id IS 'Referência lógica ao webhook_inbox. Sem FK física para permitir retention policy.';
COMMENT ON COLUMN orders.api_public_response IS 'Cache raw da API pública Cardapioweb (Fase 1).';
COMMENT ON COLUMN orders.api_dashboard_response IS 'Cache raw da API dashboard Cardapioweb (Fase 2).';