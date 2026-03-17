-- ============================================
-- TABELA: PEDIDOS (Dados limpos e enriquecidos)
-- ============================================
-- NOTA: Cálculo de distância geoespacial (Haversine) e 
-- classificação de zona são realizados em Python (GeoService), não no BD.
-- Motivo: Testabilidade, evitar carga no PostgreSQL, permitir lógica 
-- complexa de fallback quando coordenadas são inválidas.

CREATE TABLE IF NOT EXISTS orders (
    id BIGINT PRIMARY KEY,
    uid VARCHAR(100),
    display_id VARCHAR(50),
    merchant_id VARCHAR(100) NOT NULL REFERENCES merchants(merchant_id),
    operation_day_id INTEGER REFERENCES operation_days(id),
    source_event_id VARCHAR(100),
    
    -- Tempos (Event Sourcing interno)
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    status_changed_at TIMESTAMPTZ,
    confirmed_at TIMESTAMPTZ,
    ready_at TIMESTAMPTZ,
    released_at TIMESTAMPTZ,
    waiting_to_catch_at TIMESTAMPTZ,
    canceling_at TIMESTAMPTZ,
    cancelled_at TIMESTAMPTZ,
    closed_at TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ,

    order_type VARCHAR(50) NOT NULL,
    sales_channel VARCHAR(50),
    status VARCHAR(50) NOT NULL,
    cancellation_reason TEXT,
    
    customer_id BIGINT,
    customer_name VARCHAR(255),
    customer_phone VARCHAR(50),
    customer_orders_count INTEGER,
    
    delivery_address JSONB,
    delivery_neighborhood VARCHAR(255),
    delivery_city VARCHAR(255),
    distance_km NUMERIC(10, 2),
    distance_zone VARCHAR(50),
    
    total_value NUMERIC(10, 2) NOT NULL,
    delivery_fee NUMERIC(10, 2),
    
    delivery_man_id INTEGER,
    delivery_man_name VARCHAR(255),
    delivery_man_phone VARCHAR(50),
    delivery_route VARCHAR(255),
    
    api_dashboard_response JSONB,

    used_for_training BOOLEAN DEFAULT FALSE,
    ml_features_version VARCHAR(50)
);

CREATE TRIGGER trigger_orders_updated_at
    BEFORE UPDATE ON orders
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE UNIQUE INDEX idx_unique_source_event 
ON orders (source_event_id);

CREATE INDEX idx_day_status 
ON orders (operation_day_id, status);

CREATE INDEX idx_merchant_temporal 
ON orders (merchant_id, created_at);

CREATE INDEX idx_driver_day 
ON orders (delivery_man_name, operation_day_id) 
WHERE delivery_man_name IS NOT NULL;

CREATE INDEX idx_distance_zone 
ON orders (distance_zone) 
WHERE distance_zone IS NOT NULL;

CREATE INDEX idx_distance_km 
ON orders (distance_km) 
WHERE distance_km IS NOT NULL;

CREATE INDEX idx_sales_channel 
ON orders (sales_channel) 
WHERE sales_channel IS NOT NULL;

CREATE INDEX idx_uid_lookup 
ON orders (uid);

CREATE INDEX idx_training 
ON orders (used_for_training, ml_features_version) 
WHERE used_for_training = FALSE;

CREATE INDEX idx_active_orders 
ON orders (merchant_id, status, created_at) 
WHERE status NOT IN ('delivered', 'cancelled', 'closed');

COMMENT ON COLUMN orders.distance_km IS 'Distância calculada em Python via GeoService.haversine(). NULL se coordenadas inválidas.';
COMMENT ON COLUMN orders.distance_zone IS 'Classificação calculada em Python: near/medium/far baseado nos thresholds do merchant.';
COMMENT ON COLUMN orders.source_event_id IS 'Referência lógica ao webhook_inbox. Sem FK física para permitir retention policy.';
COMMENT ON COLUMN orders.api_dashboard_response IS 'Cache raw da API dashboard Cardapioweb (Fase 2).';
COMMENT ON COLUMN orders.created_at IS 'Timestamp de inserção no banco de dados (metadado interno).';
COMMENT ON COLUMN orders.updated_at IS 'Timestamp de última atualização no banco (atualizado via trigger).';