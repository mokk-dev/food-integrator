-- ============================================
-- TABELA: SNAPSHOTS OPERACIONAIS (ML Core)
-- ============================================

CREATE TABLE operation_snapshots (
    id BIGSERIAL,
    
    operation_day_id INT NOT NULL,
    merchant_id VARCHAR(50) NOT NULL REFERENCES merchants(merchant_id),
    
    snapshot_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Estado da cozinha (WIP)
    orders_in_queue INT DEFAULT 0,
    orders_ready_waiting INT DEFAULT 0,
    orders_in_delivery INT DEFAULT 0,
    orders_total_active INT DEFAULT 0,
    
    -- Capacidade de entrega
    delivery_men_active INT DEFAULT 0,
    delivery_men_busy INT DEFAULT 0,
    delivery_capacity_total INT DEFAULT 0,
    
    -- Métricas de velocidade (janela móvel)
    throughput_per_hour DECIMAL(5, 2),
    avg_preparation_time_last_5 INT,
    avg_delivery_time_last_5 INT,
    
    -- Predições do modelo (quando implementado)
    predicted_wait_minutes INT,
    model_version VARCHAR(10),
    prediction_confidence DECIMAL(3, 2),
    
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    PRIMARY KEY (id, snapshot_at)
);

-- Hypertable para séries temporais de snapshots
SELECT create_hypertable('operation_snapshots', 'snapshot_at', chunk_time_interval => INTERVAL '1 day');

CREATE INDEX idx_snapshots_day 
ON operation_snapshots (operation_day_id, snapshot_at);

CREATE INDEX idx_snapshots_merchant 
ON operation_snapshots (merchant_id, snapshot_at);

CREATE OR REPLACE FUNCTION calculate_recent_averages(
    p_operation_day_id INT,
    p_limit INT DEFAULT 5
)
RETURNS TABLE (
    avg_prep_time INT,
    avg_delivery_time INT
) AS $$
BEGIN
    RETURN QUERY
    WITH recent_orders AS (
        SELECT 
            o.id,
            o.confirmed_at,
            o.ready_at,
            o.released_at,
            o.delivered_at
        FROM orders o
        WHERE o.operation_day_id = p_operation_day_id
          AND o.status = 'delivered'
          AND o.delivered_at IS NOT NULL
          AND o.released_at IS NOT NULL
          AND o.ready_at IS NOT NULL
          AND o.confirmed_at IS NOT NULL
        ORDER BY o.delivered_at DESC
        LIMIT p_limit
    )
    SELECT 
        COALESCE(AVG(EXTRACT(EPOCH FROM (ready_at - confirmed_at)) / 60)::INT, 0),
        COALESCE(AVG(EXTRACT(EPOCH FROM (delivered_at - released_at)) / 60)::INT, 0)
    FROM recent_orders;
END;
$$ LANGUAGE plpgsql STABLE;