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

-- Índices para queries de ML
CREATE INDEX idx_snapshots_day 
ON operation_snapshots (operation_day_id, snapshot_at);

CREATE INDEX idx_snapshots_merchant 
ON operation_snapshots (merchant_id, snapshot_at);

-- Função para calcular médias móveis de tempo (CORRIGIDA)
-- VERSÃO OTIMIZADA (substituir a função existente):
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
            o.dispatched_at,
            o.delivered_at,
            oe_confirmed.event_at as confirmed_at,
            oe_ready.event_at as ready_at
        FROM orders o
        LEFT JOIN LATERAL (
            SELECT event_at FROM order_events 
            WHERE order_id = o.id AND status = 'confirmed' 
            ORDER BY event_at LIMIT 1
        ) oe_confirmed ON true
        LEFT JOIN LATERAL (
            SELECT event_at FROM order_events 
            WHERE order_id = o.id AND status = 'ready' 
            ORDER BY event_at LIMIT 1
        ) oe_ready ON true
        WHERE o.operation_day_id = p_operation_day_id
          AND o.status = 'delivered'
          AND o.delivered_at IS NOT NULL
        ORDER BY o.delivered_at DESC
        LIMIT p_limit
    )
    SELECT 
        COALESCE(AVG(EXTRACT(EPOCH FROM (ready_at - confirmed_at)) / 60)::INT, 0),
        COALESCE(AVG(EXTRACT(EPOCH FROM (delivered_at - dispatched_at)) / 60)::INT, 0)
    FROM recent_orders;
END;
$$ LANGUAGE plpgsql STABLE;
