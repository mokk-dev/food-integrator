-- ============================================
-- TABELA: EVENTOS DE STATUS (Histórico)
-- ============================================

CREATE TABLE order_events (
    event_id VARCHAR(30) NOT NULL,
    
    order_id BIGINT NOT NULL REFERENCES orders(id),
    operation_day_id INT NOT NULL,
    
    event_type VARCHAR(30) NOT NULL,
    status VARCHAR(30),
    
    event_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    
    -- Metadados de rastreabilidade
    inbox_received_at TIMESTAMPTZ NOT NULL,
    
    created_at TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (event_id, event_at)
);

-- Converter em hypertable para séries temporais eficientes
SELECT create_hypertable('order_events', 'event_at', chunk_time_interval => INTERVAL '7 days');

-- Índices
CREATE INDEX idx_events_sequence 
ON order_events (order_id, event_at);

CREATE INDEX idx_events_day_timeline 
ON order_events (operation_day_id, event_at);

-- Função para obter timeline completa de um pedido
CREATE OR REPLACE FUNCTION get_order_timeline(p_order_id BIGINT)
RETURNS TABLE (
    event_at TIMESTAMPTZ,
    event_type VARCHAR(30),
    status VARCHAR(30),
    lag_from_previous_minutes INT
) AS $$
BEGIN
    RETURN QUERY
    WITH timeline AS (
        SELECT 
            oe.event_at,
            oe.event_type,
            oe.status,
            LAG(oe.event_at) OVER (ORDER BY oe.event_at) as prev_event_at
        FROM order_events oe
        WHERE oe.order_id = p_order_id
        ORDER BY oe.event_at
    )
    SELECT 
        t.event_at,
        t.event_type,
        t.status,
        COALESCE((EXTRACT(EPOCH FROM (t.event_at - t.prev_event_at)) / 60), 0)::INT
    FROM timeline t
    ORDER BY t.event_at;
END;
$$ LANGUAGE plpgsql STABLE;