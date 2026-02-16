-- ============================================
-- TABELA: DIAS DE OPERAÇÃO
-- ============================================

CREATE TABLE operation_days (
    id SERIAL,
    merchant_id VARCHAR(50) NOT NULL REFERENCES merchants(merchant_id),
    
    operation_day DATE NOT NULL,
    
    start_time TIME NOT NULL,
    end_time TIME NOT NULL,
    
    opened_at TIMESTAMPTZ NOT NULL,
    closed_at TIMESTAMPTZ,
    
    delivery_capacity INT NOT NULL,
    
    was_edited BOOLEAN DEFAULT FALSE,
    
    -- Métricas consolidadas (atualizadas ao fechar)
    total_orders INT,
    canceled_orders INT DEFAULT 0,
    total_revenue DECIMAL(12, 2),
    avg_preparation_minutes INT,
    avg_delivery_minutes INT,
    
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    PRIMARY KEY (id, operation_day)
);

-- Converter em hypertable para otimizar queries temporais
SELECT create_hypertable('operation_days', 'operation_day', chunk_time_interval => INTERVAL '365 days');

-- Índices
CREATE UNIQUE INDEX idx_unique_expediente 
ON operation_days (merchant_id, operation_day, opened_at);

CREATE INDEX idx_open_detection 
ON operation_days (merchant_id, closed_at);

-- Função para detectar expediente aberto
CREATE OR REPLACE FUNCTION get_open_operation_day(p_merchant_id VARCHAR)
RETURNS TABLE (
    operation_day_id INT,
    operation_day_date DATE,
    opened_at TIMESTAMPTZ,
    delivery_capacity INT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        od.id,
        od.operation_day,
        od.opened_at,
        od.delivery_capacity
    FROM operation_days od
    WHERE od.merchant_id = p_merchant_id
      AND od.closed_at IS NULL
    ORDER BY od.opened_at DESC
    LIMIT 1;
END;
$$ LANGUAGE plpgsql STABLE;