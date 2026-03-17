-- ============================================
-- TABELA: DIAS DE OPERAÇÃO
-- ============================================

CREATE TABLE operation_days (
    id SERIAL PRIMARY KEY,
    merchant_id VARCHAR(50) NOT NULL REFERENCES merchants(merchant_id),
    operation_cw_id VARCHAR(50),
    
    operation_day DATE NOT NULL,
    
    start_time TIME NOT NULL,
    end_time TIME NOT NULL,
    
    opened_at TIMESTAMPTZ NOT NULL,
    closed_at TIMESTAMPTZ,
    
    delivery_capacity INT NOT NULL,
    
    was_edited BOOLEAN DEFAULT FALSE,
    
    total_orders INT,
    canceled_orders INT DEFAULT 0,
    total_revenue DECIMAL(12, 2),
    avg_preparation_minutes INT,
    avg_delivery_minutes INT,

    cash_flow_id BIGINT,
    cash_flow_initial_value NUMERIC(10,2),
    cash_flow_expected_value NUMERIC(10,2),
    cash_flow_final_value NUMERIC(10,2),
    cash_flow_summary JSONB;
    
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS cash_flow_operations (
    id BIGINT PRIMARY KEY,
    operation_day_id BIGINT REFERENCES operation_days(id) ON DELETE CASCADE,
    cash_flow_id BIGINT NOT NULL,
    kind VARCHAR(50),
    description TEXT,
    value NUMERIC(10,2),
    payment_method_id INT,
    payment_method_name VARCHAR(100),
    created_at TIMESTAMPTZ,
    order_id BIGINT,
    user_name VARCHAR(100)
);

CREATE UNIQUE INDEX idx_unique_expediente 
ON operation_days (merchant_id, operation_day, opened_at);

CREATE INDEX idx_open_detection 
ON operation_days (merchant_id, closed_at);

CREATE INDEX idx_cash_flow_ops_day 
ON cash_flow_operations (operation_day_id);

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