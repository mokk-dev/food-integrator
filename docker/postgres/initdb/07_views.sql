-- ============================================
-- VIEWS E FUNÇÕES ANALÍTICAS
-- ============================================

-- View: Resumo do expediente (materializada para performance)
-- VERSÃO CORRIGIDA: Subqueries mais seguras para evitar agregações complexas
CREATE MATERIALIZED VIEW shift_summary AS
SELECT 
    od.id as operation_day_id,
    od.merchant_id,
    od.operation_day,
    
    od.opened_at,
    od.closed_at,
    EXTRACT(EPOCH FROM (COALESCE(od.closed_at, NOW()) - od.opened_at)) / 3600 as duration_hours,
    
    od.total_orders,
    od.canceled_orders,
    COALESCE(od.total_revenue, 0) as total_revenue,
    
    -- Ticket médio calculado diretamente das orders para precisão
    COALESCE(
        (SELECT AVG(total_value) 
         FROM orders o 
         WHERE o.operation_day_id = od.id 
           AND o.status != 'cancelled'), 
        0
    ) as avg_ticket,
    
    od.avg_preparation_minutes,
    od.avg_delivery_minutes,
    
    -- Subqueries são mais seguras para contagens condicionais em Views complexas
    (SELECT COUNT(*) 
     FROM orders o 
     WHERE o.operation_day_id = od.id 
       AND o.distance_zone = 'near') as orders_near,
       
    (SELECT COUNT(*) 
     FROM orders o 
     WHERE o.operation_day_id = od.id 
       AND o.distance_zone = 'medium') as orders_medium,
       
    (SELECT COUNT(*) 
     FROM orders o 
     WHERE o.operation_day_id = od.id 
       AND o.distance_zone = 'far') as orders_far,
    
    -- Agregação segura de canais
    (
        SELECT JSONB_OBJECT_AGG(channel, count)
        FROM (
            SELECT sales_channel as channel, COUNT(*) as count
            FROM orders o 
            WHERE o.operation_day_id = od.id 
              AND o.sales_channel IS NOT NULL
            GROUP BY sales_channel
        ) channels
    ) as orders_by_channel,
    
    od.delivery_capacity,
    
    CASE 
        WHEN od.delivery_capacity > 0 THEN ROUND(od.total_orders::DECIMAL / od.delivery_capacity, 2)
        ELSE 0 
    END as orders_per_driver,
    
    CASE 
        WHEN od.total_orders > 0 THEN ROUND(od.canceled_orders::DECIMAL / od.total_orders * 100, 2)
        ELSE 0 
    END as cancellation_rate

FROM operation_days od;

-- Índice na view materializada
CREATE UNIQUE INDEX idx_shift_summary_pk ON shift_summary (operation_day_id);

-- Função para atualizar métricas do expediente ao fechar
CREATE OR REPLACE FUNCTION finalize_operation_day(
    p_operation_day_id INT,
    p_closed_at TIMESTAMPTZ
)
RETURNS VOID AS $$
DECLARE
    v_total_orders INT;
    v_canceled_orders INT;
    v_total_revenue DECIMAL(12, 2);
    v_avg_delivery INT;
BEGIN
    -- Calcular métricas
    SELECT 
        COUNT(*),
        COUNT(*) FILTER (WHERE status = 'cancelled'),
        COALESCE(SUM(total_value), 0),
        AVG(EXTRACT(EPOCH FROM (delivered_at - created_at)) / 60)::INT
    INTO 
        v_total_orders,
        v_canceled_orders,
        v_total_revenue,
        v_avg_delivery
    FROM orders
    WHERE operation_day_id = p_operation_day_id;
    
    -- Atualizar operation_day
    UPDATE operation_days
    SET closed_at = p_closed_at,
        total_orders = v_total_orders,
        canceled_orders = v_canceled_orders,
        total_revenue = v_total_revenue,
        avg_delivery_minutes = v_avg_delivery
    WHERE id = p_operation_day_id;
    
    -- Refresh da view materializada
    REFRESH MATERIALIZED VIEW CONCURRENTLY shift_summary;
END;
$$ LANGUAGE plpgsql;