-- Tabela de Itens do Pedido
CREATE TABLE IF NOT EXISTS order_items (
    id BIGSERIAL PRIMARY KEY,
    order_id BIGINT REFERENCES orders(id) ON DELETE CASCADE,
    item_id BIGINT,
    name VARCHAR(255),
    quantity INTEGER,
    unit_price NUMERIC(10, 2),
    total_price NUMERIC(10, 2),
    category_name VARCHAR(255)
);

-- Tabela de Pagamentos do Pedido
CREATE TABLE IF NOT EXISTS order_payments (
    id BIGSERIAL PRIMARY KEY,
    order_id BIGINT REFERENCES orders(id) ON DELETE CASCADE,
    payment_method VARCHAR(100),
    payment_type VARCHAR(100),
    total_value NUMERIC(10, 2)
);

CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_order_payments_order_id ON order_payments(order_id);