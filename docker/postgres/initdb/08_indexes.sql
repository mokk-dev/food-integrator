-- ============================================
-- ÍNDICES ADICIONAIS DE PERFORMANCE
-- ============================================

-- Índices GIN para JSONB (buscas em campos aninhados)
CREATE INDEX idx_orders_address_gin 
ON orders USING GIN (delivery_address);

CREATE INDEX idx_orders_items_gin 
ON orders USING GIN (items);

CREATE INDEX idx_inbox_payload_gin 
ON webhook_inbox USING GIN (payload);

-- Índices parciais para queries comuns
CREATE INDEX idx_orders_delivery_pending 
ON orders (id) 
WHERE order_type = 'delivery' 
  AND delivery_man_name IS NULL 
  AND status NOT IN ('delivered', 'cancelled');

CREATE INDEX idx_orders_ifood 
ON orders (created_at) 
WHERE sales_channel = 'ifood';

-- Índice para limpeza de dados antigos (retention policy)
CREATE INDEX idx_inbox_old_processed 
ON webhook_inbox (processed_at) 
WHERE status = 'processed';

-- Comentários de documentação
COMMENT ON TABLE webhook_inbox IS 'Buffer de proteção para webhooks Cardapioweb. Processamento assíncrono via worker.';
COMMENT ON TABLE orders IS 'Dados limpos e enriquecidos dos pedidos. Fonte da verdade para operação e ML.';
COMMENT ON TABLE operation_snapshots IS 'Snapshots periódicos do estado operacional para predição de tempos de entrega.';