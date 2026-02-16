-- ============================================
-- TABELA: INBOX DE WEBHOOKS (Buffer de proteção)
-- ============================================

CREATE TABLE webhook_inbox (
    event_id VARCHAR(30) PRIMARY KEY,
    
    order_id BIGINT,  -- Pode ser NULL se falhar parse inicial
    event_type VARCHAR(30) NOT NULL,
    order_status VARCHAR(30),
    
    payload JSONB NOT NULL,
    received_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    
    -- Controle de processamento
    status VARCHAR(20) DEFAULT 'pending',  -- pending | processing | processed | failed
    processed_at TIMESTAMPTZ,
    processing_attempts INT DEFAULT 0,
    last_error TEXT,
    
    -- Rastreabilidade
    worker_id VARCHAR(50),
    processing_duration_ms INT,
    
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Índices para o worker
CREATE INDEX idx_pending_order 
ON webhook_inbox (status, processing_attempts, received_at) 
WHERE status IN ('pending', 'failed');

CREATE INDEX idx_order_events_lookup 
ON webhook_inbox (order_id, event_type);

CREATE INDEX idx_received_at 
ON webhook_inbox (received_at);

-- Função para reprocessar evento falho
CREATE OR REPLACE FUNCTION retry_failed_event(p_event_id VARCHAR)
RETURNS BOOLEAN AS $$
BEGIN
    UPDATE webhook_inbox
    SET status = 'pending',
        processing_attempts = 0,
        last_error = NULL,
        processed_at = NULL
    WHERE event_id = p_event_id
      AND status = 'failed';
    
    RETURN FOUND;
END;
$$ LANGUAGE plpgsql;