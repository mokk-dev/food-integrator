-- ============================================
-- TABELA: ESTABELECIMENTOS
-- ============================================

CREATE TABLE merchants (
    merchant_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    
    -- Horário padrão de funcionamento
    default_start_time TIME NOT NULL,
    default_end_time TIME NOT NULL,
    
    -- Localização para cálculo de distância
    address_lat DECIMAL(10, 8) NOT NULL,
    address_lng DECIMAL(11, 8) NOT NULL,
    
    -- Thresholds de classificação de distância (km)
    distance_threshold_near DECIMAL(4, 2) DEFAULT 2.0,
    distance_threshold_medium DECIMAL(4, 2) DEFAULT 5.0,
    
    -- Capacidade padrão
    default_delivery_capacity INT DEFAULT 3,
    
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Trigger para updated_at
CREATE TRIGGER trigger_merchants_updated_at
    BEFORE UPDATE ON merchants
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ============================================
-- SEED: Dados iniciais do estabelecimento
-- ============================================

INSERT INTO merchants (
    merchant_id,
    name,
    default_start_time,
    default_end_time,
    address_lat,
    address_lng,
    distance_threshold_near,
    distance_threshold_medium,
    default_delivery_capacity,
    is_active
) VALUES (
    '6758',
    'Caramelo Hamburgueria e Lanches',
    '18:00',
    '02:00',
    -23.4200,
    -51.9100,
    2.0,
    5.0,
    4,
    TRUE
) ON CONFLICT (merchant_id) DO NOTHING;