-- ============================================
-- EXTENSÕES E CONFIGURAÇÕES BASE
-- ============================================

-- TimescaleDB para séries temporais
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- UUID para IDs internos se necessário
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Configuração de timezone (fixo America/Sao_Paulo)
ALTER DATABASE delivery SET timezone TO 'America/Sao_Paulo';
SET timezone = 'America/Sao_Paulo';

-- Função de atualização automática de updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';