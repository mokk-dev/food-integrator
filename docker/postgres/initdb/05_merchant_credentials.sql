CREATE TABLE IF NOT EXISTS merchant_credentials (
    merchant_id UUID PRIMARY KEY REFERENCES merchants(id) ON DELETE CASCADE,
    access_token TEXT NOT NULL,
    refresh_token TEXT NOT NULL,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    auth_status VARCHAR(20) DEFAULT 'ACTIVE' CHECK (auth_status IN ('ACTIVE', 'REQUIRED', 'EXPIRED'))
);

CREATE INDEX idx_credentials_expiry ON merchant_credentials (expires_at);