CREATE TABLE IF NOT EXISTS sync_jobs (
    id SERIAL PRIMARY KEY,
    merchant_id VARCHAR(50) NOT NULL,
    start_date TIMESTAMP WITH TIME ZONE NOT NULL,
    end_date TIMESTAMP WITH TIME ZONE NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    total_shifts INT DEFAULT 0,
    processed_shifts INT DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sync_jobs_worker_queue 
ON sync_jobs (status, created_at ASC);

CREATE INDEX IF NOT EXISTS idx_sync_jobs_merchant_latest 
ON sync_jobs (merchant_id, created_at DESC);