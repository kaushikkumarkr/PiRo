-- Experiment Registry Schema

CREATE TABLE IF NOT EXISTS experiments (
    experiment_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    hypothesis TEXT,
    category_id VARCHAR(50),
    treatment_stores JSONB NOT NULL, -- List of Store IDs
    control_stores JSONB,            -- List of Control Store IDs (or Synthetic Weights)
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    status VARCHAR(50) DEFAULT 'planned', -- planned, active, completed, analyzed
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_experiments_category ON experiments(category_id);
CREATE INDEX IF NOT EXISTS idx_experiments_status ON experiments(status);
