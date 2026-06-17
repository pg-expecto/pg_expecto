--------------------------------------------------------------------------------
-- markov_chain_exp_smoothing_tables.sql

CREATE SCHEMA IF NOT EXISTS exp_smoothing;

CREATE TABLE exp_smoothing.experiments (
    id SERIAL PRIMARY KEY,
    smoothing_alpha REAL NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    description TEXT
);

CREATE TABLE exp_smoothing.predictions (
    experiment_id INT REFERENCES exp_smoothing.experiments(id),
    prediction_time TIMESTAMPTZ NOT NULL,
    predicted_risk REAL NOT NULL,
    actual_outcome SMALLINT NOT NULL,
    current_state_id SMALLINT,
    PRIMARY KEY (experiment_id, prediction_time)
);


CREATE TABLE exp_smoothing.metrics (
    experiment_id INT PRIMARY KEY REFERENCES exp_smoothing.experiments(id),
    total_predictions INT,
    incident_rate REAL,
    brier_score REAL,
    log_loss REAL,
    roc_auc REAL,
    precision_at_05 REAL,
    recall_at_05 REAL,
    mae REAL,
    calibration_summary JSONB,
    calculated_at TIMESTAMPTZ DEFAULT now()
);


CREATE TABLE exp_smoothing.state_descriptions AS 
SELECT * FROM state_descriptions;

CREATE TABLE exp_smoothing.markov_frequencies AS 
SELECT * FROM markov_frequencies;




