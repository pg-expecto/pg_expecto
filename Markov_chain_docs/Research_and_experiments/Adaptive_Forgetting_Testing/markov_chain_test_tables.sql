--------------------------------------------------------------------------------
-- markov_chain_test_tables.sql
-- ============================================================================
-- Справочник экспериментов по настройке забывания
-- ============================================================================
DROP TABLE IF EXISTS forgetting_experiments;
CREATE TABLE forgetting_experiments (
    id                 SERIAL PRIMARY KEY,
    experiment_name    TEXT NOT NULL,                -- Краткое название
    description        TEXT,                         -- Описание параметров
    base_alpha         REAL NOT NULL,                -- Базовый коэффициент забывания
    half_life_days     REAL NOT NULL,                -- Период полураспада (дни)
    min_alpha          REAL NOT NULL,                -- Минимальный alpha
    interval_minute    INT NOT NULL,                 -- Интервал применения забывания (минуты)
    reliability_score  INT,                          -- Значение mchain_forecast_reliability на момент старта
    created_at         TIMESTAMPTZ DEFAULT now(),    -- Время создания записи
    notes              TEXT                          -- Дополнительные пометки
);
COMMENT ON TABLE forgetting_experiments IS 'Справочник экспериментов по настройке гиперпараметров адаптивного забывания';
COMMENT ON COLUMN forgetting_experiments.id IS 'Идентификатор эксперимента (первичный ключ)';
COMMENT ON COLUMN forgetting_experiments.experiment_name IS 'Краткое название для идентификации (например, "Baseline", "Fast forgetting")';
COMMENT ON COLUMN forgetting_experiments.description IS 'Описание комбинации параметров';
COMMENT ON COLUMN forgetting_experiments.base_alpha IS 'Значение base_alpha (скорость забывания при частых инцидентах)';
COMMENT ON COLUMN forgetting_experiments.half_life_days IS 'Период полураспада в днях (влияет на скорость снижения alpha при отсутствии инцидентов)';
COMMENT ON COLUMN forgetting_experiments.min_alpha IS 'Минимальное значение alpha (ниже не опускается)';
COMMENT ON COLUMN forgetting_experiments.interval_minute IS 'Интервал между применениями забывания (минуты)';
COMMENT ON COLUMN forgetting_experiments.reliability_score IS 'Рейтинг достоверности модели на момент запуска эксперимента (из mchain_forecast_reliability)';
COMMENT ON COLUMN forgetting_experiments.created_at IS 'Время создания записи';
COMMENT ON COLUMN forgetting_experiments.notes IS 'Дополнительные замечания (например, период ретроспективного тестирования)';

-- ============================================================================
-- Журнал прогнозов, полученных в рамках экспериментальных конфигураций
-- ============================================================================
DROP TABLE IF EXISTS exp_predictions;
CREATE TABLE exp_predictions (
    experiment_id      INT NOT NULL REFERENCES forgetting_experiments(id) ON DELETE CASCADE,
    prediction_time    TIMESTAMPTZ NOT NULL,
    predicted_risk     REAL NOT NULL,                -- Предсказанная вероятность (0..1)
    actual_outcome     SMALLINT NOT NULL,            -- 1 – инцидент произошёл в течение 15 минут, 0 – нет
    current_state_id   SMALLINT,                     -- Состояние на момент прогноза (если известно)
    -- Дополнительные поля (опционально, для диагностики)
    situation          TEXT,                         -- 'unknown_state', 'no_risk', 'risk_calculated'
    transitions_to_risk INT,
    total_transitions_known INT,
    PRIMARY KEY (experiment_id, prediction_time)
);
CREATE INDEX idx_exp_predictions_time ON exp_predictions (prediction_time);
CREATE INDEX idx_exp_predictions_experiment ON exp_predictions (experiment_id);
COMMENT ON TABLE exp_predictions IS 'Прогнозы, собранные для каждого эксперимента по настройке забывания';
COMMENT ON COLUMN exp_predictions.experiment_id IS 'Ссылка на эксперимент';
COMMENT ON COLUMN exp_predictions.prediction_time IS 'Момент времени, на который был сделан прогноз';
COMMENT ON COLUMN exp_predictions.predicted_risk IS 'Вероятность возникновения инцидента в течение 15 минут';
COMMENT ON COLUMN exp_predictions.actual_outcome IS 'Фактическое наличие инцидента в интервале (1 – да, 0 – нет)';
COMMENT ON COLUMN exp_predictions.current_state_id IS 'Идентификатор состояния системы на момент прогноза (для анализа)';
COMMENT ON COLUMN exp_predictions.situation IS 'Диагностическая метка (unknown_state, no_risk, risk_calculated)';
COMMENT ON COLUMN exp_predictions.transitions_to_risk IS 'Количество прямых переходов в аварийные состояния из текущего состояния';
COMMENT ON COLUMN exp_predictions.total_transitions_known IS 'Общее число известных переходов из текущего состояния';

-- ============================================================================
-- Агрегированные метрики качества прогнозов по каждому эксперименту
-- ============================================================================
DROP TABLE IF EXISTS exp_quality_metrics;
CREATE TABLE exp_quality_metrics (
    experiment_id      INT PRIMARY KEY REFERENCES forgetting_experiments(id) ON DELETE CASCADE,
    total_predictions  INT NOT NULL,                 -- Количество прогнозов с известным исходом
    incident_rate      REAL,                         -- Доля инцидентов (среднее actual_outcome)
    brier_score        REAL,                         -- Среднеквадратичная ошибка
    log_loss           REAL,                         -- Логистическая потеря (с обрезкой)
    roc_auc            REAL,                         -- Площадь под ROC-кривой
    precision_at_05    REAL,                         -- Точность при пороге 0.5
    recall_at_05       REAL,                         -- Полнота при пороге 0.5
    mae                REAL,                         -- Средняя абсолютная ошибка
    calibration_summary JSONB,                       -- Калибровочная таблица (массив бинов)
    calculated_at      TIMESTAMPTZ DEFAULT now()     -- Время расчёта метрик
);
COMMENT ON TABLE exp_quality_metrics IS 'Агрегированные метрики качества для каждого эксперимента';
COMMENT ON COLUMN exp_quality_metrics.experiment_id IS 'Идентификатор эксперимента (первичный ключ, ссылка на forgetting_experiments)';
COMMENT ON COLUMN exp_quality_metrics.total_predictions IS 'Общее число прогнозов, использованных для расчёта метрик';
COMMENT ON COLUMN exp_quality_metrics.incident_rate IS 'Фактическая доля инцидентов среди прогнозов';
COMMENT ON COLUMN exp_quality_metrics.brier_score IS 'Brier score (среднеквадратичная ошибка вероятности)';
COMMENT ON COLUMN exp_quality_metrics.log_loss IS 'Log-loss (логистическая потеря)';
COMMENT ON COLUMN exp_quality_metrics.roc_auc IS 'ROC‑AUC (дискриминационная способность)';
COMMENT ON COLUMN exp_quality_metrics.precision_at_05 IS 'Precision при пороге 0.5';
COMMENT ON COLUMN exp_quality_metrics.recall_at_05 IS 'Recall при пороге 0.5';
COMMENT ON COLUMN exp_quality_metrics.mae IS 'Средняя абсолютная ошибка (MAE)';
COMMENT ON COLUMN exp_quality_metrics.calibration_summary IS 'Калибровочная таблица в формате JSON (массив бинов с полями bin_low, bin_high, avg_pred, obs_freq, count)';
COMMENT ON COLUMN exp_quality_metrics.calculated_at IS 'Время вычисления и сохранения метрик';

