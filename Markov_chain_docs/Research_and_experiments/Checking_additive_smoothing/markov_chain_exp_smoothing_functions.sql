--------------------------------------------------------------------------------
-- markov_chain_exp_smoothing_functions.sql

-- 2.1. Функция построения сглаженных вероятностей
-- Аналог update_markov_probabilities, но работает с таблицами в схеме exp_smoothing и принимает параметр сглаживания.
CREATE OR REPLACE FUNCTION exp_smoothing.build_smoothed_probabilities(p_alpha REAL)
RETURNS TABLE (from_state SMALLINT, to_state SMALLINT, probability REAL)
LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    WITH all_states AS (
        SELECT state_id FROM exp_smoothing.state_descriptions
    ),
    freq_with_all AS (
        SELECT 
            a.state_id AS from_state,
            b.state_id AS to_state,
            COALESCE(mf.frequency, 0) AS frequency
        FROM all_states a
        CROSS JOIN all_states b
        LEFT JOIN exp_smoothing.markov_frequencies mf 
            ON mf.from_state = a.state_id AND mf.to_state = b.state_id
    ),
    total_per_from AS (
        SELECT 
            f.from_state,
            SUM(f.frequency + p_alpha) AS total
        FROM freq_with_all f
        GROUP BY f.from_state
        HAVING SUM(f.frequency + p_alpha) > 0
    )
    SELECT 
        f.from_state,
        f.to_state,
        (f.frequency + p_alpha) / t.total AS probability
    FROM freq_with_all f
    JOIN total_per_from t ON f.from_state = t.from_state;
END;
$$;

-- 2.2. Функция построения поглощающей матрицы из сглаженных вероятностей
-- Использует ту же логику, что и rebuild_markov_absorbing, но работает с переданной таблицей вероятностей (может быть временной).
CREATE OR REPLACE FUNCTION exp_smoothing.build_absorbing(p_prob_table TEXT)
RETURNS TABLE (from_state SMALLINT, to_state SMALLINT, probability REAL)
LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY EXECUTE format('
        WITH non_absorbing_transitions AS (
            SELECT 
                p.from_state,
                p.to_state,
                p.probability,
                SUM(p.probability) OVER (PARTITION BY p.from_state) AS total_prob
            FROM %I p
            JOIN exp_smoothing.state_descriptions sd_from ON p.from_state = sd_from.state_id
            JOIN exp_smoothing.state_descriptions sd_to   ON p.to_state = sd_to.state_id
            WHERE NOT (sd_from.correlation < 0 AND sd_from.os_trend = -1 AND sd_from.wait_trend = 1)
              AND NOT (sd_to.correlation   < 0 AND sd_to.os_trend   = -1 AND sd_to.wait_trend   = 1)
        )
        SELECT 
            from_state,
            to_state,
            CASE 
                WHEN total_prob > 0 THEN probability / total_prob
                ELSE 1.0
            END
        FROM non_absorbing_transitions
        UNION ALL
        SELECT 
            sd.state_id,
            sd.state_id,
            1.0
        FROM exp_smoothing.state_descriptions sd
        WHERE NOT (sd.correlation < 0 AND sd.os_trend = -1 AND sd.wait_trend = 1)
          AND NOT EXISTS (
              SELECT 1 FROM non_absorbing_transitions tmp 
              WHERE tmp.from_state = sd.state_id
          )
        UNION ALL
        SELECT state_id, state_id, 1.0
        FROM exp_smoothing.state_descriptions
        WHERE correlation < 0 AND os_trend = -1 AND wait_trend = 1
    ', p_prob_table);
END;
$$;

-- 2.3. Функция расчёта риска за 15 минут по переданной поглощающей матрице
CREATE OR REPLACE FUNCTION exp_smoothing.predict_risk(p_absorb_table REGCLASS, p_from_state SMALLINT, p_k INT DEFAULT 15)
RETURNS REAL
LANGUAGE plpgsql STABLE AS $$
DECLARE
    total_states CONSTANT INT := 189;
    v REAL[];
    v_new REAL[];
    av_states INT[];
    from_s SMALLINT;
    to_s SMALLINT;
    prob REAL;
    step INT;
    risk REAL;
BEGIN
    SELECT array_agg(state_id) INTO av_states
    FROM exp_smoothing.state_descriptions
    WHERE correlation < 0 AND os_trend = -1 AND wait_trend = 1;

    v := array_fill(0.0, ARRAY[total_states]);
    v[p_from_state + 1] := 1.0;

    FOR step IN 1..p_k LOOP
        v_new := array_fill(0.0, ARRAY[total_states]);
        FOR from_s IN 0..188 LOOP
            IF v[from_s + 1] > 0.0 THEN
                FOR to_s, prob IN
                    EXECUTE format('SELECT to_state, probability FROM %s WHERE from_state = %s', p_absorb_table::text, from_s)
                LOOP
                    v_new[to_s + 1] := v_new[to_s + 1] + v[from_s + 1] * prob;
                END LOOP;
            END IF;
        END LOOP;
        v := v_new;
    END LOOP;

    SELECT SUM(v[state_id + 1]) INTO risk FROM unnest(av_states) AS state_id;
    RETURN COALESCE(risk, 0.0);
END;
$$;

-- 2.4. Основная функция прогона эксперимента для одного значения alpha
CREATE OR REPLACE FUNCTION exp_smoothing.run_experiment(
    p_alpha REAL,
    p_start TIMESTAMPTZ DEFAULT now() - INTERVAL '14 days',
    p_end TIMESTAMPTZ DEFAULT now()
)
RETURNS TEXT
LANGUAGE plpgsql AS $$
DECLARE
    exp_id INT;
    prob_table TEXT;
    absorb_table TEXT;
    t TIMESTAMPTZ;
    curr_state SMALLINT;
    risk_val REAL;
    outcome SMALLINT;
    cnt INT := 0;
    total_steps INT;
    step INT := 0;
BEGIN
    -- Регистрируем эксперимент
    INSERT INTO exp_smoothing.experiments (smoothing_alpha, description)
    VALUES (p_alpha, format('Smoothing alpha = %s, period %s to %s', p_alpha, p_start, p_end))
    RETURNING id INTO exp_id;

    -- Формируем имена временных таблиц
    prob_table := format('exp_prob_%s', exp_id);
    absorb_table := format('exp_absorb_%s', exp_id);

    -- Создаём временные таблицы
    EXECUTE format('CREATE TEMP TABLE %I AS SELECT * FROM exp_smoothing.build_smoothed_probabilities(%s)', prob_table, p_alpha);
    -- ИСПРАВЛЕНИЕ: используем %L для передачи имени таблицы как строкового литерала
    EXECUTE format('CREATE TEMP TABLE %I AS SELECT * FROM exp_smoothing.build_absorbing(%L)', absorb_table, prob_table);

    total_steps := CEIL(EXTRACT(EPOCH FROM (p_end - p_start)) / 300);
    RAISE NOTICE 'Эксперимент с alpha=%s: всего шагов %s', p_alpha, total_steps;

    FOR t IN SELECT generate_series(p_start, p_end, '5 minutes'::interval)
    LOOP
        step := step + 1;
        SELECT to_state INTO curr_state
        FROM transition_log
        WHERE ts <= t
        ORDER BY ts DESC
        LIMIT 1;

        IF curr_state IS NOT NULL THEN
            risk_val := exp_smoothing.predict_risk(absorb_table::regclass, curr_state, 15);
            SELECT CASE WHEN EXISTS (
                SELECT 1 FROM performance_incident
                WHERE start_timepoint > t
                  AND start_timepoint <= t + INTERVAL '15 minutes'
            ) THEN 1 ELSE 0 END INTO outcome;

            INSERT INTO exp_smoothing.predictions (experiment_id, prediction_time, predicted_risk, actual_outcome, current_state_id)
            VALUES (exp_id, t, risk_val, outcome, curr_state);
            cnt := cnt + 1;
        END IF;

        IF step % 100 = 0 OR step = total_steps THEN
            RAISE NOTICE 'Шаг % из %, прогнозов собрано: %', step, total_steps, cnt;
        END IF;
    END LOOP;

    -- Расчёт метрик
    PERFORM exp_smoothing.calculate_metrics(exp_id);

    -- Очистка временных таблиц
    EXECUTE format('DROP TABLE %I', prob_table);
    EXECUTE format('DROP TABLE %I', absorb_table);

    RETURN format('Эксперимент %s завершён, собрано %s прогнозов', exp_id, cnt);
END;
$$;

-- 2.5. Функция расчёта метрик (аналогична предыдущей, но работает с таблицей exp_smoothing.predictions)
CREATE OR REPLACE FUNCTION exp_smoothing.calculate_metrics(p_experiment_id INT)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    WITH predictions AS (
        SELECT predicted_risk, actual_outcome
        FROM exp_smoothing.predictions
        WHERE experiment_id = p_experiment_id
    ),
    stats AS (
        SELECT
            COUNT(*) AS total,
            AVG(actual_outcome) AS incident_rate,
            AVG((predicted_risk - actual_outcome)^2) AS brier,
            AVG(CASE 
                WHEN actual_outcome = 1 THEN -ln(GREATEST(predicted_risk, 1e-15))
                ELSE -ln(GREATEST(1 - predicted_risk, 1e-15))
            END) AS log_loss,
            AVG(ABS(predicted_risk - actual_outcome)) AS mae
        FROM predictions
    ),
    roc_auc_calc AS (
        SELECT
            CASE
                WHEN COUNT(CASE WHEN actual_outcome = 1 THEN 1 END) = 0
                     OR COUNT(CASE WHEN actual_outcome = 0 THEN 1 END) = 0
                THEN NULL
                ELSE (SUM(CASE WHEN actual_outcome = 1 THEN rank ELSE 0 END) -
                     (COUNT(CASE WHEN actual_outcome = 1 THEN 1 END) *
                      (COUNT(CASE WHEN actual_outcome = 1 THEN 1 END) + 1) / 2.0)
                    ) / (COUNT(CASE WHEN actual_outcome = 1 THEN 1 END) *
                         COUNT(CASE WHEN actual_outcome = 0 THEN 1 END))
            END AS auc
        FROM (
            SELECT predicted_risk, actual_outcome,
                   ROW_NUMBER() OVER (ORDER BY predicted_risk DESC) AS rank
            FROM predictions
        ) ranked
        WHERE actual_outcome IN (0,1)
    ),
    pr_at_05 AS (
        SELECT
            SUM(CASE WHEN predicted_risk >= 0.5 AND actual_outcome = 1 THEN 1 ELSE 0 END) AS tp,
            SUM(CASE WHEN predicted_risk >= 0.5 AND actual_outcome = 0 THEN 1 ELSE 0 END) AS fp,
            SUM(CASE WHEN predicted_risk < 0.5 AND actual_outcome = 1 THEN 1 ELSE 0 END) AS fn
        FROM predictions
    ),
    calib AS (
        SELECT jsonb_agg(
            jsonb_build_object(
                'bin_low', bin_low,
                'bin_high', bin_high,
                'avg_pred', avg_pred,
                'obs_freq', obs_freq,
                'count', cnt
            )
        ) AS calib
        FROM (
            SELECT
                WIDTH_BUCKET(predicted_risk, 0, 1, 10) AS bin,
                (WIDTH_BUCKET(predicted_risk, 0, 1, 10) - 1) / 10.0 AS bin_low,
                WIDTH_BUCKET(predicted_risk, 0, 1, 10) / 10.0 AS bin_high,
                AVG(predicted_risk) AS avg_pred,
                AVG(actual_outcome) AS obs_freq,
                COUNT(*) AS cnt
            FROM predictions
            GROUP BY bin
            ORDER BY bin
        ) b
    )
    INSERT INTO exp_smoothing.metrics (
        experiment_id, total_predictions, incident_rate, brier_score, log_loss,
        roc_auc, precision_at_05, recall_at_05, mae, calibration_summary
    )
    SELECT
        p_experiment_id,
        s.total,
        s.incident_rate,
        s.brier,
        s.log_loss,
        COALESCE(r.auc, 0.0),
        CASE WHEN (p.tp + p.fp) > 0 THEN p.tp::REAL / (p.tp + p.fp) ELSE 0.0 END,
        CASE WHEN (p.tp + p.fn) > 0 THEN p.tp::REAL / (p.tp + p.fn) ELSE 0.0 END,
        s.mae,
        c.calib
    FROM stats s
    CROSS JOIN roc_auc_calc r
    CROSS JOIN pr_at_05 p
    CROSS JOIN calib c;
END;
$$;

