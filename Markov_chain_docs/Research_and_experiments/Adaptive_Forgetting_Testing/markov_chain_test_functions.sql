--------------------------------------------------------------------------------
-- markov_chain_test_functions.sql

-- Функция построения вероятностей из заданных частот
CREATE OR REPLACE FUNCTION build_probabilities_from_frequencies(
    p_freq_table TEXT -- имя временной таблицы с колонками from_state, to_state, frequency
)
RETURNS TABLE (from_state SMALLINT, to_state SMALLINT, probability REAL)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY EXECUTE format('
        SELECT from_state, to_state, frequency / SUM(frequency) OVER (PARTITION BY from_state) AS probability
        FROM %I
        WHERE frequency > 0
    ', p_freq_table);
END;
$$;

-- Функция построения поглощающей матрицы из вероятностей
-- Аналогична rebuild_markov_absorbing, но работает с переданной таблицей вероятностей (временной) и возвращает набор строк для вставки.
CREATE OR REPLACE FUNCTION build_absorbing_from_probabilities(
    p_prob_table TEXT -- имя временной таблицы с колонками from_state, to_state, probability
)
RETURNS TABLE (from_state SMALLINT, to_state SMALLINT, probability REAL)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY EXECUTE format('
        WITH non_absorbing_transitions AS (
            SELECT 
                p.from_state,
                p.to_state,
                p.probability,
                SUM(p.probability) OVER (PARTITION BY p.from_state) AS total_prob
            FROM %I p
            JOIN state_descriptions sd_from ON p.from_state = sd_from.state_id
            JOIN state_descriptions sd_to   ON p.to_state = sd_to.state_id
            WHERE NOT (sd_from.correlation < 0 AND sd_from.os_trend = -1 AND sd_from.wait_trend = 1)
              AND NOT (sd_to.correlation   < 0 AND sd_to.os_trend   = -1 AND sd_to.wait_trend   = 1)
        )
        SELECT 
            from_state,
            to_state,
            CASE 
                WHEN total_prob > 0 THEN probability / total_prob
                ELSE 1.0
            END AS probability
        FROM non_absorbing_transitions
        UNION ALL
        SELECT 
            sd.state_id,
            sd.state_id,
            1.0
        FROM state_descriptions sd
        WHERE NOT (sd.correlation < 0 AND sd.os_trend = -1 AND sd.wait_trend = 1)
          AND NOT EXISTS (
              SELECT 1 FROM non_absorbing_transitions tmp 
              WHERE tmp.from_state = sd.state_id
          )
        UNION ALL
        SELECT state_id, state_id, 1.0
        FROM state_descriptions
        WHERE correlation < 0 AND os_trend = -1 AND wait_trend = 1
    ', p_prob_table);
END;
$$;

-- Функция расчёта риска за k шагов по заданной поглощающей матрице
-- Аналог mchain_predict_risk_k, но принимает имя временной таблицы с поглощающей матрицей.
CREATE OR REPLACE FUNCTION predict_risk_from_absorbing(
    p_absorb_table TEXT,
    p_from_state SMALLINT,
    p_k INT DEFAULT 15
)
RETURNS REAL
LANGUAGE plpgsql
STABLE
AS $$
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
    -- Список аварийных состояний (фиксированный)
    SELECT array_agg(state_id) INTO av_states
    FROM state_descriptions
    WHERE correlation < 0 AND os_trend = -1 AND wait_trend = 1;

    -- Инициализация вектора
    v := array_fill(0.0, ARRAY[total_states]);
    v[p_from_state + 1] := 1.0;

    -- Умножение на матрицу
    FOR step IN 1..p_k LOOP
        v_new := array_fill(0.0, ARRAY[total_states]);
        FOR from_s IN 0..188 LOOP
            IF v[from_s + 1] > 0.0 THEN
                FOR to_s, prob IN
                    EXECUTE format('SELECT to_state, probability FROM %I WHERE from_state = %s', p_absorb_table, from_s)
                LOOP
                    v_new[to_s + 1] := v_new[to_s + 1] + v[from_s + 1] * prob;
                END LOOP;
            END IF;
        END LOOP;
        v := v_new;
    END LOOP;

    -- Суммируем по аварийным состояниям
    SELECT SUM(v[state_id + 1]) INTO risk FROM unnest(av_states) AS state_id;
    RETURN COALESCE(risk, 0.0);
END;
$$;

-- Основная функция выполнения эксперимента
CREATE OR REPLACE FUNCTION run_experiment(
    p_experiment_id INT,
    p_start TIMESTAMPTZ DEFAULT now() - INTERVAL '14 days',
    p_end TIMESTAMPTZ DEFAULT now()
)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    exp RECORD;
    alpha REAL;
    freq_temp TEXT := 'freq_exp';
    prob_temp TEXT := 'prob_exp';
    absorb_temp TEXT := 'absorb_exp';
    t TIMESTAMPTZ;
    curr_state SMALLINT;
    risk_val REAL;
    outcome SMALLINT;
    cnt INT := 0;
    total_steps INT;
    step INT := 0;
    start_ts TIMESTAMPTZ := clock_timestamp();
    elapsed INTERVAL;
BEGIN
    -- Получаем параметры эксперимента
    SELECT * INTO exp FROM forgetting_experiments WHERE id = p_experiment_id;
    IF NOT FOUND THEN
        RETURN format('Ошибка: эксперимент с id=%s не найден', p_experiment_id);
    END IF;

    -- Используем фиксированный alpha (base_alpha) для упрощения
    alpha := exp.base_alpha;

    RAISE NOTICE 'Запуск эксперимента "%s" (id=%) с alpha=%, период с % по %',
        exp.experiment_name, p_experiment_id, alpha, p_start, p_end;

    -- 1. Копия частот и применение забывания
    CREATE TEMP TABLE freq_exp AS SELECT * FROM markov_frequencies;
    EXECUTE format('UPDATE %I SET frequency = frequency * (1.0 - %s) WHERE frequency > 0', freq_temp, alpha);
    EXECUTE format('DELETE FROM %I WHERE frequency < 1e-6', freq_temp);

    -- 2. Построение вероятностей
    CREATE TEMP TABLE prob_exp AS
    SELECT * FROM build_probabilities_from_frequencies(freq_temp);

    -- 3. Построение поглощающей матрицы
    CREATE TEMP TABLE absorb_exp AS
    SELECT * FROM build_absorbing_from_probabilities(prob_temp);

    -- 4. Подготовка цикла по времени
    total_steps := CEIL(EXTRACT(EPOCH FROM (p_end - p_start)) / 300); -- шаг 5 минут = 300 секунд
    RAISE NOTICE 'Всего шагов (моментов времени): %', total_steps;

    -- 5. Цикл по моментам времени (каждые 5 минут)
    FOR t IN SELECT generate_series(p_start, p_end, '5 minutes'::interval)
    LOOP
        step := step + 1;
        -- Определяем текущее состояние – последнее состояние из transition_log до момента t
        SELECT to_state INTO curr_state
        FROM transition_log
        WHERE ts <= t
        ORDER BY ts DESC
        LIMIT 1;

        IF curr_state IS NOT NULL THEN
            -- Вычисляем риск по поглощающей матрице
            risk_val := predict_risk_from_absorbing(absorb_temp, curr_state, 15);

            -- Определяем фактический исход – наличие инцидента в (t, t+15 мин]
            SELECT CASE WHEN EXISTS (
                SELECT 1 FROM performance_incident
                WHERE start_timepoint > t
                  AND start_timepoint <= t + INTERVAL '15 minutes'
            ) THEN 1 ELSE 0 END INTO outcome;

            -- Сохраняем прогноз
            INSERT INTO exp_predictions (
                experiment_id, prediction_time, predicted_risk, actual_outcome, current_state_id
            ) VALUES (p_experiment_id, t, risk_val, outcome, curr_state);

            cnt := cnt + 1;
        END IF;

        -- Вывод прогресса каждые 100 шагов или на последнем шаге
        IF step % 100 = 0 OR step = total_steps THEN
            RAISE NOTICE 'Эксперимент %: обработано % из % шагов (%.1f%%), прогнозов собрано: %',
                p_experiment_id, step, total_steps, (step::float / total_steps * 100), cnt;
        END IF;
    END LOOP;

    -- 6. Расчёт метрик качества
    PERFORM calculate_exp_quality_metrics(p_experiment_id);

    -- 7. Очистка временных таблиц
    DROP TABLE IF EXISTS freq_exp, prob_exp, absorb_exp;

    elapsed := clock_timestamp() - start_ts;
    RAISE NOTICE 'Эксперимент % завершён за % (сек). Собрано прогнозов: %',
        p_experiment_id, EXTRACT(EPOCH FROM elapsed), cnt;

    RETURN format('Эксперимент %s завершён, собрано %s прогнозов', p_experiment_id, cnt);
END;
$$;

-- Функция расчёта метрик для эксперимента (аналог calculate_daily_quality_metrics)
CREATE OR REPLACE FUNCTION calculate_exp_quality_metrics(p_experiment_id INT)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    WITH predictions AS (
        SELECT predicted_risk, actual_outcome
        FROM exp_predictions
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
    INSERT INTO exp_quality_metrics (
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



