-- v3.5.0_C.value.sql
-- =============================================================================
-- Текущее значение операционного индикатора ALERT v3.5.0 (C-only)
-- Читает singleton: operational_alert_state (обновляется ежеминутно из cron)
-- =============================================================================
SELECT
    updated_at,
    alert_active,
    fired_c,
    fired_a_would       AS a_disabled_but_would_fire,
    fired_b,
    risk_p90,
    js_avg,
    -- Компактный статус (для отображения)
    CASE
        WHEN alert_active THEN '🔴 ALERT'
        WHEN fired_a_would THEN '🟡 A-only (отключён)'
        ELSE '🟢 NORMAL'
    END AS status_text,
    -- Проверка свежести: если > 2 мин, значит cron упал или не запущен
    CASE
        WHEN now() - updated_at > INTERVAL '2 minutes' THEN 'STALE'
        ELSE 'OK'
    END AS freshness,
    now() - updated_at AS age
FROM operational_alert_state
WHERE id = 1;