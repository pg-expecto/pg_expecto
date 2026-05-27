## Таблица `apply_forgetting_log`

### 1. Назначение

Таблица `apply_forgetting_log` служит **журналом всех вызовов функции `apply_forgetting()`**, независимо от того, была ли инициация плановой (по таймеру в `markov_chain_training`) или форсированной (через `check_and_forget()` или `emergency_forget()`). В отличие от таблицы `forget_log`, которая фиксирует только **форсированные забывания по решению `check_and_forget`**, `apply_forgetting_log` регистрирует **каждое фактическое применение забывания**:

- Плановое забывание по истечении `interval_minute` (вызывается внутри `markov_chain_training`).
- Форсированное забывание через `check_and_forget()` (с динамическим `alpha`).
- Ручное забывание через `emergency_forget()`.
- Любой прямой вызов `apply_forgetting(alpha_override)`.

Журнал позволяет:

- Отслеживать, как часто и с каким `alpha` применяется забывание в нормальном режиме.
- Анализировать влияние адаптивного режима (`use_adaptive_alpha`) на скорость забывания.
- Прослеживать изменения `effective_alpha` в зависимости от времени с последнего инцидента.
- Контролировать переопределение `alpha` через параметр `alpha_override`.
- Иметь полный аудит действий, влияющих на матрицу частот.

### 2. Определение (DDL)

```sql
DROP TABLE IF EXISTS apply_forgetting_log;
CREATE TABLE apply_forgetting_log (
    id                  BIGSERIAL PRIMARY KEY,
    ts                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    effective_alpha     REAL NOT NULL,
    adaptive_used       BOOLEAN NOT NULL,
    days_since_incident REAL,
    alpha_override      REAL,
    details             TEXT
);

COMMENT ON TABLE apply_forgetting_log IS 'Журнал вызовов apply_forgetting (помимо RAISE NOTICE)';
COMMENT ON COLUMN apply_forgetting_log.effective_alpha IS 'Фактически применённый коэффициент забывания';
COMMENT ON COLUMN apply_forgetting_log.adaptive_used IS 'Использовалось ли адаптивное забывание (cfg.use_adaptive_alpha)';
COMMENT ON COLUMN apply_forgetting_log.days_since_incident IS 'Количество дней с последнего инцидента (NULL, если инцидентов не было или адаптивный режим выключен)';
COMMENT ON COLUMN apply_forgetting_log.alpha_override IS 'Значение переданного параметра alpha_override (если не NULL)';
COMMENT ON COLUMN apply_forgetting_log.details IS 'Дополнительная информация (например, значения параметров)';
```

### 3. Детальное описание колонок

| Колонка | Тип | Обязательность | Описание |
|---------|-----|----------------|-----------|
| `id` | `BIGSERIAL` | NOT NULL (PK) | Уникальный идентификатор записи, автоинкремент. |
| `ts` | `TIMESTAMPTZ` | NOT NULL, DEFAULT now() | Момент вызова `apply_forgetting()` (с часовым поясом). Используется для хронологического анализа и очистки. |
| `effective_alpha` | `REAL` | NOT NULL | Коэффициент забывания, который реально был применён к таблице `markov_frequencies` (умножение частот на `1 - effective_alpha`). Может быть вычислен как: из конфигурации (`alpha`), адаптивно (на основе `last_incident_time`), либо передан через `alpha_override`. Всегда находится в диапазоне `(0, 1]`. |
| `adaptive_used` | `BOOLEAN` | NOT NULL | Показывает, был ли включён адаптивный режим (`markov_config.use_adaptive_alpha = true`) на момент вызова. Позволяет отличать адаптивные забывания от постоянного `alpha`. |
| `days_since_incident` | `REAL` | NULL | Количество дней (дробное) от времени последнего инцидента (`markov_config.last_incident_time`) до момента вызова. Значение `NULL` означает, что инцидентов не было или адаптивный режим выключен. Поле полезно для анализа того, насколько быстро снижается `effective_alpha` после аварии. |
| `alpha_override` | `REAL` | NULL | Если вызов `apply_forgetting(alpha_override)` был выполнен с явным параметром, здесь сохраняется переданное значение. `NULL` означает, что `alpha` был определён по конфигурации или адаптивно. |
| `details` | `TEXT` | NULL | Строка с дополнительной информацией, формируемая в теле функции. Например, в адаптивном режиме содержит значения `base_alpha`, `half_life`, вычисленное `effective_alpha`. В неадаптивном режиме – указание на использование `config.alpha`. |

### 4. Связи с другими объектами

- **`markov_config`** – определяет режим забывания (`use_adaptive_alpha`, `base_alpha`, `min_alpha`, `incident_half_life_days`, `last_incident_time`, `alpha` (неадаптивный)). Значения из этой таблицы напрямую влияют на `effective_alpha` и флаг `adaptive_used`.
- **`apply_forgetting(REAL)`** – единственная функция, которая вставляет записи в эту таблицу. Вызывается из:
  - `markov_chain_training()` (плановое забывание) – без параметра `alpha_override`.
  - `check_and_forget()` – с параметром `alpha_eff` (рассчитанным по признакам).
  - `emergency_forget()` – с заданным `alpha`.
  - Любого другого кода, который явно вызывает `apply_forgetting()`.
- **`clean_apply_forgetting_log(INT)`** – удаляет записи старше заданного срока (по умолчанию берёт `apply_forgetting_log_retention_days` из `markov_config`).
- **`get_apply_forgetting_log(TIMESTAMPTZ, TIMESTAMPTZ)`** – возвращает записи за период в виде текстового массива для удобного просмотра.

### 5. Логика заполнения (из функции `apply_forgetting()`)

Функция `apply_forgetting(alpha_override DEFAULT NULL)` реализует следующую логику (сокращённо):

```sql
-- 1. Чтение конфигурации
SELECT use_adaptive_alpha, alpha, base_alpha, min_alpha,
       incident_half_life_days, last_incident_time INTO cfg
FROM markov_config;

-- 2. Определение effective_alpha
IF alpha_override IS NOT NULL THEN
    effective_alpha := alpha_override;
    adaptive_used := cfg.use_adaptive_alpha;  -- фактически, флаг не зависит от override? По коду, adaptive_used = cfg.use_adaptive_alpha всегда
    days_since_incident := NULL;
    details := format('alpha_override = %s', alpha_override);
ELSIF cfg.use_adaptive_alpha THEN
    IF cfg.last_incident_time IS NULL THEN
        effective_alpha := cfg.min_alpha;
        days_since_incident := NULL;
        details := 'adaptive mode, no incident -> min_alpha';
    ELSE
        days_since_incident := EXTRACT(EPOCH FROM (now() - cfg.last_incident_time)) / 86400.0;
        effective_alpha := cfg.base_alpha * exp(-days_since_incident / cfg.incident_half_life_days);
        effective_alpha := GREATEST(effective_alpha, cfg.min_alpha);
        details := format('adaptive mode, days_since_incident = %s, base_alpha = %s, half_life = %s -> effective_alpha = %s', ...);
    END IF;
ELSE
    effective_alpha := cfg.alpha;
    days_since_incident := NULL;
    details := format('non-adaptive mode, config.alpha = %s', cfg.alpha);
END IF;

-- 3. Вставка записи
INSERT INTO apply_forgetting_log (effective_alpha, adaptive_used, days_since_incident, alpha_override, details)
VALUES (effective_alpha, cfg.use_adaptive_alpha, days_since_incident, alpha_override, details_text);

-- 4. Применение забывания
UPDATE markov_frequencies SET frequency = frequency * (1.0 - effective_alpha);
DELETE FROM markov_frequencies WHERE frequency < 1e-6;
PERFORM update_markov_probabilities();
UPDATE markov_config SET last_forget_time = now();
```

**Важные замечания:**

- Поле `adaptive_used` всегда устанавливается в значение `cfg.use_adaptive_alpha` (текущая настройка), даже если был передан `alpha_override`. Это позволяет видеть, был ли адаптивный режим включён, но фактический `alpha` мог быть переопределён.
- `days_since_incident` заполняется только в адаптивном режиме при наличии инцидента; иначе `NULL`.
- Запись создаётся **до** фактического изменения таблиц, что гарантирует, что даже при сбое после вставки журнал сохранит информацию о попытке.

### 6. Пример содержимого

| id | ts | effective_alpha | adaptive_used | days_since_incident | alpha_override | details |
|----|----|----------------|---------------|---------------------|----------------|---------|
| 1 | 2025-05-27 10:00:00+00 | 0.01 | false | NULL | NULL | non-adaptive mode, config.alpha = 0.01 |
| 2 | 2025-05-27 11:30:00+00 | 0.08 | true | 2.5 | NULL | adaptive mode, days_since_incident = 2.5, base_alpha = 0.1, half_life = 7 -> effective_alpha = 0.08 |
| 3 | 2025-05-27 14:15:00+00 | 0.30 | true | 0.1 | 0.3 | alpha_override = 0.3 |
| 4 | 2025-05-27 20:00:00+00 | 0.01 | true | NULL | NULL | adaptive mode, no incident -> min_alpha |

### 7. Функции для работы с таблицей

#### `clean_apply_forgetting_log(p_retention_days INT DEFAULT NULL)`

Удаляет записи старше заданного количества дней. Если `p_retention_days` не передан, берёт значение из `markov_config.apply_forgetting_log_retention_days` (по умолчанию 30, как указано в комментариях к таблицам, но в конфиге это поле называется `apply_forgetting_log_retention_days`). Возвращает текст с количеством удалённых строк.

**Пример использования в cron:**
```sql
-- Ежедневная очистка в 02:00
0 2 * * * psql -d expecto_db -U expecto_user -c "SELECT clean_apply_forgetting_log();"
```

#### `get_apply_forgetting_log(p_start TIMESTAMPTZ, p_end TIMESTAMPTZ)`

Возвращает `TEXT[]` – массив строк, каждая из которых содержит поля `id`, `ts`, `effective_alpha`, `adaptive_used`, `days_since_incident`, `alpha_override`, `details`. Период по умолчанию – последние 7 дней.

**Пример:**
```sql
SELECT unnest(get_apply_forgetting_log('2025-05-01', '2025-05-31'));
```

### 8. Различия между `apply_forgetting_log` и `forget_log`

| Характеристика | `apply_forgetting_log` | `forget_log` |
|----------------|------------------------|--------------|
| **Что регистрирует** | Каждый вызов `apply_forgetting()` (плановый, форсированный, ручной) | Только форсированные забывания, инициированные `check_and_forget()` |
| **Связь с признаками** | Нет; содержит только вычисленный `effective_alpha` и режим | Содержит `triggered_by` (массив признаков), значения метрик (`kl_div`, `chi2_val`, ...) |
| **Поле `alpha`** | `effective_alpha` (с учётом адаптивности или переопределения) | `alpha` (то же значение, но только при форсированном забывании) |
| **Назначение** | Полный аудит всех забываний, мониторинг частоты и режимов | Анализ причин форсированных забываний, калибровка порогов |
| **Очистка по умолчанию** | 30 дней (задаётся отдельным параметром) | 90 дней |

### 9. Рекомендации по использованию

- **Мониторинг частоты плановых забываний** – анализируйте записи с `adaptive_used = false` и `alpha_override IS NULL`. Если интервал между соседними `ts` заметно меньше `interval_minute` из `markov_config`, возможно, функция `markov_chain_training()` вызывается чаще, чем нужно, или `last_forget_time` обновляется некорректно.
- **Оценка адаптивного режима** – смотрите на `days_since_incident` и соответствующее `effective_alpha`. Убедитесь, что после инцидента `alpha` постепенно снижается к `min_alpha` в течение периода полураспада.
- **Обнаружение частых ручных вмешательств** – записи с не-NULL `alpha_override` (особенно из `emergency_forget`) сигнализируют о проблемах, требующих анализа.
- **Настройка срока хранения** – задайте `apply_forgetting_log_retention_days` в `markov_config` (например, 30–60 дней) в зависимости от требований к аудиту. Для экономии места можно уменьшить до 14 дней, для детального анализа – увеличить до 90.
- **Интеграция с алертами** – можно создать запрос, который предупреждает, если количество записей в `apply_forgetting_log` за последние 24 часа превышает ожидаемое (например, более 48 вызовов при `interval_minute = 30`).

### 10. Пример запроса для анализа

```sql
-- Средний effective_alpha по часам для адаптивного режима
SELECT 
    EXTRACT(HOUR FROM ts) AS hour_of_day,
    AVG(effective_alpha) AS avg_alpha,
    COUNT(*) AS calls
FROM apply_forgetting_log
WHERE adaptive_used = true
  AND ts > now() - INTERVAL '30 days'
GROUP BY hour_of_day
ORDER BY hour_of_day;
```

```sql
-- Проверка, как часто используется alpha_override
SELECT 
    CASE WHEN alpha_override IS NULL THEN 'config/adaptive' ELSE 'override' END AS source,
    COUNT(*) AS count,
    AVG(effective_alpha) AS avg_alpha
FROM apply_forgetting_log
WHERE ts > now() - INTERVAL '7 days'
GROUP BY source;
```

### 11. Очистка и обслуживание

- **Автоматическая очистка** – настройте cron-задачу на ежедневный вызов `SELECT clean_apply_forgetting_log();`. Функция сама определит срок хранения из конфигурации.
- **Ручная очистка** – `SELECT clean_apply_forgetting_log(60);` удалит записи старше 60 дней.
- **Влияние на производительность** – таблица имеет первичный ключ по `id` и не содержит индексов на `ts`. При очень большом количестве записей (миллионы) рекомендуется создать индекс `CREATE INDEX idx_apply_forgetting_log_ts ON apply_forgetting_log (ts);` для ускорения очистки.

Таким образом, `apply_forgetting_log` является **фундаментальным журналом низкоуровневых событий забывания**, позволяющим инженерам и администраторам контролировать, как часто и насколько агрессивно модель «забывает» историю, а также проверять корректность работы адаптивного механизма.
