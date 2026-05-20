# Хранимые функции цепи Маркова для анализа производительности PostgreSQL

## Общее назначение

Представленный набор хранимых функций реализует **обучение, прогнозирование, адаптивное забывание и оценку качества** цепи Маркова. Модель предназначена для анализа производительности PostgreSQL и прогнозирования риска перехода в аварийные состояния (отрицательная корреляция + падение операционной скорости).

---

## 1. Функции заполнения справочников и идентификации состояний

### `fill_state_descriptions()`

**Сигнатура:**  
`fill_state_descriptions() RETURNS void`

**Назначение:**  
Генерирует все 189 комбинаций состояний и заполняет таблицу `state_descriptions`.  
Состояние кодируется как:  
`state_id = correlation_index * 9 + (os_trend+1)*3 + (wait_trend+1)`  
где `correlation_index` от 0 (r=-1.0) до 20 (r=+1.0) с шагом 0.1.

**Алгоритм:**  
- Очищает таблицу `TRUNCATE`
- Генерирует декартово произведение корреляций (-1.0 … +1.0) и трендов (-1,0,1)
- Вычисляет `state_id` и вставляет записи

**Использование:**  
Вызывается однократно при инициализации модели.

---

### `get_state_id(r REAL, os_trend SMALLINT, wait_trend SMALLINT)`

**Сигнатура:**  
`get_state_id(r, os_trend, wait_trend) RETURNS SMALLINT`

**Назначение:**  
Преобразует три параметра состояния в числовой идентификатор по той же формуле, что и в `fill_state_descriptions`.

**Свойства:**  
- `IMMUTABLE` – всегда возвращает одинаковый результат для одинаковых входных данных
- `LANGUAGE sql` – максимальная производительность

**Пример:**  
`get_state_id(-0.5, -1, 1) → 105`

---

## 2. Функции учёта переходов и частот

### `update_markov_frequency(...)`

**Сигнатура:**  
```sql
update_markov_frequency(
    r_from REAL, os_trend_from SMALLINT, wait_trend_from SMALLINT,
    r_to   REAL, os_trend_to   SMALLINT, wait_trend_to   SMALLINT
) RETURNS void
```

**Назначение:**  
Увеличивает частоту перехода из одного состояния в другое в таблице `markov_frequencies`.  
Если пара `(from_state, to_state)` не существует – вставляет со значением `1.0`, иначе увеличивает на `1.0`.

**Алгоритм:**  
1. Вычисляет `from_id` и `to_id` через `get_state_id`
2. Выполняет `INSERT ... ON CONFLICT DO UPDATE`

---

### `log_transition_and_update(...)`

**Сигнатура:**  
Аналогична `update_markov_frequency`

**Назначение:**  
Комбинирует две операции:
- Записывает переход в журнал `transition_log` с меткой времени `now()`
- Обновляет частоты через `update_markov_frequency`

**Использование:**  
Основная функция для фиксации каждого наблюдаемого перехода в системе мониторинга.

---

### `get_current_os_waiting_correlation_for_markov_chain()`

**Сигнатура:**  
```sql
get_current_os_waiting_correlation_for_markov_chain()
RETURNS TABLE(
    current_correlation REAL,
    current_os_trend    SMALLINT,
    current_wait_trend  SMALLINT
)
```

**Назначение:**  
Вычисляет текущее состояние производительности на основе данных за последний час из таблицы `cluster_stat_median`.

**Алгоритм:**
1. Определяет максимальный `curr_timestamp` в `cluster_stat_median`
2. Вычисляет корреляцию Пирсона между `curr_op_speed` и `curr_waitings` за последний час
3. Для тренда операционной скорости:
   - Нормирует временные точки и значения скорости
   - Вычисляет угол наклона линии регрессии
   - Возвращает `SIGN(угла)`: -1 (падение), 0 (стабильно), +1 (рост)
4. Аналогично для тренда ожиданий (`curr_waitings`)
5. Обрабатывает исключение `division_by_zero` (при нулевом стандартном отклонении)

**Выходные данные:**
- `current_correlation` – корреляция скорость–ожидания
- `current_os_trend` – направление тренда скорости
- `current_wait_trend` – направление тренда ожиданий

---

## 3. Функции обучения и обновления модели

### `markov_chain_training()`

**Сигнатура:**  
`markov_chain_training() RETURNS void`

**Назначение:**  
**Основная ежеминутная функция обучения.**  
Она вызывается планировщиком каждую минуту в рабочие часы (Пн–Пт, 8:00–19:00).  
Реализует полный цикл:
- плановое забывание
- сбор текущих метрик
- сдвиг состояния
- прогнозирование риска
- логирование прогноза
- обновление частот переходов

**Подробный алгоритм:**

1. **Проверка рабочего времени** – если выходной или нерабочий час, функция завершается с уведомлением.

2. **Плановое забывание**:
   - Читает `last_forget_time`, `alpha`, `interval_hours` из `markov_config`
   - Если прошло больше `interval_hours` – вызывает `apply_forgetting(alpha)` и обновляет `last_forget_time`

3. **Сбор метрик** – вызов `get_current_os_waiting_correlation_for_markov_chain()`

4. **Инициализация состояния** – если таблица `markov_chain` пуста (`prev_correlation IS NULL`), сохраняет текущие метрики как предыдущее и текущее состояние и завершается.

5. **Сдвиг состояния** – обновляет строку в `markov_chain`: предыдущее ← текущее, текущее ← новые метрики.

6. **Прогноз риска** – из предыдущего состояния вычисляет вероятность перехода в аварийные состояния (корреляция < 0 AND `os_trend = -1`) по таблице `markov_probabilities`.

7. **Фактический исход** – проверяет, является ли новое текущее состояние аварийным.

8. **Логирование прогноза** – вставляет запись в `forecast_log`.

9. **Обновление частот** – вызывает `log_transition_and_update` для перехода `prev_state → curr_state`.

**Примечание:**  
Функция предполагает, что таблица `markov_chain` содержит ровно одну строку (управляется через `UPDATE`).

---

### `update_markov_probabilities()`

**Сигнатура:**  
`update_markov_probabilities() RETURNS void`

**Назначение:**  
Пересчитывает матрицу вероятностей из частот и обновляет поглощающую матрицу.

**Алгоритм:**
1. Очищает `markov_probabilities` (`TRUNCATE`)
2. Вставляет нормализованные вероятности:
   ```sql
   frequency / SUM(frequency) OVER (PARTITION BY from_state)
   ```
3. Вызывает `rebuild_markov_absorbing()`

**Вызов:**  
Выполняется автоматически после забывания или по расписанию.

---

### `rebuild_markov_absorbing()`

**Сигнатура:**  
`rebuild_markov_absorbing() RETURNS void`

**Назначение:**  
Формирует поглощающую матрицу, где аварийные состояния становятся поглощающими (вероятность остаться в себе = 1.0).

**Алгоритм:**
1. Очищает `markov_absorbing`
2. Копирует все переходы из неаварийных исходных состояний (оставляя переходы в любые состояния)
3. Для каждого аварийного состояния вставляет петлю `(state_id, state_id, 1.0)`

**Логика аварийного состояния:**  
`correlation < 0 AND os_trend = -1`

---

## 4. Функции прогнозирования риска

### `predict_risk_1min()`

**Сигнатура:**  
```sql
predict_risk_1min() RETURNS TABLE(
    current_risk REAL,
    current_situation TEXT,
    current_transitions_to_risk BIGINT,
    current_total_transitions_known BIGINT
)
```

**Назначение:**  
Возвращает **вероятность перехода в аварийное состояние на следующей минуте** из текущего состояния (на основе последних метрик).

**Выходные столбцы:**

| Столбец | Тип | Описание |
|---------|-----|----------|
| `current_risk` | REAL | Вероятность (0..1). Для неизвестного состояния – априорная 0.05 |
| `current_situation` | TEXT | `risk_calculated`, `no_risk` или `unknown_state` |
| `current_transitions_to_risk` | BIGINT | Количество известных переходов из данного состояния в аварийные |
| `current_total_transitions_known` | BIGINT | Общее количество известных переходов из данного состояния (0 – если состояние не встречалось) |

**Алгоритм:**
1. Получает текущее состояние через `get_current_os_waiting_correlation_for_markov_chain`
2. Определяет `state_id` через `get_state_id`
3. Агрегирует вероятности из `markov_probabilities` для целевых аварийных состояний
4. Классифицирует ситуацию:
   - `unknown_state` – нет записей в `markov_probabilities` для этого `from_state` → риск = 0.05
   - `no_risk` – есть записи, но нет переходов в аварию → риск = 0
   - `risk_calculated` – есть хотя бы один аварийный переход → риск = сумма вероятностей

---

### `predict_risk_k_diag(k INT)`

**Сигнатура:**  
```sql
predict_risk_k_diag(k INT) RETURNS TABLE(
    risk REAL,
    situation TEXT,
    transitions_to_risk INT,
    total_transitions_known INT
)
```

**Назначение:**  
Вычисляет **вероятность попасть в аварийное состояние хотя бы один раз за `k` шагов** (минут), используя поглощающую матрицу.

**Алгоритм:**
1. Если текущее состояние не известно модели → возвращает `risk = 1 - (1-0.05)^k`
2. Инициализирует вектор вероятностей `v` размером 189, где `v[state_id] = 1`
3. Повторяет `k` раз умножение вектора на матрицу `markov_absorbing`
4. Суммирует вероятности всех аварийных состояний в полученном векторе
5. Возвращает сумму как `risk`

**Сложность:** O(k × 189 × avg_степень_исхода) – приемлемо для k до 60–120.

**Применение:**  
Прогноз риска инцидента в течение следующего часа (k=60) или смены (k=480).

---

### `predict_risk_1min_archived(p_train_date DATE, p_from_state SMALLINT)`

**Сигнатура:**  
`predict_risk_1min_archived(p_train_date DATE, p_from_state SMALLINT) RETURNS REAL`

**Назначение:**  
Вспомогательная функция для оценки прогнозов по архивным матрицам (версиям модели).

**Алгоритм:**  
Извлекает сумму вероятностей перехода из `p_from_state` в аварийные состояния из таблицы `markov_probabilities_archive` для указанной даты обучения. Если данных нет – возвращает 0.05.

**Использование:**  
Используется внутри процедур сравнения моделей (например, для расчёта Brier Score).

---

## 5. Функции оценки качества модели

### `log_forecast(p_predicted_risk REAL, p_actual_risk SMALLINT, p_model_train_date DATE, p_from_state SMALLINT DEFAULT NULL, p_to_state SMALLINT DEFAULT NULL)`

**Сигнатура:**  
см. выше

**Назначение:**  
Регистрирует прогноз и фактический исход для последующего расчёта метрик точности (Brier Score, калибровка).

**Параметры:**
- `p_predicted_risk` – предсказанная вероятность (0..1)
- `p_actual_risk` – 1 если переход произошёл в аварийное состояние, иначе 0
- `p_model_train_date` – идентификатор версии модели (дата обучения)
- `p_from_state`, `p_to_state` – опционально, для детального анализа

**Применение:**  
Вызывается в двух сценариях:
1. **Непрерывный мониторинг** – каждую минуту после получения фактического состояния
2. **Оценка разных версий модели** – прогон тестового периода по фиксированным снимкам

---

### `compare_brier_scores(test_start DATE, test_end DATE, model_date_old DATE, model_date_new DATE)`

**Сигнатура:**  
```sql
compare_brier_scores(...) RETURNS TABLE(
    older_model DATE,
    newer_model DATE,
    older_bs REAL,
    newer_bs REAL,
    bs_improvement REAL,
    sufficient BOOLEAN
)
```

**Назначение:**  
Сравнивает качество прогнозов двух версий модели (старшей и более новой) на тестовом периоде с помощью **Brier Score** – среднеквадратичной ошибки вероятностного прогноза.

**Формула:**  
`BS = 1/N * Σ (predicted_risk - actual_risk)²`

**Возвращаемые значения:**
- `older_bs`, `newer_bs` – Brier Score для каждой модели
- `bs_improvement` – улучшение (старый BS - новый BS, неотрицательное)
- `sufficient` – `TRUE`, если улучшение меньше 0.01 (критерий достаточности обучения)

**Использование:**  
Применяется в еженедельной процедуре `evaluate_training_sufficiency` для критерия C3.

---

### `check_kl_divergence()`

**Сигнатура:**  
```sql
check_kl_divergence() RETURNS TABLE(
    kl_value REAL,
    threshold REAL,
    passed BOOLEAN
)
```

**Назначение:**  
Вычисляет **KL-дивергенцию** между стационарным распределением цепи и эмпирическим распределением состояний за последнюю неделю.

**Формула:**  
`KL(π || emp) = Σ π_i * ln(π_i / emp_i)` (только для состояний с ненулевыми вероятностями в обоих распределениях)

**Алгоритм:**
1. Получает стационарное распределение `π` через `get_stationary_distribution()`
2. Подсчитывает частоту каждого состояния за последние 7 дней из `transition_log`
3. Вычисляет KL-дивергенцию
4. Возвращает `passed = (kl_value < 0.1)`

**Интерпретация:**  
`KL < 0.1` означает, что стационарное распределение хорошо описывает реальное распределение состояний – модель стабильна.

---

### `evaluate_training_sufficiency(...)`

**Сигнатура:**  
```sql
evaluate_training_sufficiency(
    test_start DATE DEFAULT NULL,
    test_end   DATE DEFAULT NULL,
    model_date_old DATE DEFAULT NULL,
    model_date_new DATE DEFAULT NULL
) RETURNS TABLE(
    criterion TEXT,
    value REAL,
    threshold TEXT,
    passed BOOLEAN,
    details TEXT
)
```

**Назначение:**  
**Основная функция верификации модели.** Проверяет четыре критерия достаточности обучения.

**Критерии:**

| Критерий | Описание | Порог |
|----------|----------|-------|
| **C1** | Для состояний, встречающихся с частотой >1%, количество наблюдений `n_i >= 50` | 0 состояний с `n_i < 50` |
| **C2** | Максимальное изменение вероятностей между текущей матрицей и снимком недельной давности | `< 0.05` |
| **C3** | Улучшение Brier Score при добавлении новой недели обучения | `< 0.01` |
| **C4** | KL-дивергенция стационарного и эмпирического распределений | `< 0.1` |

**Алгоритм:**  
- Для C1: анализирует `transition_log`, находит состояния с частотой >1%, проверяет `n_i >= 50`
- Для C2: сравнивает `markov_probabilities` с `markov_probabilities_prev_week`
- Для C3: вызывает `compare_brier_scores`, если переданы все четыре параметра
- Для C4: вызывает `check_kl_divergence()`

**Возврат:**  
Четыре строки – по одной на критерий. Каждая строка содержит: значение, порог, флаг `passed` и пояснение.

**Пример вызова:**
```sql
SELECT * FROM evaluate_training_sufficiency(
    test_start => '2025-05-19',
    test_end   => '2025-05-20',
    model_date_old => '2025-05-12',
    model_date_new => '2025-05-19'
);
```

---

## 6. Функции адаптивного забывания и архивации

### `apply_forgetting(alpha REAL)`

**Сигнатура:**  
`apply_forgetting(alpha REAL) RETURNS void`

**Назначение:**  
Экспоненциально уменьшает влияние старых наблюдений, умножая все частоты в `markov_frequencies` на `(1 - alpha)`.  
Удаляет частоты, ставшие меньше `1e-6`, и полностью пересчитывает вероятности.

**Алгоритм:**
```sql
UPDATE markov_frequencies SET frequency = frequency * (1.0 - alpha);
DELETE WHERE frequency < 1e-6;
PERFORM update_markov_probabilities();
```

**Вызов:**  
Автоматически из `markov_chain_training()` при наступлении интервала забывания (по умолчанию 1 час, alpha = 0.01).

---

### `archive_markov_probabilities(p_train_date DATE DEFAULT current_date)`

**Сигнатура:**  
`archive_markov_probabilities(p_train_date DATE) RETURNS void`

**Назначение:**  
Сохраняет снимок текущей матрицы вероятностей в архивную таблицу `markov_probabilities_archive` с указанной датой обучения.

**Алгоритм:**
1. Удаляет старые записи для этой даты (если есть)
2. Вставляет копию всех строк из `markov_probabilities`

**Использование:**  
Вызывается внутри `snapshot_markov_prev_week()` для еженедельного архивирования.

---

### `snapshot_markov_prev_week()`

**Сигнатура:**  
`snapshot_markov_prev_week() RETURNS void`

**Назначение:**  
Создаёт снимок матрицы вероятностей для сравнения через неделю.  
Вызывается по расписанию (например, каждую пятницу в 18:00).

**Алгоритм:**
1. Очищает `markov_probabilities_prev_week`
2. Копирует текущие вероятности из `markov_probabilities`
3. Архивирует текущую матрицу через `archive_markov_probabilities(current_date)`

**Пример задания в cron:**
```bash
5 18 * * 5 psql -d expecto_db -U expecto_user -c "SELECT snapshot_markov_prev_week();"
```

---

### `get_stationary_distribution(max_iter INT DEFAULT 1000, tol REAL DEFAULT 1e-6)`

**Сигнатура:**  
`get_stationary_distribution(max_iter INT, tol REAL) RETURNS REAL[]`

**Назначение:**  
Вычисляет **стационарное распределение** цепи Маркова (собственный вектор, соответствующий собственному значению 1) итеративным умножением вектора на матрицу переходов.

**Алгоритм:**
1. Инициализирует равномерное распределение `v = [1/189, ..., 1/189]`
2. Повторяет до `max_iter` или сходимости по `tol`:
   - `v_new = v * P` (матричное умножение)
   - Вычисляет сумму абсолютных разностей `diff`
   - Обновляет `v = v_new`
3. Возвращает `v` как массив `REAL[]`

**Использование:**  
Вызывается внутри `check_kl_divergence()`.

---

## 7. Вспомогательные и служебные функции

### Отсутствуют в явном виде, но упомянуты в комментариях

В коде присутствуют ссылки на предполагаемые функции, такие как `check_and_forget` (для форсированного забывания при обнаружении дрейфа) – они не реализованы, но их логика описана в комментариях к `markov_chain_training`.

---

## Рекомендации по эксплуатации

### Планировщик заданий (pg_cron или cron)

| Частота | Действие | Функция |
|---------|----------|---------|
| Каждую минуту (в рабочие часы) | Обучение модели | `markov_chain_training()` |
| Каждый час | Забывание (автоматически внутри обучения) | `apply_forgetting` (вызывается по условию) |
| Еженедельно в пятницу 18:05 | Снимок и архивация | `snapshot_markov_prev_week()` |
| Еженедельно после снимка | Проверка достаточности | `evaluate_training_sufficiency(...)` |

### Порядок инициализации

1. Создать таблицы (файл `markov_chain_tables.sql`)
2. Заполнить справочник: `SELECT fill_state_descriptions();`
3. Убедиться, что источник метрик `cluster_stat_median` существует и заполняется
4. Запустить `markov_chain_training()` вручную один раз для инициализации состояния
5. Настроить планировщик

### Мониторинг качества

- Ежедневно проверять `forecast_log` на наличие записей (должно быть ~60 × часы_работы)
- Еженедельно выполнять `evaluate_training_sufficiency` и логировать результаты
- При нарушении критериев – корректировать `alpha` и `interval_hours` в `markov_config`

---

## Зависимости между функциями

```
fill_state_descriptions
        ↓
get_state_id ← update_markov_frequency ← log_transition_and_update
        ↓                              ↓
get_current_os_waiting_correlation → markov_chain_training
        ↓                              ↓
predict_risk_1min ← update_markov_probabilities ← apply_forgetting
        ↓                              ↓
predict_risk_k_diag              rebuild_markov_absorbing
        ↓                              ↓
predict_risk_1min_archived ← archive_markov_probabilities ← snapshot_markov_prev_week

get_stationary_distribution ← check_kl_divergence ← evaluate_training_sufficiency
                                                          ↑
                                            compare_brier_scores ← log_forecast
```

---

## Примечания по производительности

- Функции, помеченные `LANGUAGE sql` или `STABLE`, выполняются быстро и могут использоваться в высокочастотных запросах.
- `predict_risk_k_diag` с большим `k` ( > 120 ) может быть затратной – рекомендуется кэшировать результат для типовых значений `k`.
- `get_stationary_distribution` итеративная, но сходится за 50–200 итераций; не вызывайте её чаще раза в сутки.
- Все функции используют только определённые таблицы и не создают блокировок на длительное время.

---

*Документация подготовлена на основе версии 10.0 марковских функций.*
