# Описание функций марковской цепи для прогнозирования рисков

## Описание всех функций, сгруппированных по функциональному назначению.

---

## 1. Вспомогательные функции справочников и идентификации состояний

### `fill_state_descriptions()`
```sql
CREATE OR REPLACE FUNCTION fill_state_descriptions() RETURNS void
```
Заполняет таблицу `state_descriptions` всеми возможными комбинациями корреляции (21 значение: -1.0 … +1.0 с шагом 0.1) и трендов операционной скорости и ожиданий (-1, 0, 1). Итоговое количество состояний = 21×3×3 = 189.

### `get_state_id(r REAL, os_trend SMALLINT, wait_trend SMALLINT) RETURNS SMALLINT`
Вычисляет числовой идентификатор состояния по трём параметрам. Формула: `(round((r+1)/0.1) * 9) + ((os_trend+1)*3) + (wait_trend+1)`. Используется во всех функциях, где требуется кодирование состояний.

---

## 2. Функции обучения и обновления матрицы

### `update_markov_frequency(...)`
```sql
CREATE OR REPLACE FUNCTION update_markov_frequency(
    r_from REAL, os_trend_from SMALLINT, wait_trend_from SMALLINT,
    r_to   REAL, os_trend_to   SMALLINT, wait_trend_to   SMALLINT
) RETURNS void
```
Увеличивает частоту перехода `(from_state → to_state)` в таблице `markov_frequencies` на 1. Если пары не существовало, вставляется со значением 1.0.

### `log_transition_and_update(...)`
```sql
CREATE OR REPLACE FUNCTION log_transition_and_update(...) RETURNS void
```
Выполняет две операции:
- вставляет запись в `transition_log` (время, исходное и целевое состояния);
- вызывает `update_markov_frequency` для увеличения частоты перехода.

### `update_markov_probabilities() RETURNS void`
Пересчитывает таблицу `markov_probabilities` на основе текущих частот из `markov_frequencies`. Для каждого `from_state` вычисляется вероятность перехода как `frequency / SUM(frequency) OVER (PARTITION BY from_state)`. После этого автоматически вызывает `rebuild_markov_absorbing()`.

### `rebuild_markov_absorbing() RETURNS void`
Заполняет таблицу `markov_absorbing`:
- Для неаварийных состояний переносятся все переходы из `markov_probabilities` (кроме переходов в чужие аварийные состояния, которые обнуляются).
- Для аварийных состояний (корреляция < 0 и `os_trend = -1`) создаётся поглощающая петля с вероятностью 1.0.

### `markov_chain_training() RETURNS void`
**Главная функция обучения, вызываемая ежеминутно** (через внешний сборщик метрик).  
Последовательность:
1. При необходимости инициализирует `state_descriptions`.
2. Проверяет, не наступило ли время планового забывания (согласно `interval_minute` из `markov_config`). Если да – вызывает `apply_forgetting()`.
3. Получает текущие метрики (корреляцию, тренды) через `get_current_os_waiting_correlation_for_markov_chain()`.
4. Обновляет состояние в таблице `markov_chain` (сдвиг: предыдущее ← текущее, текущее ← новое).
5. Для образовавшегося перехода вычисляет прогноз риска (по `markov_probabilities`) и фактический исход (авария или нет), записывает в `forecast_log`.
6. Логирует переход и обновляет частоты через `log_transition_and_update()`.

---

## 3. Функции прогнозирования

### `predict_risk_1min()`
```sql
RETURNS TABLE (
    current_risk REAL,
    current_situation TEXT,
    current_transitions_to_risk BIGINT,
    current_total_transitions_known BIGINT
)
```
Возвращает вероятность перехода в аварийное состояние **на следующей минуте** из текущего состояния (определённого по свежим метрикам).  
- Если состояние известно, риск = сумма вероятностей переходов во все аварийные состояния.
- Если состояние неизвестно – возвращается априорная вероятность 0.05.
- Дополнительно выдаётся диагностика: `'risk_calculated'`, `'no_risk'` или `'unknown_state'`, а также количество прямых аварийных переходов и общее число известных переходов из этого состояния.

### `predict_risk_k_diag(k INT)`
```sql
RETURNS TABLE (
    risk REAL,
    situation TEXT,
    transitions_to_risk INT,
    total_transitions_known INT
)
```
Вычисляет вероятность **хотя бы одного попадания в аварийное состояние за K шагов**. Использует поглощающую матрицу `markov_absorbing` и итеративное умножение вектора начального распределения. Аналогично возвращает диагностические поля.

### `predict_risk_1min_archived(p_train_date DATE, p_from_state SMALLINT) RETURNS REAL`
Вспомогательная функция, которая по архивному снимку матрицы (`markov_probabilities_archive`) возвращает вероятность аварии для заданного состояния и даты модели. При отсутствии данных – 0.05.

---

## 4. Функции забывания (планового и форсированного)

### `apply_forgetting(alpha_override REAL DEFAULT NULL) RETURNS void`
Применяет забывание к частотам переходов:
- Умножает все `frequency` в `markov_frequencies` на `(1 - effective_alpha)`.
- Удаляет записи с частотой < 1e-6.
- Пересчитывает вероятности (`update_markov_probabilities()`).
- Обновляет `last_forget_time` в конфигурации.
- Логирует вызов в `apply_forgetting_log`.

`effective_alpha` определяется так:
  - если передан `alpha_override` – используется он;
  - иначе если `use_adaptive_alpha = true` – alpha вычисляется по экспоненциальной формуле:  
    `alpha = base_alpha * exp(-days_since_incident / incident_half_life_days)`, но не ниже `min_alpha`;
  - иначе – берётся значение `alpha` из `markov_config`.

### `emergency_forget(event_type TEXT, alpha REAL DEFAULT 0.4) RETURNS void`
Экстренное забывание по внешнему событию. Добавляет запись в `infrastructure_events` и вызывает `apply_forgetting(LEAST(alpha, 0.5))`.

### `check_and_forget() RETURNS TEXT`
**Основная процедура, запускаемая каждые 15 минут по cron** (см. `crontab.txt`).  
Выполняет:
1. Проверяет флаг `adaptive_forgetting_enabled`. Если `false` – сразу возвращает сообщение.
2. Удаляет устаревшие записи из `check_state`.
3. Рассчитывает метрики:
   - KL-дивергенцию за последний час (через `calculate_kl_divergence`)
   - χ² (через `calculate_chi_squared`)
   - отклонение операционной скорости (`get_os_deviation`)
   - Brier Score за последние 2 часа (только если наблюдений ≥ `brier_min_observations`)
   - события инфраструктуры за последний час
   - внутридневную KL-дивергенцию (текущий час против эталона)
4. Сохраняет флаги превышения порогов в `check_state`.
5. Если количество **последовательных** проверок (согласно `confirmation_cycles`), в которых хотя бы один флаг истинен, достигло порога, вычисляет `alpha_eff` (суммируя 0.1 за каждый сработавший признак) и вызывает `apply_forgetting(alpha_eff)`, логируя событие в `forget_log`.

### `enable_adaptive_forgetting(p_base_alpha REAL DEFAULT 0.1, p_min_alpha REAL DEFAULT 0.01, p_half_life_days REAL DEFAULT 7.0) RETURNS TEXT`
Включает адаптивный режим забывания: устанавливает `use_adaptive_alpha = true` и задаёт параметры `base_alpha`, `min_alpha`, `incident_half_life_days`. Возвращает подтверждающее сообщение.

### `disable_adaptive_forgetting() RETURNS TEXT`
Отключает адаптивный режим (`use_adaptive_alpha = false`). Забывание будет использовать фиксированное значение `alpha` из конфигурации.

### `get_adaptive_forgetting_status() RETURNS TEXT`
Возвращает строку с текущими параметрами адаптивного забывания.

### `set_last_incident_time(p_time TIMESTAMPTZ DEFAULT now()) RETURNS TEXT`
Вручную устанавливает время последнего инцидента (поле `last_incident_time`). Используется для тестов или внешних событий.

---

## 5. Функции оценки качества и достаточности обучения

### `compare_brier_scores(test_start DATE, test_end DATE, model_date_old DATE, model_date_new DATE)`
```sql
RETURNS TABLE (
    older_model DATE, newer_model DATE,
    older_bs REAL, newer_bs REAL,
    bs_improvement REAL,
    sufficient BOOLEAN
)
```
Сравнивает Brier Score двух моделей (старой и новой) на заданном тестовом периоде. Возвращает улучшение и флаг `sufficient` (улучшение < 0.01). Использует данные из `forecast_log`.

### `get_stationary_distribution(max_iter INT DEFAULT 1000, tol DOUBLE PRECISION DEFAULT 1e-6) RETURNS DOUBLE PRECISION[]`
Вычисляет стационарное распределение текущей матрицы вероятностей (`markov_probabilities`) методом итераций. Возвращает массив из 189 значений.

### `check_kl_divergence()`
```sql
RETURNS TABLE (kl_value DOUBLE PRECISION, threshold DOUBLE PRECISION, passed BOOLEAN)
```
Рассчитывает KL-дивергенцию между стационарным распределением модели и эмпирическим распределением состояний за последние 7 дней (из `transition_log`). Критерий `passed` = `kl_value < 0.1`.

### `evaluate_training_sufficiency(test_start DATE DEFAULT NULL, test_end DATE DEFAULT NULL, model_date_old DATE DEFAULT NULL, model_date_new DATE DEFAULT NULL)`
```sql
RETURNS TABLE (
    criterion TEXT, value REAL, threshold TEXT, passed BOOLEAN, details TEXT
)
```
**Комплексная проверка достаточности обучения** (рекомендуется запускать еженедельно). Оценивает четыре критерия:
- **C1**: для состояний с частотой >1% – количество переходов ≥ 50.
- **C2**: максимальное изменение вероятностей за две недели (по сравнению со снимком `markov_probabilities_prev_week`) – должно быть < 0.05.
- **C3**: улучшение Brier Score между старой и новой моделью – должно быть < 0.01.
- **C4**: KL-дивергенция стационарного и эмпирического распределений – < 0.1.
Для C3 необходимо передать параметры тестового периода и даты моделей, иначе критерий считается невыполненным.

---

## 6. Функции для мониторинга дрейфа (check_and_forget)

### `update_state_baseline() RETURNS void`
**Вызывается ежедневно в 01:00** (см. cron).  
Для каждого часа и дня недели вычисляет распределение состояний за последние 7 дней и сохраняет в таблицу `state_baseline`. Эталон используется для расчёта KL и χ².

### `refresh_os_stats() RETURNS void`
**Вызывается ежедневно в 01:30** (cron).  
Обновляет таблицу `operational_speed_stats` – среднее и стандартное отклонение операционной скорости за последние 20 дней для каждого часа.

### `calculate_kl_divergence(recent_minutes INT DEFAULT 60, baseline_hour INT DEFAULT NULL, baseline_dow INT DEFAULT NULL) RETURNS REAL`
Вычисляет KL-дивергенцию между распределением состояний за последние `recent_minutes` минут и эталонным распределением для заданного часа/дня. Если параметры эталона не заданы – используются текущие час и день.

### `calculate_chi_squared(recent_minutes INT DEFAULT 60, baseline_hour INT DEFAULT NULL, baseline_dow INT DEFAULT NULL) RETURNS REAL`
Аналогично вычисляет χ² критерий.

### `get_os_deviation() RETURNS REAL`
Возвращает относительное отклонение SMA20 операционной скорости от исторического среднего для текущего часа. Используется как один из признаков дрейфа.

---

## 7. Функции архивации и очистки данных (cron-задачи)

### `snapshot_markov_prev_week() RETURNS void`
**Запускается по пятницам в 19:05** (`5 19 * * 5`).  
Копирует текущую матрицу вероятностей в таблицу `markov_probabilities_prev_week`, вызывает `archive_markov_probabilities(current_date)` и обновляет `last_snapshot_date`.

### `archive_markov_probabilities(p_train_date DATE DEFAULT current_date) RETURNS void`
Сохраняет текущую матрицу вероятностей в `markov_probabilities_archive` с указанной датой обучения. Предварительно удаляет старые записи за ту же дату.

### `clean_forecast_log() RETURNS void`
**Ежедневно в 01:30** (cron). Удаляет из `forecast_log` записи старше `forecast_log_retention_days` дней.

### `clean_transition_log() RETURNS void`
**Ежедневно в 01:15** (cron). Удаляет из `transition_log` записи старше `transition_log_retention_days` дней.

### `clean_markov_probabilities_archive() RETURNS void`
**Еженедельно в воскресенье в 02:00** (cron). Удаляет архивные снимки матрицы старше `archive_retention_days` дней.

### `clean_check_state() RETURNS void`
**Ежедневно в 03:00** (cron). Удаляет из `check_state` записи старше `check_state_retention_days` дней.

### `clean_forget_log() RETURNS void`
**Ежемесячно 1-го числа в 04:00** (cron). Удаляет записи из `forget_log` старше `forget_log_retention_days` дней.

### `clean_apply_forgetting_log(p_retention_days INT DEFAULT NULL) RETURNS TEXT`
**Ежедневно в 02:00** (cron). Удаляет записи из `apply_forgetting_log` старше заданного количества дней (берётся из конфигурации или переданного параметра). Возвращает количество удалённых строк.

---

## 8. Прочие сервисные функции

### `get_current_os_waiting_correlation_for_markov_chain()`
```sql
RETURNS TABLE (current_correlation REAL, current_os_trend SMALLINT, current_wait_trend SMALLINT)
```
На основе таблицы `cluster_stat_median` (поставляется отдельно) вычисляет за последний час:
- коэффициент корреляции Пирсона между операционной скоростью и ожиданиями (округляется до 0.1);
- знак угла наклона линии регрессии для скорости (тренд: -1, 0, 1);
- знак угла наклона для ожиданий (тренд: -1, 0, 1).
Используется в `markov_chain_training` и функциях прогноза.

### `log_forecast(p_predicted_risk REAL, p_actual_risk SMALLINT, p_model_train_date DATE, p_from_state SMALLINT DEFAULT NULL, p_to_state SMALLINT DEFAULT NULL) RETURNS void`
Предназначена для ручной записи прогнозов (например, при тестировании или сравнении моделей). В обычном режиме `forecast_log` заполняется автоматически внутри `markov_chain_training`.

### `update_last_incident_time() RETURNS TRIGGER`
Триггерная функция, вызываемая **после вставки в `transition_log`**. Если целевое состояние является аварийным, обновляет `last_incident_time` в `markov_config`.

### `sync_model_date_from_archive() RETURNS TEXT`
Утилита для миграции: устанавливает `last_snapshot_date` в конфигурации равной максимальной дате из `markov_probabilities_archive`.

### `reset_markov_chain() RETURNS TEXT`
**Полный сброс модели** – очищает все таблицы, содержащие данные модели: `markov_frequencies`, `markov_probabilities`, `markov_absorbing`, `transition_log`, `forecast_log`, `markov_chain`, `apply_forgetting_log`, `forget_log`, `check_state`, `markov_probabilities_prev_week`, `markov_probabilities_archive`, `state_baseline`, `operational_speed_stats`. Сбрасывает служебные поля в `markov_config`. Заново создаёт справочник состояний. Конфигурация (пороги, alpha и т.д.) не изменяется.

### `get_forget_log(p_start TIMESTAMPTZ DEFAULT now() - INTERVAL '7 days', p_end TIMESTAMPTZ DEFAULT now()) RETURNS TEXT[]`
Возвращает массив строк из таблицы `forget_log` за указанный период в удобочитаемом виде.

### `get_apply_forgetting_log(p_start TIMESTAMPTZ DEFAULT now() - INTERVAL '7 days', p_end TIMESTAMPTZ DEFAULT now()) RETURNS TEXT[]`
Аналогично возвращает массив строк из `apply_forgetting_log`.

---

## Связь с заданиями cron

| Cron-выражение | Вызываемая функция | Назначение |
|----------------|--------------------|-------------|
| `*/15 * * * *` | `check_and_forget()` | Мониторинг дрейфа и форсированное забывание |
| `5 19 * * 5` | `snapshot_markov_prev_week()` | Еженедельный снимок матрицы и архивация |
| `30 1 * * *` | `clean_forecast_log()` | Очистка лога прогнозов |
| `15 1 * * *` | `clean_transition_log()` | Очистка лога переходов |
| `0 1 * * *` | `update_state_baseline()` | Обновление эталонных распределений |
| `30 1 * * *` | `refresh_os_stats()` | Обновление статистики операционной скорости |
| `0 2 * * 0` | `clean_markov_probabilities_archive()` | Удаление старых архивов матриц |
| `0 3 * * *` | `clean_check_state()` | Очистка истории проверок |
| `0 4 1 * *` | `clean_forget_log()` | Очистка журнала забываний |
| `0 2 * * *` | `clean_apply_forgetting_log()` | Очистка журнала вызовов apply_forgetting |

> **Примечание:** Функция `markov_chain_training()` **не вызывается через cron** – она запускается внешним сборщиком метрик (каждую минуту) после получения медианных значений операционной скорости и ожиданий.
