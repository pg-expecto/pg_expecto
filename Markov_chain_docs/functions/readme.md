# Функции цепи Маркова для прогнозирования инцидентов производительности PostgreSQL

## Обзор

В данном разделе представлено полное описание функций, реализующих **дискретную цепь Маркова** для мониторинга и прогнозирования аварийных состояний СУБД PostgreSQL. Функции обеспечивают:

- Пошаговое обучение модели на основе реальных переходов между состояниями производительности.
- Адаптивное забывание устаревших наблюдений с динамическим коэффициентом `alpha`.
- Прогнозирование риска аварии на горизонтах от 1 минуты до 1 часа.
- Оценку достоверности прогнозов и диагностику модели.
- Автоматическую очистку журналов и архивирование матриц вероятностей.

Все функции имеют префикс `mchain_` (Markov chain) и используют таблицы, описанные в `markov_chain_tables.sql`.

---

## 1. Функции получения текущих метрик и идентификации состояний

### `get_current_os_waiting_correlation_for_markov_chain()`

**Назначение** – получение текущих значений производительности из аналитической таблицы `pgh_stat_cluster_analysis` (окно 1 час).

**Сигнатура**
```sql
get_current_os_waiting_correlation_for_markov_chain()
RETURNS TABLE (
    current_correlation REAL,
    current_os_trend    SMALLINT,
    current_wait_trend  SMALLINT
)
```

**Логика работы**
- Проверяет существование таблицы `pgh_stat_cluster_analysis`.
- Вычисляет коэффициент корреляции Пирсона между операционной скоростью (`op_speed_long`) и временем ожидания (`waitings_long`) за последний час.
- Строит линейную регрессию для скорости и для ожиданий по времени; угол наклона преобразуется в тренд (`-1`, `0`, `+1`).
- При отсутствии данных или ошибках возвращает `(0.0, 0, 0)`.

**Используется в** – `mchain_train_step()`, `mchain_predict_risk_*`, `mchain_get_current_state_id()`.

---

### `fill_state_descriptions()`

**Назначение** – заполнение справочника `state_descriptions` всеми 189 комбинациями корреляции (от -1.0 до +1.0 с шагом 0.1) и трендов (`-1,0,1` для OS и wait).

**Сигнатура**
```sql
fill_state_descriptions() RETURNS void
```

**Логика**
- `TRUNCATE state_descriptions`.
- `INSERT` из `generate_series(0,20) AS c_idx`, перекрёстного с `generate_series(-1,1)` для OS и wait.
- `state_id = c_idx * 9 + (os+1)*3 + (wt+1)`.

**Вызов** – один раз при первом запуске `mchain_train_step()`.

---

### `get_state_id(r REAL, os_trend SMALLINT, wait_trend SMALLINT)`

**Назначение** – преобразование тройки параметров в числовой идентификатор состояния (0…188).

**Сигнатура**
```sql
get_state_id(r REAL, os_trend SMALLINT, wait_trend SMALLINT) RETURNS SMALLINT
```
**Атрибуты** – `IMMUTABLE`, `LANGUAGE sql`.

**Формула**
```sql
(round((round(r,1) + 1.0) / 0.1)::int * 9) + ((os_trend + 1) * 3) + (wait_trend + 1)
```

---

### `mchain_get_current_state_id()`

**Назначение** – вспомогательная функция для отладки; возвращает `state_id` текущего состояния системы.

**Сигнатура**
```sql
mchain_get_current_state_id() RETURNS SMALLINT
```

**Логика** – вызывает `get_current_os_waiting_correlation_for_markov_chain()` и `get_state_id()`. При отсутствии данных возвращает `NULL`.

---

## 2. Основной цикл обучения

### `mchain_train_step()`

**Назначение** – главный шаг обучения, который должен вызываться **каждую минуту** (например, через cron или pgAgent).

**Сигнатура**
```sql
mchain_train_step() RETURNS TEXT
```

**Логика**
1. Проверяет и при необходимости инициализирует `state_descriptions`.
2. Получает текущие метрики через `get_current_os_waiting_correlation_for_markov_chain()`.
3. Вычисляет `curr_state` через `get_state_id()`.
4. Читает последнее состояние из таблицы `markov_chain`.
   - Если цепь пуста – сохраняет только текущее состояние и возвращает `'Initial state saved'`.
5. Определяет `prev_state` как предыдущее `curr_correlation/trends`.
6. Логирует переход через `mchain_log_transition(prev_state, curr_state)`.
7. Обновляет `markov_chain` (сдвигает предыдущее состояние).
8. Проверяет, не пора ли применить забывание:  
   `now() - last_forget_time >= interval_minute`. Если да – вызывает `mchain_apply_forgetting()`.

**Возвращаемое значение** – текстовая диагностика: `'Step completed'` или сообщение об ошибке.

**Обработка ошибок** – все исключения логируются в `mchain_error_log`.

---

### `mchain_log_transition(p_from_state SMALLINT, p_to_state SMALLINT)`

**Назначение** – запись перехода в журнал и обновление накопленных частот.

**Сигнатура**
```sql
mchain_log_transition(p_from_state SMALLINT, p_to_state SMALLINT) RETURNS void
```

**Логика**
- `INSERT INTO transition_log (ts, from_state, to_state) VALUES (now(), ...)`.
- `INSERT INTO markov_frequencies ... ON CONFLICT DO UPDATE SET frequency = frequency + 1`.

---

## 3. Управление забыванием (Forgetting)

### `mchain_apply_forgetting(alpha_override REAL DEFAULT NULL)`

**Назначение** – применение забывания: уменьшение всех частот, удаление шумов, пересчёт вероятностей и поглощающей матрицы.

**Сигнатура**
```sql
mchain_apply_forgetting(alpha_override REAL DEFAULT NULL) RETURNS void
```

**Логика**
1. Читает параметры из `markov_config`:
   - `adaptive_forgetting_enabled` – если `false`, выход.
   - `use_adaptive_alpha`, `base_alpha`, `min_alpha`, `incident_half_life_days`, `last_incident_time`.
2. Проверяет достаточность обучения через `mchain_check_sufficiency()`.
3. Вычисляет `effective_alpha`:
   - Если `alpha_override` задан – использует его.
   - Иначе если `use_adaptive_alpha`:
     - Если `last_incident_time IS NULL` → `effective_alpha = min_alpha`.
     - Иначе `days_since = (now() - last_incident_time)/86400`  
       `alpha = base_alpha * exp(-days_since / half_life)`, затем `GREATEST(alpha, min_alpha)`.
   - Иначе – фиксированный `alpha` из конфигурации.
4. Если `effective_alpha <= 0` – выход без изменений.
5. Выполняет:
   ```sql
   UPDATE markov_frequencies SET frequency = frequency * (1.0 - effective_alpha);
   DELETE FROM markov_frequencies WHERE frequency < 1e-6;
   PERFORM update_markov_probabilities();   -- пересчёт вероятностей
   UPDATE markov_config SET last_forget_time = now();
   ```
6. Логирует вызов в `apply_forgetting_log`.

**Комментарий** – используется механизм адаптивного забывания: чем дольше не было аварий, тем медленнее забывание.

---

### `mchain_check_sufficiency(min_transitions INT DEFAULT NULL, max_prob_change REAL DEFAULT 0.05, weeks_history INT DEFAULT 2)`

**Назначение** – проверка, достаточно ли накоплено данных для применения забывания и получения достоверных прогнозов.

**Сигнатура**
```sql
mchain_check_sufficiency(...) RETURNS BOOLEAN
```

**Логика**
- Проверяет общее число переходов в `transition_log` ≥ `min_transitions` (из `markov_config.min_transitions_for_forgetting` или переданного).
- Если данных достаточно (≥ 2× минимума), вычисляет максимальное изменение вероятностей между двумя периодами (последние `weeks_history/2` недель и предшествующие `weeks_history/2` недель). Если изменение > `max_prob_change` – возвращает `FALSE`.
- Возвращает `TRUE`, если оба условия выполнены.

---

### `update_last_incident_time()`

**Назначение** – триггерная функция, обновляющая `markov_config.last_incident_time` при каждом переходе в **аварийное состояние** (корреляция < 0 и OS trend = -1).

**Сигнатура**
```sql
update_last_incident_time() RETURNS TRIGGER
```

**Привязка** – триггер `trigger_update_incident_time` на `transition_log` (AFTER INSERT).

---

### `mchain_enable_forgetting_when_sufficient()`

**Назначение** – ручное включение адаптивного забывания только если модель достаточно обучена.

**Сигнатура**
```sql
mchain_enable_forgetting_when_sufficient() RETURNS TEXT
```

**Логика** – вызывает `mchain_check_sufficiency()`; при `TRUE` устанавливает `adaptive_forgetting_enabled = true`.

---

### `mchain_force_enable_forgetting()`

**Назначение** – принудительное включение забывания (без проверки).

**Сигнатура**
```sql
mchain_force_enable_forgetting() RETURNS TEXT
```

---

## 4. Расчёт вероятностей и поглощающей матрицы

### `update_markov_probabilities()`

**Назначение** – пересчёт матрицы условных вероятностей из `markov_frequencies`.

**Сигнатура**
```sql
update_markov_probabilities() RETURNS void
```

**Логика**
- `TRUNCATE markov_probabilities`.
- `INSERT INTO markov_probabilities (from_state, to_state, probability)`  
  `SELECT from_state, to_state, frequency / SUM(frequency) OVER (PARTITION BY from_state) FROM markov_frequencies WHERE frequency > 0`.
- Вызывает `rebuild_markov_absorbing()`.

---

### `rebuild_markov_absorbing()`

**Назначение** – построение поглощающей матрицы: аварийные состояния становятся «чёрными дырами» (вероятность остаться = 1).

**Сигнатура**
```sql
rebuild_markov_absorbing() RETURNS void
```

**Логика**
- `TRUNCATE markov_absorbing`.
- Вставка переходов из `markov_probabilities` для всех **неаварийных** исходных состояний (условие: `NOT (correlation < 0 AND os_trend = -1)`).
- Для каждого аварийного состояния вставляется строка `(state_id, state_id, 1.0)`.

---

### `archive_markov_probabilities(p_train_date DATE DEFAULT current_date)`

**Назначение** – сохранение текущей матрицы вероятностей в архивную таблицу.

**Сигнатура**
```sql
archive_markov_probabilities(p_train_date DATE DEFAULT current_date) RETURNS void
```

**Логика**
- Удаляет старые записи за указанную дату.
- Вставляет снимок из `markov_probabilities`.
- Обновляет `markov_config.last_snapshot_date`.

---

### `mchain_snapshot_prev_week()`

**Назначение** – создание еженедельного снимка для сравнения стабильности модели.

**Сигнатура**
```sql
mchain_snapshot_prev_week() RETURNS void
```

**Логика**
- Копирует `markov_probabilities` → `markov_probabilities_prev_week`.
- Вызывает `archive_markov_probabilities(current_date)`.
- Обновляет `last_snapshot_date`.

**Вызов по cron** – пятница, 19:05.

---

## 5. Прогнозирование риска аварии

### `mchain_predict_risk_1min()`

**Назначение** – одношаговый прогноз: вероятность попасть в аварийное состояние **на следующей минуте**.

**Сигнатура**
```sql
mchain_predict_risk_1min() RETURNS TABLE (
    risk REAL,
    curr_situation TEXT,
    curr_transitions_to_risk BIGINT,
    curr_total_transitions_known BIGINT
)
```

**Логика**
- Определяет текущее состояние.
- Ищет в `markov_probabilities` прямые переходы из этого состояния в любое аварийное.
- Возвращает:
  - `risk` – суммарная вероятность (0.05, если состояние неизвестно).
  - `curr_situation` – `'unknown_state'`, `'no_risk'` или `'risk_calculated'`.
  - `curr_transitions_to_risk` – количество аварийных целевых состояний.
  - `curr_total_transitions_known` – общее число известных переходов из данного состояния.

---

### `mchain_predict_risk_k(k INT)`

**Назначение** – универсальный прогноз риска **хотя бы одного попадания в аварию** за `k` шагов (минут) с использованием поглощающей матрицы.

**Сигнатура**
```sql
mchain_predict_risk_k(k INT) RETURNS TABLE (
    risk REAL,
    curr_situation TEXT,
    curr_transitions_to_risk INT,
    curr_total_transitions_known INT
)
```

**Логика**
1. Получает текущее состояние, определяет список аварийных состояний.
2. Если состояние неизвестно – возвращает априорный риск:  
   `risk = 1 - (1 - 0.05)^k`.
3. Инициализирует вектор вероятностей `v` размером 189 (единица на текущем состоянии).
4. `k` раз умножает вектор на матрицу `markov_absorbing` (матричное умножение разреженным способом).
5. Суммирует вероятности по аварийным состояниям – это и есть риск.
6. Возвращает результат вместе с диагностической информацией.

**Ограничения** – рекомендуется `k` от 1 до 60.

---

### Функции-обёртки для типовых горизонтов

| Функция | Эквивалент |
|---------|------------|
| `mchain_predict_risk_15min()` | `mchain_predict_risk_k(15)` |
| `mchain_predict_risk_30min()` | `mchain_predict_risk_k(30)` |
| `mchain_predict_risk_1hour()` | `mchain_predict_risk_k(60)` |

Все возвращают ту же структуру строк.

---

## 6. Оценка качества и достоверности прогнозов

### `mchain_forecast_reliability()`

**Назначение** – оценка достоверности прогнозов по шкале **0…5**.

**Сигнатура**
```sql
mchain_forecast_reliability() RETURNS INT
```

**Критерии**
- **0** – менее 100 переходов.
- **1** – 100…499 переходов.
- **2** – 500…4999 переходов.
- **3** – ≥5000 переходов (база).
- **+1** (до 4) – стабильность вероятностей: максимальное изменение за 14 дней < 0.05.
- **+1** (до 5) – покрытие частых состояний (с частотой >1%) ≥90%.

**Использование** – для принятия решения, насколько можно доверять прогнозам риска.

---

### `mchain_reliability_report()`

**Назначение** – расширенный текстовый отчёт о достоверности модели.

**Сигнатура**
```sql
mchain_reliability_report() RETURNS TEXT
```

**Содержание отчёта**
- Общий рейтинг и его интерпретация.
- Общее число переходов vs порог `min_transitions_for_forgetting`.
- Максимальное изменение вероятностей и статус стабильности.
- Покрытие частых состояний в процентах.
- Рекомендации (продолжить обучение, использовать с осторожностью, модель готова).

---

## 7. Функции очистки и обслуживания (cron-задачи)

Все функции ниже удаляют устаревшие записи из соответствующих таблиц на основе retention-периодов, заданных в `markov_config` или переданных параметром.

| Функция | Таблица | Retention по умолчанию | Cron-расписание |
|---------|---------|----------------------|-----------------|
| `mchain_clean_transition_log(p_retention_days INT DEFAULT NULL)` | `transition_log` | 21 день | ежедневно в 01:15 |
| `mchain_clean_forecast_log(p_retention_days INT DEFAULT NULL)` | `forecast_log` | 21 день | ежедневно в 01:30 |
| `mchain_clean_archive(p_retention_days INT DEFAULT NULL)` | `markov_probabilities_archive` | 21 день | воскресенье, 02:00 |
| `mchain_clean_forget_log(p_retention_days INT DEFAULT 90)` | `forget_log` | 90 дней | 1-е число месяца, 04:00 |
| `mchain_clean_apply_forgetting_log(p_retention_days INT DEFAULT NULL)` | `apply_forgetting_log` | 21 день | ежедневно в 02:00 |

**Сигнатура** – все возвращают `TEXT` с количеством удалённых строк.

---

## 8. Функции обновления вспомогательных статистик

### `mchain_update_baseline()`

**Назначение** – обновление эталонного распределения состояний по часам дня и дням недели (таблица `state_baseline`). Используется для KL-дивергенции.

**Сигнатура**
```sql
mchain_update_baseline() RETURNS void
```

**Логика** – для каждого дня недели (1..7) и часа (0..23) вычисляет частоту каждого состояния за последние 7 дней и сохраняет в `state_baseline` (upsert).

**Cron** – ежедневно в 01:00.

---

### `mchain_refresh_os_stats()`

**Назначение** – расчёт средней операционной скорости и её стандартного отклонения по часам за последние 20 дней. Результат сохраняется в `operational_speed_stats`.

**Сигнатура**
```sql
mchain_refresh_os_stats() RETURNS void
```

**Cron** – ежедневно в 01:30.

---

## 9. Логирование ошибок

### `mchain_log_error(p_function_name TEXT, p_error_message TEXT, p_error_detail TEXT, p_error_hint TEXT, p_context JSONB)`

**Назначение** – централизованная запись ошибок в таблицу `mchain_error_log` с одновременным выводом `RAISE WARNING`.

**Сигнатура**
```sql
mchain_log_error(...) RETURNS void
```

**Используется** внутри всех `mchain_*` функций в блоках `EXCEPTION`.

---

## 10. Полный cron-расписание (из файла `crontab.txt`)

| Время | Команда | Назначение |
|-------|---------|------------|
| `5 19 * * 5` | `SELECT mchain_snapshot_prev_week();` | Еженедельный снимок матрицы (пятница) |
| `15 1 * * *` | `SELECT mchain_clean_transition_log();` | Очистка журнала переходов |
| `30 1 * * *` | `SELECT mchain_clean_forecast_log();` | Очистка журнала прогнозов |
| `0 1 * * *` | `SELECT mchain_update_baseline();` | Обновление эталонного распределения |
| `30 1 * * *` | `SELECT mchain_refresh_os_stats();` | Обновление статистики OS |
| `0 2 * * 0` | `SELECT mchain_clean_archive();` | Очистка архива матриц (воскресенье) |
| `0 4 1 * *` | `SELECT mchain_clean_forget_log();` | Очистка forget_log (1-е число) |
| `0 2 * * *` | `SELECT mchain_clean_apply_forgetting_log();` | Очистка журнала забывания |

**Дополнительно** – основная функция `mchain_train_step()` должна запускаться **каждую минуту** (в crontab не указана, но подразумевается отдельной строкой).

---

## Заключение

Представленный набор функций образует **полноценную адаптивную цепь Маркова** для прогнозирования аварий производительности PostgreSQL. Ключевые особенности:

- Дискретизация пространства состояний (189 узлов).
- Обучение в реальном времени с забыванием.
- Адаптивный коэффициент забывания на основе времени после последнего инцидента.
- Многошаговый прогноз риска через поглощающие состояния.
- Встроенные метрики достоверности и отчётность.
- Полная автоматизация через cron.

Использование этих функций позволяет **упреждающе** выявлять вероятность перехода системы в аварийный режим и принимать меры до наступления инцидента.
