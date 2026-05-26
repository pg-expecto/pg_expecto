## Настройка cron для цепи Маркова
```
# Основная процедура check_and_forget
*/15 * * * * psql -d expecto_db -U expecto_user  -c "SELECT check_and_forget()"

# Функция создания/обновления снимка матрицы прошлой недели
5 19 * * 5 psql -d expecto_db -U expecto_user  -c "SELECT snapshot_markov_prev_week();"

# Ежедневная очистка forecast_log в 01:30
30 1 * * * psql -d expecto_db -U expecto_user -c "SELECT clean_forecast_log()"

# Ежедневная очистка transition_log в 01:15
15 1 * * * psql -d expecto_db -U expecto_user -c "SELECT clean_transition_log()"

# Ежедневное обновление эталонного распределения состояний (в 01:00)
0 1 * * * psql -d expecto_db -U expecto_user -c "SELECT update_state_baseline()"

# Ежедневное обновление статистики операционной скорости (в 01:30)
30 1 * * * psql -d expecto_db -U expecto_user -c "SELECT refresh_os_stats()"

# Очистка архивных снимков матрицы (раз в неделю, в воскресенье в 02:00)
0 2 * * 0 psql -d expecto_db -U expecto_user -c "SELECT clean_markov_probabilities_archive()"

# Очистка check_state (ежедневно в 03:00)
0 3 * * * psql -d expecto_db -U expecto_user -c "SELECT clean_check_state()"

# Очистка forget_log (раз в месяц, например, 1-го числа в 04:00)
0 4 1 * * psql -d expecto_db -U expecto_user -c "SELECT clean_forget_log()"

#  Очистка журнала apply_forgetting_log (каждый день в 02:00):
0 2 * * * psql -d expecto_db -U expecto_user -c "SELECT clean_apply_forgetting_log();"
```
