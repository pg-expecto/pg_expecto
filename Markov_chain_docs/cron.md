## Настройка cron для цепи Маркова
```
# ============================================================
# Создание снимка матрицы прошлой недели (пятница, 19:05)
# ============================================================
5 19 * * 5 psql -d expecto_db -U expecto_user -c "SELECT mchain_snapshot_prev_week();"

# ============================================================
# Ежедневная очистка журналов и обновление статистик
# ============================================================
# Очистка transition_log (в 01:15)
15 1 * * * psql -d expecto_db -U expecto_user -c "SELECT mchain_clean_transition_log();"

# Очистка forecast_log (в 01:30)
30 1 * * * psql -d expecto_db -U expecto_user -c "SELECT mchain_clean_forecast_log();"

# Обновление эталонного распределения состояний (в 01:00)
0 1 * * * psql -d expecto_db -U expecto_user -c "SELECT mchain_update_baseline();"

# Обновление статистики операционной скорости (в 01:30)
30 1 * * * psql -d expecto_db -U expecto_user -c "SELECT mchain_refresh_os_stats();"

# ============================================================
# Очистка архивных данных (раз в неделю)
# ============================================================
# Очистка архивных снимков матрицы (воскресенье, 02:00)
0 2 * * 0 psql -d expecto_db -U expecto_user -c "SELECT mchain_clean_archive();"

# Очистка forget_log (1-го числа месяца, 04:00)
0 4 1 * * psql -d expecto_db -U expecto_user -c "SELECT mchain_clean_forget_log();"

# Очистка apply_forgetting_log (ежедневно в 02:00)
0 2 * * * psql -d expecto_db -U expecto_user -c "SELECT mchain_clean_apply_forgetting_log();"```
