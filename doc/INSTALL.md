# ВАЖНО
******
Для работы расширения pg_expecto **требуются** установленные библиотеки расширений **pg_stat_statments** и **pg_wait_sampling**

**Значение параметра shared_preload_libraries должно быть:**

**shared_preload_libraries='pg_stat_statments , pg_wait_sampling'**

**Порядок библиотек ВАЖЕН**
******
# 1.установка расширения pg_expecto

## Без использования development-пакетов

### postgres

Уточнить расположение **SHAREDIR/extension**

Например */opt/pgpro/ent-17/share/extension/*

1. Сохранить файлы: pg_expecto--3.0.sql pg_expecto.control в папке **/tmp**

2. mkdir /postgres/pge

3. cp /tmp/pg_expecto--3.0.sql /postgres/pge

4. cp /tmp/pg_expecto.control /postgres/pge

### c правами root

1. cd /postgres/pge/

2. cp pg_expecto--3.0.sql   **SHAREDIR/extension**

3. cp pg_expecto.control  **SHAREDIR/extension**

## Используя development-пакеты

### postgres

1. Сохранить файлы: pg_expecto--3.0.sql pg_expecto.control в папке **/tmp**

2. mkdir /postgres/pge

3. cp /tmp/pg_expecto--3.0.sql /postgres/pge

4. cp /tmp/pg_expecto.control /postgres/pge

### c правами root

1. cd /postgres/pge/

2. make install

 

# 2.Установка и настройка сервисных скриптов pg_expecto

## 2.1 Сохранить дистрибутив в папке /tmp 
- Архив, содержащий сервисные скрипты: pg_expecto.zip
- Инсталлятор: pg_expecto_install.sh

## 2.2 Создать сервисные папки

1. mkdir -p /postgres/scripts/

2. mkdir -p /postgres/pg_expecto

3. mkdir /tmp/pg_expecto_reports

## 2.3 Установить расширение и сервисные скрипты

1. cp /tmp/pg_expecto_install.sh /postgres/scripts/

2. chmod 755 *.sh

3. cd /postgres/scripts/

4. ./pg_expecto_install.sh

## 2.4 Добавить в pg_hba.conf

-local   expecto_db    expecto_user md5

-local   pgbench_db    expecto_user md5

## 2.5 Добавить в crontab

*/1 * * * * /postgres/pg_expecto/pg_expecto.sh

*/1 * * * * /postgres/pg_expecto/load_test/load_test.sh

## 3. Контроль работоспособности pg_expecto

- tail -f /postgres/pg_expecto/pg_expecto.log

## PS.

После инсталляции, рекомендуется измененить пароль для роли **expecto_user** и отредактировать файл **pgpass**
