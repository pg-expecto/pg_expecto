#!/bin/sh
########################################################################################################
# pg_expecto_install.sh
# Инсталлятор
# version 3.0
# Исходная папка /tmp/pg_expecto
# mkdir /postgres/pg_expecto
# cp /tmp/pg_expecto/pg_expecto_install.sh /postgres/pg_expecto/
# cd /postgres/pg_expecto
# chmod 750 pg_expecto_install.sh
# ./pg_expecto_install.sh
########################################################################################################

#Обработать код возврата 
function exit_code {
ecode=$1
if [[ $ecode != 0 ]];
then
	ecode=$1
	LOG_FILE=$2
	ERR_FILE=$3
	
	echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : ERROR : Details in '$ERR_FILE
	echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : ERROR : Details in '$ERR_FILE >>$LOG_FILE
	
    exit $ecode
fi
}

script=$(readlink -f $0)
current_path=`dirname $script`

LOG_FILE=$current_path'/pg_expecto_install.log'
ERR_FILE=$current_path'/pg_expecto_install.err'

timestamp_label=$(date "+%Y%m%d")'T'$(date "+%H%M%S")

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : ИНСТАЛЛЯЦИЯ pg_expecto'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : ИНСТАЛЛЯЦИЯ pg_expecto'> $LOG_FILE


echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СОЗДАТЬ ПАПКУ ДЛЯ ОТЧЕТОВ /tmp/pg_expecto_reports'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СОЗДАТЬ ПАПКУ ДЛЯ ОТЧЕТОВ /tmp/pg_expecto_reports' >>$LOG_FILE
mkdir /tmp/pg_expecto_reports
exit_code $? $LOG_FILE $ERR_FILE

################################################################################################
# СОЗДАТЬ СТРУКТУРУ СЕРВИСНЫХ ПАПОК
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СОЗДАТЬ СЕРВИСНЫЕ ПАПКИ'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СОЗДАТЬ СЕРВИСНЫЕ ПАПКИ ' >>$LOG_FILE
mkdir $current_path'/sh' >>$LOG_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE

mkdir $current_path'/sql' >>$LOG_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE

mkdir $current_path'/sh/load_test' >>$LOG_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE

mkdir $current_path'/sh/performance_reports' >>$LOG_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE

mkdir $current_path'/sh/wait_event_kb' >>$LOG_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE
# СОЗДАТЬ СТРУКТУРУ СЕРВИСНЫХ ПАПОК
################################################################################################

################################################################################################
# СКОПИРОВАТЬ СЕРВИСНЫЕ ФАЙЛЫ
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СКОПИРОВАТЬ ФАЙЛЫ ИЗ /tmp/pg_expecto'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СКОПИРОВАТЬ ФАЙЛЫ ИЗ /tmp/pg_expecto' >>$LOG_FILE

cp -n /tmp/pg_expecto/sh/*.* $current_path'/sh/' >>$LOG_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE
for file in $current_path/sh/*.sh; do
    if [ -f "$file" ]; then
        chmod 750 "$file" >>$LOG_FILE 2>$ERR_FILE
        exit_code $? $LOG_FILE $ERR_FILE
    fi
done

cp -n /tmp/pg_expecto/sql/*.* $current_path'/sql/' >>$LOG_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE

cp -n /tmp/pg_expecto/sh/load_test/*.* $current_path'/sh/load_test/' >>$LOG_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE
for file in $current_path/sh/load_test/*.sh; do
    if [ -f "$file" ]; then
        chmod 750 "$file" >>$LOG_FILE 2>$ERR_FILE
        exit_code $? $LOG_FILE $ERR_FILE
    fi
done


cp -n /tmp/pg_expecto/sh/performance_reports/*.* $current_path'/sh/performance_reports/' >>$LOG_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE
for file in $current_path/sh/performance_reports/*.sh; do
    if [ -f "$file" ]; then
        chmod 750 "$file" >>$LOG_FILE 2>$ERR_FILE
        exit_code $? $LOG_FILE $ERR_FILE
    fi
done

cp -n /tmp/pg_expecto/sh/wait_event_kb/*.* $current_path'/sh/wait_event_kb/' >>$LOG_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE
for file in $current_path/sh/wait_event_kb/*.sh; do
    if [ -f "$file" ]; then
        chmod 750 "$file" >>$LOG_FILE 2>$ERR_FILE
        exit_code $? $LOG_FILE $ERR_FILE
    fi
done

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СЕРВИСНЫЕ СКРИПТЫ - ГОТОВЫ'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СЕРВИСНЫЕ СКРИПТЫ - ГОТОВЫ' >>$LOG_FILE
# СКОПИРОВАТЬ СЕРВИСНЫЕ ФАЙЛЫ
################################################################################################


echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СОЗДАНИЕ РЕПОЗИТОРИЯ И ТЕСТОВОЙ БД '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СОЗДАНИЕ РЕПОЗИТОРИЯ И ТЕСТОВОЙ БД' >>$LOG_FILE
psql -c "DROP DATABASE IF EXISTS expecto_db WITH (FORCE)"  >>$LOG_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE

psql -c "DROP DATABASE IF EXISTS pgbench_db WITH (FORCE)"   >>$LOG_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE

  
psql -c "DROP ROLE IF EXISTS expecto_user"  >>$LOG_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE


psql -c "CREATE USER expecto_user PASSWORD 'ChangeAfterInstall'"  >>$LOG_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE


psql -c "GRANT pg_read_all_stats TO expecto_user"  >>$LOG_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE


##################################################################################################################
# НАСТРОЙКА pgpass и pg_hba.conf
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : НАСТРОЙКА pgpass и pg_hba.conf'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : НАСТРОЙКА pgpass и pg_hba.conf' >>$LOG_FILE

pgpass_counter=`cat ~/.pgpass | grep "expecto_user" | wc -l`
if [ $pgpass_counter -ne 0 ]
then 
  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : pgpass УЖЕ СОДЕРЖИТ ЗАПИСИ ДЛЯ expecto_user'
  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : pgpass УЖЕ СОДЕРЖИТ ЗАПИСИ ДЛЯ expecto_user' >>$LOG_FILE
else
  echo '127.0.0.1:5432:expecto_db:expecto_user:ChangeAfterInstall' >> ~/.pgpass
  echo 'localhost:5432:expecto_db:expecto_user:ChangeAfterInstall' >> ~/.pgpass
  echo '127.0.0.1:5432:pgbench_db:expecto_user:ChangeAfterInstall' >> ~/.pgpass
  echo 'localhost:5432:pgbench_db:expecto_user:ChangeAfterInstall' >> ~/.pgpass
  
  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : В pgpass ДОБАВЛЕНЫ ЗАПИСИ ДЛЯ expecto_user'
  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : В pgpass ДОБАВЛЕНЫ ЗАПИСИ ДЛЯ expecto_user' >>$LOG_FILE
    
  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : НЕ ЗАБУДЬТЕ ИЗМЕНИТЬ ПАРОЛЬ роли expecto_user И НАСТРОИТЬ pgpass'
  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : НЕ ЗАБУДЬТЕ ИЗМЕНИТЬ ПАРОЛЬ роли expecto_user И НАСТРОИТЬ pgpass' >>$LOG_FILE	

fi
pg_hba=`psql -Aqtc 'SHOW hba_file'`
pghba_counter=`cat $pg_hba | grep "expecto_user" | wc -l`
if [ $pghba_counter -ne 0 ]
then 
  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : pg_hba.conf УЖЕ СОДЕРЖИТ ЗАПИСИ ДЛЯ expecto_user'
  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : pg_hba.conf УЖЕ СОДЕРЖИТ ЗАПИСИ ДЛЯ expecto_user' >>$LOG_FILE
else
  echo "#pg_expecto" >> $pg_hba
  echo "local expecto_db expecto_user md5" >> $pg_hba
  echo "local pgbench_db expecto_user md5" >> $pg_hba
  echo "#pg_expecto" >> $pg_hba
  
  psql -c "select pg_reload_conf()"
  exit_code $? $LOG_FILE $ERR_FILE  
fi 
# НАСТРОЙКА pgpass и pg_hba.conf
##################################################################################################################

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СОЗДАНИЕ РЕПОЗИТОРИЯ'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СОЗДАНИЕ РЕПОЗИТОРИЯ' >>$LOG_FILE
psql -c "CREATE DATABASE expecto_db WITH OWNER expecto_user"  >>$LOG_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : РЕПОЗИТОРИЙ - ГОТОВ'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : РЕПОЗИТОРИЙ - ГОТОВ' >>$LOG_FILE

expecto_db='expecto_db'
expecto_user='expecto_user'


################################################################################################
# НАСТРОЙКА pg_expecto
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : ДОБАВИТЬ РАСШИРЕНИЕ pg_stat_statements '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : ДОБАВИТЬ РАСШИРЕНИЕ pg_stat_statements ' >>$LOG_FILE
psql -d $expecto_db -v ON_ERROR_STOP=on --echo-errors -c 'CREATE EXTENSION pg_stat_statements ' >>$LOG_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : ДОБАВИТЬ РАСШИРЕНИЕ pg_wait_sampling '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : ДОБАВИТЬ РАСШИРЕНИЕ pg_wait_sampling ' >>$LOG_FILE
psql -d $expecto_db -v ON_ERROR_STOP=on --echo-errors -c 'CREATE EXTENSION pg_wait_sampling ' >>$LOG_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : ЗАПОЛНЕНИЕ РЕПОЗИТОРИЯ PG_EXPECTO'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : ЗАПОЛНЕНИЕ РЕПОЗИТОРИЯ PG_EXPECTO' >>$LOG_FILE
psql -d $expecto_db -U $expecto_user  -v ON_ERROR_STOP=on --echo-errors -f $current_path'/sql/pg_expecto.sql' >>$LOG_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE

psql -d $expecto_db -U $expecto_user -v ON_ERROR_STOP=on --echo-errors -Aqtc 'select default_configuration()' >>$LOG_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : ЗАПОЛНЕНИЕ KB'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : ЗАПОЛНЕНИЕ KB' >>$LOG_FILE
#psql -d $expecto_db -v ON_ERROR_STOP=on --echo-errors -f $current_path'/sh/wait_event_kb/load_to_wait_event_kb.sql' >>$LOG_FILE 2>$ERR_FILE
kb_path=$current_path'/sh/wait_event_kb/kb.txt'
psql -d $expecto_db -v ON_ERROR_STOP=on --echo-errors -c "COPY wait_event_knowledge_base (wait_event , advice ) FROM '$kb_path' WITH ( FORMAT text, DELIMITER '|', ENCODING 'UTF8' ) " >>$LOG_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE
rm -rf $current_path'/sh/wait_event_kb'
# НАСТРОЙКА pg_expecto
################################################################################################


#########################################################################################################
# СБРОС СТАТИСТИКИ 
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СБРОС СТАТИСТИКИ  '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СБРОС СТАТИСТИКИ  ' >>$LOG_FILE
psql -d $expecto_db -c "SELECT pg_stat_statements_reset()" >>$LOG_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' OK : pg_stat_statements_reset  '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' OK :  pg_stat_statements_reset  ' >>$LOG_FILE

psql -d $expecto_db -c "SELECT pg_wait_sampling_reset_profile()" >>$LOG_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' OK : pg_wait_sampling_reset_profile  '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' OK :  pg_wait_sampling_reset_profile  ' >>$LOG_FILE
# СБРОС СТАТИСТИКИ 
#########################################################################################################

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : VMSTAT - START '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : VMSTAT - START ' >>$LOG_FILE

echo 'kill vmstat'

pkill -u postgres -x "vmstat"

echo 'start vmstat'
vmstat 60 -S M -t >$current_path'/sh/vmstat.log' 2>&1 &
echo 'vmstat started'

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : VMSTAT - IN PROCESS'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : VMSTAT - IN PROCESS' >>$LOG_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : IOSTAT - START '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : IOSTAT - START ' >>$LOG_FILE

echo 'kill iostat'

pkill -u postgres -x "iostat"

echo 'start iostat'
iostat 60 -d -x -m -t >$current_path'/sh/iostat.log' 2>&1 &
echo 'iostat started '

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : IOSTAT - IN PROCESS'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : IOSTAT - IN PROCESS' >>$LOG_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : ИНСТАЛЛЯЦИЯ pg_expecto - ЗАВЕРШЕНА'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : ИНСТАЛЛЯЦИЯ pg_expecto - ЗАВЕРШЕНА' >>$LOG_FILE


echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : INFO : ДЛЯ НАЧАЛА РАБОТЫ НЕОБХОДИМО ДОБАВИТЬ В CRON'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : INFO : ДЛЯ НАЧАЛА РАБОТЫ НЕОБХОДИМО ДОБАВИТЬ В CRON' >>$LOG_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : INFO : */1 * * * * '$current_path'/sh/pg_expecto.sh'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : INFO : */1 * * * * '$current_path'/sh/pg_expecto.sh' >>$LOG_FILE
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : INFO : */1 * * * * '$current_path'/sh/load_test/load_test.sh'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : INFO : */1 * * * * '$current_path'/sh/load_test/load_test.sh' >>$LOG_FILE

exit 0
