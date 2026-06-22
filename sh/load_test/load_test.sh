#!/bin/sh
# Copyright 2026 Ринат (pg_expecto)
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#####################################################################################
# load_test.sh
# version 12.2
# 22.06.2026
#####################################################################################
# Нагрузочное тестирование
# 
# */1 * * * * /postgres/pg_expecto/load_test/load_test.sh
#####################################################################################
 
#Обработать код возврата 
function exit_code {
ecode=$1
if [[ $ecode != 0 ]];
then
	ecode=$1
	LOG_FILE=$2
	ERR_FILE=$3
	
	echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : ERROR : Details in '$ERR_FILE
	echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : ERROR : Details in '$ERR_FILE >> $LOG_FILE
	
	$current_path'/'load_test_stop.sh
	
    exit $ecode
fi
}

#script=$(readlink -f $0)
#current_path=`dirname $script`
current_path="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
#echo "Скрипт находится в: $current_path"

expecto_db='expecto_db'
pgbench_db='pgbench_db'
expecto_user='expecto_user'

LOG_FILE=$current_path'/load_test.log'
ERR_FILE=$current_path'/load_test.err'
PROGRESS_FILE=$current_path'/load_test.progress'
TIMESTAMP_LOG_FILE=$current_path'/timestamp.log'

#################################################
#Если тест не начат - выход
if [ ! -f $current_path'/LOAD_TEST_STARTED' ]; 
then

  exit 0
fi
#################################################


#################################################
# Если флаг поднят - выход
if [ -f $current_path'/LOAD_TEST_IN_PROGRESS' ]; 
then
  current_pass=`psql -d $expecto_db -U $expecto_user -Aqtc 'select load_test_current_pass()' 2>$ERR_FILE`
  pgbench_clients=`psql -d $expecto_db -U $expecto_user -Aqtc 'select load_test_get_load()'` 2>$ERR_FILE  
  
  period_hours=`$current_path'/'get_conf_param.sh $current_path period_hours 2>$ERR_FILE`
  exit_code $? $LOG_FILE $ERR_FILE

  average_load=`$current_path'/'get_conf_param.sh $current_path average_load 2>$ERR_FILE`
  exit_code $? $LOG_FILE $ERR_FILE

if [ "$period_hours" != "0" ] && [ "$average_load" != "0" ]
then
 vacuum_incident=`$current_path'/'get_conf_param.sh $current_path vacuum_incident 2>$ERR_FILE`
 exit_code $? $LOG_FILE $ERR_FILE
 if [ "$vacuum_incident" == "1" ]
 then 
   echo 'INFO : ВКЛЮЧЕНА ДОПОЛНИТЕЛЬНАЯ НАГРУЗКА VACUUM/FREEZE ДЛЯ ИММИТАЦИИ ИНЦИДЕНТА' >> $LOG_FILE
   /postgres/pg_expecto/sh/load_test/run_incident.sh >> /postgres/pg_expecto/sh/load_test/run_incident.log 2>&1  
  fi
  
  let total_intervals=6+period_hours*6
  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : ИТЕРАЦИЯ :'$current_pass' : ПУАССОНОВСКАЯ НАГРУЗКА: total_intervals='$total_intervals' period_hours='$period_hours' pgbench_clients='$pgbench_clients' average_load='$average_load >> $LOG_FILE
else
  finish_load=`$current_path'/'get_conf_param.sh $current_path finish_load 2>$ERR_FILE`
  exit_code $? $LOG_FILE $ERR_FILE
  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : ИТЕРАЦИЯ : '$current_pass' ЭКСПОНЕНЦИАЛЬНАЯ НАГРУЗКА: pgbench_clients='$pgbench_clients ' finish_load='$finish_load >> $LOG_FILE
fi  

  psql -d $expecto_db -U $expecto_user -Aqtc 'select load_test_set_scenario_queryid()' 2>$ERR_FILE
  exit_code $? $LOG_FILE $ERR_FILE	
  
  exit 0
fi
#################################################

#################################################
# Поднять флаг
touch  $current_path'/LOAD_TEST_IN_PROGRESS'
#################################################

###########################################################################################################
# НАЧАТЬ СБОР ДАННЫХ 	
start_collect_data_result=`psql -d $expecto_db -U $expecto_user -Aqtc 'select load_test_start_collect_data()' 2>>$ERR_FILE`
exit_code $? $LOG_FILE $ERR_FILE
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : СБОР ДАННЫХ ПО НАГРУЗОЧНОМУ ТЕСТУ'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : СБОР ДАННЫХ ПО НАГРУЗОЧНОМУ ТЕСТУ' > $LOG_FILE
# НАЧАТЬ СБОР ДАННЫХ 	
###########################################################################################################

##############################################################################################################
# КЛИЕНТЫ И ВРЕМЯ - PGBENCH
current_pass=`psql -d $expecto_db -U $expecto_user -Aqtc 'select load_test_current_pass()' 2>$ERR_FILE`
exit_code $? $LOG_FILE $ERR_FILE

pgbench_clients=`psql -d $expecto_db -U $expecto_user -Aqtc 'select load_test_set_load()'` 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE

pg_bench_time="600"

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : Текущий проход: '$current_pass
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : Текущий проход: '$current_pass >> $LOG_FILE	
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : Количество сессий: '$pgbench_clients
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : Количество сессий: '$pgbench_clients >> $LOG_FILE
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : Время теста в секундах: '$pg_bench_time
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : Время теста в секундах: '$pg_bench_time >> $LOG_FILE
	
#--jobs=потоки Число рабочих потоков в pgbench. Использовать нескольких потоков может быть полезно на многопроцессорных компьютерах
jobs=`cat /proc/cpuinfo|grep processor|wc -l`
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : jobs= '$jobs
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : jobs= '$jobs>> $LOG_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : ИТЕРАЦИЯ pg_bench '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : ИТЕРАЦИЯ pg_bench ' >> $LOG_FILE
#############################################################################
# PGBENCH
touch $current_path'/PGBENCH_WORKING'

testdb=`$current_path'/'get_conf_param.sh $current_path testdb 2>$ERR_FILE`
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : ТЕСТОВАЯ БД  = '$testdb
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : ТЕСТОВАЯ БД  = '$testdb >> $LOG_FILE	

current_test_id=`psql -d $expecto_db -U $expecto_user -Aqtc "SELECT load_test_get_current_test_id()"` 2>$ERR_FILE
max_sc_count=`psql -d $expecto_db -U $expecto_user -Aqtc "SELECT count(id) FROM testing_scenarios WHERE test_id = $current_test_id"` 2>$ERR_FILE

#################################################################################
# ЕСЛИ ТЕСТОВАЯ БД - ПО УМОЛЧАНИЮ
if [ "$testdb" == "default" ]
then 
  for (( scenario_id=1; scenario_id <= max_sc_count; scenario_id++ ))
  do
    pgbench_clients=`psql -d $expecto_db -U $expecto_user -Aqtc "select load_test_get_load_by_scenario("$scenario_id")"` 2>$ERR_FILE
    exit_code $? $LOG_FILE $ERR_FILE

	pgbench_param='--file='$current_path'/do_scenario'$scenario_id'.sql --protocol=extended --report-per-command --jobs='"$jobs"' --client='"$pgbench_clients"' --time='"$pg_bench_time"' '$pgbench_db		
	
	echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : СЦЕНАРИЙ-'$scenario_id': pgbench_clients= '$pgbench_clients
    echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : СЦЕНАРИЙ-'$scenario_id': pgbench_clients= '$pgbench_clients>> $LOG_FILE
	
    echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : СЦЕНАРИЙ-'$scenario_id': pgbench_param= '$pgbench_param
    echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : СЦЕНАРИЙ-'$scenario_id': pgbench_param= '$pgbench_param>> $LOG_FILE
	
	pgbench --username=expecto_user $pgbench_param & >>$LOG_FILE 2>$PROGRESS_FILE
  done
  wait
  exit_code $? $LOG_FILE $PROGRESS_FILE  	
  
# ЕСЛИ ТЕСТОВАЯ БД - ПО УМОЛЧАНИЮ  
#################################################################################
#################################################################################
# КАСТОМНАЯ ТЕСТОВАЯ БД
else
	#########################################################################################################
	#  ТЕСТОВЫЕ СЦЕНАРИИ
	  testdb_owner=`psql  -Aqtc "SELECT r.rolname FROM pg_database d JOIN pg_roles r ON d.datdba = r.oid WHERE d.datname = '$testdb'"`  2>$ERR_FILE
	  exit_code $? $LOG_FILE $ERR_FILE
	  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : ВЛАДЕЛЕЦ ТЕСТОВОЙ БД  = '$testdb_owner
	  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : ВЛАДЕЛЕЦ ТЕСТОВОЙ БД  = '$testdb_owner >> $LOG_FILE	
	  
	  for (( scenario_id=1; scenario_id <= max_sc_count; scenario_id++ ))
	  do
	  
		pgbench_clients=`psql -d $expecto_db -U $expecto_user -Aqtc "select load_test_get_load_by_scenario("$scenario_id")"` 2>$ERR_FILE
		exit_code $? $LOG_FILE $ERR_FILE

		pgbench_param='--file='$current_path'/do_scenario'$scenario_id'.sql --protocol=extended --report-per-command --jobs='"$jobs"' --client='"$pgbench_clients"' --time='"$pg_bench_time"' '$testdb		
	
		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : СЦЕНАРИЙ-'$scenario_id': pgbench_clients= '$pgbench_clients
		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : СЦЕНАРИЙ-'$scenario_id': pgbench_clients= '$pgbench_clients>> $LOG_FILE
	
		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : СЦЕНАРИЙ-'$scenario_id': pgbench_param= '$pgbench_param
		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : СЦЕНАРИЙ-'$scenario_id': pgbench_param= '$pgbench_param>> $LOG_FILE
	
		pgbench  --no-vacuum --username=$testdb_owner $pgbench_param & >>$LOG_FILE 2>$PROGRESS_FILE
	  done
	  wait
	  exit_code $? $LOG_FILE $PROGRESS_FILE  	

	  psql -d $expecto_db -U $expecto_user -Aqtc 'select load_test_set_scenario_queryid()' 2>$ERR_FILE
	  exit_code $? $LOG_FILE $ERR_FILE	
	#  ТЕСТОВЫЕ СЦЕНАРИИ
	#########################################################################################################
fi
# КАСТОМНАЯ ТЕСТОВАЯ БД
#################################################################################



rm $current_path'/PGBENCH_WORKING'
# PGBENCH
#############################################################################
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : ИТЕРАЦИЯ pg_bench ЗАВЕРШЕНА'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : ИТЕРАЦИЯ pg_bench ЗАВЕРШЕНА' >> $LOG_FILE		
	
###########################################################################################################
# ОСТАНОВИТЬ СБОР ДАННЫХ 	
psql -d $expecto_db -U $expecto_user -c 'select  load_test_stop_collect_data()' >>$LOG_FILE 2>$PROGRESS_FILE
exit_code $? $LOG_FILE $PROGRESS_FILE
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : СБОР ДАННЫХ ПО НАГРУЗОЧНОМУ ТЕСТУ - ОСТАНОВЛЕН'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : СБОР ДАННЫХ ПО НАГРУЗОЧНОМУ ТЕСТУ - ОСТАНОВЛЕН' >> $LOG_FILE
# ОСТАНОВИТЬ СБОР ДАННЫХ 	
###########################################################################################################

#################################################
#Если тест завершен принудительно - выход
#Отчет не формировать
if [ ! -f $current_path'/LOAD_TEST_STARTED' ]; 
then
  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : ОЧИСТКА ТАБЛИЦЫ pgbench_history '
  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : ОЧИСТКА ТАБЛИЦЫ pgbench_history ' >> $LOG_FILE
  psql -v ON_ERROR_STOP=on --echo-errors -v ON_ERROR_STOP=on --echo-errors -d $pgbench_db -U $expecto_user -Aqtc 'TRUNCATE TABLE pgbench_history' >> $LOG_FILE 2>>$ERR_FILE
  exit_code $? $LOG_FILE $ERR_FILE
  	
  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : НАГРУЗОЧНЫЙ ТЕСТ ОСТАНОВЛЕН С ПОМОЩЬЮ load_test_stop.sh '
  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : НАГРУЗОЧНЫЙ ТЕСТ ОСТАНОВЛЕН С ПОМОЩЬЮ load_test_stop.sh ' >> $LOG_FILE
  exit 0
fi

# CHECK FINISH 
is_test_could_be_finished=`psql -d $expecto_db -U $expecto_user -Aqtc 'select load_test_is_test_could_be_finished()' 2>$ERR_FILE`
exit_code $? $LOG_FILE $ERR_FILE
# CHECK FINISH 

#################################################
#ЕСЛИ ТЕСТ МОЖЕТ БЫТЬ ЗАВЕРШЕН
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : ФЛАГ ОСТАНОВКИ НАГРУЗОЧНОГО ТЕСТА = '$is_test_could_be_finished
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : ФЛАГ ОСТАНОВКИ НАГРУЗОЧНОГО ТЕСТА = '$is_test_could_be_finished >> $LOG_FILE
if [ "$is_test_could_be_finished" == "1" ];
then 	
  if [ "$testdb" == "default" ]
  then 
    echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : ОЧИСТКА ТАБЛИЦЫ pgbench_history '
    echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : ОЧИСТКА ТАБЛИЦЫ pgbench_history ' >> $LOG_FILE
    psql -v ON_ERROR_STOP=on --echo-errors -v ON_ERROR_STOP=on --echo-errors -d $pgbench_db -U $expecto_user -Aqtc 'TRUNCATE TABLE pgbench_history' >> $LOG_FILE 2>>$ERR_FILE
    exit_code $? $LOG_FILE $ERR_FILE
  fi
  
  #################################################
  # Опустить флаг
  $current_path'/'load_test_stop.sh
  #################################################
  
  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : НАГРУЗОЧНЫЙ ТЕСТ ЗАВЕРШЕН'
  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : НАГРУЗОЧНЫЙ ТЕСТ ЗАВЕРШЕН' >> $LOG_FILE

  exit 0 
fi
#ЕСЛИ ТЕСТ МОЖЕТ БЫТЬ ЗАВЕРШЕН
#################################################

#################################################
# Опустить флаг
rm  $current_path'/LOAD_TEST_IN_PROGRESS'
#################################################
exit 0
