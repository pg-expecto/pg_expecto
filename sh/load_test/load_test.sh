#!/bin/sh
#####################################################################################
# load_test.sh
# version 4.0
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
	
	/postgres/pg_expecto/load_test/load_test_stop.sh
	
    exit $ecode
fi
}

script=$(readlink -f $0)
current_path=`dirname $script`

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

  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : ИТЕРАЦИЯ : '$current_pass' СЕССИЙ pgbench : '$pgbench_clients >> $LOG_FILE
  
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

#################################################################################
# ЕСЛИ ТЕСТОВАЯ БД - ПО УМОЛЧАНИЮ
if [ "$testdb" == "default" ]
then 
  for i in {1..3}
  do
    pgbench_clients=`psql -d $expecto_db -U $expecto_user -Aqtc "select load_test_get_load_by_scenario("$i")"` 2>$ERR_FILE
    exit_code $? $LOG_FILE $ERR_FILE

	pgbench_param='--file='$current_path'/do_scenario'$i'.sql --protocol=extended --report-per-command --jobs='"$jobs"' --client='"$pgbench_clients"' --time='"$pg_bench_time"' '$pgbench_db		
	
	echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : СЦЕНАРИЙ-'$i': pgbench_clients= '$pgbench_clients
    echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : СЦЕНАРИЙ-'$i': pgbench_clients= '$pgbench_clients>> $LOG_FILE
	
    echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : СЦЕНАРИЙ-'$i': pgbench_param= '$pgbench_param
    echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : СЦЕНАРИЙ-'$i': pgbench_param= '$pgbench_param>> $LOG_FILE
	
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
		
	  let i=0
	  flag='1'
	  while [ "$flag" != "0" ]
	  do
	    let "i++"
		flag=`cat $current_path'/param.conf' | grep 'scenario'$i | wc -l`	
	  done 	  
	  
	  for (( scenario_id=1; scenario_id < i; scenario_id++ ))
	  do
	  
		pgbench_clients=`psql -d $expecto_db -U $expecto_user -Aqtc "select load_test_get_load_by_scenario("$scenario_id")"` 2>$ERR_FILE
		exit_code $? $LOG_FILE $ERR_FILE

		pgbench_param='--file='$current_path'/do_scenario'$scenario_id'.sql --protocol=extended --report-per-command --jobs='"$jobs"' --client='"$pgbench_clients"' --time='"$pg_bench_time"' '$testdb		
	
		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : СЦЕНАРИЙ-'$scenario_id': pgbench_clients= '$pgbench_clients
		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : СЦЕНАРИЙ-'$scenario_id': pgbench_clients= '$pgbench_clients>> $LOG_FILE
	
		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : СЦЕНАРИЙ-'$scenario_id': pgbench_param= '$pgbench_param
		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : СЦЕНАРИЙ-'$scenario_id': pgbench_param= '$pgbench_param>> $LOG_FILE
	
		pgbench --username=$testdb_owner $pgbench_param & >>$LOG_FILE 2>$PROGRESS_FILE
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
  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : ОЧИСТКА ТАБЛИЦЫ pgbench_history '
  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : ОЧИСТКА ТАБЛИЦЫ pgbench_history ' >> $LOG_FILE
  psql -v ON_ERROR_STOP=on --echo-errors -v ON_ERROR_STOP=on --echo-errors -d $pgbench_db -U $expecto_user -Aqtc 'TRUNCATE TABLE pgbench_history' >> $LOG_FILE 2>>$ERR_FILE
  exit_code $? $LOG_FILE $ERR_FILE
  
  #################################################
  # ФИНАЛЬНЫЙ ОТЧЕТ
  # ФИНАЛЬНЫЙ ОТЧЕТ
  #################################################
  
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
