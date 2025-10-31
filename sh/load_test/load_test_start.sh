#!/bin/sh
#####################################################################################
# load_test_start.sh
# version 1.0
#####################################################################################
# Старт нагрузочного тестирования
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

LOG_FILE=$current_path'/load_test.log'
ERR_FILE=$current_path'/load_test.err'

pgbench_db='pgbench_db'
expecto_db='expecto_db'
expecto_user='expecto_user'

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : НАГРУЗОЧНЫЙ ТЕСТ СУБД - ПОДГОТОВКА'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : НАГРУЗОЧНЫЙ ТЕСТ СУБД - ПОДГОТОВКА' > $LOG_FILE

######################################################################
# Инициализировать тестовую БД ?
init_test_db=`$current_path'/'get_conf_param.sh $current_path init_test_db 2>$ERR_FILE`
exit_code $? $LOG_FILE $ERR_FILE
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : init_test_db = '$init_test_db
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : init_test_db = '$init_test_db >> $LOG_FILE	

#################################################################################
# ИНИЦИАЛИЗИРОВАТЬ ТЕСТОВУЮ БД
if [ "$init_test_db" == "on" ]
then 
	echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") '  : OK : СОЗДАНИЕ ТЕСТОВОЙ БД'
	echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") '  : OK : СОЗДАНИЕ ТЕСТОВОЙ БД'>> $LOG_FILE
		  
		psql -c "DROP DATABASE IF EXISTS pgbench_db ( FORCE ) " 2>>$ERR_FILE
		exit_code $? $LOG_FILE $ERR_FILE

		psql -c "CREATE DATABASE pgbench_db WITH OWNER = "$expecto_user 2>>$ERR_FILE
		exit_code $? $LOG_FILE $ERR_FILE
		
		#########################################################################################################
		#Параметры инициализации	
		scale_factor=`$current_path'/'get_conf_param.sh $current_path scale 2>$ERR_FILE`
		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : МАСШТАБ = '$scale_factor
		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : МАСШТАБ = '$scale_factor >> $LOG_FILE	
			
		pgbench_init_param='--quiet --foreign-keys --scale='"$scale_factor"' -i pgbench_db'	
		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : pgbench_init_param= '$pgbench_init_param
		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : pgbench_init_param= '$pgbench_init_param>> $LOG_FILE

		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : ИНИЦИАЛИЗАЦИЯ ТЕСТОВОЙ БД'
		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : ИНИЦИАЛИЗАЦИЯ ТЕСТОВОЙ БД' >> $LOG_FILE

		pgbench --username=$expecto_user $pgbench_init_param >>$LOG_FILE
		exit_code $? $LOG_FILE $LOG_FILE

		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : ИНИЦИАЛИЗАЦИЯ ТЕСТОВОЙ БД - ЗАКОНЧЕНА'
		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : ИНИЦИАЛИЗАЦИЯ ТЕСТОВОЙ БД - ЗАКОНЧЕНА' >> $LOG_FILE
		#Параметры инициализации	
		#########################################################################################################

		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СОЗДАТЬ ФУНКЦИЮ ДЛЯ СЦЕНАРИЯ-1'
		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СОЗДАТЬ ФУНКЦИЮ ДЛЯ СЦЕНАРИЯ-1' >> $LOG_FILE
		psql -v ON_ERROR_STOP=on --echo-errors -v ON_ERROR_STOP=on --echo-errors -d $pgbench_db -U $expecto_user -f $current_path'/scenario1.sql' >> $LOG_FILE 2>>$ERR_FILE
		exit_code $? $LOG_FILE $ERR_FILE

		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СОЗДАТЬ ФУНКЦИЮ ДЛЯ СЦЕНАРИЯ-2'
		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СОЗДАТЬ ФУНКЦИЮ ДЛЯ СЦЕНАРИЯ-2' >> $LOG_FILE
		psql -v ON_ERROR_STOP=on --echo-errors -v ON_ERROR_STOP=on --echo-errors -d $pgbench_db -U $expecto_user -f $current_path'/scenario2.sql' >> $LOG_FILE 2>>$ERR_FILE
		exit_code $? $LOG_FILE $ERR_FILE

		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СОЗДАТЬ ФУНКЦИЮ ДЛЯ СЦЕНАРИЯ-3'
		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СОЗДАТЬ ФУНКЦИЮ ДЛЯ СЦЕНАРИЯ-3' >> $LOG_FILE
		psql -v ON_ERROR_STOP=on --echo-errors -v ON_ERROR_STOP=on --echo-errors -d $pgbench_db -U $expecto_user -f $current_path'/scenario3.sql' >> $LOG_FILE 2>>$ERR_FILE
		exit_code $? $LOG_FILE $ERR_FILE
		
		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : pgbench ВЫЗОВ ФУНКЦИИ СЦЕНАРИЯ-1'
		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : pgbench ВЫЗОВ ФУНКЦИИ СЦЕНАРИЯ-1' >> $LOG_FILE
		psql -v ON_ERROR_STOP=on --echo-errors -v ON_ERROR_STOP=on --echo-errors -d $pgbench_db -U $expecto_user -f $current_path'/do_scenario1.sql' >> $LOG_FILE 2>>$ERR_FILE
		exit_code $? $LOG_FILE $ERR_FILE
		
		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : pgbench ВЫЗОВ ФУНКЦИИ СЦЕНАРИЯ-2'
		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : pgbench ВЫЗОВ ФУНКЦИИ СЦЕНАРИЯ-2' >> $LOG_FILE
		psql -v ON_ERROR_STOP=on --echo-errors -v ON_ERROR_STOP=on --echo-errors -d $pgbench_db -U $expecto_user -f $current_path'/do_scenario2.sql' >> $LOG_FILE 2>>$ERR_FILE
		exit_code $? $LOG_FILE $ERR_FILE
		
		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : pgbench ВЫЗОВ ФУНКЦИИ СЦЕНАРИЯ-3'
		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : pgbench ВЫЗОВ ФУНКЦИИ СЦЕНАРИЯ-3' >> $LOG_FILE
		psql -v ON_ERROR_STOP=on --echo-errors -v ON_ERROR_STOP=on --echo-errors -d $pgbench_db -U $expecto_user -f $current_path'/do_scenario3.sql' >> $LOG_FILE 2>>$ERR_FILE
		exit_code $? $LOG_FILE $ERR_FILE


	echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") '  : OK : СОЗДАНИЕ И ИНИЦИАЛИЗАЦИЯ ТЕСТОВОЙ БД - ЗАВЕРШЕНО'
	echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") '  : OK : СОЗДАНИЕ И ИНИЦИАЛИЗАЦИЯ ТЕСТОВОЙ БД - ЗАВЕРШЕНО'>> $LOG_FILE
	
		
else
    #########################################################################################################	
	# ВАКУУМ ТЕСТОВОЙ БД
	echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : ANALYZE/VACUUM ТЕСТОВОЙ БД'
	echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : ANALYZE/VACUUM ТЕСТОВОЙ БД' >> $LOG_FILE

	max_parallel_maintenance_workers=`psql  -Aqtc 'show max_parallel_maintenance_workers' 2>$ERR_FILE`
	exit_code $? $LOG_FILE $ERR_FILE  
	echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : max_parallel_maintenance_workers =  '$max_parallel_maintenance_workers
	echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : max_parallel_maintenance_workers =  '$max_parallel_maintenance_workers >> $LOG_FILE

			
	echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : VACUUM ANALYZE STARTED '
	echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : VACUUM ANALYZE STARTED ' >> $LOG_FILE

	psql -d test_pgbench_custom -c 'VACUUM ( PARALLEL '$max_parallel_maintenance_workers' ) ' & psql -d $pgbench_db -c 'ANALYZE'  >> $LOG_FILE 2>$ERR_FILE
	wait
	exit_code $? $LOG_FILE $ERR_FILE
	echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : ANALYZE/VACUUM ТЕСТОВОЙ БД - ЗАВЕРШЕН'
	echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : ANALYZE/VACUUM ТЕСТОВОЙ БД - ЗАВЕРШЕН' >> $LOG_FILE
	# ВАКУУМ ТЕСТОВОЙ БД
	#########################################################################################################		
fi

# ИНИЦИАЛИЗИРОВАТЬ ТЕСТОВУЮ БД
#################################################################################

psql -d $expecto_db -U $expecto_user -c "select load_test_new_test()" >> $LOG_FILE 2>>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : НАГРУЗОЧНЫЙ ТЕСТ - ГОТОВ К СТАРТУ'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : НАГРУЗОЧНЫЙ ТЕСТ - ГОТОВ К СТАРТУ' >> $LOG_FILE

finish_load=`$current_path'/'get_conf_param.sh $current_path finish_load 2>$ERR_FILE`
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : МАКСИМАЛЬНАЯ НАГРУЗКА  = '$finish_load' СЕССИЙ'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : МАКСИМАЛЬНАЯ НАГРУЗКА  = '$finish_load' СЕССИЙ' >> $LOG_FILE	
psql -d $expecto_db -U $expecto_user -c 'select load_test_set_max_load('$finish_load')' >> $LOG_FILE 2>>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE



touch $current_path'/LOAD_TEST_STARTED'

exit 0  
