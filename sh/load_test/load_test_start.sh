#!/bin/sh
#####################################################################################
# load_test_start.sh
# version 6.0
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
	
	$current_path'/'load_test_stop.sh
	
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

psql -d $expecto_db -U $expecto_user -c "select load_test_new_test()" >> $LOG_FILE 2>>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : НОВЫЙ НАГРУЗОЧНЫЙ ТЕСТ'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : НОВЫЙ НАГРУЗОЧНЫЙ ТЕСТ' > $LOG_FILE

finish_load=`$current_path'/'get_conf_param.sh $current_path finish_load 2>$ERR_FILE`
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : МАКСИМАЛЬНАЯ НАГРУЗКА  = '$finish_load' СЕССИЙ'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : МАКСИМАЛЬНАЯ НАГРУЗКА  = '$finish_load' СЕССИЙ' >> $LOG_FILE	
psql -d $expecto_db -U $expecto_user -c 'select load_test_set_max_load('$finish_load')' >> $LOG_FILE 2>>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE


######################################################################
# Инициализировать тестовую БД ?
testdb=`$current_path'/'get_conf_param.sh $current_path testdb 2>$ERR_FILE`
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : ТЕСТОВАЯ БД  = '$testdb
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : ТЕСТОВАЯ БД  = '$testdb >> $LOG_FILE	


#################################################################################
# ЕСЛИ ТЕСТОВАЯ БД - ПО УМОЛЧАНИЮ
if [ "$testdb" == "default" ]
then
	init_test_db=`$current_path'/'get_conf_param.sh $current_path init_test_db 2>$ERR_FILE`
	exit_code $? $LOG_FILE $ERR_FILE
	echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : init_test_db = '$init_test_db
	echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : init_test_db = '$init_test_db >> $LOG_FILE	
	
	load_mode=`$current_path'/'get_conf_param.sh $current_path load_mode 2>$ERR_FILE`
	exit_code $? $LOG_FILE $ERR_FILE
	echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : load_mode = '$load_mode
	echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : load_mode = '$load_mode >> $LOG_FILE	

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
			
			#########################################################################################################
			# before_start.sql - Изменения/добавления тестовых таблиц
			  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : BEFORE_START.SQL'
			  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : BEFORE_START.SQL' >> $LOG_FILE
			  psql -v ON_ERROR_STOP=on --echo-errors -v ON_ERROR_STOP=on --echo-errors -d $pgbench_db -U $expecto_user -f $current_path'/before_start.sql' >> $LOG_FILE 2>>$ERR_FILE
			  exit_code $? $LOG_FILE $ERR_FILE
			# before_start.sql - Изменения/добавления тестовых таблиц
			#########################################################################################################
			
			#########################################################################################################
			#  ТЕСТОВЫЕ СЦЕНАРИИ
			  let i=1
			  flag='1'
			  while [ "$flag" != "0" ]
			  do
				
				# Тип нагрузки OLTP
				if [ "$load_mode" == "oltp" ]
				then 
					echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СОЗДАТЬ ФУНКЦИЮ ДЛЯ СЦЕНАРИЯ-'$i' ТИП НАГРУЗКИ = OLTP '
					echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СОЗДАТЬ ФУНКЦИЮ ДЛЯ СЦЕНАРИЯ-'$i' ТИП НАГРУЗКИ = OLTP ' >> $LOG_FILE
					psql -v ON_ERROR_STOP=on --echo-errors -v ON_ERROR_STOP=on --echo-errors -d $pgbench_db -U $expecto_user -f  $current_path'/scenario'$i'.oltp.sql' >> $LOG_FILE 2>>$ERR_FILE
					exit_code $? $LOG_FILE $ERR_FILE	
				fi
				
				# Тип нагрузки OLAP
				if [ "$load_mode" == "olap" ]
				then 
					echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СОЗДАТЬ ФУНКЦИЮ ДЛЯ СЦЕНАРИЯ-'$i' ТИП НАГРУЗКИ = OLAP '
					echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СОЗДАТЬ ФУНКЦИЮ ДЛЯ СЦЕНАРИЯ-'$i' ТИП НАГРУЗКИ = OLAP ' >> $LOG_FILE
					psql -v ON_ERROR_STOP=on --echo-errors -v ON_ERROR_STOP=on --echo-errors -d $pgbench_db -U $expecto_user -f  $current_path'/scenario'$i'.olap.sql' >> $LOG_FILE 2>>$ERR_FILE
					exit_code $? $LOG_FILE $ERR_FILE	
				fi
				
			  
				echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СОЗДАТЬ СКРИПТ ВЫЗОВА СЦЕНАРИЯ-'$i
				echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СОЗДАТЬ СКРИПТ ВЫЗОВА СЦЕНАРИЯ-'$i >> $LOG_FILE
				echo 'select scenario'$i'();' > $current_path'/do_scenario'$i'.sql'
				
				current_scenario='scenario'$i
				current_weight=`$current_path'/'get_conf_param.sh $current_path $current_scenario 2>$ERR_FILE`
				echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : СЦЕНАРИЙ-'$i' ВЕС = '$current_weight
				echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : СЦЕНАРИЙ-'$i' ВЕС = '$current_weight >> $LOG_FILE	
			
				psql -d $expecto_db -U $expecto_user -c "select load_test_set_weight_for_scenario($i , $current_weight )" >> $LOG_FILE 2>>$ERR_FILE
				exit_code $? $LOG_FILE $ERR_FILE    
				
				let "i++"
				flag=`cat $current_path'/param.conf' | grep 'scenario'$i | wc -l`		
			  done 	  
			#  ТЕСТОВЫЕ СЦЕНАРИИ
			######################################################################################################### 
			

		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") '  : OK : СОЗДАНИЕ И ИНИЦИАЛИЗАЦИЯ ТЕСТОВОЙ БД - ЗАВЕРШЕНО'
		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") '  : OK : СОЗДАНИЕ И ИНИЦИАЛИЗАЦИЯ ТЕСТОВОЙ БД - ЗАВЕРШЕНО'>> $LOG_FILE
		
			
	else		
		#########################################################################################################
		# before_start.sql - Изменения/добавления тестовых таблиц
		  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : BEFORE_START.SQL'
		  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : BEFORE_START.SQL' >> $LOG_FILE
		  psql -v ON_ERROR_STOP=on --echo-errors -v ON_ERROR_STOP=on --echo-errors -d $pgbench_db -U $expecto_user -f $current_path'/before_start.sql' >> $LOG_FILE 2>>$ERR_FILE
		  exit_code $? $LOG_FILE $ERR_FILE
		# before_start.sql - Изменения/добавления тестовых таблиц
		#########################################################################################################
				
		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : VACUUM ANALYZE STARTED '
		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : VACUUM ANALYZE STARTED ' >> $LOG_FILE
		
		#########################################################################################################
		#  ТЕСТОВЫЕ СЦЕНАРИИ
		  let i=1
		  flag='1'
		  while [ "$flag" != "0" ]
		  do
			
				# Тип нагрузки OLTP
				if [ "$load_mode" == "oltp" ]
				then 
					echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СОЗДАТЬ ФУНКЦИЮ ДЛЯ СЦЕНАРИЯ-'$i' ТИП НАГРУЗКИ = OLTP '
					echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СОЗДАТЬ ФУНКЦИЮ ДЛЯ СЦЕНАРИЯ-'$i' ТИП НАГРУЗКИ = OLTP ' >> $LOG_FILE
					psql -v ON_ERROR_STOP=on --echo-errors -v ON_ERROR_STOP=on --echo-errors -d $pgbench_db -U $expecto_user -f  $current_path'/scenario'$i'.oltp.sql' >> $LOG_FILE 2>>$ERR_FILE
					exit_code $? $LOG_FILE $ERR_FILE	
				fi
				
				# Тип нагрузки OLAP
				if [ "$load_mode" == "olap" ]
				then 
					echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СОЗДАТЬ ФУНКЦИЮ ДЛЯ СЦЕНАРИЯ-'$i' ТИП НАГРУЗКИ = OLAP '
					echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СОЗДАТЬ ФУНКЦИЮ ДЛЯ СЦЕНАРИЯ-'$i' ТИП НАГРУЗКИ = OLAP ' >> $LOG_FILE
					psql -v ON_ERROR_STOP=on --echo-errors -v ON_ERROR_STOP=on --echo-errors -d $pgbench_db -U $expecto_user -f  $current_path'/scenario'$i'.olap.sql' >> $LOG_FILE 2>>$ERR_FILE
					exit_code $? $LOG_FILE $ERR_FILE	
				fi
		  
			echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СОЗДАТЬ СКРИПТ ВЫЗОВА СЦЕНАРИЯ-'$i
			echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СОЗДАТЬ СКРИПТ ВЫЗОВА СЦЕНАРИЯ-'$i >> $LOG_FILE
			echo 'select scenario'$i'();' > $current_path'/do_scenario'$i'.sql'
			
			current_scenario='scenario'$i
			current_weight=`$current_path'/'get_conf_param.sh $current_path $current_scenario 2>$ERR_FILE`
			echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : СЦЕНАРИЙ-'$i' ВЕС = '$current_weight
			echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : СЦЕНАРИЙ-'$i' ВЕС = '$current_weight >> $LOG_FILE	
		
			psql -d $expecto_db -U $expecto_user -c "select load_test_set_weight_for_scenario($i , $current_weight )" >> $LOG_FILE 2>>$ERR_FILE
			exit_code $? $LOG_FILE $ERR_FILE    
			
			let "i++"
			flag=`cat $current_path'/param.conf' | grep 'scenario'$i | wc -l`		
		  done 	  
		#  ТЕСТОВЫЕ СЦЕНАРИИ
		######################################################################################################### 
		
		#########################################################################################################	
		# ВАКУУМ ТЕСТОВОЙ БД
		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : ANALYZE/VACUUM ТЕСТОВОЙ БД'
		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : ANALYZE/VACUUM ТЕСТОВОЙ БД' >> $LOG_FILE

		max_parallel_maintenance_workers=`psql  -Aqtc 'show max_parallel_maintenance_workers' 2>$ERR_FILE`
		exit_code $? $LOG_FILE $ERR_FILE  
		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : max_parallel_maintenance_workers =  '$max_parallel_maintenance_workers
		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : max_parallel_maintenance_workers =  '$max_parallel_maintenance_workers >> $LOG_FILE

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
	
# ЕСЛИ ТЕСТОВАЯ БД - ПО УМОЛЧАНИЮ
#################################################################################
#################################################################################
# КАСТОМНАЯ ТЕСТОВАЯ БД
else
	psql -d $expecto_db -U $expecto_user -c "select load_test_set_testdb('$testdb')" >> $LOG_FILE 2>>$ERR_FILE
	exit_code $? $LOG_FILE $ERR_FILE
	echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : ТЕСТОВАЯ БД НАГРУЗОЧНОГО ТЕСТИРОВАНИЯ = '$testdb
	echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : ТЕСТОВАЯ БД НАГРУЗОЧНОГО ТЕСТИРОВАНИЯ = '$testdb >> $LOG_FILE

	testdb_owner=`psql  -Aqtc "SELECT r.rolname FROM pg_database d JOIN pg_roles r ON d.datdba = r.oid WHERE d.datname = '$testdb'"`  2>$ERR_FILE
	exit_code $? $LOG_FILE $ERR_FILE
	echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : ВЛАДЕЛЕЦ ТЕСТОВОЙ БД  = '$testdb_owner
	echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : ВЛАДЕЛЕЦ ТЕСТОВОЙ БД  = '$testdb_owner >> $LOG_FILE	

	
	#########################################################################################################
	# before_start.sql - Изменения/добавления тестовых таблиц
	  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : BEFORE_START.SQL'
	  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : BEFORE_START.SQL' >> $LOG_FILE
	  psql -v ON_ERROR_STOP=on --echo-errors -v ON_ERROR_STOP=on --echo-errors -d $testdb -U $testdb_owner -f $current_path'/before_start.sql' >>$LOG_FILE 2>$ERR_FILE
	  exit_code $? $LOG_FILE $ERR_FILE
	# before_start.sql - Изменения/добавления тестовых таблиц
	#########################################################################################################

	#########################################################################################################
	#  ТЕСТОВЫЕ СЦЕНАРИИ
	  let i=1
	  flag='1'
	  while [ "$flag" != "0" ]
	  do
		
		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СОЗДАТЬ ФУНКЦИЮ ДЛЯ СЦЕНАРИЯ-'$i
		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СОЗДАТЬ ФУНКЦИЮ ДЛЯ СЦЕНАРИЯ-'$i >> $LOG_FILE
		psql -v ON_ERROR_STOP=on --echo-errors -v ON_ERROR_STOP=on --echo-errors -d $testdb -U $testdb_owner -f $current_path'/scenario'$i'.sql' >> $LOG_FILE 2>>$ERR_FILE
		exit_code $? $LOG_FILE $ERR_FILE
	  
		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СОЗДАТЬ СКРИПТ ВЫЗОВА СЦЕНАРИЯ-'$i
		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СОЗДАТЬ СКРИПТ ВЫЗОВА СЦЕНАРИЯ-'$i >> $LOG_FILE
		echo 'select scenario'$i'();' > $current_path'/do_scenario'$i'.sql'
		
		current_scenario='scenario'$i
		current_weight=`$current_path'/'get_conf_param.sh $current_path $current_scenario 2>$ERR_FILE`
		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : СЦЕНАРИЙ-'$i' ВЕС = '$current_weight
		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : СЦЕНАРИЙ-'$i' ВЕС = '$current_weight >> $LOG_FILE	
	
		psql -d $expecto_db -U $expecto_user -c "select load_test_set_weight_for_scenario($i , $current_weight )" >> $LOG_FILE 2>>$ERR_FILE
		exit_code $? $LOG_FILE $ERR_FILE    
		
		let "i++"
		flag=`cat $current_path'/param.conf' | grep 'scenario'$i | wc -l`		
	  done 	  
	#  ТЕСТОВЫЕ СЦЕНАРИИ
	#########################################################################################################   
	
	

# КАСТОМНАЯ ТЕСТОВАЯ БД
#################################################################################
fi 

#################################################################################
# ЗАФИКСИРОВАТЬ ПАРАМЕТРЫ vm
VM_VALUES='/postgres/pg_expecto/vm_current_settings.txt'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : ЗАФИКСИРОВАТЬ ПАРАМЕТРЫ vm'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : ЗАФИКСИРОВАТЬ ПАРАМЕТРЫ vm' >> $LOG_FILE	

$current_path'/'get_vm_values.sh >$VM_VALUES 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE

cat $VM_VALUES >> $LOG_FILE

value=`cat $VM_VALUES | grep vm.dirty_background_ratio | awk -F ':' '{print $2}'`
new_vales=`echo $value | sed -e "s/[[:space:]]\+/ /g" | sed 's/^[[:space:]]*//'`
psql -d $expecto_db -U $expecto_user -c "select save_dirty_background_ratio( $new_vales )" >> $LOG_FILE 2>>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : vm.dirty_background_ratio='$new_vales
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : vm.dirty_background_ratio='$new_vales >> $LOG_FILE	


value=` cat $VM_VALUES | grep vm.dirty_ratio | awk -F ':' '{print $2}'`
new_vales=`echo $value | sed -e "s/[[:space:]]\+/ /g" | sed 's/^[[:space:]]*//'`
psql -d $expecto_db -U $expecto_user -c "select save_dirty_ratio( $new_vales )" >> $LOG_FILE 2>>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : vm.dirty_ratio='$new_vales
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : vm.dirty_ratio='$new_vales >> $LOG_FILE	

value=` cat $VM_VALUES | grep vm.dirty_background_bytes | awk -F ':' '{print $2}'`
new_vales=`echo $value | sed -e "s/[[:space:]]\+/ /g" | sed 's/^[[:space:]]*//'`
psql -d $expecto_db -U $expecto_user -c "select save_dirty_background_bytes( $new_vales )" >> $LOG_FILE 2>>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : vm.dirty_background_bytes='$new_vales
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : vm.dirty_background_bytes='$new_vales >> $LOG_FILE	

value=` cat $VM_VALUES | grep vm.dirty_bytes | awk -F ':' '{print $2}'`
new_vales=`echo $value | sed -e "s/[[:space:]]\+/ /g" | sed 's/^[[:space:]]*//'`
psql -d $expecto_db -U $expecto_user -c "select save_dirty_bytes( $new_vales )" >> $LOG_FILE 2>>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : vm.dirty_bytes='$new_vales
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : vm.dirty_bytes='$new_vales >> $LOG_FILE	

value=` cat $VM_VALUES | grep vm.dirty_expire_centisecs | awk -F ':' '{print $2}'`
new_vales=`echo $value | sed -e "s/[[:space:]]\+/ /g" | sed 's/^[[:space:]]*//'`
psql -d $expecto_db -U $expecto_user -c "select save_dirty_expire_centisecs( $new_vales )" >> $LOG_FILE 2>>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : vm.dirty_expire_centisecs='$new_vales
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : vm.dirty_expire_centisecs='$new_vales >> $LOG_FILE	

value=` cat $VM_VALUES | grep vm.dirty_writeback_centisecs | awk -F ':' '{print $2}'`
new_vales=`echo $value | sed -e "s/[[:space:]]\+/ /g" | sed 's/^[[:space:]]*//'`
psql -d $expecto_db -U $expecto_user -c "select save_dirty_writeback_centisecs( $new_vales )" >> $LOG_FILE 2>>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : vm.dirty_writeback_centisecs='$new_vales
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : vm.dirty_writeback_centisecs='$new_vales >> $LOG_FILE	

value=` cat $VM_VALUES | grep vm.vfs_cache_pressure | awk -F ':' '{print $2}'`
new_vales=`echo $value | sed -e "s/[[:space:]]\+/ /g" | sed 's/^[[:space:]]*//'`
psql -d $expecto_db -U $expecto_user -c "select save_vfs_cache_pressure( $new_vales )" >> $LOG_FILE 2>>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : vm.vfs_cache_pressure='$new_vales
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : vm.vfs_cache_pressure='$new_vales >> $LOG_FILE	

value=` cat $VM_VALUES | grep vm.swappiness | awk -F ':' '{print $2}'`
new_vales=`echo $value | sed -e "s/[[:space:]]\+/ /g" | sed 's/^[[:space:]]*//'`
psql -d $expecto_db -U $expecto_user -c "select save_swappiness( $new_vales )" >> $LOG_FILE 2>>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : vm.swappiness='$new_vales
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' :  OK : vm.swappiness='$new_vales >> $LOG_FILE	

# ЗАФИКСИРОВАТЬ ПАРАМЕТРЫ vm
#################################################################################



touch $current_path'/LOAD_TEST_STARTED'

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : НАГРУЗОЧНЫЙ ТЕСТ СУБД - ГОТОВ К СТАРТУ'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : НАГРУЗОЧНЫЙ ТЕСТ СУБД - ГОТОВ К СТАРТУ' > $LOG_FILE

exit 0  

