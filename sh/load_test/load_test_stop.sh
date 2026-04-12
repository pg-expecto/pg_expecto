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
# load_test_stop.sh
# version 1.0
#####################################################################################
# Остановка нагрузочного тестирования
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
	
    exit $ecode
fi
}

script=$(readlink -f $0)
current_path=`dirname $script`

performance_monitoring_db='performance_monitoring_db'
performance_monitoring_user='performance_monitoring_user'

pgbench_db='pgbench_db'
expecto_user='expecto_user'

LOG_FILE=$current_path'/tester_stop.log'
ERR_FILE=$current_path'/tester_stop.err'

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : ОСТАНОВКА НАГРУЗОЧНОГО ТЕСТА'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : ОСТАНОВКА НАГРУЗОЧНОГО ТЕСТА' > $LOG_FILE

#НАГРУЗОЧНЫЙ ТЕСТ НАЧАТ 
rm $current_path'/LOAD_TEST_STARTED' 

#PGBENCH РАБОТАЕТ
rm $current_path'/PGBENCH_WORKING'

#ТЕСТОВАЯ ИТЕРАЦИЯ РАБОТАЕТ
rm $current_path'/LOAD_TEST_IN_PROGRESS'


psql -d $performance_monitoring_db -U $performance_monitoring_user -c 'select stop_test()' >> $LOG_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE  

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : ЗАВЕРШЕНИЕ PGBENCH'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : ЗАВЕРШЕНИЕ PGBENCH' >> $LOG_FILE
psql -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = 'pgbench'";
exit_code $? $LOG_FILE $ERR_FILE  

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : ЗАВЕРШЕНИЕ СЦЕНАРИЕВ НАГРУЗОЧНОГО ТЕСТИРОВАНИЯ'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : ЗАВЕРШЕНИЕ СЦЕНАРИЕВ НАГРУЗОЧНОГО ТЕСТИРОВАНИЯ' >> $LOG_FILE
psql -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name like '%select scenario%'";
exit_code $? $LOG_FILE $ERR_FILE  

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : ЗАВЕРШЕНИЕ Airlines processor'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : ЗАВЕРШЕНИЕ Airlines processor' >> $LOG_FILE
psql -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name like '%Airlines processor%'";
exit_code $? $LOG_FILE $ERR_FILE  


echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : НАГРУЗОЧНЫЙ ТЕСТ - ОСТАНОВЛЕН'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : НАГРУЗОЧНЫЙ ТЕСТ - ОСТАНОВЛЕН' >> $LOG_FILE

exit 0 

