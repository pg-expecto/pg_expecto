#!/bin/bash
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
########################################################################################################
# pg_expecto.sh
# Корневой скрипт 
# version 12.4 (markov_chain 14.6)
# updated 13.07.2026
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
	echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : ERROR : Details in '$ERR_FILE >> $LOG_FILE
	
	#################################################
	# Опустить флаг
	rm /postgres/pg_expecto/PG_EXPECTO_IN_PROGRESS
	#################################################
	
    exit $ecode
fi
}

#################################################
# Если флаг поднят - выход
if [ -f /postgres/pg_expecto/PG_EXPECTO_IN_PROGRESS ]; 
then
  exit 0
fi
#################################################


script=$(readlink -f $0)
current_path=`dirname $script`
timestamp_label=$(date "+%Y%m%d")'T'$(date "+%H%M")


expecto_db='expecto_db'
expecto_user='expecto_user'

LOG_FILE=$current_path'/pg_expecto.log'
ERR_FILE=$current_path'/pg_expecto.err'
REPORT_DIR='/tmp/bb'

#################################################
# Поднять флаг
touch /postgres/pg_expecto/PG_EXPECTO_IN_PROGRESS
#################################################

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : START '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : START '> $LOG_FILE

##########################################################################################################
## СОБРАТЬ СТАТИСТИКУ ПО SQL 
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СБОР ИСХОДНЫХ ДАННЫХ ПО SQL ВЫРАЖЕНИЯМ - НАЧАТ'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СБОР ИСХОДНЫХ ДАННЫХ ПО SQL ВЫРАЖЕНИЯМ - НАЧАТ'>> $LOG_FILE
psql -d $expecto_db -U $expecto_user  -v ON_ERROR_STOP=on --echo-errors -Aqtc "select statement_stat()" 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СБОР ИСХОДНЫХ ДАННЫХ ПО SQL ВЫРАЖЕНИЯМ - ЗАКОНЧЕН'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СБОР ИСХОДНЫХ ДАННЫХ ПО SQL ВЫРАЖЕНИЯМ - ЗАКОНЧЕН'>> $LOG_FILE
## СОБРАТЬ СТАТИСТИКУ ПО SQL 
#########################################################################################################

#########################################################################################################
# СОБРАТЬ СТАТИСТИКУ ПРОИЗВОДИТЕЛЬНОСТИ ПО КЛАСТЕРУ
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : РАСЧЕТ СТАТИСТИКИ ПРОИЗВОДИТЕЛЬНОСТИ И ОЖИДАНИЙ СУБД - НАЧАТ'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : РАСЧЕТ СТАТИСТИКИ ПРОИЗВОДИТЕЛЬНОСТИ И ОЖИДАНИЙ СУБД - НАЧАТ'>> $LOG_FILE
psql -d $expecto_db -U $expecto_user  -v ON_ERROR_STOP=on --echo-errors -Aqtc 'select cluster_stat_median()'  >> $LOG_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE

performance_metrics=`psql -d $expecto_db -U $expecto_user -Aqtc 'select performance_metrics()' 2>$ERR_FILE`
exit_code $? $LOG_FILE $ERR_FILE
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  '$performance_metrics
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  '$performance_metrics >> $LOG_FILE

speed=`echo $performance_metrics |  awk -F "|" '{print $1}' `
speed="$(tr -d ' ' <<< "$speed")"

waitings=`echo $performance_metrics |  awk -F "|" '{print $2}' `
waitings="$(tr -d ' ' <<< "$waitings")"

indicator=`echo $performance_metrics |  awk -F "|" '{print $3}' `
indicator="$(tr -d ' ' <<< "$indicator")"

echo $speed     >  /tmp/pg_expecto_speed.txt 
echo $waitings  >  /tmp/pg_expecto_waitings.txt 
echo $indicator >  /tmp/pg_expecto_indicator.txt 

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : РАСЧЕТ СТАТИСТИКИ ПРОИЗВОДИТЕЛЬНОСТИ И ОЖИДАНИЙ СУБД - ЗАКОНЧЕН'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : РАСЧЕТ СТАТИСТИКИ ПРОИЗВОДИТЕЛЬНОСТИ И ОЖИДАНИЙ СУБД - ЗАКОНЧЕН'>> $LOG_FILE
# СОБРАТЬ СТАТИСТИКУ ПРОИЗВОДИТЕЛЬНОСТИ ПО КЛАСТЕРУ
#########################################################################################################

#########################################################################################################
# СБРОС СТАТИСТИКИ 
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СБРОС СТАТИСТИКИ  '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СБРОС СТАТИСТИКИ  ' >> $LOG_FILE

psql -d $expecto_db -Aqtc "SELECT pg_stat_statements_reset()" >> $LOG_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' OK : pg_stat_statements_reset  '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' OK :  pg_stat_statements_reset  ' >> $LOG_FILE

psql -d $expecto_db -Aqtc "SELECT pg_wait_sampling_reset_profile()" >> $LOG_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' OK : pg_wait_sampling_reset_profile  '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' OK :  pg_wait_sampling_reset_profile  ' >> $LOG_FILE
# СБРОС СТАТИСТИКИ 
#########################################################################################################

#########################################################################################################
# СОБРАТЬ СТАТИСТИКУ VMSTAT
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : РАСЧЕТ СТАТИСТИКИ VMSTAT - НАЧАТ'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : РАСЧЕТ СТАТИСТИКИ VMSTAT - НАЧАТ'>> $LOG_FILE

vmstat_string=`cat /postgres/pg_expecto/vmstat.log | tail -1 | sed -e "s/[[:space:]]\+/ /g" | sed 's/^[[:space:]]*//'`

vm_dirty=`$current_path'/'vm_dirty_values.sh`

vmstat_string=$vmstat_string' '$vm_dirty

psql -d $expecto_db -U $expecto_user  -v ON_ERROR_STOP=on --echo-errors -Aqtc "select os_stat_vmstat( '$vmstat_string' )"  >> $LOG_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : РАСЧЕТ СТАТИСТИКИ VMSTAT - ЗАКОНЧЕН'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : РАСЧЕТ СТАТИСТИКИ VMSTAT - ЗАКОНЧЕН'>> $LOG_FILE
# СОБРАТЬ СТАТИСТИКУ VMSTAT
#########################################################################################################

#########################################################################################################
# СОБРАТЬ СТАТИСТИКУ IOSTAT
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : РАСЧЕТ СТАТИСТИКИ IOSTAT - НАЧАТ'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : РАСЧЕТ СТАТИСТИКИ IOSTAT - НАЧАТ'>> $LOG_FILE
SOURCE_FILE='/postgres/pg_expecto/iostat.log'

	line_start=`grep -n "Device" $SOURCE_FILE | cut -d: -f1 | tail -1 `
	
	let line_counter=1
	while string= read -r line 
	do 
		size=${#line}
		
		if [ "$size" == 0 ] && [ "$line_counter" -ge "$line_start" ];
		then
			break
		fi
		
		if [ "$line_counter" -le "$line_start" ];
		then 
			let line_counter=line_counter+1
			continue;
		fi 
		
		dev_string=`echo $line | sed -e "s/[[:space:]]\+/ /g" | sed 's/^[[:space:]]*//'`
		echo "DEVICE STRING = "$dev_string 	  
		psql -d $expecto_db -U $expecto_user   -v ON_ERROR_STOP=on --echo-errors -Aqtc "select os_stat_iostat_device( '$dev_string' )"  2>$ERR_FILE
		exit_code $? $LOG_FILE $ERR_FILE

	let line_counter=line_counter+1
	done < $SOURCE_FILE
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : РАСЧЕТ СТАТИСТИКИ IOSTAT - ЗАКОНЧЕН'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : РАСЧЕТ СТАТИСТИКИ IOSTAT - ЗАКОНЧЕН'>> $LOG_FILE
# СОБРАТЬ СТАТИСТИКУ IOSTAT
########################################################################################################

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : ОЧИСТКА СТАРЫХ ДАННЫХ - НАЧАТА'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : ОЧИСТКА СТАРЫХ ДАННЫХ - НАЧАТА'>> $LOG_FILE

psql -d $expecto_db -U $expecto_user  -v ON_ERROR_STOP=on --echo-errors -Aqtc 'select cleaning()'  >> $LOG_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : ОЧИСТКА СТАРЫХ ДАННЫХ - ЗАКОНЧЕНА'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : ОЧИСТКА СТАРЫХ ДАННЫХ - ЗАКОНЧЕНА'>> $LOG_FILE


let current_minute=`echo $(date "+%M")`
let mod=current_minute%10

if [ "$mod" == "0" ]
then 
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : ОЧИСТКА VMSTAT/IOSTAT ФАЙЛОВ - КАЖДЫЕ 10 МИНУТ'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : ОЧИСТКА VMSTAT/IOSTAT ФАЙЛОВ - КАЖДЫЕ 10 МИНУТ'>> $LOG_FILE

pkill -u postgres -x "vmstat"
vmstat 60 -S M -t >/postgres/pg_expecto/vmstat.log 2>&1 &
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : РЕСТАРТ VMSTAT - КАЖДЫЕ 10 МИНУТ'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : РЕСТАРТ VMSTAT - КАЖДЫЕ 10 МИНУТ'>> $LOG_FILE

pkill -u postgres -x "iostat"
iostat 60 -d -x -m -t >/postgres/pg_expecto/iostat.log 2>&1 &
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : РЕСТАРТ IOSTAT - КАЖДЫЕ 10 МИНУТ'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : РЕСТАРТ IOSTAT - КАЖДЫЕ 10 МИНУТ'>> $LOG_FILE

fi	

########################################################################################################
#
is_markov_chain_enabled=`psql -d $expecto_db -U $expecto_user  -v ON_ERROR_STOP=on --echo-errors -Aqtc 'select is_markov_chain_enabled()' 2>$ERR_FILE`
exit_code $? $LOG_FILE $ERR_FILE

if [[ "$is_markov_chain_enabled" == "t" ]]
then 

	echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : АНАЛИЗ ЦЕПИ МАРКОВА - НАЧАТ '
	echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : АНАЛИЗ ЦЕПИ МАРКОВА - НАЧАТ '>> $LOG_FILE

	MARKOV_CHAIN_LOG=$current_path'/markov_chain.log'
	psql -d $expecto_db -U $expecto_user  -v ON_ERROR_STOP=on --echo-errors -Aqtc 'select mchain_reliability_report()'  > $MARKOV_CHAIN_LOG 2>$ERR_FILE
	exit_code $? $LOG_FILE $ERR_FILE

	echo 'INFO : ПОСЛЕДНИЕ 3 ОШИБКИ '>> $LOG_FILE	
	echo 'INFO : ПОСЛЕДНИЕ 3 ОШИБКИ '>> $MARKOV_CHAIN_LOG

	psql -d $expecto_db -U $expecto_user  -v ON_ERROR_STOP=on --echo-errors -Aqtc 'select * from mchain_error_log order by ts desc LIMIT 3 '  >> $LOG_FILE 2>$ERR_FILE
	exit_code $? $LOG_FILE $ERR_FILE
	psql -d $expecto_db -U $expecto_user  -v ON_ERROR_STOP=on --echo-errors -Aqtc 'select * from mchain_error_log order by ts desc LIMIT 3 '  >> $MARKOV_CHAIN_LOG 2>$ERR_FILE
	exit_code $? $LOG_FILE $ERR_FILE

	mchain_forecast_reliability=`psql -d $expecto_db -U $expecto_user  -v ON_ERROR_STOP=on --echo-errors -Aqtc 'select mchain_forecast_reliability()'`
	
	
	

	if [[ $mchain_forecast_reliability -ge 3 ]]
	then 
	  echo 'INFO : ДОСТАТОЧНО ДАННЫХ ДЛЯ ПРОГНОЗА(ОБУЧЕНИЕ ЦЕПИ ЗАКОНЧЕНО).' >> $MARKOV_CHAIN_LOG
 	  echo 'INFO : ДОСТАТОЧНО ДАННЫХ ДЛЯ ПРОГНОЗА(ОБУЧЕНИЕ ЦЕПИ ЗАКОНЧЕНО).' >> $LOG_FILE

	  echo ' ' >> $MARKOV_CHAIN_LOG
	  mchain_predict_risk_current_horizon=`psql -d $expecto_db -U $expecto_user  -v ON_ERROR_STOP=on --echo-errors -Aqtc 'select * from mchain_predict_risk_current_horizon()'`
	  exit_code $? $LOG_FILE $ERR_FILE

	  forecast_horizon_minutes=`psql -d $expecto_db -U $expecto_user  -v ON_ERROR_STOP=on --echo-errors -Aqtc 'select forecast_horizon_minutes from markov_config'`
	  exit_code $? $LOG_FILE $ERR_FILE
	  
	  profile_comparison_log=`psql -d $expecto_db -U $expecto_user  -Aqtc "select date_trunc('minute',baseline_window_start ) AS baseline_window_start , date_trunc('minute',baseline_window_end ) AS baseline_window_end , date_trunc('minute',current_window_start ) AS current_window_start , date_trunc('minute',current_window_end ) AS current_window_end , substring(status FROM 0 FOR 11) AS status , js_divergence from profile_comparison_log order by 1 desc limit 1"`
	  
	  profile_comparison_log_status=`echo $profile_comparison_log |  awk -F "|" '{print $5}' `
	  profile_comparison_log_js_divergence=`echo $profile_comparison_log |  awk -F "|" '{print $6}' `

	  
	  if [[ "$profile_comparison_log_status" == "INCIDENT" ]]
	  then 
		echo 'INCIDENT : ПРОФИЛЬ ПРОИЗВОДИТЕЛЬНОСТИ НЕ РАСЧИТЫВАЕТСЯ : ИНЦИДЕНТ ИЛИ ВОССТАНОВЛЕНИЕ ПОСЛЕ ИНЦИДЕНТА' >> $MARKOV_CHAIN_LOG 
		echo 'INCIDENT : ПРОФИЛЬ ПРОИЗВОДИТЕЛЬНОСТИ НЕ РАСЧИТЫВАЕТСЯ : ИНЦИДЕНТ ИЛИ ВОССТАНОВЛЕНИЕ ПОСЛЕ ИНЦИДЕНТА' >> $LOG_FILE 
		incident_timepoint=`psql -d $expecto_db -U $expecto_user  -Aqtc "select start_timepoint , finish_timepoint from performance_incident where start_timepoint = (select max(start_timepoint) from performance_incident )"`
		start_timepoint=`echo $incident_timepoint |  awk -F "|" '{print $1}' `
		finish_timepoint=`echo $incident_timepoint |  awk -F "|" '{print $2}' `
		echo 'INCIDENT : НАЧАЛО ИНЦИДЕНТА : '$start_timepoint' ОКОНЧАНИЕ ИНЦИДЕНТА : '$finish_timepoint >> $MARKOV_CHAIN_LOG 
		echo 'INCIDENT : НАЧАЛО ИНЦИДЕНТА : '$start_timepoint' ОКОНЧАНИЕ ИНЦИДЕНТА : '$finish_timepoint >> $LOG_FILE 	
	  elif [[ "$profile_comparison_log_status" == "CRITICAL" ]]
	  then 
	    echo 'CRITICAL : ПРОФИЛЬ ПРОИЗВОДИТЕЛЬНОСТИ : JS-ДИВЕРГЕНЦИЯ = '$profile_comparison_log_js_divergence >> $MARKOV_CHAIN_LOG 			  
		echo 'CRITICAL : ПРОФИЛЬ ПРОИЗВОДИТЕЛЬНОСТИ : JS-ДИВЕРГЕНЦИЯ = '$profile_comparison_log_js_divergence >> $LOG_FILE 		
		echo 'INFO : ВЕРОЯТНОСТЬ ИНЦИДЕНТА В ТЕЧЕНИИ '$forecast_horizon_minutes' МИНУТ = '$mchain_predict_risk_current_horizon >> $MARKOV_CHAIN_LOG 2>$ERR_FILE
		echo 'INFO : ВЕРОЯТНОСТЬ ИНЦИДЕНТА В ТЕЧЕНИИ '$forecast_horizon_minutes' МИНУТ = '$mchain_predict_risk_current_horizon >> $LOG_FILE 2>$ERR_FILE
	  elif [[ "$profile_comparison_log_status" == "WARNING" ]]
	  then 
	    echo 'WARNING : ПРОФИЛЬ ПРОИЗВОДИТЕЛЬНОСТИ : JS-ДИВЕРГЕНЦИЯ = '$profile_comparison_log_js_divergence >> $MARKOV_CHAIN_LOG 			  	  
		echo 'WARNING : ПРОФИЛЬ ПРОИЗВОДИТЕЛЬНОСТИ : JS-ДИВЕРГЕНЦИЯ = '$profile_comparison_log_js_divergence >> $LOG_FILE 			  	  
		echo 'INFO : ВЕРОЯТНОСТЬ ИНЦИДЕНТА В ТЕЧЕНИИ '$forecast_horizon_minutes' МИНУТ = '$mchain_predict_risk_current_horizon >> $MARKOV_CHAIN_LOG 2>$ERR_FILE
		echo 'INFO : ВЕРОЯТНОСТЬ ИНЦИДЕНТА В ТЕЧЕНИИ '$forecast_horizon_minutes' МИНУТ = '$mchain_predict_risk_current_horizon >> $LOG_FILE 2>$ERR_FILE
	  else	  
	    echo 'INFO : ПРОФИЛЬ ПРОИЗВОДИТЕЛЬНОСТИ : JS-ДИВЕРГЕНЦИЯ = '$profile_comparison_log_js_divergence >> $MARKOV_CHAIN_LOG 		
		echo 'INFO : ПРОФИЛЬ ПРОИЗВОДИТЕЛЬНОСТИ : JS-ДИВЕРГЕНЦИЯ = '$profile_comparison_log_js_divergence >> $LOG_FILE 		
		echo 'INFO : ВЕРОЯТНОСТЬ ИНЦИДЕНТА В ТЕЧЕНИИ '$forecast_horizon_minutes' МИНУТ = '$mchain_predict_risk_current_horizon >> $MARKOV_CHAIN_LOG 2>$ERR_FILE
		echo 'INFO : ВЕРОЯТНОСТЬ ИНЦИДЕНТА В ТЕЧЕНИИ '$forecast_horizon_minutes' МИНУТ = '$mchain_predict_risk_current_horizon >> $LOG_FILE 2>$ERR_FILE
	  fi 
	  

	  mchain_health_check=`psql -d $expecto_db -U $expecto_user  -v ON_ERROR_STOP=on --echo-errors -Aqtc 'select status , message from mchain_health_check()'`
	  exit_code $? $LOG_FILE $ERR_FILE

	  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : '$mchain_health_check >> $MARKOV_CHAIN_LOG  

	  psql -d $expecto_db -U $expecto_user  -v ON_ERROR_STOP=on --echo-errors -Aqtc 'select unnest(description) from mchain_health_check()' >> $MARKOV_CHAIN_LOG    
	else
	  echo 'ALARM: НЕДОСТАТОЧНО ДАННЫХ ДЛЯ ПРОГНОЗА(ОБУЧЕНИЕ ЦЕПИ НЕ ЗАКОНЧЕНО).' >> $MARKOV_CHAIN_LOG
	  echo 'ALARM: НЕДОСТАТОЧНО ДАННЫХ ДЛЯ ПРОГНОЗА(ОБУЧЕНИЕ ЦЕПИ НЕ ЗАКОНЧЕНО).' >> $LOG_FILE
	fi

	echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : АНАЛИЗ ЦЕПИ МАРКОВА - ЗАКОНЧЕН '
	echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : АНАЛИЗ ЦЕПИ МАРКОВА - ЗАКОНЧЕН '>> $LOG_FILE
fi
#
########################################################################################################


#################################################
# Опустить флаг
rm /postgres/pg_expecto/PG_EXPECTO_IN_PROGRESS
#################################################

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : FINISH '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : FINISH '>> $LOG_FILE

exit 0 


