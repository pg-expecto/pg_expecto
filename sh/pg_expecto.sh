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
# version 10.0.3
# updated 02/06/2026
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
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : АНАЛИЗ ЦЕПИ МАРКОВА - НАЧАТ '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : АНАЛИЗ ЦЕПИ МАРКОВА - НАЧАТ '>> $LOG_FILE

MARKOV_CHAIN_LOG=$current_path'/markov_chain.log'
MARKOV_CHAIN_TRAINING=$current_path'/markov_chain_evaluate_training_sufficiency.txt'
MARKOV_CHAIN_RESULT=$current_path'/markov_chain_result.txt'

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") > $MARKOV_CHAIN_LOG

adaptive_forgetting_status=`psql -d expecto_db -U expecto_user  -Aqtc 'select get_adaptive_forgetting_status()'`
exit_code $? $LOG_FILE $ERR_FILE
echo 'INFO: ПАРАМЕТРЫ АДАПТИВНОГО ЗАБЫВАНИЯ	' >> $MARKOV_CHAIN_LOG
echo $adaptive_forgetting_status >> $MARKOV_CHAIN_LOG


psql -d expecto_db -U expecto_user  -c 'SELECT * FROM evaluate_training_sufficiency( test_start => current_date - 2, test_end => current_date - 1, model_date_old => current_date - 7, model_date_new => current_date )' > $MARKOV_CHAIN_TRAINING
exit_code $? $LOG_FILE $ERR_FILE

echo 'INFO: КРИТЕРИИ ДОСТАТОЧНОСТИ ОБУЧЕНИЯ ЦЕПИ' >> $MARKOV_CHAIN_LOG
cat $MARKOV_CHAIN_TRAINING >> $MARKOV_CHAIN_LOG

file=$MARKOV_CHAIN_TRAINING

# Проверка существования файла
if [[ ! -f "$file" ]]; then
    echo "Ошибка: файл '$file' не найден."
    exit 1
fi

# Ищем строки, где между двумя вертикальными чертами стоит 't' (с пробелами)
# Исключаем строки с "criterion" и "---", чтобы не считать заголовок
count=$(grep -E '\|\s*t\s*\|' "$file" | grep -vE 'criterion|---' | wc -l)
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : INFO : ВЫПОЛНЕНО КРИТЕРИЕВ: '$count' ИЗ 4-х' >> $MARKOV_CHAIN_LOG

##############################################
#НАДЕЖНОСТЬ ПРОГНОЗА 
if [[ $count -gt 0 ]]; 
then 
  ###############################################################################
  # Расчет надежности прогнозов цепи Маркова по критериям C1..C4
	# Нормализация файла: удаление BOM, замена CRLF на LF
	TMP_FILE=$(mktemp)
	sed '1s/^\xEF\xBB\xBF//' "$MARKOV_CHAIN_TRAINING" | tr -d '\r' > "$TMP_FILE"

	# Подсчёт выполненных критериев:
	#   - ищем строки, начинающиеся с C1..C4 (возможны пробелы в начале)
	#   - проверяем, что после символа "|" идёт значение "t" (возможно с пробелами)
	CRITERIA_COUNT=$(grep -E '^[[:space:]]*C[1-4]:' "$TMP_FILE" | \
		grep -E '\|\s*t\s*\|' | wc -l)

	rm -f "$TMP_FILE"

	# Базовый балл по количеству выполненных критериев
	case $CRITERIA_COUNT in
		0) BASE=0 ;;
		1) BASE=1 ;;
		2) BASE=2 ;;
		3) BASE=3 ;;
		4) BASE=5 ;;
		*) BASE=0 ;;
	esac

	# Типы прогнозов и коэффициенты γ(k)
	PRED_TYPES=(
		"Следующий шаг "
		"5 шагов       "
		"15 шагов      "
		"30 шагов      "
		"60 шагов      "
	)

	GAMMA=(1.0 1.0 0.9 0.8 0.7)

	{
		echo "Тип прогноза   | Балльная оценка (0-ненадежный прогноз , 5-надежный прогноз)"
		for i in "${!PRED_TYPES[@]}"; do
			raw=$(echo "$BASE * ${GAMMA[$i]}" | bc -l)
			# Используем int() для отбрасывания дробной части
			score=$(awk -v val="$raw" 'BEGIN {printf "%d", int(val)}')
			# Ограничиваем диапазон [0,5]
			if [[ $score -gt 5 ]]; then score=5; fi
			if [[ $score -lt 0 ]]; then score=0; fi
			echo "${PRED_TYPES[$i]} | $score"
		done
	} > "$MARKOV_CHAIN_RESULT"  
  ###############################################################################
  
  cat $MARKOV_CHAIN_RESULT >> $MARKOV_CHAIN_LOG  
  
  echo ' ' >> $MARKOV_CHAIN_LOG	
  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : INFO : ТЕКУЩЕЕ СОСТОЯНИЕ' >> $MARKOV_CHAIN_LOG	
  CURRENT_STATE=$current_path'/current_state.txt'
  psql -d expecto_db -U expecto_user -c 'select * from get_current_os_waiting_correlation_for_markov_chain()' > $CURRENT_STATE
  cat $CURRENT_STATE >> $MARKOV_CHAIN_LOG  

  PREDICT_RISK=$current_path'/predict_risk.txt'
  # Построчное чтение файла, начиная со 2-й строки
  line_num=1
  while IFS= read -r line; do
    if [ $line_num -ge 2 ]; then
        # Извлекаем второй столбец (разделитель '|') и убираем пробелы
        value=$(echo "$line" | awk -F '|' '{gsub(/^[[:space:]]+|[[:space:]]+$/, "", $2); print $2}')
echo 'line_num='$line_num  
echo 'value='$value  

		if [[ $value -gt 0 ]]; 
		then 
			if [ "$line_num" == "2"  ]
			then 
				echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : INFO : СЛЕДУЮЩИЙ ШАГ' >> $MARKOV_CHAIN_LOG	

				psql -d expecto_db -U expecto_user  -c 'select * from predict_risk_1min()' > $PREDICT_RISK
				exit_code $? $LOG_FILE $ERR_FILE
				cat $PREDICT_RISK >> $MARKOV_CHAIN_LOG  			  
			fi

			if [ "$line_num" == "3"  ]
			then 
				echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : INFO : ПРОГНОЗ НА 5 ШАГОВ' >> $MARKOV_CHAIN_LOG		  
				
				psql -d expecto_db -U expecto_user  -c 'select * from predict_risk_k_diag( 5 )' > $PREDICT_RISK
				exit_code $? $MARKOV_CHAIN_LOG $ERR_FILE
				cat $PREDICT_RISK >> $MARKOV_CHAIN_LOG  
			fi
			
			if [ "$line_num" == "4"  ]
			then 
				echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : INFO : ПРОГНОЗ НА 15 ШАГОВ' >> $MARKOV_CHAIN_LOG		  
				
				psql -d expecto_db -U expecto_user  -c 'select * from predict_risk_k_diag( 15 )' > $PREDICT_RISK
				exit_code $? $LOG_FILE $ERR_FILE
				cat $PREDICT_RISK >> $MARKOV_CHAIN_LOG  
			fi

			if [ "$line_num" == "5"  ]
			then 
				echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : INFO : ПРОГНОЗ НА 30 ШАГОВ' >> $MARKOV_CHAIN_LOG		  
				
				psql -d expecto_db -U expecto_user  -c 'select * from predict_risk_k_diag( 30 )' > $PREDICT_RISK
				exit_code $? $LOG_FILE $ERR_FILE
				cat $PREDICT_RISK >> $MARKOV_CHAIN_LOG  
			fi

			if [ "$line_num" == "6"  ]
			then 
			    echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : INFO : ПРОГНОЗ НА 60 ШАГОВ' >> $MARKOV_CHAIN_LOG	

				psql -d expecto_db -U expecto_user  -c 'select * from predict_risk_k_diag( 60 )' > $PREDICT_RISK
				exit_code $? $LOG_FILE $ERR_FILE
				cat $PREDICT_RISK >> $MARKOV_CHAIN_LOG  
			fi
		
		fi 

    fi
    ((line_num++))
  done < "$MARKOV_CHAIN_RESULT"
else
  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : WARNING : НЕДОСТАТОЧНО ДАННЫХ ДЛЯ ОБУЧЕНИЯ ЦЕПИ МАРКОВА' >> $MARKOV_CHAIN_LOG  
fi 

echo '**********************************************' >> $MARKOV_CHAIN_LOG

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : АНАЛИЗ ЦЕПИ МАРКОВА - ЗАКОНЧЕН '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : АНАЛИЗ ЦЕПИ МАРКОВА - ЗАКОНЧЕН '>> $LOG_FILE

#
########################################################################################################


#################################################
# Опустить флаг
rm /postgres/pg_expecto/PG_EXPECTO_IN_PROGRESS
#################################################

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : FINISH '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : FINISH '>> $LOG_FILE

exit 0 


