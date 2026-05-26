-- Copyright 2026 Ринат (pg_expecto)
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
-- 
-- http://www.apache.org/licenses/LICENSE-2.0
-- 
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

--------------------------------------------------------------------------------
-- core_cluster_functions.sql
--------------------------------------------------------------------------------
-- Статистика производительности по кластеру
--
-- cluster_stat_median Текущие метрики оценки производительности
--
-- performance_metrics      Расчитать метрики оценки производительности  
-- start_incident 			Начать инцидент производительности приоритет с заданным приоритетов
-- stop_incidents           Завершить инциденты производительности 

--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--Текущие метрики оценки производительности
CREATE OR REPLACE FUNCTION cluster_stat_median() RETURNS integer 
AS $$
DECLARE
  current_calls numeric ; 
  current_rows numeric ; 
  
  current_calls_long  numeric ; 
  current_rows_long  numeric ; 
  
  curr_operating_speed_long  numeric ; 
  curr_waiting_events_long  numeric ; 
    
  --------------------------------------------
  -- ОЖИДАНИЯ  
  bufferpin_long numeric ;
  extension_long numeric ;
  io_long numeric ;
  ipc_long numeric ;
  lock_long numeric ;
  lwlock_long numeric ;
  timeout_long numeric ;
  
  current_bufferpin  bigint  ;
  current_extension  bigint  ;
  current_io  bigint  ;
  current_ipc  bigint  ;
  current_lock  bigint  ;
  current_lwlock  bigint  ;
  current_timeout  bigint  ;  
  wait_stats_rec	record ;
  -- ОЖИДАНИЯ
  --------------------------------------------
  
  --------------------------------------------
  -- Чтение/Запись разделяемых блоков
  current_shared_blks_hit bigint ;
  current_shared_blks_read bigint ;
  current_shared_blks_dirtied bigint ;
  current_shared_blks_written bigint ; 
  current_shared_blk_read_time double precision ;
  current_shared_blk_write_time double precision;
  
  shared_blks_hit_long numeric ;
  shared_blks_read_long numeric ;
  shared_blks_dirtied_long numeric ;
  shared_blks_written_long numeric ;
  shared_blk_read_time_long numeric ;
  shared_blk_write_time_long numeric ;
  
  shared_blk_rec record ; 
  -- Чтение/Запись разделяемых блоков
  --------------------------------------------
  
	
	max_timestamp timestamptz ; 
	

	
BEGIN	
	SELECT 	date_trunc( 'minute' , CURRENT_TIMESTAMP ) 
	INTO 		max_timestamp ;
		
RAISE NOTICE '-- ТЕКУЩИЕ ЗНАЧЕНИЯ';	
-------------------------------------------------------------------------------------------------------------------------
-- ТЕКУЩИЕ ЗНАЧЕНИЯ
    ---------------------------------------------------------------
	-- ОПЕРАЦИОННАЯ СКОРОСТЬ
	SELECT 
		SUM ( st.calls )
	INTO 
		current_calls
	FROM 
		pg_stat_statements st JOIN pg_database pd ON ( pd.oid = st.dbid )
	WHERE 
	    st.toplevel AND  --True, если данный запрос выполнялся на верхнем уровне (всегда true, если для параметра pg_stat_statements.track задано значение top)
		pd.datname NOT IN ('postgres' , 'template1' , 'template0' , 'pgpropwr' , 'expecto_db' ) ;
	IF current_calls IS NULL THEN current_calls = 0 ; END IF ;

	SELECT SUM ( st.rows )
	INTO current_rows
	FROM 
		pg_stat_statements st JOIN pg_database pd ON ( pd.oid = st.dbid )
	WHERE 
	    st.toplevel AND  --True, если данный запрос выполнялся на верхнем уровне (всегда true, если для параметра pg_stat_statements.track задано значение top)
		pd.datname NOT IN ('postgres' , 'template1' , 'template0' , 'pgpropwr' , 'expecto_db' ) ;
	IF current_rows IS NULL THEN current_rows = 0 ; END IF ;
	-- ОПЕРАЦИОННАЯ СКОРОСТЬ
	---------------------------------------------------------------

	---------------------------------------------------------------
	-- ОЖИДАНИЯ		
	FOR wait_stats_rec IN 
	SELECT w.event_type , SUM( w.count ) AS  event_type_count
	FROM pg_wait_sampling_profile w 
		JOIN pg_stat_statements st ON (st.queryid = w.queryid) 
		JOIN pg_database pd ON ( st.dbid = pd.oid )
	WHERE 
	    st.toplevel AND  --True, если данный запрос выполнялся на верхнем уровне (всегда true, если для параметра pg_stat_statements.track задано значение top)
		w.event_type IS NOT NULL AND w.event_type NOT IN ('Activity' , 'Client') AND 
		pd.datname NOT IN ('postgres' , 'template1' , 'template0' , 'pgpropwr' , 'expecto_db' ) 
	GROUP BY w.event_type	
	LOOP
		CASE 
			WHEN wait_stats_rec.event_type = 'BufferPin' THEN current_bufferpin = COALESCE( wait_stats_rec.event_type_count , 0 );
			WHEN wait_stats_rec.event_type = 'Extension' THEN current_extension = COALESCE( wait_stats_rec.event_type_count , 0 );
			WHEN wait_stats_rec.event_type = 'IO' THEN current_io = COALESCE( wait_stats_rec.event_type_count , 0 );
			WHEN wait_stats_rec.event_type = 'IPC' THEN current_ipc = COALESCE( wait_stats_rec.event_type_count , 0 );
			WHEN wait_stats_rec.event_type = 'Lock' THEN current_lock = COALESCE( wait_stats_rec.event_type_count , 0 );
			WHEN wait_stats_rec.event_type = 'LWLock' THEN current_lwlock = COALESCE( wait_stats_rec.event_type_count , 0 );
			WHEN wait_stats_rec.event_type = 'Timeout' THEN current_timeout = COALESCE( wait_stats_rec.event_type_count , 0 );
		END CASE ;		
	END LOOP;
	-- ОЖИДАНИЯ		
	---------------------------------------------------------------
	
	---------------------------------------------------------------
    -- Чтение/Запись разделяемых блоков
	SELECT 
		SUM(shared_blks_hit ) AS sum_shared_blks_hit ,
		SUM(shared_blks_read ) AS sum_shared_blks_read ,
		SUM(shared_blks_dirtied ) AS sum_shared_blks_dirtied ,
		SUM(shared_blks_written ) AS sum_shared_blks_written ,
		SUM(shared_blk_read_time ) AS sum_shared_blk_read_time ,
		SUM(shared_blk_write_time ) AS sum_shared_blk_write_time 
	INTO 
		shared_blk_rec
	FROM 
		pg_stat_statements st JOIN pg_database pd ON ( pd.oid = st.dbid )
	WHERE 
	    st.toplevel AND  --True, если данный запрос выполнялся на верхнем уровне (всегда true, если для параметра pg_stat_statements.track задано значение top)
		pd.datname NOT IN ('postgres' , 'template1' , 'template0' , 'pgpropwr' , 'expecto_db' );
	-- Чтение/Запись разделяемых блоков
	---------------------------------------------------------------

	INSERT INTO cluster_stat
	(
		curr_timestamp  , 
		curr_calls ,
		curr_rows ,
		curr_bufferpin  ,
		curr_extension  ,
		curr_io  ,
		curr_ipc  ,
		curr_lock  ,
		curr_lwlock ,
		curr_timeout , 
		curr_shared_blks_hit , 
		curr_shared_blks_read , 
		curr_shared_blks_dirtied, 
		curr_shared_blks_written, 
		curr_shared_blk_read_time ,
		curr_shared_blk_write_time
	)
	VALUES 
	( 
		max_timestamp , 
		current_calls , 
		current_rows , 
		current_bufferpin  ,
		current_extension  ,
		current_io    ,
		current_ipc   ,
		current_lock  ,
		current_lwlock ,
		current_timeout ,
		shared_blk_rec.sum_shared_blks_hit,
		shared_blk_rec.sum_shared_blks_read,
		shared_blk_rec.sum_shared_blks_dirtied,
		shared_blk_rec.sum_shared_blks_written,
		shared_blk_rec.sum_shared_blk_read_time,
		shared_blk_rec.sum_shared_blk_write_time 
	);		
-- ТЕКУЩИЕ ЗНАЧЕНИЯ	
-------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------
-- МЕДИАНЫ 
	-----------------------------------------------------------------------------
	-- ОПЕРАЦИОННАЯ СКОРОСТЬ
	--Долгая медиана calls	
	SELECT (percentile_cont(0.5) within group (order by curr_calls))::numeric
	INTO current_calls_long
	FROM cluster_stat
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp ;	
	IF current_calls_long IS NULL THEN current_calls_long = 0 ; END IF ;	
	
	--Долгая медиана rows
	SELECT (percentile_cont(0.5) within group (order by curr_rows))::numeric
	INTO current_rows_long
	FROM cluster_stat
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp ;	
	IF current_rows_long IS NULL THEN current_rows_long = 0 ; END IF ;	
	-- ОПЕРАЦИОННАЯ СКОРОСТЬ
	-----------------------------------------------------------------------------
	
	---------------------------------------------------------------------
	-- ОЖИДАНИЯ
	-- Долгая медиана по wait_event_type
	SELECT (percentile_cont(0.5) within group (order by curr_bufferpin))::numeric
	INTO bufferpin_long
	FROM cluster_stat
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp ;	
	IF bufferpin_long IS NULL THEN bufferpin_long = 0 ; END IF ; 
	
	SELECT (percentile_cont(0.5) within group (order by curr_extension))::numeric
	INTO extension_long
	FROM cluster_stat
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp ;	
	IF extension_long IS NULL THEN extension_long = 0 ; END IF ; 

	SELECT (percentile_cont(0.5) within group (order by curr_io))::numeric
	INTO io_long
	FROM cluster_stat
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp ;	
	IF io_long IS NULL THEN io_long = 0 ; END IF ; 
		
	SELECT (percentile_cont(0.5) within group (order by curr_ipc))::numeric
	INTO ipc_long
	FROM cluster_stat
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp ;	
	IF ipc_long IS NULL THEN ipc_long = 0 ; END IF ; 

	SELECT (percentile_cont(0.5) within group (order by curr_lock))::numeric
	INTO lock_long
	FROM cluster_stat
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp ;	
	IF lock_long IS NULL THEN lock_long = 0 ; END IF ; 

	SELECT (percentile_cont(0.5) within group (order by curr_lwlock))::numeric
	INTO lwlock_long
	FROM cluster_stat
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp ;	
	IF lwlock_long IS NULL THEN lwlock_long = 0 ; END IF ; 
	
	SELECT (percentile_cont(0.5) within group (order by curr_timeout))::numeric
	INTO timeout_long
	FROM cluster_stat
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp ;	
	IF timeout_long IS NULL THEN timeout_long = 0 ; END IF ; 	
	
	-- Чтение/Запись разделяемых блоков
	SELECT (percentile_cont(0.5) within group (order by curr_shared_blks_hit))::numeric
	INTO shared_blks_hit_long
	FROM cluster_stat
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp ;	
	IF shared_blks_hit_long IS NULL THEN shared_blks_hit_long = 0 ; END IF ; 	
	
	SELECT (percentile_cont(0.5) within group (order by curr_shared_blks_read))::numeric
	INTO shared_blks_read_long
	FROM cluster_stat
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp ;	
	IF shared_blks_read_long IS NULL THEN shared_blks_read_long = 0 ; END IF ; 	
	
	SELECT (percentile_cont(0.5) within group (order by curr_shared_blks_dirtied))::numeric
	INTO shared_blks_dirtied_long
	FROM cluster_stat
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp ;	
	IF shared_blks_dirtied_long IS NULL THEN shared_blks_dirtied_long = 0 ; END IF ; 	
	
	SELECT (percentile_cont(0.5) within group (order by curr_shared_blks_written))::numeric
	INTO shared_blks_written_long
	FROM cluster_stat
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp ;	
	IF shared_blks_written_long IS NULL THEN shared_blks_written_long = 0 ; END IF ; 	
	
	SELECT (percentile_cont(0.5) within group (order by curr_shared_blk_read_time))::numeric
	INTO shared_blk_read_time_long
	FROM cluster_stat
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp ;	
	IF shared_blk_read_time_long IS NULL THEN shared_blk_read_time_long = 0 ; END IF ; 	
	
	SELECT (percentile_cont(0.5) within group (order by curr_shared_blk_write_time))::numeric
	INTO shared_blk_write_time_long
	FROM cluster_stat
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp ;	
	IF shared_blk_write_time_long IS NULL THEN shared_blk_write_time_long = 0 ; END IF ; 	
-- МЕДИАНЫ 
-----------------------------------------------------------------------------------

-------------------------------------------------------------------------------------------------------------------------
-- РАСЧЕТ МЕТРИК ПРОИЗВОДИТЕЛЬНОСТИ	
	-- ОПЕРАЦИОННАЯ СКОРОСТЬ 
		curr_operating_speed_long =  ( current_calls_long + current_rows_long  )  ;
	-- ОПЕРАЦИОННАЯ СКОРОСТЬ 
	
	-- ОЖИДАНИЯ  
		curr_waiting_events_long = bufferpin_long + extension_long +  io_long + ipc_long + lock_long + lwlock_long + timeout_long; 
	-- ОЖИДАНИЯ  
	
-- РАСЧЕТ МЕТРИК ПРОИЗВОДИТЕЛЬНОСТИ 	
-------------------------------------------------------------------------------------------------------------------------

-- СОХРАНИТЬ СТАТИСТИКУ
INSERT INTO cluster_stat_median 
	(
		curr_timestamp, 
		curr_op_speed ,		
		curr_waitings ,
		curr_bufferpin  ,
		curr_extension ,
		curr_io  ,
		curr_ipc  ,
		curr_lock  ,
		curr_lwlock ,  
		curr_timeout ,
		curr_shared_blks_hit ,
		curr_shared_blks_read ,
		curr_shared_blks_dirtied ,
		curr_shared_blks_written ,
		curr_shared_blk_read_time ,
		curr_shared_blk_write_time
	)
	VALUES 
	(
		max_timestamp , 
		curr_operating_speed_long , 		
		curr_waiting_events_long , 		
		bufferpin_long ,
		extension_long ,
		io_long ,
		ipc_long ,
		lock_long ,
		lwlock_long ,
		timeout_long ,
		shared_blks_hit_long ,
		shared_blks_read_long ,
		shared_blks_dirtied_long ,
		shared_blks_written_long ,
		shared_blk_read_time_long ,
		shared_blk_write_time_long 
	);
-- СОХРАНИТЬ СТАТИСТИКУ
----------------------------------------------------------------------------

return 0 ; 

END
$$ LANGUAGE plpgsql; 
COMMENT ON FUNCTION cluster_stat_median IS 'Очистка старых статистических данных.';
--Текущие метрики оценки производительности
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--Расчитать метрики оценки производительности  
CREATE OR REPLACE FUNCTION performance_metrics() RETURNS text AS $$
DECLARE
 result_str text;
 timepoint timestamptz ;
 cluster_stat_median_rec record ;
 
 speed_waitings_correlation DOUBLE PRECISION ;

 regr_slope_value DOUBLE PRECISION;

 speed_degradation_indicator integer ;

 speed_regr_slope_value DOUBLE PRECISION;
 waitings_regr_slope_value DOUBLE PRECISION;

 speed_regr_rec record;
 waitings_regr_rec record;

BEGIN
	SELECT MAX(curr_timestamp)
	INTO timepoint 
	FROM cluster_stat_median ; 
	
	
	SELECT 	
		COALESCE ( curr_op_speed , 0 ) AS curr_op_speed , 			
		COALESCE ( curr_waitings , 0 ) AS curr_waitings	
	INTO 	
		cluster_stat_median_rec
	FROM 	
		cluster_stat_median 
	WHERE 	
		curr_timestamp = timepoint ; 
	
	
	-------------------------------------------------------------------
	--КОРРЕЛЯЦИЯ СКОРОСТЬ - ОЖИДАНИЯ
	SELECT COALESCE( corr( curr_op_speed ,  curr_waitings ) , 0 ) AS correlation_value 
	INTO speed_waitings_correlation
	FROM
		 cluster_stat_median
	WHERE 
		curr_timestamp BETWEEN timepoint - interval '1 hour' AND timepoint ; 
	--КОРРЕЛЯЦИЯ АКТИВНЫЕ СЕССИИ - СКОРОСТЬ 
	-------------------------------------------------------------------
	DROP TABLE IF EXISTS tmp_timepoints;
	CREATE TEMPORARY TABLE tmp_timepoints
	(
		curr_timestamp timestamptz  ,   
		curr_timepoint integer 
	);


	INSERT INTO tmp_timepoints
	(
		curr_timestamp ,	
		curr_timepoint 
	)
	SELECT 
		curr_timestamp , 
		row_number() over (order by curr_timestamp) AS x
	FROM
	cluster_stat_median
	WHERE 
		curr_timestamp BETWEEN timepoint - interval '1 hour' AND timepoint 
	ORDER BY curr_timestamp	;

	----------------------------------------------------------------------------------------------------
	-- ОПЕРАЦИОННАЯ СКОРОСТЬ
    -- 	линия регрессии  скорости  : Y = a + bX
	BEGIN
		WITH stats AS 
		(
		  SELECT 
			AVG(t.curr_timepoint::DOUBLE PRECISION) as avg1, 
			STDDEV(t.curr_timepoint::DOUBLE PRECISION) as std1,
			AVG(s.curr_op_speed::DOUBLE PRECISION) as avg2, 
			STDDEV(s.curr_op_speed::DOUBLE PRECISION) as std2
		  FROM
			cluster_stat_median s JOIN tmp_timepoints t ON ( s.curr_timestamp  = t.curr_timestamp )
		  WHERE 
			t.curr_timestamp BETWEEN timepoint - interval '1 hour' AND timepoint 
		),
		standardized_data AS 
		(
			SELECT 
				(t.curr_timepoint::DOUBLE PRECISION - avg1) / std1 as x_z,
				(s.curr_op_speed::DOUBLE PRECISION - avg2) / std2 as y_z
			FROM
				cluster_stat_median s JOIN tmp_timepoints t ON ( s.curr_timestamp  = t.curr_timestamp ) , stats
			WHERE 
				t.curr_timestamp BETWEEN timepoint - interval '1 hour' AND timepoint  
		)	
		SELECT
			REGR_SLOPE(y_z, x_z) as slope, --b
			ATAN(REGR_SLOPE(y_z, x_z)) * 180 / PI() as slope_angle_degrees, --угол наклона
			REGR_R2(y_z, x_z) as r_squared -- Коэффициент детерминации
		INTO 
			speed_regr_rec
		FROM standardized_data;
	EXCEPTION
	  --STDDEV(s.curr_op_speed::DOUBLE PRECISION) = 0  
	  WHEN division_by_zero THEN  -- Конкретное исключение для деления на ноль
	    SELECT 
			1.0 as slope, --b
			0.0  as slope_angle_degrees, --угол наклона
			0.0  as r_squared -- Коэффициент детерминации
		INTO 
		speed_regr_rec ;
	END;
	speed_regr_slope_value = speed_regr_rec.slope_angle_degrees ; 	
	-- 	линия регрессии  скорости  : Y = a + bX
	-- ОПЕРАЦИОННАЯ СКОРОСТЬ
	-------------------------------------------------------------------
	
	----------------------------------------------------------------------------------------------------
	-- ОЖИДАНИЯ
    -- 	линия регрессии  скорости  : Y = a + bX
	BEGIN 
		WITH stats AS 
		(
		  SELECT 
			AVG(t.curr_timepoint::DOUBLE PRECISION) as avg1, 
			STDDEV(t.curr_timepoint::DOUBLE PRECISION) as std1,
			AVG(s.curr_waitings::DOUBLE PRECISION) as avg2, 
			STDDEV(s.curr_waitings::DOUBLE PRECISION) as std2
		  FROM
			cluster_stat_median s JOIN tmp_timepoints t ON ( s.curr_timestamp  = t.curr_timestamp )
		  WHERE 
			t.curr_timestamp BETWEEN timepoint - interval '1 hour' AND timepoint  
		),
		standardized_data AS 
		(
			SELECT 
				(t.curr_timepoint::DOUBLE PRECISION - avg1) / std1 as x_z,
				(s.curr_waitings::DOUBLE PRECISION - avg2) / std2 as y_z
			FROM
				cluster_stat_median s JOIN tmp_timepoints t ON ( s.curr_timestamp  = t.curr_timestamp ) , stats
			WHERE 
				t.curr_timestamp BETWEEN timepoint - interval '1 hour' AND timepoint  
		)	
		SELECT
			REGR_SLOPE(y_z, x_z) as slope, --b
			ATAN(REGR_SLOPE(y_z, x_z)) * 180 / PI() as slope_angle_degrees, --угол наклона
			REGR_R2(y_z, x_z) as r_squared -- Коэффициент детерминации
		INTO 
			waitings_regr_rec
		FROM standardized_data;
	EXCEPTION
	  --STDDEV(s.curr_waitings::DOUBLE PRECISION) = 0  
	  WHEN division_by_zero THEN  -- Конкретное исключение для деления на ноль
	    SELECT 
			1.0 as slope, --b
			0.0  as slope_angle_degrees, --угол наклона
			0.0  as r_squared -- Коэффициент детерминации
		INTO 
		waitings_regr_rec ;
	END;
	waitings_regr_slope_value = waitings_regr_rec.slope_angle_degrees ; 	
	-- 	линия регрессии  скорости  : Y = a + bX
	-- ОЖИДАНИЯ
	-------------------------------------------------------------------
	
	-- ЕСЛИ 
	--  угол наклона линии скорости < 0 
	-- И 
	--  угол наклона линии ожиданий > 0 
	-- ТО деградация

	IF speed_regr_slope_value < 0 
	   AND 
	   waitings_regr_slope_value > 0 
	THEN		
		--Слабая и умеренная корреляция
		IF ABS(speed_waitings_correlation) < 0.7 AND SIGN(speed_waitings_correlation) = -1 
		THEN 
			speed_degradation_indicator = -50 ;
			CALL start_incident(4);
		END IF ;
		
		--Сильная корреляция
		IF ABS(speed_waitings_correlation) >= 0.7 AND SIGN(speed_waitings_correlation) = -1 
		THEN 
			speed_degradation_indicator = -100 ;
			CALL start_incident(3);
		END IF ;
	ELSE
		speed_degradation_indicator = 0 ;
		CALL stop_incidents();
	END IF ;
	-------------------------------------------------------------------
	
	
    -- операционная скорость | ожидания |  индикатор 
	result_str= ROUND( cluster_stat_median_rec.curr_op_speed::numeric , 2 )||'|'||  --1
				ROUND( cluster_stat_median_rec.curr_waitings::numeric , 2 )||'|'||  --2
				speed_degradation_indicator ||'|'  --3
				;
	
	
    --------------------------------------------------------------------------------------------------------
	--	ОБУЧЕНИЕ ЦЕПИ МАРКОВА
		PERFORM markov_chain_training();
	--	ОБУЧЕНИЕ ЦЕПИ МАРКОВА
	--------------------------------------------------------------------------------------------------------
	
	
	return result_str;
END
$$ LANGUAGE plpgsql  ;
COMMENT ON FUNCTION performance_metrics IS 'Расчитать метрики оценки производительности ';
-- Расчитать метрики оценки производительности 
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Завершить инциденты производительности 
CREATE OR REPLACE PROCEDURE stop_incidents() AS $$
DECLARE 
  timepoint timestamp with time zone ;   
BEGIN
	SELECT date_trunc( 'minute' , CURRENT_TIMESTAMP )
	INTO timepoint ;

	-------------------------------
	-- Закрыть инциденты 
	UPDATE performance_incident
	SET finish_timepoint = timepoint 
	WHERE finish_timepoint IS NULL  ; 
	-- Закрыть инциденты 
	-------------------------------	
END 
$$ LANGUAGE plpgsql ;
COMMENT ON PROCEDURE  stop_incidents IS 'Завершить инциденты производительности';
-- Завершить инциденты производительности 
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Начать инцидент производительности приоритет с заданным приоритетов 
CREATE OR REPLACE PROCEDURE start_incident( curr_priority integer ) 
AS $$
DECLARE 
  p_count integer ;
BEGIN
	SELECT 
		count(id)
	INTO 
		p_count
	FROM 
		performance_incident
	WHERE
		priority = curr_priority AND 
		finish_timepoint IS NULL ; 

	
	--Новый инцидент
	IF p_count = 0 
	THEN 
		INSERT INTO performance_incident
		(
			priority ,
			start_timepoint
		)
		VALUES 
		( 
			curr_priority , 
			date_trunc( 'minute' , CURRENT_TIMESTAMP )
		);	
	END IF ; 
END 
$$ LANGUAGE plpgsql ;
COMMENT ON PROCEDURE  start_incident IS 'Начать инцидент производительности приоритет с заданным приоритетов';
-- Начать инцидент производительности приоритет с заданным приоритетов
--------------------------------------------------------------------------------





--------------------------------------------------------------------------------
-- core_cluster_tables.sql
--------------------------------------------------------------------------------
--Статистика уровня кластера 
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--Текущая статистика
DROP TABLE IF EXISTS cluster_stat;
CREATE UNLOGGED TABLE cluster_stat 
(
  	id SERIAL , 	
	curr_timestamp timestamp with time zone , 

	curr_calls bigint,
	curr_rows bigint,
	
	curr_bufferpin  bigint,
	curr_extension  bigint,
	curr_io  bigint,
	curr_ipc  bigint,
	curr_lock  bigint,
	curr_lwlock bigint,
	curr_timeout bigint , 
	
	curr_shared_blks_hit  bigint , 
	curr_shared_blks_read  bigint , 
	curr_shared_blks_dirtied bigint , 
	curr_shared_blks_written bigint , 
	--если включён track_io_timing, иначе ноль
	curr_shared_blk_read_time double precision ,
	curr_shared_blk_write_time double precision
	--если включён track_io_timing, иначе ноль
);
ALTER TABLE cluster_stat ADD CONSTRAINT cluster_stat_pk PRIMARY KEY (id);
CREATE INDEX cluster_stat_idx ON cluster_stat ( curr_timestamp );

COMMENT ON TABLE cluster_stat IS 'Исходные данные для расчетов метрик оценки производительности и ожиданий';
COMMENT ON COLUMN cluster_stat.curr_timestamp IS 'Точка времени сбора данных ';
COMMENT ON COLUMN cluster_stat.curr_calls IS 'Число выполнений';
COMMENT ON COLUMN cluster_stat.curr_rows IS 'Общее число строк, полученных или затронутых оператором';
COMMENT ON COLUMN cluster_stat.curr_bufferpin IS 'Количество ожиданий типа BufferPin';
COMMENT ON COLUMN cluster_stat.curr_extension IS 'Количество ожиданий типа Extension';
COMMENT ON COLUMN cluster_stat.curr_io IS 'Количество ожиданий типа IO';
COMMENT ON COLUMN cluster_stat.curr_ipc IS 'Количество ожиданий типа IPC';
COMMENT ON COLUMN cluster_stat.curr_lock IS 'Количество ожиданий типа Lock';
COMMENT ON COLUMN cluster_stat.curr_lwlock IS 'Количество ожиданий типа LWLock';
COMMENT ON COLUMN cluster_stat.curr_timeout IS 'Количество ожиданий типа Timeout';
COMMENT ON COLUMN cluster_stat.curr_shared_blks_hit IS 'Общее число попаданий разделяемых блоков в кеш';
COMMENT ON COLUMN cluster_stat.curr_shared_blks_read IS 'Общее число прочитанных разделяемых блоков';
COMMENT ON COLUMN cluster_stat.curr_shared_blks_dirtied IS 'Общее число загрязнённых разделяемых блоков';
COMMENT ON COLUMN cluster_stat.curr_shared_blks_written IS 'Общее число записанных разделяемых блоков';
COMMENT ON COLUMN cluster_stat.curr_shared_blk_read_time IS 'Общее время на чтение разделяемых блоков';
COMMENT ON COLUMN cluster_stat.curr_shared_blk_write_time IS 'Общее время на запись разделяемых блоков';
--Текущая статистика
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--Скользящие медианы
DROP TABLE IF EXISTS cluster_stat_median;
CREATE TABLE cluster_stat_median 
(
	id SERIAL , 
	curr_timestamp timestamp with time zone , 
	
	curr_op_speed numeric ,
	curr_waitings numeric ,		

	curr_bufferpin  numeric,
	curr_extension  numeric,
	curr_io  numeric,
	curr_ipc  numeric,
	curr_lock  numeric,
	curr_lwlock numeric , 
	curr_timeout numeric , 
	
	curr_shared_blks_hit numeric , 
	curr_shared_blks_read numeric , 
	curr_shared_blks_dirtied numeric , 
	curr_shared_blks_written numeric , 
	--если включён track_io_timing, иначе ноль
	curr_shared_blk_read_time double precision ,
	curr_shared_blk_write_time double precision
	--если включён track_io_timing, иначе ноль
);
ALTER TABLE cluster_stat_median ADD CONSTRAINT cluster_stat_median_pk PRIMARY KEY (id);
CREATE INDEX cluster_stat_median_idx ON cluster_stat_median ( curr_timestamp );

COMMENT ON TABLE cluster_stat_median IS 'Скользящие медианы метрик оценки производительности и ожиданий';
COMMENT ON COLUMN cluster_stat_median.curr_timestamp IS 'Точка времени сбора данных ';
COMMENT ON COLUMN cluster_stat_median.curr_op_speed IS 'Медиана операционной скорости';
COMMENT ON COLUMN cluster_stat_median.curr_waitings IS 'Медиана ожиданий';
COMMENT ON COLUMN cluster_stat_median.curr_bufferpin IS 'Медиана количества  ожиданий типа BufferPin';
COMMENT ON COLUMN cluster_stat_median.curr_extension IS 'Медиана количества  ожиданий типа Extension';
COMMENT ON COLUMN cluster_stat_median.curr_io IS 'Медиана количества  ожиданий типа IO';
COMMENT ON COLUMN cluster_stat_median.curr_ipc IS 'Медиана количества  ожиданий типа IPC';
COMMENT ON COLUMN cluster_stat_median.curr_lock IS 'Медиана количества  ожиданий типа Lock';
COMMENT ON COLUMN cluster_stat_median.curr_lwlock IS 'Медиана количества  ожиданий типа LWLock';
COMMENT ON COLUMN cluster_stat_median.curr_timeout IS 'Медиана количества  ожиданий типа Timeout';
COMMENT ON COLUMN cluster_stat_median.curr_shared_blks_hit IS 'Общее число попаданий разделяемых блоков в кеш';
COMMENT ON COLUMN cluster_stat_median.curr_shared_blks_read IS 'Общее число прочитанных разделяемых блоков';
COMMENT ON COLUMN cluster_stat_median.curr_shared_blks_dirtied IS 'Общее число загрязнённых разделяемых блоков';
COMMENT ON COLUMN cluster_stat_median.curr_shared_blks_written IS 'Общее число записанных разделяемых блоков';
COMMENT ON COLUMN cluster_stat_median.curr_shared_blk_read_time IS 'Общее время на чтение разделяемых блоков';
COMMENT ON COLUMN cluster_stat_median.curr_shared_blk_write_time IS 'Общее время на запись разделяемых блоков';
--Скользящие медианы
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Инциденты производительности
DROP TABLE IF EXISTS performance_incident ;
CREATE TABLE performance_incident 
(
	id BIGSERIAL ,
	priority smallint , 
	
	start_timepoint timestamptz , 
	finish_timepoint timestamptz 
);
--------------------------------------------------------------------------------
ALTER TABLE performance_incident ADD CONSTRAINT performance_incident_pk PRIMARY KEY (id);
CREATE INDEX performance_incident_idx1 ON performance_incident ( start_timepoint );
CREATE INDEX performance_incident_idx_priority ON performance_incident (id) WHERE finish_timepoint IS NULL;

-- Инциденты производительности
--------------------------------------------------------------------------------





--------------------------------------------------------------------------------
-- core_functions.sql
-- Updated 23.04.2026
--------------------------------------------------------------------------------
-- Корневые и сервисные функции
--
-- default_configuration() Установить базовую конфигурацию
-- set_day_for_store Установить глубину хранения
--
-- cleaning() Удалить старые данные из статистических таблиц
--
-- get_hour_before Получить текстовую строчку времени на час раньше
--

-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Установить базовую конфигурацию
CREATE OR REPLACE FUNCTION default_configuration() RETURNS integer AS $$
BEGIN
	TRUNCATE TABLE configuration ; 
	INSERT INTO  configuration ( day_for_store  ) VALUES ( 7 ) ; 	
return  0 ;
END
$$ LANGUAGE plpgsql ;
COMMENT ON FUNCTION default_configuration IS 'Установить значение параметров конфигурации по умолчанию.';
-- Установить базовую конфигурацию
-------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Установить глубину хранения
CREATE OR REPLACE FUNCTION set_day_for_store( new_counter integer ) RETURNS integer AS $$
DECLARE 
 configuration_rec record ;
BEGIN
	UPDATE  configuration 
	SET day_for_store = new_counter ; 
	
return  0 ;
END
$$ LANGUAGE plpgsql ;
COMMENT ON FUNCTION set_day_for_store IS 'Установить грубину хранения статистических данных.';
-- Установить глубину хранения
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Удалить старые данные из статистических таблиц
CREATE OR REPLACE FUNCTION cleaning() RETURNS integer AS $$
DECLARE 
current_day_for_store integer ;
BEGIN
	SELECT day_for_store
	INTO current_day_for_store
	FROM  configuration	; 
	
	--Исходные данные по кластеру актуальны 1 час
		DELETE FROM cluster_stat WHERE curr_timestamp < CURRENT_TIMESTAMP - ( interval '1 hour'  );
		DELETE FROM os_stat_vmstat WHERE curr_timestamp < CURRENT_TIMESTAMP - ( interval '1 hour'  );
		DELETE FROM os_stat_iostat_device WHERE curr_timestamp < CURRENT_TIMESTAMP - ( interval '1 hour'  );	
	--Исходные данные по кластеру  актуальны 1 час
	
	--Очистить старые статистические данные
		DELETE FROM statement_stat WHERE curr_timestamp < CURRENT_TIMESTAMP - ( interval '1 day' * current_day_for_store );	
		DELETE FROM cluster_stat_median WHERE curr_timestamp < CURRENT_TIMESTAMP - ( interval '1 day' * current_day_for_store );	
		DELETE FROM statement_stat_wait_events WHERE curr_timestamp < CURRENT_TIMESTAMP - ( interval '1 day' * current_day_for_store );	
		DELETE FROM statement_stat_median WHERE curr_timestamp < CURRENT_TIMESTAMP - ( interval '1 day' * current_day_for_store );	
		DELETE FROM statement_stat_waitings_median WHERE curr_timestamp < CURRENT_TIMESTAMP - ( interval '1 day' * current_day_for_store );
		DELETE FROM os_stat_vmstat_median WHERE curr_timestamp < CURRENT_TIMESTAMP - ( interval '1 day' * current_day_for_store );
		DELETE FROM os_stat_iostat_device_median WHERE curr_timestamp < CURRENT_TIMESTAMP - ( interval '1 day' * current_day_for_store );	
	--Очистить старые статистические данные
	
	--Удалить тексты запросов для которых уже нет исходных данных  
		DELETE FROM statement_stat_sql sss WHERE NOT EXISTS ( SELECT 1 FROM statement_stat ssm WHERE sss.queryid = ssm.queryid );
	--Удалить тексты запросов для которых уже нет исходных данных 
	
	--Удалить старые инциденты производительности 
		DELETE FROM performance_incident WHERE start_timepoint < CURRENT_TIMESTAMP - ( interval '1 day' * current_day_for_store );	
	--Удалить старые инциденты производительности 
	
	--Удалить старые данные по статистике autovacuum актуальны 1 час
		DELETE FROM autovacuum_log_events WHERE curr_timestamp  < CURRENT_TIMESTAMP - ( interval '1 hour'  );
	--Удалить старые данные по статистике autovacuum актуальны 1 час
	
return 0 ;
END
$$ LANGUAGE plpgsql ;
COMMENT ON FUNCTION cleaning IS 'Очистка старых статистических данных.';
-- Удалить старые данные из статистических таблиц
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Получить текстовую строчку времени на час раньше
CREATE OR REPLACE FUNCTION get_hour_before( finish_timestamp text ) RETURNS text AS $$
DECLARE
 result_str text;
BEGIN
	SELECT 
		to_char(to_timestamp(finish_timestamp , 'YYYY-MM-DD HH24:MI') - interval '1 hour','YYYY-MM-DD HH24:MI')
	INTO 
		result_str ; 
		
	return result_str ; 
END
$$ LANGUAGE plpgsql  ;
COMMENT ON FUNCTION get_hour_before IS 'Получить текстовую строчку времени на час раньше';
-- get_hour_before Получить текстовую строчку времени на час раньше
--------------------------------------------------------------------------------


--------------------------------------------------------------------------------
-- core_os_functions.sql
--------------------------------------------------------------------------------
-- Статистика уровня ОС
--------------------------------------------------------------------------------
--
-- os_stat_vmstat Сформировать статистику по метрикам vmstat
--
-- os_stat_iostat_device Сформировать статистику по метрикам iostat
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Сформировать статистику по метрикам vmstat
CREATE OR REPLACE FUNCTION os_stat_vmstat( vmstat_string text ) RETURNS integer 

AS $$
DECLARE
    current_procs_r numeric ; -- r — процессы в run queue (готовы к выполнению)
	current_procs_b numeric ; -- b — процессы в uninterruptible sleep (обычно ждут IO)
	
	current_memory_swpd numeric ; -- swpd — объём свопа
	current_memory_free numeric ; -- free — свободная RAM
	current_memory_buff numeric ; -- buff — буферы
	current_memory_cache numeric ;-- cache — кэш
	
	current_swap_si numeric ; -- si — swap in (из swap в RAM)
	current_swap_so numeric ; -- so — swap out (из RAM в swap)
	
	current_io_bi numeric; -- bi — блоки, считанные с устройств
	current_io_bo numeric; -- bo — записанные на устройства 
	
	current_system_in numeric; -- in — прерывания
	current_system_cs numeric; -- cs — переключения контекста
	
	current_cpu_us numeric ; -- us — user time
	current_cpu_sy numeric ; -- sy — system time
	current_cpu_id numeric ; -- id — idle
	current_cpu_wa numeric ; --  wa — ожидание IO
	current_cpu_st numeric ;  --  st — stolen (украдено гипервизором)
	
	--VM
	current_dirty_kb numeric;		--dirty pages size (KB)
	current_dirty_percent numeric ;	--% от dirty_ratio
	current_dirty_bg_percent numeric;	--% от dirty_background_ratio
	current_available_mem_mb numeric ;		--free + cached memory
	--VM
	
	
    curr_procs_r_long numeric ; -- r — процессы в run queue (готовы к выполнению)
	curr_procs_b_long numeric ; -- b — процессы в uninterruptible sleep (обычно ждут IO)
	
	curr_memory_swpd_long numeric ; -- swpd — объём свопа
	curr_memory_free_long numeric ; -- free — свободная RAM
	curr_memory_buff_long numeric ; -- buff — буферы
	curr_memory_cache_long numeric ;-- cache — кэш
	
	curr_swap_si_long numeric ; -- si — swap in (из swap в RAM)
	curr_swap_so_long numeric ; -- so — swap out (из RAM в swap)
	
	curr_io_bi_long numeric ; -- bi — блоки, считанные с устройств
	curr_io_bo_long numeric ;-- bo — записанные на устройства 
	
	curr_system_in_long numeric ; -- in — прерывания
	curr_system_cs_long numeric ; -- cs — переключения контекста
	
	curr_cpu_us_long numeric ; -- us — user time
	curr_cpu_sy_long numeric ; -- sy — system time
	curr_cpu_id_long numeric ; -- id — idle
	curr_cpu_wa_long numeric ; --  wa — ожидание IO
	curr_cpu_st_long numeric	;  --  st — stolen (украдено гипервизором)
	
	--VM
	curr_dirty_kb_long numeric ;		--dirty pages size (KB)
	curr_dirty_percent_long numeric ;		--% от dirty_ratio
	curr_dirty_bg_percent_long numeric ;		--% от dirty_background_ratio
	curr_available_mem_mb_long numeric 	;	--free + cached memory
	--VM
	
	vmstat_array  text[] ;
	
	min_timestamp timestamptz ; 
	max_timestamp timestamptz ;  
	
BEGIN	
	SELECT 	date_trunc( 'minute' , CURRENT_TIMESTAMP ) 
	INTO 		max_timestamp ;
	
	SELECT string_to_array(vmstat_string , ' ' )
	INTO vmstat_array ;

	-- r — процессы в run queue (готовы к выполнению)
	current_procs_r = vmstat_array[1]::numeric ; 
	-- b — процессы в uninterruptible sleep (обычно ждут IO)
	current_procs_b = vmstat_array[2]::numeric ; 
	-- swpd — объём свопа
	current_memory_swpd = vmstat_array[3]::numeric ; 
	-- free — свободная RAM
	current_memory_free = vmstat_array[4]::numeric ; 
	-- buff — буферы
	current_memory_buff = vmstat_array[5]::numeric ; 
	-- cache — кэш
	current_memory_cache = vmstat_array[6]::numeric ; 
	-- si — swap in (из swap в RAM)
	current_swap_si = vmstat_array[7]::numeric ; 
	-- so — swap out (из RAM в swap)
	current_swap_so = vmstat_array[8]::numeric ; 
	-- bi — блоки, считанные с устройств
	current_io_bi = vmstat_array[9]::numeric ; 
	-- bo — записанные на устройства 
	current_io_bo = vmstat_array[10]::numeric ; 
	-- in — прерывания
	current_system_in = vmstat_array[11]::numeric ; 
	-- cs — переключения контекста
	current_system_cs = vmstat_array[12]::numeric ; 
	-- us — user time
	current_cpu_us = vmstat_array[13]::numeric ; 
	-- sy — system time
	current_cpu_sy = vmstat_array[14]::numeric ; 
	-- id — idle
	current_cpu_id = vmstat_array[15]::numeric ; 
	--  wa — ожидание IO
	current_cpu_wa = vmstat_array[16]::numeric ; 
	--  st — stolen (украдено гипервизором)
	current_cpu_st = vmstat_array[17]::numeric ; 
	
	
	--dirty pages size (KB)
	current_dirty_kb  = vmstat_array[20]::numeric ; 		
	--% от dirty_ratio
	current_dirty_percent = vmstat_array[21]::numeric ; 
	--% от dirty_background_ratio
	current_dirty_bg_percent = vmstat_array[22]::numeric ; 	
	--free + cached memory
	current_available_mem_mb  = vmstat_array[23]::numeric ; 	
	

--Сохранить текущие значения
	INSERT INTO os_stat_vmstat
	(
		curr_timestamp , 

		procs_r  , -- r — процессы в run queue (готовы к выполнению)
		procs_b  , -- b — процессы в uninterruptible sleep (обычно ждут IO)
		
		memory_swpd  , -- swpd — объём свопа
		memory_free  , -- free — свободная RAM
		memory_buff  , -- buff — буферы
		memory_cache  ,-- cache — кэш
		
		swap_si  , -- si — swap in (из swap в RAM)
		swap_so  , -- so — swap out (из RAM в swap)
		
		io_bi , -- bi — блоки, считанные с устройств
		io_bo , -- bo — записанные на устройства 
		
		system_in , -- in — прерывания
		system_cs , -- cs — переключения контекста
		
		cpu_us  , -- us — user time
		cpu_sy   , -- sy — system time
		cpu_id   , -- id — idle
		cpu_wa   , --  wa — ожидание IO
		cpu_st 	  ,--  st — stolen (украдено гипервизором)
		
		dirty_kb , 			    --dirty pages size (KB)
		dirty_percent  ,		--% от dirty_ratio
		dirty_bg_percent  ,		--% от dirty_background_ratio
		available_mem_mb  		--free + cached memory
	--VM
	)
	VALUES 
	(
		max_timestamp ,
		current_procs_r  , -- r — процессы в run queue (готовы к выполнению)
		current_procs_b  , -- b — процессы в uninterruptible sleep (обычно ждут IO)
		
		current_memory_swpd  , -- swpd — объём свопа
		current_memory_free  , -- free — свободная RAM
		current_memory_buff  , -- buff — буферы
		current_memory_cache  ,-- cache — кэш
		
		current_swap_si  , -- si — swap in (из swap в RAM)
		current_swap_so  , -- so — swap out (из RAM в swap)
		
		current_io_bi , -- bi — блоки, считанные с устройств
		current_io_bo , -- bo — записанные на устройства 
		
		current_system_in , -- in — прерывания
		current_system_cs , -- cs — переключения контекста
		
		current_cpu_us  , -- us — user time
		current_cpu_sy  , -- sy — system time
		current_cpu_id  , -- id — idle
		current_cpu_wa  , --  wa — ожидание IO
		current_cpu_st 	, --  st — stolen (украдено гипервизором)
		
		current_dirty_kb , --dirty pages size (KB)
		current_dirty_percent , --% от dirty_ratio
		current_dirty_bg_percent , --% от dirty_background_ratio
		current_available_mem_mb  --free + cached memory
		
	);
--Сохранить текущие значения	
	
--Скользящие медианы
	-- r — процессы в run queue (готовы к выполнению)
	SELECT (percentile_cont(0.5) within group (order by procs_r))::numeric
	INTO curr_procs_r_long
	FROM os_stat_vmstat
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp ;	
	IF curr_procs_r_long IS NULL THEN curr_procs_r_long = 0 ; END IF ; 
	
	-- b — процессы в uninterruptible sleep (обычно ждут IO)
	SELECT (percentile_cont(0.5) within group (order by procs_b))::numeric
	INTO curr_procs_b_long
	FROM os_stat_vmstat
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp ;	
	IF curr_procs_b_long IS NULL THEN curr_procs_b_long = 0 ; END IF ; 
	
	-- swpd — объём свопа
	SELECT (percentile_cont(0.5) within group (order by memory_swpd))::numeric
	INTO curr_memory_swpd_long
	FROM os_stat_vmstat
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp ;	
	IF curr_memory_swpd_long IS NULL THEN curr_memory_swpd_long = 0 ; END IF ; 
	
	-- free — свободная RAM
	SELECT (percentile_cont(0.5) within group (order by memory_free))::numeric
	INTO curr_memory_free_long
	FROM os_stat_vmstat
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp ;	
	IF curr_memory_free_long IS NULL THEN curr_memory_free_long = 0 ; END IF ;
	
	-- buff — буферы
	SELECT (percentile_cont(0.5) within group (order by memory_buff))::numeric
	INTO curr_memory_buff_long
	FROM os_stat_vmstat
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp ;	
	IF curr_memory_buff_long IS NULL THEN curr_memory_buff_long = 0 ; END IF ;
	
	-- cache — кэш
	SELECT (percentile_cont(0.5) within group (order by memory_cache))::numeric
	INTO curr_memory_cache_long
	FROM os_stat_vmstat
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp ;	
	IF curr_memory_cache_long IS NULL THEN curr_memory_cache_long = 0 ; END IF ;
	
	-- si — swap in (из swap в RAM)
	SELECT (percentile_cont(0.5) within group (order by swap_si))::numeric
	INTO curr_swap_si_long
	FROM os_stat_vmstat
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp ;	
	IF curr_swap_si_long IS NULL THEN curr_swap_si_long = 0 ; END IF ;
	
	-- so — swap out (из RAM в swap)
	SELECT (percentile_cont(0.5) within group (order by swap_so))::numeric
	INTO curr_swap_so_long
	FROM os_stat_vmstat
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp ;	
	IF curr_swap_so_long IS NULL THEN curr_swap_so_long = 0 ; END IF ;
	
	-- bi — блоки, считанные с устройств
	SELECT (percentile_cont(0.5) within group (order by io_bi))::numeric
	INTO curr_io_bi_long
	FROM os_stat_vmstat
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp ;	
	IF curr_io_bi_long IS NULL THEN curr_io_bi_long = 0 ; END IF ;
	
	-- bo — записанные на устройства 
	SELECT (percentile_cont(0.5) within group (order by io_bo))::numeric
	INTO curr_io_bo_long 
	FROM os_stat_vmstat
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp ;	
	IF curr_io_bo_long  IS NULL THEN curr_io_bo_long  = 0 ; END IF ;
	
	-- in — прерывания	
	SELECT (percentile_cont(0.5) within group (order by system_in))::numeric
	INTO curr_system_in_long 
	FROM os_stat_vmstat
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp ;	
	IF curr_system_in_long  IS NULL THEN curr_system_in_long  = 0 ; END IF;
	
	-- cs — переключения контекста
	SELECT (percentile_cont(0.5) within group (order by system_cs))::numeric
	INTO curr_system_cs_long 
	FROM os_stat_vmstat
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp ;	
	IF curr_system_cs_long  IS NULL THEN curr_system_cs_long  = 0 ; END IF;
	
	-- us — user time
	SELECT (percentile_cont(0.5) within group (order by cpu_us))::numeric
	INTO curr_cpu_us_long 
	FROM os_stat_vmstat
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp ;	
	IF curr_cpu_us_long  IS NULL THEN curr_cpu_us_long  = 0 ; END IF;
	
	-- sy — system time
	SELECT (percentile_cont(0.5) within group (order by cpu_sy))::numeric
	INTO curr_cpu_sy_long 
	FROM os_stat_vmstat
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp ;	
	IF curr_cpu_sy_long  IS NULL THEN curr_cpu_sy_long  = 0 ; END IF;
	
	-- id — idle
	SELECT (percentile_cont(0.5) within group (order by cpu_id))::numeric
	INTO curr_cpu_id_long 
	FROM os_stat_vmstat
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp ;	
	IF curr_cpu_id_long  IS NULL THEN curr_cpu_id_long  = 0 ; END IF;
	
	--  wa — ожидание IO
	SELECT (percentile_cont(0.5) within group (order by cpu_wa))::numeric
	INTO curr_cpu_wa_long 
	FROM os_stat_vmstat
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp ;	
	IF curr_cpu_wa_long  IS NULL THEN curr_cpu_wa_long  = 0 ; END IF;
	
	--  st — stolen (украдено гипервизором)
	SELECT (percentile_cont(0.5) within group (order by cpu_st))::numeric
	INTO curr_cpu_st_long 
	FROM os_stat_vmstat
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp ;	
	IF curr_cpu_st_long  IS NULL THEN curr_cpu_st_long  = 0 ; END IF;
	
	
	--dirty pages size (KB)
	SELECT (percentile_cont(0.5) within group (order by dirty_kb))::numeric
	INTO curr_dirty_kb_long 
	FROM os_stat_vmstat
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp ;	
	IF curr_dirty_kb_long  IS NULL THEN curr_dirty_kb_long  = 0 ; END IF;
	
	---% от dirty_ratio
	SELECT (percentile_cont(0.5) within group (order by dirty_percent))::numeric
	INTO curr_dirty_percent_long 
	FROM os_stat_vmstat
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp ;	
	IF curr_dirty_percent_long  IS NULL THEN curr_dirty_percent_long  = 0 ; END IF;
	
	--% от dirty_background_ratio
	SELECT (percentile_cont(0.5) within group (order by dirty_bg_percent))::numeric
	INTO curr_dirty_bg_percent_long 
	FROM os_stat_vmstat
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp ;	
	IF curr_dirty_bg_percent_long  IS NULL THEN curr_dirty_bg_percent_long  = 0 ; END IF;
	
	--free + cached memory
	SELECT (percentile_cont(0.5) within group (order by available_mem_mb))::numeric
	INTO curr_available_mem_mb_long 
	FROM os_stat_vmstat
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp ;	
	IF curr_available_mem_mb_long  IS NULL THEN curr_available_mem_mb_long  = 0 ; END IF;
	
--Скользящие медианы	

--Сохранить статистические данные
	INSERT INTO os_stat_vmstat_median
	(
		curr_timestamp , 
		
		procs_r_long  , -- r — процессы в run queue (готовы к выполнению)
		procs_b_long  , -- b — процессы в uninterruptible sleep (обычно ждут IO)
		
		memory_swpd_long  , -- swpd — объём свопа
		memory_free_long  , -- free — свободная RAM
		memory_buff_long  , -- buff — буферы
		memory_cache_long  ,-- cache — кэш
		
		swap_si_long  , -- si — swap in (из swap в RAM)
		swap_so_long  , -- so — swap out (из RAM в swap)
		
		io_bi_long , -- bi — блоки, считанные с устройств
		io_bo_long , -- bo — записанные на устройства 
		
		system_in_long , -- in — прерывания
		system_cs_long , -- cs — переключения контекста
		
		cpu_us_long  , -- us — user time
		cpu_sy_long  , -- sy — system time
		cpu_id_long  , -- id — idle
		cpu_wa_long  , --  wa — ожидание IO
		cpu_st_long  ,	  --  st — stolen (украдено гипервизором)
		
		dirty_kb_long ,  --dirty pages size (KB)
		dirty_percent_long  ,		--% от dirty_ratio
		dirty_bg_percent_long  ,		--% от dirty_background_ratio
		available_mem_mb_long  		--free + cached memory	
	)
	VALUES 
	(
		max_timestamp , 
		curr_procs_r_long  , -- r — процессы в run queue (готовы к выполнению)
		curr_procs_b_long  , -- b — процессы в uninterruptible sleep (обычно ждут IO)
		
		curr_memory_swpd_long  , -- swpd — объём свопа
		curr_memory_free_long  , -- free — свободная RAM
		curr_memory_buff_long  , -- buff — буферы
		curr_memory_cache_long  ,-- cache — кэш
		
		curr_swap_si_long  , -- si — swap in (из swap в RAM)
		curr_swap_so_long  , -- so — swap out (из RAM в swap)	
		
		curr_io_bi_long , -- bi — блоки, считанные с устройств
		curr_io_bo_long , -- bo — записанные на устройства 
		
		curr_system_in_long , -- in — прерывания
		curr_system_cs_long , -- cs — переключения контекста
		
		curr_cpu_us_long  , -- us — user time
		curr_cpu_sy_long  , -- sy — system time
		curr_cpu_id_long  , -- id — idle
		curr_cpu_wa_long  , --  wa — ожидание IO
		curr_cpu_st_long  , --  st — stolen (украдено гипервизором)	

		curr_dirty_kb_long , --dirty pages size (KB)
		curr_dirty_percent_long	, --% от dirty_ratio
		curr_dirty_bg_percent_long , --% от dirty_background_ratio
		curr_available_mem_mb_long --free + cached memory
	);
--Сохранить статистические данные
	
return 0;
END
$$ LANGUAGE plpgsql; 
COMMENT ON FUNCTION os_stat_vmstat IS 'Сформировать статистику по метрикам vmstat';
-- Сформировать статистику по метрикам vmstat
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Сформировать статистику по метрикам iostat
CREATE OR REPLACE FUNCTION os_stat_iostat_device( iostat_string text ) RETURNS integer 
AS $$
DECLARE
--DEVICE
	device_name text ;
	current_dev_rps double precision ;  --r/s Количество операций чтения в секунду.
	current_dev_rmbs double precision ;  --rMB/s Скорость чтения (МБ/с).
	current_dev_rrqmps double precision ;  --rrqm/s Количество прочитанных запросов; слитых в очередь (merge) в секунду.
	current_dev_rrqm_pct double precision ;  --%rrqm Процент операций чтения; слитых перед отправкой на устройство.
	current_dev_r_await double precision ;  --r_await (мс) Среднее время выполнения запросов чтения (включая время в очереди).
	current_dev_rareq_sz double precision ;  --rareq-sz Средний размер запроса чтения (в КБ).
	current_dev_wps double precision ; --w/s Количество операций записи в секунду.
	current_dev_wmbps double precision ;  --wMB/s Скорость записи (МБ/с).
	current_dev_wrqmps double precision ;  --wrqm/s Количество записанных запросов; слитых в очередь в секунду.
	current_dev_wrqm_pct double precision ;  --%wrqm Процент операций записи; слитых перед отправкой на устройство.
	current_dev_w_await double precision ;  --w_await (мс) Среднее время выполнения запросов записи (включая время в очереди).
	current_dev_wareq_sz double precision ;  --wareq_sz Средний размер запроса записи (в КБ).
	current_dev_dps double precision ;  --d/s Операции для discard-запросов (актуально для SSD).
	current_dev_dmbps double precision ;  --dMB/s скорость для discard-запросов (актуально для SSD).
	current_dev_drqmps double precision ; --drqm/s Количество запросов, слитых в очередь в секунду для discard-запросов (актуально для SSD).
	current_dev_drqm_pct double precision ;  --%drqm Процент операций слитых перед отправкой на устройство для discard-запросов (актуально для SSD).
	current_dev_d_await double precision ;  --d_await Среднее время выполнения (включая время в очереди) для discard-запросов (актуально для SSD).
	current_dev_dareq_sz double precision ;  --dareq_sz Средний размер для discard-запросов (актуально для SSD)..
	current_dev_aqu_sz double precision ;  --aqu_sz Средняя длина очереди запросов (глубина очереди).
	current_dev_util_pct double precision ;  --%util Процент загрузки устройства (чем ближе к 100%; тем выше нагрузка).
	current_dev_fps double precision ;  --f/s скорость выполнения запросов от flush-операций.
	current_dev_f_await double precision ; --f_await Среднее время выполнения запросов от flush-операций.	
	
	dev_rps_long  numeric ;  --r/s Количество операций чтения в секунду.
	dev_rmbs_long  numeric ;  --rMB/s Скорость чтения (МБ/с).
	dev_rrqmps_long  numeric ;  --rrqm/s Количество прочитанных запросов; слитых в очередь (merge) в секунду.
	dev_rrqm_pct_long  numeric ;  --%rrqm Процент операций чтения; слитых перед отправкой на устройство.
	dev_r_await_long  numeric ;  --r_await (мс) Среднее время выполнения запросов чтения (включая время в очереди).
	dev_rareq_sz_long  numeric ;  --rareq_sz Средний размер запроса чтения (в КБ).
	dev_wps_long  numeric ; --w/s Количество операций записи в секунду.
	dev_wmbps_long  numeric ;  --wMB/s Скорость записи (МБ/с).
	dev_wrqmps_long  numeric ;  --wrqm/s Количество записанных запросов; слитых в очередь в секунду.
	dev_wrqm_pct_long  numeric ;  --%wrqm Процент операций записи; слитых перед отправкой на устройство.
	dev_w_await_long  numeric ;  --w_await (мс) Среднее время выполнения запросов записи (включая время в очереди).
	dev_wareq_sz_long  numeric ;  --wareq_sz Средний размер запроса записи (в КБ).
	dev_dps_long  numeric ;  --d/s Операции для discard-запросов (актуально для SSD).
	dev_dmbps_long  numeric ;  --dMB/s скорость для discard-запросов (актуально для SSD).
	dev_drqmps_long  numeric ; --drqm/s Количество запросов, слитых в очередь в секунду для discard-запросов (актуально для SSD).
	dev_drqm_pct_long  numeric ;  --%drqm Процент операций слитых перед отправкой на устройство для discard-запросов (актуально для SSD).
	dev_d_await_long  numeric ;  --d_await Среднее время выполнения (включая время в очереди) для discard-запросов (актуально для SSD).
	dev_dareq_sz_long  numeric ;  --dareq_sz Средний размер для discard-запросов (актуально для SSD)..
	dev_aqu_sz_long  numeric ;  --aqu_sz Средняя длина очереди запросов (глубина очереди).
	dev_util_pct_long  numeric ;  --%util Процент загрузки устройства (чем ближе к 100%; тем выше нагрузка).
	dev_fps_long  numeric ;  --f/s скорость выполнения запросов от flush-операций.
	dev_f_await_long  numeric ; --f_await Среднее время выполнения запросов от flush-операций.
	
		
	iostat_array  text[] ;
	
	min_timestamp timestamptz ; 
	max_timestamp timestamptz ;  
	
BEGIN	
	SELECT 	date_trunc( 'minute' , CURRENT_TIMESTAMP ) 
	INTO 		max_timestamp ; 
	
	SELECT string_to_array(iostat_string , ' ' )
	INTO iostat_array ;

	--Device-1
	device_name= iostat_array[1]; 
	
	--r/s-2
    --r/s Количество операций чтения в секунду.
	current_dev_rps = iostat_array[2]::double precision; 
	
	--rMB/s-3
	--rMB/s Скорость чтения (МБ/с).
	current_dev_rmbs = iostat_array[3]::double precision; 

    --rrqm/s-4
	--rrqm/s Количество прочитанных запросов; слитых в очередь (merge) в секунду.
	current_dev_rrqmps = iostat_array[4]::double precision; 
	
	--%rrqm-5
	--%rrqm Процент операций чтения; слитых перед отправкой на устройство.
	current_dev_rrqm_pct = iostat_array[5]::double precision; 
	
	--r_await-6
	--r_await (мс) Среднее время выполнения запросов чтения (включая время в очереди).
	current_dev_r_await = iostat_array[6]::double precision; 
	
	--rareq-sz-7
	--rareq-sz Средний размер запроса чтения (в КБ).
	current_dev_rareq_sz = iostat_array[7]::double precision; 
	
	--w/s-8
	--w/s Количество операций записи в секунду.
	current_dev_wps = iostat_array[8]::double precision; 
	
	--wMB/s-9
	--wMB/s Скорость записи (МБ/с).
	current_dev_wmbps = iostat_array[9]::double precision; 
	
	--wrqm/s-10
	--wrqm/s Количество записанных запросов; слитых в очередь в секунду.
	current_dev_wrqmps = iostat_array[10]::double precision; 
	
	--%wrqm-11
	--%wrqm Процент операций записи; слитых перед отправкой на устройство.
	current_dev_wrqm_pct = iostat_array[11]::double precision; 
	
	--w_await-12
	--w_await (мс) Среднее время выполнения запросов записи (включая время в очереди).
	current_dev_w_await = iostat_array[12]::double precision; 
	
	--wareq-sz-13
	--wareq_sz Средний размер запроса записи (в КБ).
	current_dev_wareq_sz = iostat_array[13]::double precision; 
	
	--d/s-14
	--d/s Операции для discard-запросов (актуально для SSD).
	current_dev_dps = iostat_array[14]::double precision; 
	
	--dMB/s-15
	--dMB/s скорость для discard-запросов (актуально для SSD).
	current_dev_dmbps = iostat_array[15]::double precision; 
	
	--drqm/s-16
	--drqm/s Количество запросов, слитых в очередь в секунду для discard-запросов (актуально для SSD).
	current_dev_drqmps = iostat_array[16]::double precision; 
	
	--%drqm-17
	--%drqm Процент операций слитых перед отправкой на устройство для discard-запросов (актуально для SSD).
	current_dev_drqm_pct = iostat_array[17]::double precision; 
	
	--d_await-18
	--d_await Среднее время выполнения (включая время в очереди) для discard-запросов (актуально для SSD).
	current_dev_d_await = iostat_array[18]::double precision; 
	
	--dareq-sz-19
	--dareq_sz Средний размер для discard-запросов (актуально для SSD)..
	current_dev_dareq_sz = iostat_array[19]::double precision; 
	
	--f/s-20
	--f/s скорость выполнения запросов от flush-операций.
	current_dev_fps = iostat_array[20]::double precision; 
	
	--f_await-21
	--f_await Среднее время выполнения запросов от flush-операций.	
	current_dev_f_await = iostat_array[21]::double precision; 
	
	--aqu-sz-22
	--aqu_sz Средняя длина очереди запросов (глубина очереди).
	current_dev_aqu_sz = iostat_array[22]::double precision; 
	
	--	%util-23
	--%util Процент загрузки устройства (чем ближе к 100%; тем выше нагрузка).
	current_dev_util_pct = iostat_array[23]::double precision; 
	
	--Сохранить текущие значения
	INSERT INTO os_stat_iostat_device
	(
		device , 
		curr_timestamp ,
		dev_rps  ,  --r/s Количество операций чтения в секунду.
		dev_rmbs ,  --rMB/s Скорость чтения (МБ/с).
		dev_rrqmps ,  --rrqm/s Количество прочитанных запросов, слитых в очередь (merge) в секунду.
		dev_rrqm_pct ,  --%rrqm Процент операций чтения, слитых перед отправкой на устройство.
		dev_r_await,  --r_await (мс) Среднее время выполнения запросов чтения (включая время в очереди).
		dev_rareq_sz  ,  --rareq-sz Средний размер запроса чтения (в КБ).
		dev_wps     , --w/s Количество операций записи в секунду.
		dev_wmbps   ,  --wMB/s Скорость записи (МБ/с).
		dev_wrqmps  ,  --wrqm/s Количество записанных запросов, слитых в очередь в секунду.
		dev_wrqm_pct  ,  --%wrqm Процент операций записи, слитых перед отправкой на устройство.
		dev_w_await   ,  --w_await (мс) Среднее время выполнения запросов записи (включая время в очереди).
		dev_wareq_sz  ,  --wareq_sz Средний размер запроса записи (в КБ).
		dev_dps    ,  --d/s Операции для discard-запросов (актуально для SSD).
		dev_dmbps  ,  --dMB/s скорость для discard-запросов (актуально для SSD).
		dev_drqmps , --drqm/s Количество запросов, слитых в очередь в секунду для discard-запросов (актуально для SSD).
		dev_drqm_pct  ,  --%drqm Процент операций слитых перед отправкой на устройство для discard-запросов (актуально для SSD).
		dev_d_await   ,  --d_await Среднее время выполнения (включая время в очереди) для discard-запросов (актуально для SSD).
		dev_dareq_sz  ,  --dareq_sz Средний размер для discard-запросов (актуально для SSD)..
		dev_aqu_sz    ,  --aqu_sz Средняя длина очереди запросов (глубина очереди).
		dev_util_pct  ,  --%util Процент загрузки устройства (чем ближе к 100%, тем выше нагрузка).
		dev_fps     ,  --f/s скорость выполнения запросов от flush-операций.
		dev_f_await    --f_await Среднее время выполнения запросов от flush-операций.
	)
	VALUES
	(
		 device_name ,
		 max_timestamp		,
		 current_dev_rps ,  --r/s Количество операций чтения в секунду.
		 current_dev_rmbs ,  --rMB/s Скорость чтения (МБ/с).
		 current_dev_rrqmps,  --rrqm/s Количество прочитанных запросов, слитых в очередь (merge) в секунду.
		 current_dev_rrqm_pct,  --%rrqm Процент операций чтения, слитых перед отправкой на устройство.
		 current_dev_r_await,  --r_await (мс) Среднее время выполнения запросов чтения (включая время в очереди).
		 current_dev_rareq_sz  ,  --rareq-sz Средний размер запроса чтения (в КБ).
		 current_dev_wps  , --w/s Количество операций записи в секунду.
		 current_dev_wmbps  ,  --wMB/s Скорость записи (МБ/с).
		 current_dev_wrqmps  ,  --wrqm/s Количество записанных запросов, слитых в очередь в секунду.
		 current_dev_wrqm_pct  ,  --%wrqm Процент операций записи, слитых перед отправкой на устройство.
		 current_dev_w_await  ,  --w_await (мс) Среднее время выполнения запросов записи (включая время в очереди).
		 current_dev_wareq_sz  ,  --wareq_sz Средний размер запроса записи (в КБ).
		 current_dev_dps  ,  --d/s Операции для discard-запросов (актуально для SSD).
		 current_dev_dmbps  ,  --dMB/s скорость для discard-запросов (актуально для SSD).
		 current_dev_drqmps , --drqm/s Количество запросов, слитых в очередь в секунду для discard-запросов (актуально для SSD).
		 current_dev_drqm_pct  ,  --%drqm Процент операций слитых перед отправкой на устройство для discard-запросов (актуально для SSD).
		 current_dev_d_await  ,  --d_await Среднее время выполнения (включая время в очереди) для discard-запросов (актуально для SSD).
		 current_dev_dareq_sz  ,  --dareq_sz Средний размер для discard-запросов (актуально для SSD)..
		 current_dev_aqu_sz  ,  --aqu_sz Средняя длина очереди запросов (глубина очереди).
		 current_dev_util_pct  ,  --%util Процент загрузки устройства (чем ближе к 100%, тем выше нагрузка).
		 current_dev_fps  ,  --f/s скорость выполнения запросов от flush-операций.
		 current_dev_f_await    --f_await Среднее время выполнения запросов от flush-операций.
	);
--Сохранить текущие значения	
	
--Скользящие медианы
	--r/s Количество операций чтения в секунду.
	SELECT (percentile_cont(0.5) within group (order by dev_rps))::numeric
	INTO dev_rps_long
	FROM os_stat_iostat_device osd
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp AND osd.device = device_name ;	
	IF dev_rps_long IS NULL THEN dev_rps_long = 0 ; END IF ; 

	--rMB/s Скорость чтения (МБ/с).
	SELECT (percentile_cont(0.5) within group (order by dev_rmbs))::numeric
	INTO dev_rmbs_long
	FROM os_stat_iostat_device osd 
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp AND osd.device = device_name ;
	IF dev_rmbs_long IS NULL THEN dev_rmbs_long = 0 ; END IF ; 

	--rrqm/s Количество прочитанных запросов; слитых в очередь (merge) в секунду.
	SELECT (percentile_cont(0.5) within group (order by dev_rrqmps))::numeric
	INTO dev_rrqmps_long
	FROM os_stat_iostat_device osd 
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp AND osd.device = device_name ;
	IF dev_rrqmps_long IS NULL THEN dev_rrqmps_long = 0 ; END IF ; 

	--%rrqm Процент операций чтения; слитых перед отправкой на устройство.
	SELECT (percentile_cont(0.5) within group (order by dev_rrqm_pct))::numeric
	INTO dev_rrqm_pct_long
	FROM os_stat_iostat_device osd 
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp AND osd.device = device_name ;
	IF dev_rrqm_pct_long IS NULL THEN dev_rrqm_pct_long = 0 ; END IF ; 

	--r_await (мс) Среднее время выполнения запросов чтения (включая время в очереди).
	SELECT (percentile_cont(0.5) within group (order by dev_r_await))::numeric
	INTO dev_r_await_long
	FROM os_stat_iostat_device osd 
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp AND osd.device = device_name ;
	IF dev_r_await_long IS NULL THEN dev_r_await_long = 0 ; END IF ; 
	
	--rareq_sz Средний размер запроса чтения (в КБ).
	SELECT (percentile_cont(0.5) within group (order by dev_rareq_sz))::numeric
	INTO dev_rareq_sz_long
	FROM os_stat_iostat_device osd 
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp AND osd.device = device_name ;
	IF dev_rareq_sz_long IS NULL THEN dev_rareq_sz_long = 0 ; END IF ; 
	
	--w/s Количество операций записи в секунду.
	SELECT (percentile_cont(0.5) within group (order by dev_wps))::numeric
	INTO dev_wps_long
	FROM os_stat_iostat_device osd 
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp AND osd.device = device_name ;
	IF dev_wps_long IS NULL THEN dev_wps_long = 0 ; END IF ; 
	
	--wMB/s Скорость записи (МБ/с).
	SELECT (percentile_cont(0.5) within group (order by dev_wmbps))::numeric
	INTO dev_wmbps_long
	FROM os_stat_iostat_device osd 
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp AND osd.device = device_name ;
	IF dev_wmbps_long IS NULL THEN dev_wmbps_long = 0 ; END IF ; 
	
	--wrqm/s Количество записанных запросов; слитых в очередь в секунду.
	SELECT (percentile_cont(0.5) within group (order by dev_wrqmps))::numeric
	INTO dev_wrqmps_long
	FROM os_stat_iostat_device osd 
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp AND osd.device = device_name ;
	IF dev_wrqmps_long IS NULL THEN dev_wrqmps_long = 0 ; END IF ; 
	
	--%wrqm Процент операций записи; слитых перед отправкой на устройство.
	SELECT (percentile_cont(0.5) within group (order by dev_wrqm_pct))::numeric
	INTO dev_wrqm_pct_long
	FROM os_stat_iostat_device osd 
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp AND osd.device = device_name ;
	IF dev_wrqm_pct_long IS NULL THEN dev_wrqm_pct_long = 0 ; END IF ;
	
	--w_await (мс) Среднее время выполнения запросов записи (включая время в очереди).
	SELECT (percentile_cont(0.5) within group (order by dev_w_await))::numeric
	INTO dev_w_await_long
	FROM os_stat_iostat_device osd 
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp AND osd.device = device_name ;
	IF dev_w_await_long IS NULL THEN dev_w_await_long = 0 ; END IF ;
	
	--wareq_sz Средний размер запроса записи (в КБ).
	SELECT (percentile_cont(0.5) within group (order by dev_wareq_sz))::numeric
	INTO dev_wareq_sz_long
	FROM os_stat_iostat_device osd 
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp AND osd.device = device_name ;
	IF dev_wareq_sz_long IS NULL THEN dev_wareq_sz_long = 0 ; END IF ;
	
	--d/s Операции для discard-запросов (актуально для SSD).
	SELECT (percentile_cont(0.5) within group (order by dev_dps))::numeric
	INTO dev_dps_long
	FROM os_stat_iostat_device osd 
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp AND osd.device = device_name ;
	IF dev_dps_long IS NULL THEN dev_dps_long = 0 ; END IF ;
	
	--dMB/s скорость для discard-запросов (актуально для SSD).
	SELECT (percentile_cont(0.5) within group (order by dev_dmbps))::numeric
	INTO dev_dmbps_long
	FROM os_stat_iostat_device osd 
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp AND osd.device = device_name ;
	IF dev_dmbps_long IS NULL THEN dev_dmbps_long = 0 ; END IF ;
	
	--drqm/s Количество запросов, слитых в очередь в секунду для discard-запросов (актуально для SSD).
	SELECT (percentile_cont(0.5) within group (order by dev_drqmps))::numeric
	INTO dev_drqmps_long
	FROM os_stat_iostat_device osd 
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp AND osd.device = device_name ;
	IF dev_drqmps_long IS NULL THEN dev_drqmps_long = 0 ; END IF ;
	
	--%drqm Процент операций слитых перед отправкой на устройство для discard-запросов (актуально для SSD).
	SELECT (percentile_cont(0.5) within group (order by dev_drqm_pct))::numeric
	INTO dev_drqm_pct_long
	FROM os_stat_iostat_device osd 
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp AND osd.device = device_name ;
	IF dev_drqm_pct_long IS NULL THEN dev_drqm_pct_long = 0 ; END IF ;
	
	--d_await Среднее время выполнения (включая время в очереди) для discard-запросов (актуально для SSD).
	SELECT (percentile_cont(0.5) within group (order by dev_d_await))::numeric
	INTO dev_d_await_long
	FROM os_stat_iostat_device osd 
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp AND osd.device = device_name ;
	IF dev_d_await_long IS NULL THEN dev_d_await_long = 0 ; END IF ;
	
	--dareq_sz Средний размер для discard-запросов (актуально для SSD)..
	SELECT (percentile_cont(0.5) within group (order by dev_d_await))::numeric
	INTO dev_dareq_sz_long
	FROM os_stat_iostat_device osd 
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp AND osd.device = device_name ;
	IF dev_dareq_sz_long IS NULL THEN dev_dareq_sz_long = 0 ; END IF ;

	--aqu_sz Средняя длина очереди запросов (глубина очереди).
	SELECT (percentile_cont(0.5) within group (order by dev_aqu_sz))::numeric
	INTO dev_aqu_sz_long
	FROM os_stat_iostat_device osd 
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp AND osd.device = device_name ;
	IF dev_aqu_sz_long IS NULL THEN dev_aqu_sz_long = 0 ; END IF ;

	--%util Процент загрузки устройства (чем ближе к 100%; тем выше нагрузка).
	SELECT (percentile_cont(0.5) within group (order by dev_util_pct))::numeric
	INTO dev_util_pct_long
	FROM os_stat_iostat_device osd 
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp AND osd.device = device_name ;
	IF dev_util_pct_long IS NULL THEN dev_util_pct_long = 0 ; END IF ;
	
	--f/s скорость выполнения запросов от flush-операций.
	SELECT (percentile_cont(0.5) within group (order by dev_fps))::numeric
	INTO dev_fps_long
	FROM os_stat_iostat_device osd 
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp AND osd.device = device_name ;
	IF dev_fps_long IS NULL THEN dev_fps_long = 0 ; END IF ;

	--f_await Среднее время выполнения запросов от flush-операций.
	SELECT (percentile_cont(0.5) within group (order by dev_f_await))::numeric
	INTO dev_f_await_long
	FROM os_stat_iostat_device osd 
	WHERE curr_timestamp BETWEEN max_timestamp - (interval '60 minute') AND max_timestamp AND osd.device = device_name ;
	IF dev_f_await_long IS NULL THEN dev_f_await_long = 0 ; END IF ;
--Скользящие медианы

--Сохранить статистические данные
	INSERT INTO  os_stat_iostat_device_median
	(
		device ,
		curr_timestamp ,
		dev_rps_long   ,  --r/s Количество операций чтения в секунду.
		dev_rmbs_long  ,  --rMB/s Скорость чтения (МБ/с).
		dev_rrqmps_long ,  --rrqm/s Количество прочитанных запросов, слитых в очередь (merge) в секунду.
		dev_rrqm_pct_long ,  --%rrqm Процент операций чтения, слитых перед отправкой на устройство.
		dev_r_await_long  ,  --r_await (мс) Среднее время выполнения запросов чтения (включая время в очереди).
		dev_rareq_sz_long ,  --rareq_sz Средний размер запроса чтения (в КБ).
		dev_wps_long  , --w/s Количество операций записи в секунду.
		dev_wmbps_long  ,  --wMB/s Скорость записи (МБ/с).
		dev_wrqmps_long ,  --wrqm/s Количество записанных запросов, слитых в очередь в секунду.
		dev_wrqm_pct_long  ,  --%wrqm Процент операций записи, слитых перед отправкой на устройство.
		dev_w_await_long   ,  --w_await (мс) Среднее время выполнения запросов записи (включая время в очереди).
		dev_wareq_sz_long  ,  --wareq_sz Средний размер запроса записи (в КБ).
		dev_dps_long ,  --d/s Операции для discard-запросов (актуально для SSD).
		dev_dmbps_long ,  --dMB/s скорость для discard-запросов (актуально для SSD).
		dev_drqmps_long , --drqm/s Количество запросов, слитых в очередь в секунду для discard-запросов (актуально для SSD).
		dev_drqm_pct_long ,  --%drqm Процент операций слитых перед отправкой на устройство для discard-запросов (актуально для SSD).
		dev_d_await_long  ,  --d_await Среднее время выполнения (включая время в очереди) для discard-запросов (актуально для SSD).
		dev_dareq_sz_long ,  --dareq_sz Средний размер для discard-запросов (актуально для SSD)..
		dev_aqu_sz_long   ,  --aqu_sz Средняя длина очереди запросов (глубина очереди).
		dev_util_pct_long ,  --%util Процент загрузки устройства (чем ближе к 100%, тем выше нагрузка).
		dev_fps_long   ,  --f/s скорость выполнения запросов от flush-операций.
		dev_f_await_long --f_await Среднее время выполнения запросов от flush-операций.
	)
	VALUES 
	(
		device_name ,
		max_timestamp ,
		 dev_rps_long ,  --r/s Количество операций чтения в секунду.
		 dev_rmbs_long ,  --rMB/s Скорость чтения (МБ/с).
		 dev_rrqmps_long ,  --rrqm/s Количество прочитанных запросов, слитых в очередь (merge) в секунду.
		 dev_rrqm_pct_long ,  --%rrqm Процент операций чтения, слитых перед отправкой на устройство.
		 dev_r_await_long ,  --r_await (мс) Среднее время выполнения запросов чтения (включая время в очереди).
		 dev_rareq_sz_long ,  --rareq_sz Средний размер запроса чтения (в КБ).
		 dev_wps_long , --w/s Количество операций записи в секунду.
		 dev_wmbps_long ,  --wMB/s Скорость записи (МБ/с).
		 dev_wrqmps_long ,  --wrqm/s Количество записанных запросов, слитых в очередь в секунду.
		 dev_wrqm_pct_long ,  --%wrqm Процент операций записи, слитых перед отправкой на устройство.
		 dev_w_await_long ,  --w_await (мс) Среднее время выполнения запросов записи (включая время в очереди).
		 dev_wareq_sz_long ,  --wareq_sz Средний размер запроса записи (в КБ).
		 dev_dps_long ,  --d/s Операции для discard-запросов (актуально для SSD).
		 dev_dmbps_long ,  --dMB/s скорость для discard-запросов (актуально для SSD).
		 dev_drqmps_long , --drqm/s Количество запросов, слитых в очередь в секунду для discard-запросов (актуально для SSD).
		 dev_drqm_pct_long ,  --%drqm Процент операций слитых перед отправкой на устройство для discard-запросов (актуально для SSD).
		 dev_d_await_long ,  --d_await Среднее время выполнения (включая время в очереди) для discard-запросов (актуально для SSD).
		 dev_dareq_sz_long ,  --dareq_sz Средний размер для discard-запросов (актуально для SSD)..
		 dev_aqu_sz_long ,  --aqu_sz Средняя длина очереди запросов (глубина очереди).
		 dev_util_pct_long ,  --%util Процент загрузки устройства (чем ближе к 100%, тем выше нагрузка).
		 dev_fps_long ,  --f/s скорость выполнения запросов от flush-операций.
		 dev_f_await_long --f_await Среднее время выполнения запросов от flush-операций.
	
	);
	
--Сохранить статистические данные
	
return 0 ;
END
$$ LANGUAGE plpgsql; 
COMMENT ON FUNCTION os_stat_iostat_device IS 'Сформировать статистику по метрикам iostat';
-- Сформировать статистику по метрикам iostat
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- core_os_tables.sql
--------------------------------------------------------------------------------
-- Статистика уровня ОС
--------------------------------------------------------------------------------
--
-- Метрики vmstat
DROP TABLE IF EXISTS os_stat_vmstat;
CREATE UNLOGGED TABLE os_stat_vmstat 
(
  
	id SERIAL , 	
	curr_timestamp timestamp with time zone , 

	procs_r integer , -- r — процессы в run queue (готовы к выполнению)
	procs_b integer , -- b — процессы в uninterruptible sleep (обычно ждут IO)
	
	memory_swpd integer , -- swpd — объём свопа
	memory_free integer , -- free — свободная RAM
	memory_buff integer , -- buff — буферы
	memory_cache integer ,-- cache — кэш
	
	swap_si integer , -- si — swap in (из swap в RAM)
	swap_so integer , -- so — swap out (из RAM в swap)
	
	io_bi bigint, -- bi — блоки, считанные с устройств
	io_bo bigint, -- bo — записанные на устройства 
	
	system_in bigint, -- in — прерывания
	system_cs bigint, -- cs — переключения контекста
	
	cpu_us integer , -- us — user time
	cpu_sy  integer , -- sy — system time
	cpu_id  integer , -- id — idle
	cpu_wa  integer , --  wa — ожидание IO
	cpu_st integer	,  --  st — stolen (украдено гипервизором)
	----------------------------------------------------------
	--VM
	dirty_kb DOUBLE PRECISION ,		--dirty pages size (KB)
	dirty_percent DOUBLE PRECISION ,		--% от dirty_ratio
	dirty_bg_percent DOUBLE PRECISION ,		--% от dirty_background_ratio
	available_mem_mb DOUBLE PRECISION 		--free + cached memory
	--VM
	----------------------------------------------------------
);

ALTER TABLE os_stat_vmstat ADD CONSTRAINT os_stat_vmstat_pk PRIMARY KEY (id);
CREATE INDEX os_stat_vmstat_idx ON os_stat_vmstat ( curr_timestamp );

COMMENT ON TABLE os_stat_vmstat IS 'Метрики vmstat';
COMMENT ON COLUMN os_stat_vmstat.curr_timestamp IS 'Точка времени сбора данных ';
COMMENT ON COLUMN os_stat_vmstat.procs_r IS 'r — процессы в run queue (готовы к выполнению) ';
COMMENT ON COLUMN os_stat_vmstat.procs_b IS 'b — процессы в uninterruptible sleep (обычно ждут IO) ';
COMMENT ON COLUMN os_stat_vmstat.memory_swpd IS 'swpd — объём свопа ';
COMMENT ON COLUMN os_stat_vmstat.memory_free IS 'free — свободная RAM ';
COMMENT ON COLUMN os_stat_vmstat.memory_buff IS 'buff — буферы';
COMMENT ON COLUMN os_stat_vmstat.memory_cache IS 'cache — кэш';
COMMENT ON COLUMN os_stat_vmstat.swap_si IS 'si — swap in (из swap в RAM)';
COMMENT ON COLUMN os_stat_vmstat.swap_so IS 'si — so — swap out (из RAM в swap)';
COMMENT ON COLUMN os_stat_vmstat.io_bi IS 'bi — блоки, считанные с устройств';
COMMENT ON COLUMN os_stat_vmstat.io_bo IS 'bo — записанные на устройства';
COMMENT ON COLUMN os_stat_vmstat.system_in IS 'in — прерывания';
COMMENT ON COLUMN os_stat_vmstat.system_cs IS 'cs — переключения контекста';
COMMENT ON COLUMN os_stat_vmstat.cpu_us IS 'us — user time';
COMMENT ON COLUMN os_stat_vmstat.cpu_sy IS 'sy — system time';
COMMENT ON COLUMN os_stat_vmstat.cpu_id IS 'id — idle';
COMMENT ON COLUMN os_stat_vmstat.cpu_wa IS 'wa — ожидание IO';
COMMENT ON COLUMN os_stat_vmstat.cpu_st IS 'st — stolen (украдено гипервизором)';

COMMENT ON COLUMN os_stat_vmstat.dirty_kb IS 'dirty pages size (KB)';
COMMENT ON COLUMN os_stat_vmstat.dirty_percent IS '% от dirty_ratio';
COMMENT ON COLUMN os_stat_vmstat.dirty_bg_percent IS '% от dirty_background_ratio';
COMMENT ON COLUMN os_stat_vmstat.available_mem_mb IS 'free + cached memory';


-- Метрики vmstat
--------------------------------------------------------------------------------


--------------------------------------------------------------------------------
--Скользящие медианы по метрикам vmstat
DROP TABLE IF EXISTS os_stat_vmstat_median;
CREATE TABLE os_stat_vmstat_median 
(
	id SERIAL , 
	curr_timestamp timestamp with time zone , 
	
	procs_r_long numeric , -- r — процессы в run queue (готовы к выполнению)
	procs_b_long numeric , -- b — процессы в uninterruptible sleep (обычно ждут IO)
	
	memory_swpd_long numeric , -- swpd — объём свопа
	memory_free_long numeric , -- free — свободная RAM
	memory_buff_long numeric , -- buff — буферы
	memory_cache_long numeric ,-- cache — кэш
	
	swap_si_long numeric , -- si — swap in (из swap в RAM)
	swap_so_long numeric , -- so — swap out (из RAM в swap)
	
	io_bi_long numeric, -- bi — блоки, считанные с устройств
	io_bo_long numeric, -- bo — блоки, записанные на устройства 
	
	system_in_long numeric, -- in — прерывания
	system_cs_long numeric, -- cs — переключения контекста
	
	cpu_us_long numeric , -- us — user time
	cpu_sy_long numeric , -- sy — system time
	cpu_id_long numeric , -- id — idle
	cpu_wa_long numeric , --  wa — ожидание IO
	cpu_st_long numeric ,  --  st — stolen (украдено гипервизором)
	----------------------------------------------------------
	--VM
	dirty_kb_long DOUBLE PRECISION ,		--dirty pages size (KB)
	dirty_percent_long numeric ,		--% от dirty_ratio
	dirty_bg_percent_long numeric ,		--% от dirty_background_ratio
	available_mem_mb_long numeric 		--free + cached memory
	--VM
	----------------------------------------------------------
	
);
ALTER TABLE os_stat_vmstat_median ADD CONSTRAINT os_stat_vmstat_median_pk PRIMARY KEY (id);
CREATE INDEX os_stat_vmstat_median_idx ON os_stat_vmstat_median ( curr_timestamp );

COMMENT ON TABLE os_stat_vmstat_median IS 'Скользящие медианы по метрикам vmstat';
COMMENT ON COLUMN os_stat_vmstat_median.curr_timestamp IS 'Точка времени сбора данных ';	
COMMENT ON COLUMN os_stat_vmstat_median.procs_r_long IS 'Скользящая медиана по метрике r — процессы в run queue (готовы к выполнению) ';
COMMENT ON COLUMN os_stat_vmstat_median.procs_b_long IS 'Скользящая медиана по метрике b — процессы в uninterruptible sleep (обычно ждут IO) ';
COMMENT ON COLUMN os_stat_vmstat_median.memory_swpd_long IS 'Скользящая медиана по метрике swpd — объём свопа ';
COMMENT ON COLUMN os_stat_vmstat_median.memory_free_long IS 'Скользящая медиана по метрике free — свободная RAM ';
COMMENT ON COLUMN os_stat_vmstat_median.memory_buff_long IS 'Скользящая медиана по метрике buff — буферы';
COMMENT ON COLUMN os_stat_vmstat_median.memory_cache_long IS 'Скользящая медиана по метрике cache — кэш';
COMMENT ON COLUMN os_stat_vmstat_median.swap_si_long IS 'Скользящая медиана по метрике si — swap in (из swap в RAM)';
COMMENT ON COLUMN os_stat_vmstat_median.swap_so_long IS 'Скользящая медиана по метрике si — so — swap out (из RAM в swap)';
COMMENT ON COLUMN os_stat_vmstat_median.io_bi_long IS 'Скользящая медиана по метрике bi — блоки, считанные с устройств';
COMMENT ON COLUMN os_stat_vmstat_median.io_bo_long IS 'Скользящая медиана по метрике bo — записанные на устройства';
COMMENT ON COLUMN os_stat_vmstat_median.system_in_long IS 'Скользящая медиана по метрике in — прерывания';
COMMENT ON COLUMN os_stat_vmstat_median.system_cs_long IS 'Скользящая медиана по метрике cs — переключения контекста';
COMMENT ON COLUMN os_stat_vmstat_median.cpu_us_long IS 'Скользящая медиана по метрике us — user time';
COMMENT ON COLUMN os_stat_vmstat_median.cpu_sy_long IS 'Скользящая медиана по метрике sy — system time';
COMMENT ON COLUMN os_stat_vmstat_median.cpu_id_long IS 'Скользящая медиана по метрике id — idle';
COMMENT ON COLUMN os_stat_vmstat_median.cpu_wa_long IS 'Скользящая медиана по метрике wa — ожидание IO';
COMMENT ON COLUMN os_stat_vmstat_median.cpu_st_long IS 'Скользящая медиана по метрике st — stolen (украдено гипервизором)';

COMMENT ON COLUMN os_stat_vmstat_median.dirty_percent_long IS 'Скользящая медиана по метрике dirty_kb_long--dirty pages size (KB)';
COMMENT ON COLUMN os_stat_vmstat_median.dirty_percent_long IS 'Скользящая медиана по метрике dirty_percent_long-% от dirty_ratio';
COMMENT ON COLUMN os_stat_vmstat_median.dirty_bg_percent_long IS 'Скользящая медиана по метрике dirty_bg_percent_long-% от dirty_background_ratio';
COMMENT ON COLUMN os_stat_vmstat_median.available_mem_mb_long IS 'Скользящая медиана по метрике available_mem_mb_long-free + cached memory';

--Скользящие медианы
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Метрики iostat
DROP TABLE IF EXISTS os_stat_iostat_device;
CREATE UNLOGGED TABLE os_stat_iostat_device 
(
  
	id SERIAL , 	
	curr_timestamp timestamp with time zone ,
	device  text ,  		    --Device
	dev_rps double precision ,  --r/s Количество операций чтения в секунду.
	dev_rmbs double precision ,  --rMB/s Скорость чтения (МБ/с).
	dev_rrqmps double precision ,  --rrqm/s Количество прочитанных запросов, слитых в очередь (merge) в секунду.
	dev_rrqm_pct double precision ,  --%rrqm Процент операций чтения, слитых перед отправкой на устройство.
	dev_r_await double precision ,  --r_await (мс) Среднее время выполнения запросов чтения (включая время в очереди).
	dev_rareq_sz double precision ,  --rareq-sz Средний размер запроса чтения (в КБ).
	dev_wps double precision , --w/s Количество операций записи в секунду.
	dev_wmbps double precision ,  --wMB/s Скорость записи (МБ/с).
	dev_wrqmps double precision ,  --wrqm/s Количество записанных запросов, слитых в очередь в секунду.
	dev_wrqm_pct double precision ,  --%wrqm Процент операций записи, слитых перед отправкой на устройство.
	dev_w_await double precision ,  --w_await (мс) Среднее время выполнения запросов записи (включая время в очереди).
	dev_wareq_sz double precision ,  --wareq_sz Средний размер запроса записи (в КБ).
	dev_dps double precision ,  --d/s Операции для discard-запросов (актуально для SSD).
	dev_dmbps double precision ,  --dMB/s скорость для discard-запросов (актуально для SSD).
	dev_drqmps double precision , --drqm/s Количество запросов, слитых в очередь в секунду для discard-запросов (актуально для SSD).
	dev_drqm_pct double precision ,  --%drqm Процент операций слитых перед отправкой на устройство для discard-запросов (актуально для SSD).
	dev_d_await double precision ,  --d_await Среднее время выполнения (включая время в очереди) для discard-запросов (актуально для SSD).
	dev_dareq_sz double precision ,  --dareq_sz Средний размер для discard-запросов (актуально для SSD)..
	dev_aqu_sz double precision ,  --aqu_sz Средняя длина очереди запросов (глубина очереди).
	dev_util_pct double precision ,  --%util Процент загрузки устройства (чем ближе к 100%, тем выше нагрузка).
	dev_fps double precision ,  --f/s скорость выполнения запросов от flush-операций.
	dev_f_await double precision  --f_await Среднее время выполнения запросов от flush-операций.	
		
);
ALTER TABLE os_stat_iostat_device ADD CONSTRAINT os_stat_iostat_device_pk PRIMARY KEY (id);
CREATE INDEX os_stat_iostat_device_idx ON os_stat_iostat_device ( curr_timestamp );
CREATE INDEX os_stat_iostat_device_idx2 ON os_stat_iostat_device ( device );

COMMENT ON TABLE os_stat_iostat_device IS 'Метрики iostat';
COMMENT ON COLUMN os_stat_iostat_device.curr_timestamp IS 'Точка времени сбора данных ';	
COMMENT ON COLUMN os_stat_iostat_device.device IS 'Дисковое устройство';	
COMMENT ON COLUMN os_stat_iostat_device.dev_rps IS 'r/s Количество операций чтения в секунду. ';	
COMMENT ON COLUMN os_stat_iostat_device.dev_rmbs IS 'rMB/s Скорость чтения (МБ/с)';	
COMMENT ON COLUMN os_stat_iostat_device.dev_rrqmps IS 'rrqm/s Количество прочитанных запросов, слитых в очередь (merge) в секунду';
COMMENT ON COLUMN os_stat_iostat_device.dev_rrqm_pct IS '%rrqm Процент операций чтения, слитых перед отправкой на устройство';
COMMENT ON COLUMN os_stat_iostat_device.dev_r_await IS 'r_await (мс) Среднее время выполнения запросов чтения (включая время в очереди)';
COMMENT ON COLUMN os_stat_iostat_device.dev_rareq_sz IS 'rareq-sz Средний размер запроса чтения (в КБ)';
COMMENT ON COLUMN os_stat_iostat_device.dev_wps IS 'w/s Количество операций записи в секунду';
COMMENT ON COLUMN os_stat_iostat_device.dev_wmbps IS 'wMB/s Скорость записи (МБ/с)';
COMMENT ON COLUMN os_stat_iostat_device.dev_wrqmps IS 'wrqm/s Количество записанных запросов, слитых в очередь в секунду';
COMMENT ON COLUMN os_stat_iostat_device.dev_wrqm_pct IS '%wrqm Процент операций записи, слитых перед отправкой на устройство';
COMMENT ON COLUMN os_stat_iostat_device.dev_w_await IS 'w_await (мс) Среднее время выполнения запросов записи (включая время в очереди)';
COMMENT ON COLUMN os_stat_iostat_device.dev_wareq_sz IS 'wareq_sz Средний размер запроса записи (в КБ)';
COMMENT ON COLUMN os_stat_iostat_device.dev_dps IS 'd/s Операции для discard-запросов (актуально для SSD)';
COMMENT ON COLUMN os_stat_iostat_device.dev_dmbps IS 'dMB/s скорость для discard-запросов (актуально для SSD)';
COMMENT ON COLUMN os_stat_iostat_device.dev_drqmps IS 'drqm/s Количество запросов, слитых в очередь в секунду для discard-запросов (актуально для SSD)';
COMMENT ON COLUMN os_stat_iostat_device.dev_drqm_pct IS '%drqm Процент операций слитых перед отправкой на устройство для discard-запросов (актуально для SSD)';
COMMENT ON COLUMN os_stat_iostat_device.dev_d_await IS 'd_await Среднее время выполнения (включая время в очереди) для discard-запросов (актуально для SSD)';
COMMENT ON COLUMN os_stat_iostat_device.dev_dareq_sz IS 'dareq_sz Средний размер для discard-запросов (актуально для SSD)';
COMMENT ON COLUMN os_stat_iostat_device.dev_aqu_sz IS 'aqu_sz Средняя длина очереди запросов (глубина очереди)';
COMMENT ON COLUMN os_stat_iostat_device.dev_util_pct IS '%util Процент загрузки устройства (чем ближе к 100%, тем выше нагрузка)';
COMMENT ON COLUMN os_stat_iostat_device.dev_fps IS 'f/s скорость выполнения запросов от flush-операций';
COMMENT ON COLUMN os_stat_iostat_device.dev_f_await IS 'f_await Среднее время выполнения запросов от flush-операций';

-- Метрики iostat
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--Скользящие медианы по метрикам iostat
DROP TABLE IF EXISTS os_stat_iostat_device_median;
CREATE TABLE os_stat_iostat_device_median 
(
	id SERIAL , 
	curr_timestamp timestamp with time zone ,	
--DEVICE
	device  text ,  		    --Device
	dev_rps_long  numeric ,  --r/s Количество операций чтения в секунду.
	dev_rmbs_long  numeric ,  --rMB/s Скорость чтения (МБ/с).
	dev_rrqmps_long  numeric ,  --rrqm/s Количество прочитанных запросов, слитых в очередь (merge) в секунду.
	dev_rrqm_pct_long  numeric ,  --%rrqm Процент операций чтения, слитых перед отправкой на устройство.
	dev_r_await_long  numeric ,  --r_await (мс) Среднее время выполнения запросов чтения (включая время в очереди).
	dev_rareq_sz_long  numeric ,  --rareq_sz Средний размер запроса чтения (в КБ).
	dev_wps_long  numeric , --w/s Количество операций записи в секунду.
	dev_wmbps_long  numeric ,  --wMB/s Скорость записи (МБ/с).
	dev_wrqmps_long  numeric ,  --wrqm/s Количество записанных запросов, слитых в очередь в секунду.
	dev_wrqm_pct_long  numeric ,  --%wrqm Процент операций записи, слитых перед отправкой на устройство.
	dev_w_await_long  numeric ,  --w_await (мс) Среднее время выполнения запросов записи (включая время в очереди).
	dev_wareq_sz_long  numeric ,  --wareq_sz Средний размер запроса записи (в КБ).
	dev_dps_long  numeric ,  --d/s Операции для discard-запросов (актуально для SSD).
	dev_dmbps_long  numeric ,  --dMB/s скорость для discard-запросов (актуально для SSD).
	dev_drqmps_long  numeric , --drqm/s Количество запросов, слитых в очередь в секунду для discard-запросов (актуально для SSD).
	dev_drqm_pct_long  numeric ,  --%drqm Процент операций слитых перед отправкой на устройство для discard-запросов (актуально для SSD).
	dev_d_await_long  numeric ,  --d_await Среднее время выполнения (включая время в очереди) для discard-запросов (актуально для SSD).
	dev_dareq_sz_long  numeric ,  --dareq_sz Средний размер для discard-запросов (актуально для SSD)..
	dev_aqu_sz_long  numeric ,  --aqu_sz Средняя длина очереди запросов (глубина очереди).
	dev_util_pct_long  numeric ,  --%util Процент загрузки устройства (чем ближе к 100%, тем выше нагрузка).
	dev_fps_long  numeric ,  --f/s скорость выполнения запросов от flush-операций.
	dev_f_await_long  numeric   --f_await Среднее время выполнения запросов от flush-операций.	
);
ALTER TABLE os_stat_iostat_device_median ADD CONSTRAINT os_stat_iostat_device_median_pk PRIMARY KEY (id);
CREATE INDEX os_stat_iostat_device_median_idx ON os_stat_iostat_device_median ( curr_timestamp );
CREATE INDEX os_stat_iostat_device_median_idx2 ON os_stat_iostat_device_median ( device );

COMMENT ON TABLE os_stat_iostat_device_median IS 'Скользящие медианы по метрикам iosta';
COMMENT ON COLUMN os_stat_iostat_device_median.curr_timestamp IS 'Точка времени сбора данных ';	
COMMENT ON COLUMN os_stat_iostat_device_median.device IS 'Дисковое устройство';	
COMMENT ON COLUMN os_stat_iostat_device_median.dev_rps_long IS 'Скользящая медиана по значению r/s Количество операций чтения в секунду. ';	
COMMENT ON COLUMN os_stat_iostat_device_median.dev_rmbs_long IS 'Скользящая медиана по значению rMB/s Скорость чтения (МБ/с)';	
COMMENT ON COLUMN os_stat_iostat_device_median.dev_rrqmps_long IS 'Скользящая медиана по значению rrqm/s Количество прочитанных запросов, слитых в очередь (merge) в секунду';
COMMENT ON COLUMN os_stat_iostat_device_median.dev_rrqm_pct_long IS 'Скользящая медиана по значению %rrqm Процент операций чтения, слитых перед отправкой на устройство';
COMMENT ON COLUMN os_stat_iostat_device_median.dev_r_await_long IS 'Скользящая медиана по значению r_await (мс) Среднее время выполнения запросов чтения (включая время в очереди)';
COMMENT ON COLUMN os_stat_iostat_device_median.dev_rareq_sz_long IS 'Скользящая медиана по значению rareq-sz Средний размер запроса чтения (в КБ)';
COMMENT ON COLUMN os_stat_iostat_device_median.dev_wps_long IS 'Скользящая медиана по значению w/s Количество операций записи в секунду';
COMMENT ON COLUMN os_stat_iostat_device_median.dev_wmbps_long IS 'Скользящая медиана по значению wMB/s Скорость записи (МБ/с)';
COMMENT ON COLUMN os_stat_iostat_device_median.dev_wrqmps_long IS 'Скользящая медиана по значению wrqm/s Количество записанных запросов, слитых в очередь в секунду';
COMMENT ON COLUMN os_stat_iostat_device_median.dev_wrqm_pct_long IS 'Скользящая медиана по значению %wrqm Процент операций записи, слитых перед отправкой на устройство';
COMMENT ON COLUMN os_stat_iostat_device_median.dev_w_await_long IS 'Скользящая медиана по значению w_await (мс) Среднее время выполнения запросов записи (включая время в очереди)';
COMMENT ON COLUMN os_stat_iostat_device_median.dev_wareq_sz_long IS 'Скользящая медиана по значению wareq_sz Средний размер запроса записи (в КБ)';
COMMENT ON COLUMN os_stat_iostat_device_median.dev_dps_long IS 'Скользящая медиана по значению d/s Операции для discard-запросов (актуально для SSD)';
COMMENT ON COLUMN os_stat_iostat_device_median.dev_dmbps_long IS 'Скользящая медиана по значению dMB/s скорость для discard-запросов (актуально для SSD)';
COMMENT ON COLUMN os_stat_iostat_device_median.dev_drqmps_long IS 'Скользящая медиана по значению drqm/s Количество запросов, слитых в очередь в секунду для discard-запросов (актуально для SSD)';
COMMENT ON COLUMN os_stat_iostat_device_median.dev_drqm_pct_long IS 'Скользящая медиана по значению %drqm Процент операций слитых перед отправкой на устройство для discard-запросов (актуально для SSD)';
COMMENT ON COLUMN os_stat_iostat_device_median.dev_d_await_long IS 'Скользящая медиана по значению d_await Среднее время выполнения (включая время в очереди) для discard-запросов (актуально для SSD)';
COMMENT ON COLUMN os_stat_iostat_device_median.dev_dareq_sz_long IS 'Скользящая медиана по значению dareq_sz Средний размер для discard-запросов (актуально для SSD)';
COMMENT ON COLUMN os_stat_iostat_device_median.dev_aqu_sz_long IS 'Скользящая медиана по значению aqu_sz Средняя длина очереди запросов (глубина очереди)';
COMMENT ON COLUMN os_stat_iostat_device_median.dev_util_pct_long IS 'Скользящая медиана по значению %util Процент загрузки устройства (чем ближе к 100%, тем выше нагрузка)';
COMMENT ON COLUMN os_stat_iostat_device_median.dev_fps_long IS 'Скользящая медиана по значению f/s скорость выполнения запросов от flush-операций';
COMMENT ON COLUMN os_stat_iostat_device_median.dev_f_await_long IS 'Скользящая медиана по значению f_await Среднее время выполнения запросов от flush-операций';
--Скользящие медианы по метрикам iosta
--------------------------------------------------------------------------------


--------------------------------------------------------------------------------
-- core_statement_functions.sql
--------------------------------------------------------------------------------
-- Статистика производительности по отдельному SQL запросу
--
-- wait_event_jsonb   Сформировать jsonb по ожиданиям SQL запроса для заданого типа ожиданий
-- wait_queryid_jsonb Сформировать jsonb по ожиданиям SQL запроса
--
-- statement_stat - собрать исходные данные в таблицу statement_stat
-- statement_stat_median( start_timestamp text , finish_timestamp text ) агрегировать статистические данные за период 
--
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Собрать исходные данные в таблицу statement_stat
CREATE OR REPLACE FUNCTION 	statement_stat() RETURNS integer 
AS $$
DECLARE
  curr_datname name ;
  curr_username name ;
  curr_stat_resets_history timestamp with time zone  ; 
  stat_resets_history_count integer ;
BEGIN		
	
	-- Исходные данные производительности и ожиданий SQL-запроса
	INSERT INTO statement_stat
	(
		dbname , 
		username , 
		queryid  ,  		 		--queryid тестового запроса
		curr_timestamp  , 
		curr_wait_stats ,  --Объект типа jsonb, содержащий статистику по событиям ожидания для каждого выполнения запроса по соответствующему плану
		curr_calls ,
		curr_rows  
	)
	WITH queries AS
	(
		SELECT 	
			pd.datname AS dbname, 
			pr.rolname AS username, 
			st.queryid ,
			date_trunc( 'minute' , CURRENT_TIMESTAMP ) AS curr_timestamp ,
			( SELECT wait_queryid_jsonb( st.dbid , st.userid , st.queryid ) ) AS curr_wait_stats , 
			st2.calls AS curr_calls, 
			st2.rows AS curr_rows
		FROM
			pg_stat_statements st 
			JOIN pg_wait_sampling_profile w ON (st.queryid = w.queryid) 						
			JOIN pg_stat_statements st2 ON ( st2.dbid = st.dbid AND st2.userid = st.userid AND  st2.queryid = st.queryid )
			JOIN pg_database pd ON ( pd.oid = st.dbid )
			JOIN pg_roles pr ON ( pr.oid = st.userid )
		WHERE 
		    st.toplevel AND  --True, если данный запрос выполнялся на верхнем уровне (всегда true, если для параметра pg_stat_statements.track задано значение top)
			w.event_type IS NOT NULL AND w.event_type NOT IN ('Activity' , 'Client') AND 
			pd.datname NOT IN ('postgres' , 'template1' , 'template0' , 'pgpropwr' , 'expecto_db' ) 
	)
	SELECT
		q.dbname , 
		q.username , 
		q.queryid  ,
		q.curr_timestamp  , 
		q.curr_wait_stats , 
		q.curr_calls ,
		q.curr_rows  
	FROM 
		queries q
	GROUP BY 
		q.dbname , 
		q.username , 
		q.queryid  ,
		q.curr_timestamp  , 
		q.curr_wait_stats , 
		q.curr_calls ,
	q.curr_rows  
	ON CONFLICT ON CONSTRAINT statement_stat_pk DO NOTHING;

	--Текст SQL выражения
    INSERT INTO statement_stat_sql
	(
	  queryid , 
	  query 
	)
	SELECT 
	  queryid , 
	  query 
	FROM 
	  pg_stat_statements st 
	  JOIN pg_database pd ON ( pd.oid = st.dbid )
	WHERE 
	  st.toplevel AND  --True, если данный запрос выполнялся на верхнем уровне (всегда true, если для параметра pg_stat_statements.track задано значение top)
	  pd.datname NOT IN ('postgres' , 'template1' , 'template0' , 'pgpropwr' , 'expecto_db' ) 
	GROUP BY 
	  st.queryid , 
	  st.query 
	ON CONFLICT ON CONSTRAINT statement_stat_sql_pk  DO NOTHING;
	
return 0 ;
	
END
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION statement_stat IS 'Собрать исходные данные в таблицу statement_stat';
-- Собрать исходные данные в таблицу statement_stat
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--Агрегировать статистические данные за период 
CREATE OR REPLACE FUNCTION 	statement_stat_median(  start_timestamp text , finish_timestamp text   ) RETURNS integer 
AS $$
DECLARE
  min_timestamp timestamptz ;
  max_timestamp timestamptz ; 
  test_timestamp timestamptz ; 
  statement_stat_rec record ; 
  current_queryid bigint ; 
  curr_datname  text ;
  curr_username text  ;
  current_wait_stats jsonb;
  
  current_bufferpin  bigint  ;
  current_extension  bigint  ;
  current_io  bigint  ;
  current_ipc  bigint  ;
  current_lock  bigint  ;
  current_lwlock  bigint  ;
  current_timeout  bigint  ;
  
  current_dbid oid ;
  current_userid oid ;
    
  wait_event_type_rec record ; 
  wait_event_rec record ; 
  current_wait_event_type_jsonb jsonb;
  
  
  curr_calls_long numeric ;  
  curr_rows_long numeric ;  
  
  curr_bufferpin_long numeric ; 
  curr_extension_long numeric ; 
  curr_io_long numeric ; 
  curr_ipc_long numeric ; 
  curr_lock_long numeric ; 
  curr_lwlock_long numeric ; 
  curr_timeout_long numeric ; 
  current_value numeric ; 
  
  curr_waiting_events_long numeric ; 
  current_minute_timestamp timestamptz ;
  last_minute_timestamp timestamptz ;
  statement_stat_wait_events_rec record ;
  wait_event_long numeric ;
  
  
BEGIN		
	SELECT 	date_trunc('minute' ,  to_timestamp( start_timestamp , 'YYYY-MM-DD HH24:MI' ) ) 
	INTO 	min_timestamp ; 
		
	SELECT 	date_trunc('minute' ,  to_timestamp( finish_timestamp , 'YYYY-MM-DD HH24:MI' ) ) 
	INTO 	max_timestamp ; 
	
	TRUNCATE TABLE statement_stat_wait_events ; 
	TRUNCATE TABLE statement_stat_median ; 
	TRUNCATE TABLE statement_stat_waitings_median ; 
	
	RAISE NOTICE 'start_timestamp=%',start_timestamp;
	RAISE NOTICE 'finish_timestamp=%',finish_timestamp;

	SELECT date_trunc('minute' , clock_timestamp() )
	INTO last_minute_timestamp ;	
	
	-----------------------------------------------------
	-- ИСХОДНЫЕ ДАННЫЕ 	
	--FOR statement_stat_rec IN 
	FOR statement_stat_rec IN 
	SELECT *
	FROM statement_stat
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp
	ORDER BY curr_timestamp
	LOOP 
		SELECT date_trunc('minute' , clock_timestamp() )
	    INTO current_minute_timestamp ;
		
		IF last_minute_timestamp != current_minute_timestamp  
		THEN
			last_minute_timestamp = current_minute_timestamp ; 
			RAISE NOTICE 'current_timestamp=%',date_trunc('minute' , statement_stat_rec.curr_timestamp );		
		END IF ;
	
	
		current_queryid = statement_stat_rec.queryid ; 
		curr_datname  = statement_stat_rec.dbname ;
		curr_username = statement_stat_rec.username ;

		current_wait_stats = statement_stat_rec.curr_wait_stats ;
		
		------------------------------------------------------------
		--WAIT_EVENT_TYPE
		    --FOR wait_event_type_rec IN
			FOR wait_event_type_rec IN
			WITH wait_event_type_jsonb AS
			(
			  SELECT jsonb_object_keys (current_wait_stats)  AS wait_event_type_key 
			)
			SELECT * 
			FROM wait_event_type_jsonb	
			WHERE  wait_event_type_key NOT IN ('Total' , 'Client' , 'Activity') 	       
			LOOP 
			
				SELECT statement_stat_rec.curr_wait_stats->>wait_event_type_rec.wait_event_type_key
				INTO current_wait_event_type_jsonb ;
				
				--FOR wait_event_rec IN 
				FOR wait_event_rec IN 
				WITH wait_event_jsonb AS
				(
					SELECT jsonb_object_keys( current_wait_stats->wait_event_type_rec.wait_event_type_key )  AS wait_event_key 
				)
				SELECT * 
				FROM wait_event_jsonb
				LOOP 
					
					-----------------------------------------------------------------------------------------------------------
					--при использовании pg_wait_sampling_profile считается количество ожиданий, а не время
					--делить на 10 не нужно
					--SELECT to_number( current_wait_event_type_jsonb->>wait_event_rec.wait_event_key , '0000000000')  / 10 
					SELECT to_number( current_wait_event_type_jsonb->>wait_event_rec.wait_event_key , '0000000000')
					INTO current_value  ;
					--при использовании pg_wait_sampling_profile считается количество ожиданий, а не время
					-----------------------------------------------------------------------------------------------------------


					SELECT * 
					INTO statement_stat_wait_events_rec
					FROM statement_stat_wait_events
					WHERE queryid = current_queryid AND dbname = curr_datname AND username = curr_username  AND 
						  curr_timestamp  = statement_stat_rec.curr_timestamp AND 
						  wait_event_type = wait_event_type_rec.wait_event_type_key AND 
						  wait_event = wait_event_rec.wait_event_key ; 

					-------------------------------------------
					--1. INSERT INTO statement_stat_wait_events
					INSERT INTO statement_stat_wait_events
						(
							curr_timestamp ,
							dbname ,
							username , 
							queryid ,
							wait_event_type , 
							wait_event , 
							curr_value 
						)
					VALUES 
						(
							statement_stat_rec.curr_timestamp , 
							curr_datname , 
							curr_username , 
							current_queryid , 
							wait_event_type_rec.wait_event_type_key  , 
							wait_event_rec.wait_event_key ,
							current_value 
						);
					--1. INSERT INTO statement_stat_wait_events
					-------------------------------------------		
					
					-------------------------------------------		
					--2. INSERT INTO  statement_stat_waitings_median
					SELECT (percentile_cont(0.5) within group (order by curr_value))::numeric
					INTO wait_event_long
					FROM statement_stat_wait_events
					WHERE   queryid = current_queryid AND 
							dbname = curr_datname AND 
							username = curr_username AND 
							wait_event_type = wait_event_type_rec.wait_event_type_key AND
							wait_event = wait_event_rec.wait_event_key AND  
							curr_timestamp BETWEEN statement_stat_rec.curr_timestamp - (interval '60 minute') AND statement_stat_rec.curr_timestamp ;
			
					IF  wait_event_long IS NOT NULL 
					THEN				
						INSERT INTO statement_stat_waitings_median
						(
							dbname, 
							username,  
							queryid ,  
							curr_timestamp ,
							wait_event_type,
							wait_event     ,				
							curr_value_long 
						)
						VALUES 
						( 
							curr_datname , 
							curr_username , 
							current_queryid , 
							statement_stat_rec.curr_timestamp , 
							wait_event_type_rec.wait_event_type_key , 
							wait_event_rec.wait_event_key ,				
							wait_event_long		
						);	
					END IF ; 
					--2. INSERT INTO  statement_stat_waitings_median
					-------------------------------------------		
				END LOOP; 
				--FOR wait_event_rec IN 					
			END LOOP;
			--FOR wait_event_type_rec IN 
		--WAIT_EVENT_TYPE
		------------------------------------------------------------
		
		----------------------------------------------------------------------------
		-- МЕДИТАНЫ		
				--Долгая медиана calls	
				SELECT (percentile_cont(0.5) within group (order by curr_calls))::numeric
				INTO curr_calls_long
				FROM statement_stat
				WHERE curr_timestamp BETWEEN statement_stat_rec.curr_timestamp - (interval '60 minute') AND statement_stat_rec.curr_timestamp 
					  AND queryid = current_queryid AND dbname = curr_datname AND username = curr_username;
				IF curr_calls_long IS NULL THEN curr_calls_long = 0 ; END IF ;	

				--Долгая медиана rows
				SELECT (percentile_cont(0.5) within group (order by curr_rows))::numeric
				INTO curr_rows_long
				FROM statement_stat
				WHERE curr_timestamp BETWEEN statement_stat_rec.curr_timestamp - (interval '60 minute') AND statement_stat_rec.curr_timestamp 
					  AND queryid = current_queryid AND dbname = curr_datname AND username = curr_username;
				IF curr_rows_long IS NULL THEN curr_rows_long = 0 ; END IF ;	
				
				-----------------------------------------------------------------------------
				-- Долгая медиана по wait_event_type
				SELECT (percentile_cont(0.5) within group (order by curr_value))::numeric
				INTO curr_bufferpin_long
				FROM statement_stat_wait_events
				WHERE curr_timestamp BETWEEN statement_stat_rec.curr_timestamp - (interval '60 minute') AND statement_stat_rec.curr_timestamp 
				AND queryid = current_queryid AND dbname = curr_datname AND username = curr_username
				AND wait_event_type = 'BufferPin';				
				IF curr_bufferpin_long IS NULL THEN curr_bufferpin_long = 0 ; END IF ; 
			
				SELECT (percentile_cont(0.5) within group (order by curr_value))::numeric
				INTO curr_extension_long
				FROM statement_stat_wait_events
				WHERE curr_timestamp BETWEEN statement_stat_rec.curr_timestamp - (interval '60 minute') AND statement_stat_rec.curr_timestamp 
				AND queryid = current_queryid AND dbname = curr_datname AND username = curr_username
				AND wait_event_type = 'Extension';  
				IF curr_extension_long IS NULL THEN curr_extension_long = 0 ; END IF ; 

				SELECT (percentile_cont(0.5) within group (order by curr_value))::numeric
				INTO curr_io_long
				FROM statement_stat_wait_events
				WHERE curr_timestamp BETWEEN statement_stat_rec.curr_timestamp - (interval '60 minute') AND statement_stat_rec.curr_timestamp 
				AND queryid = current_queryid AND dbname = curr_datname AND username = curr_username
				AND wait_event_type = 'IO';  				
				IF curr_io_long IS NULL THEN curr_io_long = 0 ; END IF ; 
				  
				SELECT (percentile_cont(0.5) within group (order by curr_value))::numeric
				INTO curr_ipc_long
				FROM statement_stat_wait_events
				WHERE curr_timestamp BETWEEN statement_stat_rec.curr_timestamp - (interval '60 minute') AND statement_stat_rec.curr_timestamp 
				AND queryid = current_queryid AND dbname = curr_datname AND username = curr_username
				AND wait_event_type = 'IPC';  				  
				IF curr_ipc_long IS NULL THEN curr_ipc_long = 0 ; END IF ; 

				SELECT (percentile_cont(0.5) within group (order by curr_value))::numeric
				INTO curr_lock_long
				FROM statement_stat_wait_events
				WHERE curr_timestamp BETWEEN statement_stat_rec.curr_timestamp - (interval '60 minute') AND statement_stat_rec.curr_timestamp 
				AND queryid = current_queryid AND dbname = curr_datname AND username = curr_username
				AND wait_event_type = 'Lock';    
				IF curr_lock_long IS NULL THEN curr_lock_long = 0 ; END IF ; 

				SELECT (percentile_cont(0.5) within group (order by curr_value))::numeric
				INTO curr_lwlock_long
				FROM statement_stat_wait_events
				WHERE curr_timestamp BETWEEN statement_stat_rec.curr_timestamp - (interval '60 minute') AND statement_stat_rec.curr_timestamp 
				AND queryid = current_queryid AND dbname = curr_datname AND username = curr_username
				AND wait_event_type = 'LWLock';    				
				IF curr_lwlock_long IS NULL THEN curr_lwlock_long = 0 ; END IF ; 

				SELECT (percentile_cont(0.5) within group (order by curr_value))::numeric
				INTO curr_timeout_long
				FROM statement_stat_wait_events
				WHERE curr_timestamp BETWEEN statement_stat_rec.curr_timestamp - (interval '60 minute') AND statement_stat_rec.curr_timestamp 
				AND queryid = current_queryid AND dbname = curr_datname AND username = curr_username
				AND wait_event_type = 'Timeout';    
				IF curr_timeout_long IS NULL THEN curr_timeout_long = 0 ; END IF ; 	
				-- Долгая медиана по wait_event_type
				-----------------------------------------------------------------------------
		-- МЕДИТАНЫ					
		----------------------------------------------------------------------------

		--------------------------------------------------
		-- ОЖИДАНИЯ  
			curr_waiting_events_long = curr_bufferpin_long + curr_extension_long +  curr_io_long + curr_ipc_long + curr_lock_long + curr_lwlock_long + curr_timeout_long; 
		-- ОЖИДАНИЯ  
		--------------------------------------------------
		
				-----------------------------------------
				--3. INSERT INTO statement_stat_median 
				INSERT INTO statement_stat_median 
				(
					curr_timestamp , 
					dbname, 
					username,   
					queryid,

					calls_long,
					rows_long,
  
					waitings_long,
					bufferpin_long,
					extension_long,
					io_long,
					ipc_long,
					lock_long,
					lwlock_long,
					timeout_long
				)
				VALUES 
				(
					statement_stat_rec.curr_timestamp , 
					curr_datname ,
					curr_username ,
					current_queryid ,

					curr_calls_long , 
					curr_rows_long ,
					
					curr_waiting_events_long , 
					curr_bufferpin_long , 
					curr_extension_long , 
					curr_io_long , 
					curr_ipc_long , 
					curr_lock_long , 
					curr_lwlock_long , 
					curr_timeout_long
				);			
				--3. INSERT INTO statement_stat_median 
				-----------------------------------------		
	END LOOP;
	--FOR statement_stat_rec IN 
	-- ИСХОДНЫЕ ДАННЫЕ 	
	-----------------------------------------------------
	
	
	
return 0 ;
END
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION statement_stat_median IS 'Агрегировать статистические данные за период ';
--Агрегировать статистические данные за период 
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Сформировать jsonb по ожиданиям SQL запроса для заданого типа ожиданий
CREATE OR REPLACE FUNCTION 	wait_event_jsonb( curr_dbid oid , curr_userid oid ,  curr_queryid bigint ,curr_wait_event_type text ) RETURNS jsonb
AS $$
DECLARE
 curr_jsonb jsonb; 
 res_jsonb jsonb; 
 total_jsonb jsonb ; 
 jsonb_rec record ; 
BEGIN
    curr_jsonb = '{}'::jsonb; 
	
	FOR jsonb_rec IN 
	WITH 
		wait_profile AS 
		(
		SELECT 
		 w.queryid , 
		 w.event AS wait_event ,
		 SUM(w.count) as counter
		FROM 
		 pg_wait_sampling_profile w
		WHERE 
		 w.queryid = curr_queryid 
		 AND w.event_type =  curr_wait_event_type
		GROUP BY 
		w.queryid , 
		w.event 
		),
		statement_stats AS 
		(
		SELECT 
		  st.dbid , 
		  st.userid , 
		  st.queryid
		FROM 
		 pg_stat_statements st   
		WHERE 
		st.toplevel AND  --True, если данный запрос выполнялся на верхнем уровне (всегда true, если для параметра pg_stat_statements.track задано значение top)
		st.queryid = curr_queryid AND 
		st.dbid = curr_dbid  AND 
		st.userid = curr_userid
		GROUP BY 
		  st.dbid , 
		  st.userid , 
		  st.queryid 
		)
		SELECT
		  st.dbid , 
		  st.userid , 
		  st.queryid , 		 
		  w.wait_event, 
		  w.counter
		FROM 
		 wait_profile w JOIN  statement_stats st ON ( w.queryid = st.queryid )
	LOOP
				
		SELECT 
			curr_jsonb || jsonb_build_object ( jsonb_rec.wait_event , jsonb_rec.counter )
		INTO 
			curr_jsonb ;		
			
		SELECT jsonb_build_object ( curr_wait_event_type , curr_jsonb )
		INTO res_jsonb;
		
--RAISE NOTICE '%' ,curr_jsonb ;  				

	END LOOP ;

--RAISE NOTICE '%' ,res_jsonb ; 
	
return res_jsonb;
END
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION wait_event_jsonb IS 'Сформировать jsonb по ожиданиям SQL запроса для заданого типа ожиданий';
-- Сформировать jsonb по ожиданиям SQL запроса для заданого типа ожиданий
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Сформировать jsonb по ожиданиям SQL запроса
CREATE OR REPLACE FUNCTION 	wait_queryid_jsonb( curr_dbid oid , curr_userid oid ,  curr_queryid bigint ) RETURNS jsonb
AS $$
DECLARE
 curr_jsonb jsonb;  
 total_jsonb jsonb ; 
 jsonb_rec record ; 
 curr_wait_event_jsonb jsonb; 
 wait_event_jsonb_array jsonb[]; 
 curr_index integer ;
 curr_length integer ;
 
BEGIN
    curr_jsonb = '{}'::jsonb; 
	
	FOR jsonb_rec IN 
	WITH 
		wait_profile AS 
		(
		SELECT 
		 w.queryid , 
		 w.event_type AS wait_event_type ,
		 SUM(w.count) as counter
		FROM 
		 pg_wait_sampling_profile w
		WHERE 
		 w.queryid = curr_queryid 
		 AND w.event_type IS NOT NULL 
		 AND w.event_type NOT IN ('Activity' , 'Client') 
		GROUP BY 
		w.queryid , 
		w.event_type 
		),
		statement_stats AS 
		(
		SELECT 
		  st.dbid , 
		  st.userid , 
		  st.queryid 
		FROM 
		 pg_stat_statements st   
		WHERE 
		st.toplevel AND  --True, если данный запрос выполнялся на верхнем уровне (всегда true, если для параметра pg_stat_statements.track задано значение top)
		st.queryid = curr_queryid AND 
		st.dbid = curr_dbid  AND 
		st.userid = curr_userid
		GROUP BY 
		  st.dbid , 
		  st.userid , 
		  st.queryid 
		)
		SELECT
		  st.dbid , 
		  st.userid , 
		  st.queryid , 		  
		  w.wait_event_type , 
		  w.counter
		FROM 
		 wait_profile w JOIN  statement_stats st ON ( w.queryid = st.queryid )
	LOOP
		SELECT 
			curr_jsonb || jsonb_build_object ( jsonb_rec.wait_event_type , jsonb_rec.counter )
		INTO 
			curr_jsonb ;
--RAISE NOTICE '%' ,curr_jsonb ;  				
			
		SELECT wait_event_jsonb( curr_dbid, curr_userid , curr_queryid ,  jsonb_rec.wait_event_type )
		INTO curr_wait_event_jsonb;
		
		SELECT 
			wait_event_jsonb_array || curr_wait_event_jsonb
		INTO 
			wait_event_jsonb_array ;
			
--RAISE NOTICE '%' ,curr_jsonb ;  				

	END LOOP ;

--RAISE NOTICE '%' ,wait_event_jsonb_array ;  				

	SELECT 
		jsonb_build_object ( 'Total' , curr_jsonb )
	INTO 
		total_jsonb ;
	
	SELECT array_length( wait_event_jsonb_array , 1 )
	INTO curr_length ; 

--RAISE NOTICE '%',curr_length;	
	
	FOR curr_index IN 1..curr_length	
	LOOP
	
--RAISE NOTICE '%',wait_event_jsonb_array[curr_index];	

		SELECT 
			total_jsonb || wait_event_jsonb_array[curr_index]
		INTO 
			total_jsonb ;
	END LOOP ;
	
return total_jsonb;
END
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION wait_queryid_jsonb IS 'Сформировать jsonb по ожиданиям SQL запроса';
-- Сформировать jsonb по ожиданиям SQL запроса
--------------------------------------------------------------------------------







--------------------------------------------------------------------------------
-- core_statement_tables.sql
--------------------------------------------------------------------------------
-- Статистика уровня SQL выражений
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Исходные данные производительности и ожиданий SQL-запроса
DROP TABLE IF EXISTS statement_stat;
CREATE UNLOGGED TABLE statement_stat 
(
  -----------------------------------------
  -- PRIMARY KEY
  curr_timestamp timestamp with time zone , 
  dbname 				name, 
  username			name ,   
  queryid bigint ,
  -- PRIMARY KEY  
  -----------------------------------------  
  curr_calls integer ,  
  curr_rows integer ,  
    
  curr_wait_stats	jsonb --Объект типа jsonb, содержащий статистику по событиям ожидания

);
ALTER TABLE statement_stat ADD CONSTRAINT statement_stat_pk PRIMARY KEY ( curr_timestamp , dbname , username , queryid);
CREATE INDEX statement_stat_idx ON statement_stat ( curr_timestamp );

COMMENT ON TABLE statement_stat IS 'Исходные данные производительности и ожиданий SQL-запроса';
COMMENT ON COLUMN statement_stat.curr_timestamp IS 'Точка времени сбора данных ';
COMMENT ON COLUMN statement_stat.dbname IS 'Наименование базы данных ';
COMMENT ON COLUMN statement_stat.username IS 'Наименование роли ';
COMMENT ON COLUMN statement_stat.queryid IS 'Идентификатор SQL выражения ';
COMMENT ON COLUMN statement_stat.curr_calls IS 'Число выполнений';
COMMENT ON COLUMN statement_stat.curr_rows IS 'Общее число строк, полученных или затронутых оператором';
COMMENT ON COLUMN statement_stat.curr_wait_stats IS 'Объект типа jsonb, содержащий статистику по событиям ожидания';
-- Текущая статистика SQL-запроса
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Текст SQL выражения
DROP TABLE IF EXISTS statement_stat_sql;
CREATE TABLE statement_stat_sql
(
 queryid bigint ,
 query text 
);
ALTER TABLE statement_stat_sql ADD CONSTRAINT statement_stat_sql_pk PRIMARY KEY ( queryid );

COMMENT ON TABLE statement_stat_sql IS 'Текст SQL выражения';
COMMENT ON COLUMN statement_stat_sql.queryid IS 'Идентификатор SQL выражения';
COMMENT ON COLUMN statement_stat_sql.query IS 'Текст SQL выражения';
-- Текст SQL выражения
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--Исходные данные ожиданий на уровне SQL
DROP TABLE IF EXISTS statement_stat_wait_events;
CREATE UNLOGGED TABLE statement_stat_wait_events 
(
  -----------------------------------------
  -- PRIMARY KEY
  curr_timestamp timestamp with time zone , 
  dbname 				name, 
  username			name ,   
  queryid bigint ,
  wait_event_type  	text ,
  wait_event       	text ,  
  -- PRIMARY KEY  
  -----------------------------------------  
  curr_value       	bigint 
);
ALTER TABLE statement_stat_wait_events ADD CONSTRAINT statement_stat_wait_events_pk PRIMARY KEY ( curr_timestamp , dbname , username , queryid , wait_event_type , wait_event );
CREATE INDEX statement_stat_wait_events_idx ON statement_stat ( curr_timestamp );
CREATE INDEX statement_stat_wait_events_current_queryid_idx ON statement_stat_wait_events ( queryid );
CREATE INDEX statement_stat_wait_events_dbname_idx ON statement_stat_wait_events ( dbname );
CREATE INDEX statement_stat_wait_events_usename_idx ON statement_stat_wait_events ( username );
CREATE INDEX statement_stat_wait_events_wait_event_type_idx ON statement_stat_wait_events ( wait_event_type );
CREATE INDEX statement_stat_wait_events_wait_event_idx ON statement_stat_wait_events ( wait_event );

COMMENT ON TABLE statement_stat_wait_events IS 'Исходные данные ожиданий на уровне SQL';
COMMENT ON COLUMN statement_stat_wait_events.curr_timestamp IS 'Точка времени сбора данных ';
COMMENT ON COLUMN statement_stat_wait_events.dbname IS 'Наименование базы данных ';
COMMENT ON COLUMN statement_stat_wait_events.username IS 'Наименование роли ';
COMMENT ON COLUMN statement_stat_wait_events.queryid IS 'Идентификатор SQL выражения ';
COMMENT ON COLUMN statement_stat_wait_events.wait_event_type IS 'Тип ожидания';
COMMENT ON COLUMN statement_stat_wait_events.wait_event IS 'Событие ожидания';
COMMENT ON COLUMN statement_stat_wait_events.curr_value IS 'Количество ожиданий';
--Исходные данные ожиданий на уровне SQL
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--Скользящие медианы на уровне SQL
DROP TABLE IF EXISTS statement_stat_median;
CREATE TABLE statement_stat_median 
(
  -----------------------------------------
  -- PRIMARY KEY
  curr_timestamp timestamp with time zone , 
  dbname 				name, 
  username			name ,   
  queryid bigint ,
  -- PRIMARY KEY  
  ----------------------------------------- 		

  calls_long numeric ,
  rows_long numeric ,
  
  waitings_long numeric ,	
  bufferpin_long numeric ,
  extension_long numeric ,
  io_long numeric ,
  ipc_long numeric ,
  lock_long numeric ,
  lwlock_long numeric ,
  timeout_long numeric 	
);
ALTER TABLE statement_stat_median ADD CONSTRAINT statement_stat_median_pk PRIMARY KEY ( curr_timestamp , dbname , username , queryid );
CREATE INDEX statement_stat_median_idx ON statement_stat_median ( curr_timestamp );
CREATE INDEX statement_stat_median_idx2 ON statement_stat_median ( queryid );

COMMENT ON TABLE statement_stat_median IS 'Скользящие медианы на уровне SQL';
COMMENT ON COLUMN  statement_stat_median.curr_timestamp IS 'Точка времени сбора данных ';
COMMENT ON COLUMN  statement_stat_median.dbname IS 'Наименование базы данных ';
COMMENT ON COLUMN  statement_stat_median.username IS 'Наименование роли ';
COMMENT ON COLUMN  statement_stat_median.queryid IS 'Идентификатор SQL выражения ';
COMMENT ON COLUMN  statement_stat_median.calls_long IS 'Число выполнений';
COMMENT ON COLUMN  statement_stat_median.rows_long IS 'Общее число строк, полученных или затронутых оператором';
COMMENT ON COLUMN  statement_stat_median.waitings_long IS 'Скользящая медиана по ожиданиям';
COMMENT ON COLUMN  statement_stat_median.bufferpin_long IS 'Скользящая медиана по ожиданиям типа BufferPin';
COMMENT ON COLUMN  statement_stat_median.extension_long IS 'Скользящая медиана по ожиданиям типа Extension';
COMMENT ON COLUMN  statement_stat_median.io_long IS 'Скользящая медиана по ожиданиям типа IO';
COMMENT ON COLUMN  statement_stat_median.ipc_long IS 'Скользящая медиана по ожиданиям типа IPC';
COMMENT ON COLUMN  statement_stat_median.lock_long IS 'Скользящая медиана по ожиданиям типа Lock';
COMMENT ON COLUMN  statement_stat_median.lwlock_long IS 'Скользящая медиана по ожиданиям типа LWLock';
COMMENT ON COLUMN  statement_stat_median.timeout_long IS 'Скользящая медиана по ожиданиям типа Timeout';
--Скользящие медианы на уровне SQL
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--Скользящие медианы по событиям ожидания для SQL выполнения
DROP TABLE IF EXISTS statement_stat_waitings_median;
CREATE TABLE statement_stat_waitings_median 
(
  -----------------------------------------
  -- PRIMARY KEY
  curr_timestamp timestamp with time zone , 
  dbname 				name, 
  username			name ,   
  queryid bigint ,
  wait_event_type  text ,
  wait_event       text ,
  -- PRIMARY KEY  
  ----------------------------------------- 
  curr_value_long       integer 

);
ALTER TABLE statement_stat_waitings_median ADD CONSTRAINT statement_stat_waitings_median_pk PRIMARY KEY ( curr_timestamp , dbname , username , queryid, wait_event_type , wait_event );
CREATE INDEX statement_stat_waitings_median_idx ON statement_stat_waitings_median ( curr_timestamp );

COMMENT ON TABLE statement_stat_waitings_median IS 'Скользящие медианы по событиям ожидания для SQL выполнения';
COMMENT ON COLUMN statement_stat_waitings_median.curr_timestamp IS 'Точка времени сбора данных ';
COMMENT ON COLUMN statement_stat_waitings_median.dbname IS 'Наименование базы данных ';
COMMENT ON COLUMN statement_stat_waitings_median.username IS 'Наименование роли ';
COMMENT ON COLUMN statement_stat_waitings_median.queryid IS 'Идентификатор SQL выражения ';
COMMENT ON COLUMN statement_stat_waitings_median.wait_event_type IS 'Тип ожидания';
COMMENT ON COLUMN statement_stat_waitings_median.wait_event IS 'Событие ожидания';
COMMENT ON COLUMN statement_stat_waitings_median.curr_value_long IS 'Скользящая медиана по событию ожиданиям';
--Скользящие медианы по событиям ожидания для SQL выполнения
--------------------------------------------------------------------------------


-------------------------------------------------------------------------------------
-- core_tables.sql
-------------------------------------------------------------------------------------
-- Корневые таблицы 
--
-------------------------------------------------------------------------------------
--Конфигурация
DROP TABLE IF EXISTS configuration ; 
CREATE UNLOGGED TABLE configuration
(  
  day_for_store integer DEFAULT 7	--Глубина хранения   
);
COMMENT ON TABLE configuration IS 'Конфигурационные параметры pg_expecto';
COMMENT ON COLUMN configuration.day_for_store IS 'Глубина хранения ';
--Конфигурационные параметры
-------------------------------------------------------------------------------------




------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- load_test_functions.sql
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Функции обеспечения нагрузочного теста
-- load_test_new_test() Начать новый тест
--
-- load_test_set_weight_for_scenario - Установить вес для тестового сценария
--
-- load_test_set_testdb - Установить имя тестовой БД
--
-- load_test_start_collect_data() Начать собирать данные для статистики для текущей фазы теста
-- load_test_stop_collect_data() Завершить сбор данных для статистики для текущей фазы теста
--
-- load_test_get_current_test_id() --Получить id текущего теста
-- load_test_get_current_test_pass_id()  --Получить id тестового прохода
-- load_test_current_pass() --Текущий проход
--
-- load_test_get_load() -- Текущее количество подключений для pgbench
-- load_test_set_load() --Установить текущую нагрузку connections
-- load_test_get_load_by_scenario( current_scenario integer )  -- Текущее количество подключений для pgbench для заданного сценария
--
-- load_test_get_start_timestamp() -- Получить время начала теста 
-- load_test_get_finish_timestamp()  -- Получить время окончания теста 
--
-- load_test_set_start_load( new_max_load integer ) --Установить начальное  количество подключений для pgbench
-- load_test_set_max_load( new_max_load integer ) --Установить максимальное  количество подключений для pgbench
-- load_test_is_test_could_be_finished()  --Если тест может быть остановлен 
--
-- load_test_has_the_first_hour_passed() --ЕСЛИ идет первый час работы
--
-- load_test_increment_pass_counter() --УВЕЛИЧИТЬ СЧЕТЧИК ИТЕРАЦИЙ
--
-- load_test_set_scenario_queryid --Установить quaryid для сценариев 

---------------------------------------------------------------------
-- ЗАФИКСИРОВАТЬ ПАРАМЕТРЫ vm
/*
  save_dirty_background_ratio( integer )
  save_dirty_ratio( integer )
  save_dirty_background_bytes( integer )
  save_dirty_bytes( integer )
  save_dirty_expire_centisecs( integer )
  save_dirty_writeback_centisecs( integer )
  save_vfs_cache_pressure( integer )
  save_swappiness( integer )
*/
-- ЗАФИКСИРОВАТЬ ПАРАМЕТРЫ vm

-- get_vm_params_list() --получить список текущих параметров управления RAM 
------------------------------------------------------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Начать новый тест
CREATE OR REPLACE FUNCTION load_test_new_test() RETURNS integer AS $$
BEGIN
	
	---------------------------------------
	-- СБРОСИТЬ СТАРЫЕ ДАННЫЕ 
	TRUNCATE TABLE load_test CASCADE ;
	-- СБРОСИТЬ СТАРЫЕ ДАННЫЕ 
	---------------------------------------
	
	INSERT INTO load_test ( test_started ) VALUES ( CURRENT_TIMESTAMP ) ;		
	
  return 0 ; 
END
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION load_test_new_test IS 'Начать новый тест';
------------------------------------------------------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Установить имя тестовой БД
CREATE OR REPLACE FUNCTION load_test_set_testdb( new_testdb text ) RETURNS integer AS $$
DECLARE
  current_test_id bigint;  
BEGIN
    SELECT load_test_get_current_test_id()
	INTO current_test_id;
	
	UPDATE load_test SET testdb_name = new_testdb WHERE test_id = current_test_id ;		
	
  return 0 ; 
END
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION load_test_new_test IS 'Установить имя тестовой БД';
-- Установить имя тестовой БД
------------------------------------------------------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Получить id текущего теста
CREATE OR REPLACE FUNCTION load_test_get_current_test_id() RETURNS integer AS $$
DECLARE 
  current_test_id bigint;  
BEGIN
    SELECT test_id
	INTO current_test_id
	FROM load_test ; 

	return current_test_id ; 
END
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION load_test_get_current_test_id IS 'Получить id текущего теста';
--Получить id текущего теста
------------------------------------------------------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Получить id тестового прохода
CREATE OR REPLACE FUNCTION load_test_get_current_test_pass_id() RETURNS integer AS $$
DECLARE    
  curr_id bigint ;    
  current_test_id bigint;
BEGIN
    SELECT load_test_get_current_test_id()
	INTO current_test_id;
	  
	SELECT MAX(id) 
	INTO curr_id
	FROM load_test_pass WHERE current_test_id = test_id;
	
	IF curr_id IS NULL 
	THEN 
		RAISE EXCEPTION ' НЕТ НАЧАТЫХ ПРОХОДОВ ДЛЯ ТЕКУЩЕГО ТЕСТА !' ;
	END IF;
	
	return curr_id ;
END
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION load_test_get_current_test_pass_id IS 'Получить id тестового прохода';
--Получить id тестового прохода
------------------------------------------------------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Текущий проход
CREATE OR REPLACE FUNCTION load_test_current_pass() RETURNS integer AS $$
DECLARE 
  current_test_id bigint;  
  current_pass_counter bigint ; 
BEGIN
    SELECT load_test_get_current_test_id()
	INTO current_test_id;
	
	SELECT pass_counter
	INTO current_pass_counter
	FROM load_test
	WHERE test_id =  current_test_id;
	
  return current_pass_counter ; 
END
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION load_test_current_pass IS 'Текущий проход';
--Текущий проход
------------------------------------------------------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Текущее количество подключений для pgbench
CREATE OR REPLACE FUNCTION load_test_get_load() RETURNS integer AS $$
DECLARE
 load_test_pass_rec record ; 
 current_test_pass_id integer ; 
BEGIN
  SELECT load_test_get_current_test_pass_id()
  INTO current_test_pass_id ; 
  
  SELECT * 
  INTO load_test_pass_rec
  FROM load_test_pass
  WHERE id = current_test_pass_id ; 
  

  return load_test_pass_rec.load_connections ; 
END
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION load_test_get_load IS 'Текущее количество подключений для pgbench';
-- Текущее количество подключений для pgbench
------------------------------------------------------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Установить текущую нагрузку connections
-- Установить текущую нагрузку connections
CREATE OR REPLACE FUNCTION load_test_set_load() RETURNS integer AS $$
DECLARE
    current_test_id integer;
    load_test_rec record;
    load_test_pass_rec record;
    current_load_connections DOUBLE PRECISION;
    result_load_connections integer;
    has_the_first_hour_passed integer;
    current_test_pass_id integer;

    -- Параметры временных интервалов (в минутах)
    total_duration_minutes CONSTANT integer := 180;   -- 3 часа
    warmup_minutes CONSTANT integer := 60;            -- 1 час прогрева
    step_minutes CONSTANT integer := 10;              -- длительность итерации

    growth_iterations integer;    -- количество итераций роста после прогрева
    N0 integer;                   -- начальная нагрузка
    Nmax integer;                 -- максимальная нагрузка
    iter integer;                 -- номер итерации роста (1..growth_iterations)
    sessions numeric;
BEGIN
    -- Получаем идентификатор текущего теста и прохода
    SELECT load_test_get_current_test_id() INTO current_test_id;
    SELECT * INTO load_test_rec FROM load_test WHERE test_id = current_test_id;
    SELECT load_test_get_current_test_pass_id() INTO current_test_pass_id;
    SELECT * INTO load_test_pass_rec FROM load_test_pass WHERE id = current_test_pass_id;

    ---------------------------------------------------------------------
    -- Первый час (прогрев) – нагрузка равна базовой
    SELECT load_test_has_the_first_hour_passed() INTO has_the_first_hour_passed;
    RAISE NOTICE 'has_the_first_hour_passed = %', has_the_first_hour_passed;

    IF has_the_first_hour_passed = 0 THEN
        UPDATE load_test_pass
        SET load_connections = load_test_rec.base_load_connections
        WHERE test_id = current_test_id AND pass_counter = load_test_rec.pass_counter;

        RETURN load_test_rec.base_load_connections;
    END IF;
    ---------------------------------------------------------------------

    ---------------------------------------------------------------------
    -- Расчёт нагрузки для оставшихся итераций (экспоненциальный рост)
    -- Общее количество итераций роста после прогрева:
    -- (общая длительность - длительность прогрева) / шаг итерации
    growth_iterations := (total_duration_minutes - warmup_minutes) / step_minutes;  -- = 12

    N0 := load_test_rec.base_load_connections;
    Nmax := load_test_rec.max_load;
    iter := load_test_rec.pass_counter - 6;   -- номер итерации роста (1..growth_iterations)

    RAISE NOTICE 'N0=%', N0;
    RAISE NOTICE 'Nmax=%', Nmax;
    RAISE NOTICE 'iter (growth iteration number)=%', iter;
    RAISE NOTICE 'growth_iterations total=%', growth_iterations;

    IF iter < growth_iterations THEN
        -- Экспоненциальный рост: N0 * (Nmax/N0)^(iter / growth_iterations)
        sessions := N0 * power(Nmax::numeric / N0, iter::numeric / growth_iterations);
        result_load_connections := round(sessions);
        RAISE NOTICE 'sessions (calculated)=%', sessions;
        RAISE NOTICE 'result_load_connections (rounded)=%', result_load_connections;
    ELSE
        -- Достигнут максимум (либо последняя итерация, либо тест идёт дольше 3 часов)
        result_load_connections := Nmax;
    END IF;

    -- Сохраняем вычисленное значение в таблице проходов
    UPDATE load_test_pass
    SET load_connections = result_load_connections
    WHERE test_id = current_test_id AND pass_counter = load_test_rec.pass_counter;

    RAISE NOTICE 'load_test_rec.pass_counter=%', load_test_rec.pass_counter;
    RAISE NOTICE 'Final load_connections set to %', result_load_connections;

    RETURN result_load_connections;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION load_test_set_load IS 'Установить текущую нагрузку connections с учётом 3-часовой длительности и 10-минутных итераций';
--Установить текущую нагрузку connections
------------------------------------------------------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Текущее количество подключений для pgbench для заданного сценария
CREATE OR REPLACE FUNCTION load_test_get_load_by_scenario( current_scenario integer ) RETURNS integer AS $$
DECLARE 
  total_load integer ;
  result_load integer ;
  current_load_connections DOUBLE PRECISION ;
  current_weight real ; 
  current_test_id integer ;
BEGIN
  SELECT load_test_get_current_test_id()
  INTO current_test_id;
  
  SELECT 
	weight  
  INTO 
	current_weight
  FROM 
	testing_scenarios
  WHERE 
	test_id = current_test_id AND 
	id = current_scenario ;
	
  IF current_weight IS NULL 
  THEN 
	RAISE EXCEPTION 'Несуществующий сценарий --> %', current_scenario USING HINT = 'Проверьте ID сценария тестирования';
	return 10;
  END IF;
 

 SELECT load_test_get_load()
 INTO total_load ; 
 
 --ЕСЛИ нагрузка для сценария не меняется 
 IF current_weight < 0 
 THEN 
	result_load = ABS( current_weight );
	RETURN result_load; 
 END IF ;
 --ЕСЛИ нагрузка для сценария не меняется 
 
 current_load_connections = total_load::DOUBLE PRECISION * current_weight ;
 
 SELECT CEIL( current_load_connections )
 INTO result_load ;
  
RETURN result_load; 
END
$$ LANGUAGE plpgsql; 
COMMENT ON FUNCTION load_test_get_load_by_scenario IS 'Текущее количество подключений для pgbench для заданного сценария';
-- Текущее количество подключений для pgbench для заданного сценария
------------------------------------------------------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Установить quaryid для сценариев 
CREATE OR REPLACE FUNCTION load_test_set_scenario_queryid() RETURNS integer  AS $$
DECLARE 
 curr_scenario_queryid bigint ; 
 current_test_id integer; 
 testing_scenarios_id integer ;
 max_testing_scenarios_id integer ;
BEGIN
	
	SELECT load_test_get_current_test_id()
	INTO current_test_id ;

	SELECT MAX(id) 
	INTO max_testing_scenarios_id
	FROM testing_scenarios
	WHERE test_id = current_test_id ;

	
	FOR testing_scenarios_id IN 1..max_testing_scenarios_id		
	LOOP 
		SELECT 
			queryid
		INTO 
			curr_scenario_queryid
		FROM 
			pg_stat_statements
		WHERE 
			query like '%select scenario'||testing_scenarios_id||'%' ;

		UPDATE 	
			testing_scenarios
		SET
			queryid = curr_scenario_queryid
		WHERE 
			test_id = current_test_id AND 
			id = testing_scenarios_id AND 
			queryid = 0  OR queryid IS NULL ; -- костыль "OR queryid IS NULL"
	END LOOP;
	

 RETURN 0 ; 

END
$$ LANGUAGE plpgsql ;
COMMENT ON FUNCTION load_test_set_scenario_queryid IS 'Установить quaryid для сценариев ';
--Установить quaryid для сценариев 
------------------------------------------------------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Получить время начала теста 
CREATE OR REPLACE FUNCTION load_test_get_start_timestamp() RETURNS text AS $$
DECLARE 
 current_test_id bigint; 
 current_start_timestamp text ;
BEGIN
    SELECT load_test_get_current_test_id()
	INTO current_test_id;
	
	SELECT to_char( date_trunc('minute' , test_started ) , 'YYYY-MM-DD HH24:MI' )
	INTO current_start_timestamp
	FROM load_test
	WHERE test_id = current_test_id ; 
	
	return  current_start_timestamp;
END
$$ LANGUAGE plpgsql ;
COMMENT ON FUNCTION load_test_get_start_timestamp IS 'Получить время начала теста';
-- Получить время начала теста 
------------------------------------------------------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Получить время окончания теста 
CREATE OR REPLACE FUNCTION load_test_get_finish_timestamp() RETURNS text AS $$
DECLARE 
 current_test_id bigint; 
 current_finish_timestamp text ;
BEGIN
    SELECT load_test_get_current_test_id()
	INTO current_test_id;
	
	SELECT to_char( date_trunc('minute' , test_finished ) , 'YYYY-MM-DD HH24:MI' )
	INTO current_finish_timestamp
	FROM load_test
	WHERE test_id = current_test_id ; 
	
	return  current_finish_timestamp;
END
$$ LANGUAGE plpgsql ;
COMMENT ON FUNCTION load_test_get_finish_timestamp IS 'Получить время окончания теста';
-- Получить время окончания теста 
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Установить начальное  количество подключений для pgbench
CREATE OR REPLACE FUNCTION load_test_set_start_load( new_start_load integer ) RETURNS integer AS $$				
DECLARE 
  current_test_id bigint; 
BEGIN
    SELECT load_test_get_current_test_id()
	INTO current_test_id;

	UPDATE load_test
	SET base_load_connections = new_start_load
    WHERE test_id = current_test_id ;	
	
	return 0 ; 
END
$$ LANGUAGE plpgsql;	
COMMENT ON FUNCTION load_test_set_start_load IS 'Установить начальное  количество подключений для pgbench';
--Установить начальное  количество подключений для pgbench
------------------------------------------------------------------------------------------------------------------------------------------------------------------------


------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Установить максимальное  количество подключений для pgbench
CREATE OR REPLACE FUNCTION load_test_set_max_load( new_max_load integer ) RETURNS integer AS $$				
DECLARE 
  current_test_id bigint; 
BEGIN
    SELECT load_test_get_current_test_id()
	INTO current_test_id;

	UPDATE load_test
	SET max_load = new_max_load
    WHERE test_id = current_test_id ;	
	
	return 0 ; 
END
$$ LANGUAGE plpgsql;	
COMMENT ON FUNCTION load_test_set_max_load IS 'Установить максимальное  количество подключений для pgbench';
--Установить максимальное  количество подключений для pgbench
------------------------------------------------------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Если тест может быть остановлен 
CREATE OR REPLACE FUNCTION load_test_is_test_could_be_finished() RETURNS integer AS $$
DECLARE
 load_test_rec record ;
 current_test_id bigint ;  
 
 current_load integer ;
BEGIN
  SELECT load_test_get_current_test_id()
  INTO current_test_id;
  
  SELECT load_test_get_load()
  INTO current_load ;

  SELECT * 
  INTO load_test_rec
  FROM load_test 
  WHERE test_id = current_test_id;
	
  	
  IF current_load >= load_test_rec.max_load
  THEN 
	return 1 ;
  ELSE
	return 0 ; 	
  END IF ;
  
END
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION load_test_is_test_could_be_finished IS 'Если тест может быть остановлен';
--Если тест может быть остановлен 
------------------------------------------------------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Начать собирать данные для статистики для текущей фазы теста
CREATE OR REPLACE FUNCTION load_test_start_collect_data() RETURNS integer AS $$
DECLARE 
  current_test_id bigint;  
  current_pass_counter bigint ;   
BEGIN
	
	  
    SELECT load_test_get_current_test_id()
	INTO current_test_id;

	SELECT pass_counter
	INTO current_pass_counter
	FROM load_test
	WHERE test_id =  current_test_id;
	
	current_pass_counter = current_pass_counter + 1 ;
	PERFORM load_test_increment_pass_counter();

	UPDATE load_test
	SET test_started = CURRENT_TIMESTAMP
	WHERE test_id = current_test_id ; 


	INSERT INTO load_test_pass 
	( 
	  test_id , 
	  start_timestamp , 
	  pass_counter   --Номер прохода 
	)
	VALUES 
	( 
	  current_test_id ,  							--test_id bigint , 
	  date_trunc('minute'  , CURRENT_TIMESTAMP) , --start_timestamp , 
	  current_pass_counter  						--pass_counter  , --Номер прохода 
	);
	
	    SELECT load_test_get_current_test_id()
	INTO current_test_id;
	


 return 0  ; 		

END
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION load_test_start_collect_data IS 'Начать собирать данные для статистики для текущей фазы теста';
--Начать собирать данные для статистики для текущей фазы теста
----------------------------------------------------------------------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Завершить сбор данных для статистики для текущей фазы теста
CREATE OR REPLACE FUNCTION load_test_stop_collect_data() RETURNS integer AS $$
DECLARE 
  curr_id bigint;  
  current_test_id bigint ;  
  
  current_load integer ; 
BEGIN
	
    SELECT load_test_get_current_test_id()
	INTO current_test_id;
	
	SELECT load_test_get_current_test_pass_id()
	INTO curr_id ;
	
	UPDATE load_test_pass SET finish_timestamp = date_trunc('minute'  , CURRENT_TIMESTAMP) WHERE id = curr_id ;
	UPDATE load_test SET test_finished = date_trunc('minute'  , CURRENT_TIMESTAMP) WHERE test_id = current_test_id ;

	----------------------------------------------------------------------------------
	--ОБНОВИТЬ ПОКАЗАТЕЛИ ТЕКУЩЕГО ПРОХОДА - ЗАГРУЗКА	
	SELECT load_test_get_load()
	INTO  current_load ; 

	UPDATE 	load_test_pass 
	SET 	load_connections = current_load
   	WHERE id = curr_id ;
	--ОБНОВИТЬ ПОКАЗАТЕЛИ ТЕКУЩЕГО ПРОХОДА - ЗАГРУЗКА
	----------------------------------------------------------------------------------				

  return 0 ; 
END
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION load_test_stop_collect_data IS 'Завершить сбор данных для статистики для текущей фазы теста';
--Завершить сбор данных для статистики для текущей фазы теста
----------------------------------------------------------------------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
--ЕСЛИ идет первый час работы
CREATE OR REPLACE FUNCTION load_test_has_the_first_hour_passed() RETURNS integer  AS $$				
DECLARE 
  min_pass_start_time timestamptz ; 
  current_test_id bigint; 
  working_pass integer ;
BEGIN
    SELECT load_test_get_current_test_id()
	INTO current_test_id;

 	SELECT MIN( start_timestamp ) 
	INTO min_pass_start_time
	FROM load_test_pass
	WHERE test_id = current_test_id ;
	
	-- РАСЧЕТ ДОЛГИХ ЗНАЧЕНИЕ ПОСЛЕ ЧАСА 
	IF ( CURRENT_TIMESTAMP - min_pass_start_time ) >= interval '60 minute' 
	THEN   
		return 1 ; 
	ELSE 
		return 0 ; 
	END IF;


END
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION load_test_has_the_first_hour_passed IS 'ЕСЛИ идет первый час работы';	
--ЕСЛИ идет первый час работы
----------------------------------------------------------------------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
--УВЕЛИЧИТЬ СЧЕТЧИК ИТЕРАЦИЙ
CREATE OR REPLACE FUNCTION load_test_increment_pass_counter() RETURNS integer AS $$				
DECLARE 
 current_test_id bigint; 
BEGIN
    SELECT load_test_get_current_test_id()
	INTO current_test_id;
	
	UPDATE load_test SET pass_counter = pass_counter + 1 WHERE test_id = current_test_id; 
	
  return 0 ; 
END
$$ LANGUAGE plpgsql;		
COMMENT ON FUNCTION load_test_increment_pass_counter IS 'УВЕЛИЧИТЬ СЧЕТЧИК ИТЕРАЦИЙ';				
--УВЕЛИЧИТЬ СЧЕТЧИК ИТЕРАЦИЙ
----------------------------------------------------------------------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- УСТАНОВИТЬ ВЕС ДЛЯ ТЕСТОВОГО СЦЕНАРИЯ
CREATE OR REPLACE FUNCTION load_test_set_weight_for_scenario( current_scenario integer  , new_weight real ) RETURNS integer AS $$				
DECLARE 
 current_test_id bigint; 
BEGIN
    SELECT load_test_get_current_test_id()
	INTO current_test_id;
	
	INSERT INTO testing_scenarios ( id , weight , test_id , queryid ) VALUES ( current_scenario ,  new_weight , current_test_id , 0 );
	
  return 0 ; 
END
$$ LANGUAGE plpgsql;		
COMMENT ON FUNCTION load_test_set_weight_for_scenario IS 'УСТАНОВИТЬ ВЕС ДЛЯ ТЕСТОВОГО СЦЕНАРИЯ';				

-- УСТАНОВИТЬ ВЕС ДЛЯ ТЕСТОВОГО СЦЕНАРИЯ
----------------------------------------------------------------------------------------------------------------------------------------------------------------------

---------------------------------------------------------------------
-- ЗАФИКСИРОВАТЬ ПАРАМЕТРЫ vm
CREATE OR REPLACE FUNCTION save_dirty_background_ratio( new_dirty_background_ratio integer ) RETURNS integer AS $$				
DECLARE 
 current_test_id bigint; 
BEGIN
    SELECT load_test_get_current_test_id()
	INTO current_test_id;
	
	UPDATE load_test
	SET dirty_background_ratio = new_dirty_background_ratio
	WHERE test_id = current_test_id ; 
	
  return 0 ; 
END
$$ LANGUAGE plpgsql;		
COMMENT ON FUNCTION load_test_set_weight_for_scenario IS 'СОХРАНИТЬ ЗНАЧЕНИЕ ПАРАМЕТРА vm.dirty_background_ratio ';				


CREATE OR REPLACE FUNCTION save_dirty_ratio( new_dirty_ratio integer ) RETURNS integer AS $$				
DECLARE 
 current_test_id bigint; 
BEGIN
    SELECT load_test_get_current_test_id()
	INTO current_test_id;
	
	UPDATE load_test
	SET dirty_ratio = new_dirty_ratio
	WHERE test_id = current_test_id ; 
	
  return 0 ; 
END
$$ LANGUAGE plpgsql;		
COMMENT ON FUNCTION load_test_set_weight_for_scenario IS 'СОХРАНИТЬ ЗНАЧЕНИЕ ПАРАМЕТРА vm.dirty_ratio ';				

CREATE OR REPLACE FUNCTION save_dirty_background_bytes( new_dirty_background_bytes integer ) RETURNS integer AS $$				
DECLARE 
 current_test_id bigint; 
BEGIN
    SELECT load_test_get_current_test_id()
	INTO current_test_id;
	
	UPDATE load_test
	SET dirty_background_bytes = new_dirty_background_bytes
	WHERE test_id = current_test_id ; 
	
  return 0 ; 
END
$$ LANGUAGE plpgsql;		
COMMENT ON FUNCTION load_test_set_weight_for_scenario IS 'СОХРАНИТЬ ЗНАЧЕНИЕ ПАРАМЕТРА vm.dirty_background_bytes ';

CREATE OR REPLACE FUNCTION save_dirty_bytes( new_dirty_bytes integer ) RETURNS integer AS $$				
DECLARE 
 current_test_id bigint; 
BEGIN
    SELECT load_test_get_current_test_id()
	INTO current_test_id;
	
	UPDATE load_test
	SET dirty_bytes = new_dirty_bytes
	WHERE test_id = current_test_id ; 
	
  return 0 ; 
END
$$ LANGUAGE plpgsql;		
COMMENT ON FUNCTION load_test_set_weight_for_scenario IS 'СОХРАНИТЬ ЗНАЧЕНИЕ ПАРАМЕТРА vm.dirty_bytes ';

CREATE OR REPLACE FUNCTION save_dirty_expire_centisecs( new_dirty_expire_centisecs integer ) RETURNS integer AS $$				
DECLARE 
 current_test_id bigint; 
BEGIN
    SELECT load_test_get_current_test_id()
	INTO current_test_id;
	
	UPDATE load_test
	SET dirty_expire_centisecs = new_dirty_expire_centisecs
	WHERE test_id = current_test_id ; 
	
  return 0 ; 
END
$$ LANGUAGE plpgsql;		
COMMENT ON FUNCTION load_test_set_weight_for_scenario IS 'СОХРАНИТЬ ЗНАЧЕНИЕ ПАРАМЕТРА vm.dirty_expire_centisecs ';					

CREATE OR REPLACE FUNCTION save_dirty_writeback_centisecs( new_dirty_writeback_centisecs integer ) RETURNS integer AS $$				
DECLARE 
 current_test_id bigint; 
BEGIN
    SELECT load_test_get_current_test_id()
	INTO current_test_id;
	
	UPDATE load_test
	SET dirty_writeback_centisecs = new_dirty_writeback_centisecs
	WHERE test_id = current_test_id ; 
	
  return 0 ; 
END
$$ LANGUAGE plpgsql;		
COMMENT ON FUNCTION load_test_set_weight_for_scenario IS 'СОХРАНИТЬ ЗНАЧЕНИЕ ПАРАМЕТРА vm.dirty_writeback_centisecs ';	

CREATE OR REPLACE FUNCTION save_vfs_cache_pressure( new_vfs_cache_pressure integer ) RETURNS integer AS $$				
DECLARE 
 current_test_id bigint; 
BEGIN
    SELECT load_test_get_current_test_id()
	INTO current_test_id;
	
	UPDATE load_test
	SET vfs_cache_pressure = new_vfs_cache_pressure
	WHERE test_id = current_test_id ; 
	
  return 0 ; 
END
$$ LANGUAGE plpgsql;		
COMMENT ON FUNCTION load_test_set_weight_for_scenario IS 'СОХРАНИТЬ ЗНАЧЕНИЕ ПАРАМЕТРА vm.vfs_cache_pressure ';	

CREATE OR REPLACE FUNCTION save_swappiness( new_swappiness integer ) RETURNS integer AS $$				
DECLARE 
 current_test_id bigint; 
BEGIN
    SELECT load_test_get_current_test_id()
	INTO current_test_id;
	
	UPDATE load_test
	SET swappiness = new_swappiness
	WHERE test_id = current_test_id ; 
	
  return 0 ; 
END
$$ LANGUAGE plpgsql;		
COMMENT ON FUNCTION load_test_set_weight_for_scenario IS 'СОХРАНИТЬ ЗНАЧЕНИЕ ПАРАМЕТРА vm.swappiness ';	

-- ЗАФИКСИРОВАТЬ ПАРАМЕТРЫ vm
------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- get_vm_params() --получить список текущих параметров управления RAM 
CREATE OR REPLACE FUNCTION get_vm_params_list() RETURNS text[] AS $$				
DECLARE 
 current_test_id bigint; 
 vm_record record ; 
 result_str text[];
 line_count integer ;
BEGIN
	line_count = 1 ;
	
    SELECT load_test_get_current_test_id()
	INTO current_test_id;
	
	result_str[line_count] = 'ТЕКУЩИЕ НАСТРОЙКИ УПРАВЛЕНИЯ VM и RAM' ;
	line_count=line_count+1;

	SELECT 
		dirty_background_ratio ,
		dirty_ratio            ,
		dirty_background_bytes ,
		dirty_bytes            ,
		dirty_expire_centisecs ,
		dirty_writeback_centisecs,
		vfs_cache_pressure ,
		swappiness         
	INTO vm_record
	FROM load_test
	WHERE test_id = current_test_id ; 
	
	
	result_str[line_count] = '1.ПОРОГИ ДЛЯ ЗАПУСКА ФОНОВОЙ ЗАПИСИ НА ДИСК:'||'|';
	line_count=line_count+1;
	result_str[line_count] = 'vm.dirty_background_ratio '||'|';
	line_count=line_count+1;
	result_str[line_count] = 'процент от общей оперативной памяти,'||'|';
	line_count=line_count+1;
	result_str[line_count] = 'по достижении которого система начинает фоновую запись "грязных" данных на диск.'||' | '||vm_record.dirty_background_ratio ||'|';
	line_count=line_count+2;
	
	result_str[line_count] = 'vm.dirty_background_bytes '||'|';
	line_count=line_count+1;
	result_str[line_count] = 'абсолютный объем "грязных" данных в байтах '||'|';
	line_count=line_count+1;
	result_str[line_count] = 'для запуска фоновой записи (имеет приоритет над ratio).'||' | '||vm_record.dirty_background_bytes ||'|';
	line_count=line_count+3;
	

	result_str[line_count] = '2.ПОРОГИ ДЛЯ ПРИНУДИТЕЛЬНОЙ СИНХРОННОЙ ЗАПИСИ:'||'|';
	line_count=line_count+1;
	result_str[line_count] = 'vm.dirty_ratio '||'|';
	line_count=line_count+1;
	result_str[line_count] = 'процент памяти, по превышении которого процессы блокируются '||'|';
	line_count=line_count+1;
	result_str[line_count] = 'и вынуждены синхронно записывать данные на диск.'||' | '||vm_record.dirty_ratio ||'|';
	line_count=line_count+2;
	
	result_str[line_count] = 'vm.dirty_bytes '||'|';
	line_count=line_count+1;
	result_str[line_count] = 'абсолютный лимит "грязных" данных в байтах'||'|';
	line_count=line_count+1;
	result_str[line_count] = 'для блокировки процессов (имеет приоритет над ratio).'||' | '||vm_record.dirty_bytes ||'|'; 
	line_count=line_count+3;

	result_str[line_count] = '3.ТАЙМИНГИ ЗАПИСИ:'||'|';
	line_count=line_count+1;
	result_str[line_count] = 'vm.dirty_expire_centisecs '||'|';
	line_count=line_count+1;
	result_str[line_count] = 'время (в сотых долях секунды)'||'|';
	line_count=line_count+1;
	result_str[line_count] = 'по истечении которого "грязные" данные считаются устаревшими и подлежат записи.'||' | '||vm_record.dirty_expire_centisecs ||'|';
	line_count=line_count+2;
	
	result_str[line_count] = 'vm.dirty_writeback_centisecs '||'|';
	line_count=line_count+1;
	result_str[line_count] = 'частота (в сотых долях секунды), '||'|';
	line_count=line_count+1;
	result_str[line_count] = ' с которой фоновый процесс проверяет и записывает устаревшие данные.'||' | '||vm_record.dirty_writeback_centisecs ||'|';
	line_count=line_count+3;
	

	result_str[line_count] = '4.НАСТРОЙКИ УПРАВЛЕНИЯ ПАМЯТЬЮ:'||'|';
	line_count=line_count+1;
	result_str[line_count] = 'vm.vfs_cache_pressure'||'|';
	line_count=line_count+1;
	result_str[line_count] = 'тенденция ядра к освобождению памяти, '||'|';
	line_count=line_count+1;
	result_str[line_count] = ' занятой кэшем файловой системы (чем выше значение, тем агрессивнее).'||' | '||vm_record.vfs_cache_pressure ||'|';
	line_count=line_count+2;
	
	result_str[line_count] = 'vm.swappiness '||'|';
	line_count=line_count+1;
	result_str[line_count] = 'склонность системы к использованию подкачки на диск (swap)'||'|';
	line_count=line_count+1;
	result_str[line_count] = 'вместо освобождения кэша страниц в RAM (диапазон от 0 до 100).'||' | '||vm_record.swappiness ||'|';
	line_count=line_count+1;
	
	
  return result_str ; 
END
$$ LANGUAGE plpgsql;		
COMMENT ON FUNCTION get_vm_params_list IS 'получить список текущих параметров управления RAM ';	

-----------------------------------------------------------------------------------
-- load_test_tables.sql
--------------------------------------------------------------------------------
--Таблицы для анализа нагрузочного тестирования
-----------------------------------------------------------------------------------

-----------------------------------------------------------------------------------
--Нагрузочный тест 
DROP TABLE IF EXISTS load_test CASCADE;
CREATE TABLE load_test
(
  test_id SERIAL , 
  base_load_connections DOUBLE PRECISION DEFAULT 5, -- Базовое количество соединений pgbench
  max_load integer DEFAULT 100 , -- Максимальная нагрука  соединений pgbench
  test_started timestamp with time zone , 
  test_finished timestamp with time zone ,
  pass_counter integer DEFAULT 0 ,    -- Счетчик проходов теста  
  testdb_name text DEFAULT 'default', --Наименование тестовой БД
  ---------------------------------------------------------------
  -- НАСТРОЙКИ ОС ПО ВРЕМЯ ТЕСТА
 /*
 Пороги для запуска фоновой записи на диск:
	vm.dirty_background_ratio — процент от общей оперативной памяти, по достижении которого система начинает фоновую запись "грязных" данных на диск.
	vm.dirty_background_bytes — абсолютный объем "грязных" данных в байтах для запуска фоновой записи (имеет приоритет над ratio).

Пороги для принудительной синхронной записи:
	vm.dirty_ratio — процент памяти, по превышении которого процессы блокируются и вынуждены синхронно записывать данные на диск.
	vm.dirty_bytes — абсолютный лимит "грязных" данных в байтах для блокировки процессов (имеет приоритет над ratio).

Тайминги записи:
	vm.dirty_expire_centisecs — время (в сотых долях секунды), по истечении которого "грязные" данные считаются устаревшими и подлежат записи.
	vm.dirty_writeback_centisecs — частота (в сотых долях секунды), с которой фоновый процесс проверяет и записывает устаревшие данные.

Настройки управления памятью:
	vm.vfs_cache_pressure — тенденция ядра к освобождению памяти, занятой кэшем файловой системы (чем выше значение, тем агрессивнее).
	vm.swappiness — склонность системы к использованию подкачки на диск (swap) вместо освобождения кэша страниц в RAM (диапазон от 0 до 100).
 */
  dirty_background_ratio          integer ,
  dirty_ratio                     integer ,
  dirty_background_bytes          integer ,
  dirty_bytes                     integer ,
  dirty_expire_centisecs          integer ,
  dirty_writeback_centisecs       integer ,
  
  vfs_cache_pressure              integer ,
  swappiness                      integer 
  -- НАСТРОЙКИ ОС ПО ВРЕМЯ ТЕСТА
  ---------------------------------------------------------------
  
  
);
ALTER TABLE load_test ADD CONSTRAINT load_test_pk PRIMARY KEY (test_id);

COMMENT ON TABLE load_test IS 'Нагрузочный тест ';
COMMENT ON COLUMN load_test.base_load_connections IS 'Базовое количество соединений pgbench';
COMMENT ON COLUMN load_test.max_load IS 'Максимальная нагрука  соединений pgbench';
COMMENT ON COLUMN load_test.test_started IS 'Начало теста';
COMMENT ON COLUMN load_test.test_finished IS 'Окончание теста';
COMMENT ON COLUMN load_test.pass_counter IS 'Счетчик проходов теста ';
COMMENT ON COLUMN load_test.testdb_name IS 'Наименование тестовой БД';

COMMENT ON COLUMN load_test.dirty_background_ratio IS 'процент от общей оперативной памяти, по достижении которого система начинает фоновую запись "грязных" данных на диск';
COMMENT ON COLUMN load_test.dirty_ratio IS 'процент памяти, по превышении которого процессы блокируются и вынуждены синхронно записывать данные на диск';
COMMENT ON COLUMN load_test.dirty_background_bytes IS 'абсолютный объем "грязных" данных в байтах для запуска фоновой записи (имеет приоритет над ratio)';
COMMENT ON COLUMN load_test.dirty_bytes IS 'абсолютный лимит "грязных" данных в байтах для блокировки процессов (имеет приоритет над ratio)';
COMMENT ON COLUMN load_test.dirty_expire_centisecs IS 'время (в сотых долях секунды), по истечении которого "грязные" данные считаются устаревшими и подлежат записи';
COMMENT ON COLUMN load_test.dirty_writeback_centisecs IS 'частота (в сотых долях секунды), с которой фоновый процесс проверяет и записывает устаревшие данные';
COMMENT ON COLUMN load_test.vfs_cache_pressure IS 'тенденция ядра к освобождению памяти, занятой кэшем файловой системы (чем выше значение, тем агрессивнее)';
COMMENT ON COLUMN load_test.swappiness IS 'склонность системы к использованию подкачки на диск (swap) вместо освобождения кэша страниц в RAM (диапазон от 0 до 100)';

--Нагрузочный тест 
-----------------------------------------------------------------------------------

-----------------------------------------------------------------------------------
-- Итерация нагрузочного теста 
DROP TABLE IF EXISTS load_test_pass CASCADE;
CREATE TABLE load_test_pass
(
  id SERIAL , 
  test_id integer , --Тест
  pass_counter integer ,   --Номер прохода   
  start_timestamp timestamp with time zone , 
  finish_timestamp timestamp with time zone , 
  load_connections DOUBLE PRECISION -- количество соединений pgbench
);
ALTER TABLE load_test_pass ADD CONSTRAINT load_test_pass_pk PRIMARY KEY (id);
ALTER TABLE load_test_pass ADD CONSTRAINT load_test_pass_fk FOREIGN KEY (test_id) REFERENCES load_test ( test_id );
CREATE INDEX load_test_pass_start_timestamp_idx ON load_test_pass ( start_timestamp );
CREATE INDEX load_test_pass_finish_timestampidx ON load_test_pass ( finish_timestamp );

COMMENT ON TABLE load_test_pass IS 'Итерация нагрузочного теста ';
COMMENT ON COLUMN load_test_pass.test_id IS 'Идентификатор теста';
COMMENT ON COLUMN load_test_pass.pass_counter IS 'Номер итерации';
COMMENT ON COLUMN load_test_pass.start_timestamp IS 'Начало итерации';
COMMENT ON COLUMN load_test_pass.finish_timestamp IS 'Окончание итерации';
COMMENT ON COLUMN load_test_pass.load_connections IS 'Текущая нагрузка в ходе итерации';

-- Итерация нагрузочного теста 
-----------------------------------------------------------------------------------

-------------------------------------------------------------------------------------
--Тестовые сценарии
DROP TABLE IF EXISTS testing_scenarios ; 
CREATE TABLE testing_scenarios
(  
  id integer  ,
  weight real ,
  queryid bigint ,
  test_id integer 
);
ALTER TABLE testing_scenarios ADD CONSTRAINT testing_scenarios_pk PRIMARY KEY (id , test_id );
ALTER TABLE testing_scenarios ADD CONSTRAINT testing_scenarios_fk FOREIGN KEY (test_id) REFERENCES load_test ( test_id );

COMMENT ON TABLE testing_scenarios IS 'Тестовые сценарии';
COMMENT ON COLUMN testing_scenarios.id IS 'ID тестового сценария ';
COMMENT ON COLUMN testing_scenarios.weight IS 'Вес сценария ';
COMMENT ON COLUMN testing_scenarios.queryid IS 'SQL запрос сценария ';
COMMENT ON COLUMN testing_scenarios.test_id IS 'ID нагурзочного тестирования';
--Конфигурационные параметры
-------------------------------------------------------------------------------------



--------------------------------------------------------------------------------
-- report_queryid_stat.sql
--------------------------------------------------------------------------------
-- Статистика по отдельному SQL запросу
--
-- report_queryid_stat История выполения и ожиданий по отдельному SQL запросу
--
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--История выполения и ожиданий по отдельному SQL запросу
CREATE OR REPLACE FUNCTION report_queryid_stat(  current_queryid bigint  , current_wait_event_type text , start_timestamp text , finish_timestamp text  ) RETURNS text[] AS $$
DECLARE
 result_str text[] ;
 line_count integer ;
 min_timestamp timestamptz ; 
 max_timestamp timestamptz ; 
 current_timepoint timestamptz ; 
 statement_stat_median_rec record ;
 
 counter integer ; 
 min_max_rec record ;
 line_counter integer ; 
 
 min_max_pct_rec record ;
 
 
 waitings_calls  DOUBLE PRECISION;
 statement_stat_waitings_median_rec record ;
 
  wait_event_count_rec record  ;
  
  temp_wait_event_names_rec record ; 
  
  corr_value DOUBLE PRECISION;  
  
  waitings_calls_rec record ;
  
  curr_calls numeric;
  curr_waitings numeric;
  curr_wait_event_type  numeric;

  
  distinct_wait_event_rec record;
  wait_event_rec record;
BEGIN
	line_count = 1 ;

	IF finish_timestamp = 'CURRENT_TIMESTAMP'
	THEN 
		SELECT 	to_timestamp( start_timestamp , 'YYYY-MM-DD HH24:MI' ) 
		INTO 	max_timestamp ; 
	
		min_timestamp = max_timestamp - interval '1 hour'; 	
	ELSE
		SELECT 	to_timestamp( start_timestamp , 'YYYY-MM-DD HH24:MI' )
		INTO 	min_timestamp ; 
		
		SELECT 	to_timestamp( finish_timestamp , 'YYYY-MM-DD HH24:MI' ) 
		INTO 	max_timestamp ; 		
	END IF ;
	
	result_str[line_count] = 'ИСТОРИЯ ВЫПОЛНЕНИЯ И СОБЫТИЙ ОЖИДАНИЯ' ; 
	line_count=line_count+1; 
	result_str[line_count] = 'QUERYID'; 
	line_count=line_count+1; 		
	result_str[line_count] = current_queryid; 
	line_count=line_count+1; 		
	
	result_str[line_count] = 'WAIT_EVENT_TYPE' ; 
	line_count=line_count+1; 	
	result_str[line_count] = current_wait_event_type ; 
	line_count=line_count+1; 	
	
	
	result_str[line_count] = to_char( min_timestamp , 'YYYY-MM-DD HH24:MI' ) ; 
	line_count=line_count+1; 
	
	result_str[line_count] = to_char( max_timestamp , 'YYYY-MM-DD HH24:MI' ); 
	line_count=line_count+2; 
	
	result_str[line_count] = 	'timestamp'||'|'|| 
                                'dbname'||'|'||
								'username'||'|'||
								'calls'||'|'
								;		
	
	FOR distinct_wait_event_rec IN 
	SELECT
		DISTINCT wait_event
	FROM 
		statement_stat_waitings_median
	WHERE 
		queryid = current_queryid AND 
		wait_event_type = current_wait_event_type AND 
		curr_timestamp BETWEEN min_timestamp AND max_timestamp 
	ORDER BY 1
	LOOP
		result_str[line_count] = result_str[line_count] || 	distinct_wait_event_rec.wait_event ||'|' ;
	END LOOP ;
	
	line_count=line_count+1;
	
	
	
	current_timepoint = min_timestamp;
	WHILE current_timepoint <= max_timestamp
	LOOP
		result_str[line_count] = to_char(current_timepoint , 'YYYY-MM-DD HH24:MI')  ||'|';
		
		FOR statement_stat_median_rec IN 
		SELECT 
			dbname , 
			username ,
			SUM( calls_long  ) AS calls_long 	
		FROM 
			statement_stat_median
		WHERE 
			curr_timestamp = current_timepoint AND  queryid = current_queryid
		GROUP BY 
			dbname , username
		LOOP
			result_str[line_count] =	result_str[line_count] ||										
										statement_stat_median_rec.dbname   ||'|'||
										statement_stat_median_rec.username   ||'|'||
										REPLACE ( TO_CHAR( ROUND( statement_stat_median_rec.calls_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )||'|';
			FOR distinct_wait_event_rec IN 
			SELECT
				DISTINCT wait_event
			FROM 
				statement_stat_waitings_median
			WHERE 
				queryid = current_queryid AND 
				wait_event_type = current_wait_event_type AND 
				curr_timestamp BETWEEN min_timestamp AND max_timestamp  
			ORDER BY 1
			LOOP
				SELECT
					SUM(curr_value_long) AS wait_event_count 
				INTO 
					wait_event_rec
				FROM 
					statement_stat_waitings_median
				WHERE 
					curr_timestamp = current_timepoint AND 
					dbname = statement_stat_median_rec.dbname AND 
					username = statement_stat_median_rec.username AND 
					queryid = current_queryid AND 
					wait_event_type = current_wait_event_type AND 
					wait_event = distinct_wait_event_rec.wait_event 
				GROUP BY 
					curr_timestamp ,
					dbname ,
					username ,
					queryid ,
					wait_event_type
				; 
				
				IF wait_event_rec.wait_event_count IS NULL OR wait_event_rec.wait_event_count = 0 
				THEN 
					result_str[line_count] = result_str[line_count] || 	'0' ||'|' ;
				ELSE
					result_str[line_count] = result_str[line_count] || 	
											REPLACE ( TO_CHAR( ROUND( wait_event_rec.wait_event_count::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )||'|' ;
				END IF;
			END LOOP;
			
			line_count = line_count + 1 ;
		
		END LOOP ;			
		
		current_timepoint = current_timepoint + interval '1 minute';
	END LOOP ;
	
	
	
  return result_str ; 
END
$$ LANGUAGE plpgsql  ;
COMMENT ON FUNCTION report_queryid_stat IS 'История выполения и ожиданий по отдельному SQL запросу';
--История выполения и ожиданий по отдельному SQL запросу
--------------------------------------------------------------------------------
	
	

--------------------------------------------------------------------------------
-- report_iostat.sql
--------------------------------------------------------------------------------
--
-- report_iostat Данные для графиков по IOSTAT
--
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Данные для графиков по IOSTAT
CREATE OR REPLACE FUNCTION report_iostat(  start_timestamp text , finish_timestamp text  , device_name text  ) RETURNS text[] AS $$
DECLARE
 result_str text[] ;
 line_count integer ;
 min_timestamp timestamptz ; 
 max_timestamp timestamptz ; 
 
 counter integer ; 
 min_max_rec record ;
 line_counter integer ; 
   
  min_max_pct_rec record ;
 
  cluster_stat_median_rec record ; 	
  os_stat_iostat_device_median_rec record ; 
  
  
BEGIN
	line_count = 1 ;	
	
	IF finish_timestamp = 'CURRENT_TIMESTAMP'
	THEN 
		SELECT 	date_trunc('minute' ,  to_timestamp( start_timestamp , 'YYYY-MM-DD HH24:MI' ) ) 
		INTO 	max_timestamp ; 

		min_timestamp = max_timestamp - interval '1 hour'; 	
	ELSE
		SELECT 	date_trunc('minute' ,  to_timestamp( start_timestamp , 'YYYY-MM-DD HH24:MI' ) ) 
		INTO 	min_timestamp ; 
		
		SELECT 	date_trunc('minute' ,  to_timestamp( finish_timestamp , 'YYYY-MM-DD HH24:MI' ) ) 
		INTO 	max_timestamp ; 
	END IF ;


	
	
	result_str[line_count] = 'Данные для графиков IOSTAT' ; 
	line_count=line_count+1;
	
	
	result_str[line_count] = 'DEVICE = '||device_name ; 
	line_count=line_count+1;
	
		
	result_str[line_count] = to_char(min_timestamp , 'YYYY-MM-DD HH24:MI') ;
	line_count=line_count+1; 
	result_str[line_count] = to_char(max_timestamp , 'YYYY-MM-DD HH24:MI') ;
	line_count=line_count+2;  
	
	
		
	SELECT 
		MIN( cl.dev_rps_long) AS min_dev_rps_long , MAX( cl.dev_rps_long) AS max_dev_rps_long,
		MIN( cl.dev_rmbs_long) AS min_dev_rmbs_long , MAX( cl.dev_rmbs_long) AS max_dev_rmbs_long ,
		MIN( cl.dev_rrqmps_long) AS min_dev_rrqmps_long , MAX( cl.dev_rrqmps_long) AS max_dev_rrqmps_long ,
		MIN( cl.dev_rrqm_pct_long) AS min_dev_rrqm_pct_long , MAX( cl.dev_rrqm_pct_long) AS max_dev_rrqm_pct_long ,
		MIN( cl.dev_r_await_long) AS min_dev_r_await_long , MAX( cl.dev_r_await_long) AS max_dev_r_await_long ,
		MIN( cl.dev_rareq_sz_long) AS min_dev_rareq_sz_long , MAX( cl.dev_rareq_sz_long) AS max_dev_rareq_sz_long ,
		MIN( cl.dev_wps_long) AS min_dev_wps_long , MAX( cl.dev_wps_long) AS max_dev_wps_long ,
		MIN( cl.dev_wmbps_long) AS min_dev_wmbps_long , MAX( cl.dev_wmbps_long) AS max_dev_wmbps_long,
		MIN( cl.dev_wrqmps_long) AS min_dev_wrqmps_long , MAX( cl.dev_wrqmps_long) AS max_dev_wrqmps_long ,
		MIN( cl.dev_wrqm_pct_long) AS min_dev_wrqm_pct_long , MAX( cl.dev_wrqm_pct_long) AS max_dev_wrqm_pct_long,
		MIN( cl.dev_w_await_long) AS min_dev_w_await_long , MAX( cl.dev_w_await_long) AS max_dev_w_await_long,
		MIN( cl.dev_wareq_sz_long) AS min_dev_wareq_sz_long , MAX( cl.dev_wareq_sz_long) AS max_dev_wareq_sz_long,
		MIN( cl.dev_dps_long) AS min_dev_dps_long , MAX( cl.dev_dps_long) AS max_dev_dps_long,
		MIN( cl.dev_dmbps_long) AS min_dev_dmbps_long , MAX( cl.dev_dmbps_long) AS max_dev_dmbps_long ,
		MIN( cl.dev_drqmps_long) AS min_dev_drqmps_long, MAX( cl.dev_drqmps_long) AS max_dev_drqmps_long ,		
		MIN( cl.dev_drqm_pct_long) AS min_dev_drqm_pct_long , MAX( cl.dev_drqm_pct_long) AS max_dev_drqm_pct_long ,		
		MIN( cl.dev_d_await_long) AS min_dev_d_await_long , MAX( cl.dev_d_await_long) AS max_dev_d_await_long ,
		MIN( cl.dev_dareq_sz_long) AS min_dev_dareq_sz_long , MAX( cl.dev_dareq_sz_long) AS max_dev_dareq_sz_long ,
		MIN( cl.dev_aqu_sz_long) AS min_dev_aqu_sz_long , MAX( cl.dev_aqu_sz_long) AS max_dev_aqu_sz_long ,
		MIN( cl.dev_util_pct_long) AS min_dev_util_pct_long , MAX( cl.dev_util_pct_long) AS max_dev_util_pct_long ,
		MIN( cl.dev_fps_long) AS min_dev_fps_long , MAX( cl.dev_fps_long) AS max_dev_fps_long ,
		MIN( cl.dev_f_await_long) AS min_dev_f_await_long , MAX( cl.dev_f_await_long) AS max_dev_f_await_long		
	INTO  	min_max_rec
	FROM 
		os_stat_iostat_device_median cl 
	WHERE 	
		cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 	
		AND cl.device = device_name ;


	DROP TABLE IF EXISTS tmp_timepoints;
	CREATE TEMPORARY TABLE tmp_timepoints
	(
		curr_timestamp timestamptz  ,   
		curr_timepoint integer 
	);


	INSERT INTO tmp_timepoints
	(
		curr_timestamp ,	
		curr_timepoint 
	)
	SELECT 
		curr_timestamp , 
		row_number() over (order by curr_timestamp) AS x
	FROM
	os_stat_iostat_device_median
	WHERE 
		curr_timestamp BETWEEN min_timestamp AND max_timestamp  
	ORDER BY curr_timestamp	;	

	result_str[line_count] = 	'timestamp'||'|'||  --1
								'№'||'|'	
								;

	IF min_max_rec.max_dev_rps_long > 0 
	THEN 
		result_str[line_count] = result_str[line_count] ||
								'r/s' ||'|'--3
								;
	END IF;

								
	IF min_max_rec.max_dev_rmbs_long > 0 
	THEN 
		result_str[line_count] = result_str[line_count] ||
								'rMB/s' ||'|'--4
								;
	END IF;
	
	IF min_max_rec.max_dev_rrqmps_long > 0 
	THEN 
		result_str[line_count] = result_str[line_count] ||
								'rrqm/s' ||'|'--5
								;
	END IF;

							
	IF min_max_rec.max_dev_rrqm_pct_long > 0 
	THEN 
		result_str[line_count] = result_str[line_count] ||
								'%rrqm' ||'|'--6
								;
	END IF;	

	IF min_max_rec.max_dev_r_await_long > 0 
	THEN 
		result_str[line_count] = result_str[line_count] ||
								'r_await' ||'|'--7
								;
	END IF;				

	IF min_max_rec.max_dev_rareq_sz_long > 0 
	THEN 
		result_str[line_count] = result_str[line_count] ||
								'rareq_sz' ||'|'--8
								;
	END IF;	

	IF min_max_rec.max_dev_wps_long > 0 
	THEN 
		result_str[line_count] = result_str[line_count] ||
								'w/s' ||'|'--9
								;
	END IF;	

	IF min_max_rec.max_dev_wmbps_long > 0 
	THEN 
		result_str[line_count] = result_str[line_count] ||
								'wMB/s' ||'|'--10
								;
	END IF;					

	IF min_max_rec.max_dev_wrqmps_long > 0 
	THEN 
		result_str[line_count] = result_str[line_count] ||
								'wrqm/s' ||'|'--11
								;
	END IF;					

	IF min_max_rec.max_dev_wrqm_pct_long > 0 
	THEN 
		result_str[line_count] = result_str[line_count] ||
								'%wrqm' ||'|'--12
								;
	END IF;					

	IF min_max_rec.max_dev_w_await_long > 0 
	THEN 
		result_str[line_count] = result_str[line_count] ||
								'w_await' ||'|'--13
								;
	END IF;					

	IF min_max_rec.max_dev_wareq_sz_long > 0 
	THEN 
		result_str[line_count] = result_str[line_count] ||
								'wareq_sz' ||'|'--14
								;
	END IF;							

	IF min_max_rec.max_dev_dps_long > 0 
	THEN 
		result_str[line_count] = result_str[line_count] ||
								'd/s' ||'|'--15
								;
	END IF;							

	IF min_max_rec.max_dev_dmbps_long > 0 
	THEN 
		result_str[line_count] = result_str[line_count] ||
								'dMB/s' ||'|'--16
								;
	END IF;							

	IF min_max_rec.max_dev_drqmps_long > 0 
	THEN 
		result_str[line_count] = result_str[line_count] ||
								'drqm/s' ||'|'--17
								;
	END IF;							

    IF min_max_rec.max_dev_drqm_pct_long > 0 
	THEN 
		result_str[line_count] = result_str[line_count] ||
								'%drqm' ||'|'--18
								;
	END IF;	

    IF min_max_rec.max_dev_d_await_long > 0 
	THEN 
		result_str[line_count] = result_str[line_count] ||
								'd_await' ||'|'--19
								;
	END IF;	

    IF min_max_rec.max_dev_dareq_sz_long > 0 
	THEN 
		result_str[line_count] = result_str[line_count] ||
								'dareq_sz' ||'|'--20
								;
	END IF;	

    IF min_max_rec.max_dev_aqu_sz_long > 0 
	THEN 
		result_str[line_count] = result_str[line_count] ||
								'aqu_sz' ||'|'--21
								;
	END IF;	

    IF min_max_rec.max_dev_util_pct_long > 0 
	THEN 
		result_str[line_count] = result_str[line_count] ||
								'%util' ||'|'--22
								;
	END IF;	

    IF min_max_rec.max_dev_fps_long > 0 
	THEN 
		result_str[line_count] = result_str[line_count] ||
								'f/s' ||'|'--23
								;
	END IF;	

    IF min_max_rec.max_dev_f_await_long > 0 
	THEN 
		result_str[line_count] = result_str[line_count] ||
								'f_await' ||'|'--24	
								;
	END IF;	
	
	line_count=line_count+1; 
	
	counter = 0 ; 
	FOR os_stat_iostat_device_median_rec IN
	SELECT 
		cl.curr_timestamp , --1
		cl.dev_rps_long ,--3
		cl.dev_rmbs_long ,--4
		cl.dev_rrqmps_long ,--5
		cl.dev_rrqm_pct_long ,--6
		cl.dev_r_await_long ,--7
		cl.dev_rareq_sz_long ,--8
		cl.dev_wps_long ,--9
		cl.dev_wmbps_long ,--10
		cl.dev_wrqmps_long ,--11
		cl.dev_wrqm_pct_long ,--12
		cl.dev_w_await_long ,--13
		cl.dev_wareq_sz_long ,--14
		cl.dev_dps_long ,--15
		cl.dev_dmbps_long ,--16
		cl.dev_drqmps_long ,--17
		cl.dev_drqm_pct_long ,--18
		cl.dev_d_await_long ,--19
		cl.dev_dareq_sz_long ,--20
		cl.dev_aqu_sz_long ,--21
		cl.dev_util_pct_long ,--22
		cl.dev_fps_long ,--23
		cl.dev_f_await_long --24
	FROM 
		os_stat_iostat_device_median cl 
	WHERE 	
		cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 	
		AND cl.device = device_name
    ORDER BY cl.curr_timestamp 
	LOOP
		counter = counter + 1 ;
		result_str[line_count] =
								to_char( os_stat_iostat_device_median_rec.curr_timestamp , 'YYYY-MM-DD HH24:MI') ||'|'|| --1
								counter ||'|'
								;

		IF min_max_rec.max_dev_rps_long > 0 
		THEN 
			result_str[line_count] = result_str[line_count] ||
									REPLACE ( TO_CHAR( ROUND( os_stat_iostat_device_median_rec.dev_rps_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|' --3
									;
		END IF;

									
		IF min_max_rec.max_dev_rmbs_long > 0 
		THEN 
			result_str[line_count] = result_str[line_count] ||
									REPLACE ( TO_CHAR( ROUND( os_stat_iostat_device_median_rec.dev_rmbs_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|' --4
									;
		END IF;
		
		IF min_max_rec.max_dev_rrqmps_long > 0 
		THEN 
			result_str[line_count] = result_str[line_count] ||
									REPLACE ( TO_CHAR( ROUND( os_stat_iostat_device_median_rec.dev_rrqmps_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|' --5
									;
		END IF;

								
		IF min_max_rec.max_dev_rrqm_pct_long > 0 
		THEN 
			result_str[line_count] = result_str[line_count] ||
									REPLACE ( TO_CHAR( ROUND( os_stat_iostat_device_median_rec.dev_rrqm_pct_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|' --6
									;
		END IF;	

		IF min_max_rec.max_dev_r_await_long > 0 
		THEN 
			result_str[line_count] = result_str[line_count] ||
									REPLACE ( TO_CHAR( ROUND( os_stat_iostat_device_median_rec.dev_r_await_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|' --7
									;
		END IF;				

		IF min_max_rec.max_dev_rareq_sz_long > 0 
		THEN 
			result_str[line_count] = result_str[line_count] ||
									REPLACE ( TO_CHAR( ROUND( os_stat_iostat_device_median_rec.dev_rareq_sz_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|' --8
									;
		END IF;	

		IF min_max_rec.max_dev_wps_long > 0 
		THEN 
			result_str[line_count] = result_str[line_count] ||
									REPLACE ( TO_CHAR( ROUND( os_stat_iostat_device_median_rec.dev_wps_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|' --9
									;
		END IF;	

		IF min_max_rec.max_dev_wmbps_long > 0 
		THEN 
			result_str[line_count] = result_str[line_count] ||
									REPLACE ( TO_CHAR( ROUND( os_stat_iostat_device_median_rec.dev_wmbps_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|' --10
									;
		END IF;					

		IF min_max_rec.max_dev_wrqmps_long > 0 
		THEN 
			result_str[line_count] = result_str[line_count] ||
									REPLACE ( TO_CHAR( ROUND( os_stat_iostat_device_median_rec.dev_wrqmps_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|' --11
									;
		END IF;					

		IF min_max_rec.max_dev_wrqm_pct_long > 0 
		THEN 
			result_str[line_count] = result_str[line_count] ||
									REPLACE ( TO_CHAR( ROUND( os_stat_iostat_device_median_rec.dev_wrqm_pct_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|' --12
									;
		END IF;					

		IF min_max_rec.max_dev_w_await_long > 0 
		THEN 
			result_str[line_count] = result_str[line_count] ||
									REPLACE ( TO_CHAR( ROUND( os_stat_iostat_device_median_rec.dev_w_await_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|' --13
									;
		END IF;					

		IF min_max_rec.max_dev_wareq_sz_long > 0 
		THEN 
			result_str[line_count] = result_str[line_count] ||
									REPLACE ( TO_CHAR( ROUND( os_stat_iostat_device_median_rec.dev_wareq_sz_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|' --14
									;
		END IF;							

		IF min_max_rec.max_dev_dps_long > 0 
		THEN 
			result_str[line_count] = result_str[line_count] ||
									REPLACE ( TO_CHAR( ROUND( os_stat_iostat_device_median_rec.dev_dps_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|' --15
									;
		END IF;							

		IF min_max_rec.max_dev_dmbps_long > 0 
		THEN 
			result_str[line_count] = result_str[line_count] ||
									REPLACE ( TO_CHAR( ROUND( os_stat_iostat_device_median_rec.dev_dmbps_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|' --16
									;
		END IF;							

		IF min_max_rec.max_dev_drqmps_long > 0 
		THEN 
			result_str[line_count] = result_str[line_count] ||
									REPLACE ( TO_CHAR( ROUND( os_stat_iostat_device_median_rec.dev_drqmps_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|' --17
									;
		END IF;							

		IF min_max_rec.max_dev_drqm_pct_long > 0 
		THEN 
			result_str[line_count] = result_str[line_count] ||
									REPLACE ( TO_CHAR( ROUND( os_stat_iostat_device_median_rec.dev_drqm_pct_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|' --18
									;
		END IF;	

		IF min_max_rec.max_dev_d_await_long > 0 
		THEN 
			result_str[line_count] = result_str[line_count] ||
									REPLACE ( TO_CHAR( ROUND( os_stat_iostat_device_median_rec.dev_d_await_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|' --19
									;
		END IF;	

		IF min_max_rec.max_dev_dareq_sz_long > 0 
		THEN 
			result_str[line_count] = result_str[line_count] ||
									REPLACE ( TO_CHAR( ROUND( os_stat_iostat_device_median_rec.dev_dareq_sz_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|' --20
									;
		END IF;	

		IF min_max_rec.max_dev_aqu_sz_long > 0 
		THEN 
			result_str[line_count] = result_str[line_count] ||
									REPLACE ( TO_CHAR( ROUND( os_stat_iostat_device_median_rec.dev_aqu_sz_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|' --21
									;
		END IF;	

		IF min_max_rec.max_dev_util_pct_long > 0 
		THEN 
			result_str[line_count] = result_str[line_count] ||
									REPLACE ( TO_CHAR( ROUND( os_stat_iostat_device_median_rec.dev_util_pct_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|' --22
									;
		END IF;	

		IF min_max_rec.max_dev_fps_long > 0 
		THEN 
			result_str[line_count] = result_str[line_count] ||
									REPLACE ( TO_CHAR( ROUND( os_stat_iostat_device_median_rec.dev_fps_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|' --23
									;
		END IF;	

		IF min_max_rec.max_dev_f_await_long > 0 
		THEN 
			result_str[line_count] = result_str[line_count] ||
									REPLACE ( TO_CHAR( ROUND( os_stat_iostat_device_median_rec.dev_f_await_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|' --23
									;
		END IF;	
		
	  line_count=line_count+1; 			
	END LOOP;		

return result_str ; 	
END
$$ LANGUAGE plpgsql  ;
COMMENT ON FUNCTION report_iostat IS 'Данные для графиков по IOSTAT';
-- Данные для графиков по IOSTAT
-------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- report_load_test_loading.sql
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- График изменения нагрузки в ходе нагрузочного тестирования
--
-- report_load_test_loading() Отчет по нагрузочному тестированию
--
------------------------------------------------------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- График изменения нагрузки в ходе нагрузочного тестирования
CREATE OR REPLACE FUNCTION report_load_test_loading() RETURNS text[] AS $$
DECLARE
 result_str text[] ;
 line_count integer ;

 current_test_id bigint;
 min_timestamp timestamptz ; 
 max_timestamp timestamptz ; 
 cluster_stat_median_rec record ;
 counter integer;  
 
BEGIN
    line_count = 1 ;	
	
	
	SELECT load_test_get_current_test_id()
	INTO current_test_id; 
	
	SELECT 
		MIN(p.start_timestamp)
	INTO 
		min_timestamp
	FROM
		load_test_pass p 
	WHERE  
		p.test_id = current_test_id AND 
		p.pass_counter >= 6 ;

    SELECT 
		MAX(p.finish_timestamp)
	INTO
		max_timestamp
	FROM
		load_test_pass p 
	WHERE  
		p.test_id = current_test_id AND 
		p.pass_counter >= 6 ;

    result_str[line_count] = 'ИЗМЕНЕНИЕ НАГРУЗКИ В ХОДЕ НАГРУЗОЧНОГО ТЕСТИРОВАНИЯ ' ; 
	line_count=line_count+2; 
	result_str[line_count] = to_char(min_timestamp , 'YYYY-MM-DD HH24:MI') ;
	line_count=line_count+1; 
	result_str[line_count] = to_char(max_timestamp , 'YYYY-MM-DD HH24:MI') ;
	line_count=line_count+2; 
	
	result_str[line_count] = 	'timestamp'||'|'||
								'№'||'|'||								
								'LOAD'||'|'
								;							
	line_count=line_count+1; 


    counter = 1 ;
	FOR cluster_stat_median_rec IN
	SELECT 
		cl.curr_timestamp , 		
		(SELECT 
			MAX(load_connections) --КОСТЫЛЬ!!!
		 FROM 
			load_test_pass 
		 WHERE 
			test_id = current_test_id AND 
			cl.curr_timestamp BETWEEN start_timestamp AND finish_timestamp
        ) AS curr_load			
	FROM 
		cluster_stat_median cl
	WHERE 	
		cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 	
    ORDER BY cl.curr_timestamp 
	LOOP
		result_str[line_count] = 	to_char( cluster_stat_median_rec.curr_timestamp , 'YYYY-MM-DD HH24:MI') ||'|'||
									counter ||'|'||	
									REPLACE ( TO_CHAR( ROUND( (cluster_stat_median_rec.curr_load::numeric)::numeric , 0 ) , '000000000000D0000') , '.' , ',' ) ||'|';
		line_count=line_count+1;
		counter = counter + 1 ; 
	END LOOP ;
	
  return result_str ; 
END
$$ LANGUAGE plpgsql STABLE ;
COMMENT ON FUNCTION report_load_test_loading IS ' График изменения нагрузки в ходе нагрузочного тестирования';
-- График изменения нагрузки в ходе нагрузочного тестирования
------------------------------------------------------------------------------------------------------------------------------------------------------------------------



--------------------------------------------------------------------------------
-- report_postgresql_cluster_performance.sql
--------------------------------------------------------------------------------
-- report_postgresql_cluster_performance Данные для построения графиков по производительности и ожиданиям  СУБД
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Данные для построения графиков по производительности и ожиданиям  СУБД
CREATE OR REPLACE FUNCTION report_postgresql_cluster_performance(  cluster_performance_start_timestamp text , cluster_performance_finish_timestamp text   ) RETURNS text[] AS $$
DECLARE
 result_str text[] ;
 line_count integer ;
 min_timestamp timestamptz ; 
 max_timestamp timestamptz ; 
 
 counter integer ; 
 min_max_rec record ;
 
 cluster_stat_median_rec record ; 	
 
BEGIN
	line_count = 1 ;
	
	
	IF cluster_performance_finish_timestamp = 'CURRENT_TIMESTAMP'
	THEN 
		SELECT 	date_trunc('minute' ,  to_timestamp( cluster_performance_start_timestamp , 'YYYY-MM-DD HH24:MI' ) ) 
		INTO 	max_timestamp ; 

		min_timestamp = max_timestamp - interval '1 hour'; 	
	ELSE
		SELECT 	date_trunc('minute' ,  to_timestamp( cluster_performance_start_timestamp , 'YYYY-MM-DD HH24:MI' ) ) 
		INTO 	min_timestamp ; 
		
		SELECT 	date_trunc('minute' ,  to_timestamp( cluster_performance_finish_timestamp , 'YYYY-MM-DD HH24:MI' ) ) 
		INTO 	max_timestamp ; 
	END IF ;


	
	result_str[line_count] = 'ИСХОДНЫЕ ДАННЫЕ ПРОИЗВОДИТЕЛЬНОСТИ И ОЖИДАНИЙ СУБД' ; 
	line_count=line_count+1;
	
	result_str[line_count] = to_char(min_timestamp , 'YYYY-MM-DD HH24:MI') ;
	line_count=line_count+1; 
	result_str[line_count] = to_char(max_timestamp , 'YYYY-MM-DD HH24:MI') ;
	line_count=line_count+2; 
	
	DROP TABLE IF EXISTS tmp_timepoints;
	CREATE TEMPORARY TABLE tmp_timepoints
	(
		curr_timestamp timestamptz  ,   
		curr_timepoint integer 
	);


	INSERT INTO tmp_timepoints
	(
		curr_timestamp ,	
		curr_timepoint 
	)
	SELECT 
		curr_timestamp , 
		row_number() over (order by curr_timestamp) AS x
	FROM
	cluster_stat_median
	WHERE 
		curr_timestamp BETWEEN min_timestamp AND max_timestamp  
	ORDER BY curr_timestamp	;
		
	SELECT 
		MIN( cl.curr_op_speed) AS min_curr_op_speed , MAX( cl.curr_op_speed) AS max_curr_op_speed , 
		MIN( cl.curr_waitings) AS min_curr_waitings , MAX( cl.curr_waitings) AS max_curr_waitings , 
		MIN( cl.curr_bufferpin) AS min_curr_bufferpin , MAX( cl.curr_bufferpin) AS max_curr_bufferpin , 
		MIN( cl.curr_extension) AS min_curr_extension , MAX( cl.curr_extension) AS max_curr_extension , 
		MIN( cl.curr_io) AS min_curr_io , MAX( cl.curr_io) AS max_curr_io , 
		MIN( cl.curr_ipc) AS min_curr_ipc , MAX( cl.curr_ipc) AS max_curr_ipc , 
		MIN( cl.curr_lock) AS min_curr_lock , MAX( cl.curr_lock) AS max_curr_lock , 
		MIN( cl.curr_lwlock) AS min_curr_lwlock , MAX( cl.curr_lwlock) AS max_curr_lwlock ,
		MIN( cl.curr_timeout) AS min_curr_timeout , MAX( cl.curr_timeout) AS max_curr_timeout 
	INTO  	min_max_rec
	FROM 
		cluster_stat_median cl 
	WHERE 	
		cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 	;

	
	result_str[line_count] = 	'timestamp'||'|'||  --1
									'№'||'|'||--2	
									'SPEED '||'|'|| --3
									'WAITINGS' ||'|' --4
									;
							
								
	IF min_max_rec.max_curr_bufferpin > 0 
	THEN 
		result_str[line_count] = result_str[line_count] || 
								 'BUFFERPIN ' ||'|' ;
	END IF ;
	
	IF min_max_rec.max_curr_extension > 0 
	THEN 
		result_str[line_count] = result_str[line_count] || 
								 'EXTENSION ' ||'|' ;
	END IF ;
	
	IF min_max_rec.max_curr_io > 0 
	THEN 
		result_str[line_count] = result_str[line_count] || 
								 'IO ' ||'|' ;
	END IF ;
	
	IF min_max_rec.max_curr_ipc > 0 
	THEN 
		result_str[line_count] = result_str[line_count] || 
								 'IPC ' ||'|' ;
	END IF ;
	
	IF min_max_rec.max_curr_lock > 0 
	THEN 
		result_str[line_count] = result_str[line_count] || 
								 'LOCK ' ||'|' ;
	END IF ;
	
	IF min_max_rec.max_curr_lwlock > 0 
	THEN 
		result_str[line_count] = result_str[line_count] || 
								 'LWLOCK ' ||'|' ;
	END IF ;
	
	IF min_max_rec.max_curr_timeout > 0 
	THEN 
		result_str[line_count] = result_str[line_count] || 
								 'TIMEOUT ' ||'|' ;
	END IF ;
								
	line_count=line_count+1; 
	
	counter = 0 ; 
	FOR cluster_stat_median_rec IN
	SELECT 
		cl.curr_timestamp , --1
		cl.curr_op_speed AS curr_op_speed ,  --2
		cl.curr_waitings AS curr_waitings  ,--3
		cl.curr_bufferpin AS curr_bufferpin , --4
		cl.curr_extension AS curr_extension , --5
		cl.curr_io AS curr_io , --8
		cl.curr_ipc AS curr_ipc , --7
		cl.curr_lock AS curr_lock , --9
		cl.curr_lwlock AS curr_lwlock, 	 --9
		cl.curr_timeout AS curr_timeout 	 --10
	FROM 
		cluster_stat_median cl
	WHERE 	
		cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 	
    ORDER BY cl.curr_timestamp 
	LOOP
		counter = counter + 1 ;

		----------------------------------------------------------------------------------------------------
		result_str[line_count] = 	to_char( cluster_stat_median_rec.curr_timestamp , 'YYYY-MM-DD HH24:MI') ||'|'|| --1
									counter ||'|'||
									REPLACE ( TO_CHAR( ROUND( cluster_stat_median_rec.curr_op_speed::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|'|| --2
									REPLACE ( TO_CHAR( ROUND( cluster_stat_median_rec.curr_waitings::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|'  --3
									;
							
								
		IF min_max_rec.max_curr_bufferpin > 0 
		THEN 
			result_str[line_count] = result_str[line_count] || 
									 REPLACE ( TO_CHAR( ROUND( cluster_stat_median_rec.curr_bufferpin::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|';  --4 
		END IF ;
		
		IF min_max_rec.max_curr_extension > 0 
		THEN 
			result_str[line_count] = result_str[line_count] || 
									 REPLACE ( TO_CHAR( ROUND( cluster_stat_median_rec.curr_extension::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|';  --5 
		END IF ;
		
		IF min_max_rec.max_curr_io > 0 
		THEN 
			result_str[line_count] = result_str[line_count] || 
									 REPLACE ( TO_CHAR( ROUND( cluster_stat_median_rec.curr_io::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|';  --6 ;
		END IF ;
		
		IF min_max_rec.max_curr_ipc > 0 
		THEN 
			result_str[line_count] = result_str[line_count] || 
									 REPLACE ( TO_CHAR( ROUND( cluster_stat_median_rec.curr_ipc::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|'; --7;
		END IF ;
		
		IF min_max_rec.max_curr_lock > 0 
		THEN 
			result_str[line_count] = result_str[line_count] || 
									 REPLACE ( TO_CHAR( ROUND( cluster_stat_median_rec.curr_lock::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|';  --8;
		END IF ;
		
		IF min_max_rec.max_curr_lwlock > 0 
		THEN 
			result_str[line_count] = result_str[line_count] || 
									 REPLACE ( TO_CHAR( ROUND( cluster_stat_median_rec.curr_lwlock::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|'; --9
		END IF ;
		
		IF min_max_rec.max_curr_timeout > 0 
		THEN 
			result_str[line_count] = result_str[line_count] || 
									 REPLACE ( TO_CHAR( ROUND( cluster_stat_median_rec.curr_timeout::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|';  --10
		END IF ;	
		
	  line_count=line_count+1; 			
	END LOOP;

  return result_str ; 
END
$$ LANGUAGE plpgsql  ;
COMMENT ON FUNCTION report_postgresql_cluster_performance IS 'Данные для построения графиков по производительности и ожиданиям  СУБД';
-- Данные для построения графиков по производительности и ожиданиям  СУБД
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- report_postgresql_wait_event_type.sql
--------------------------------------------------------------------------------
-- report_postgresql_wait_event_type КОРРЕЛЯЦИЯ ОЖИДАНИЙ СУБД и vmstat
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Отчет по производительности и ожиданиям на уровне СУБД
CREATE OR REPLACE FUNCTION report_postgresql_wait_event_type(  cluster_performance_start_timestamp text , cluster_performance_finish_timestamp text   ) RETURNS text[] AS $$
DECLARE
 result_str text[] ;
 line_count integer ;
 min_timestamp timestamptz ; 
 max_timestamp timestamptz ; 
 
 counter integer ; 
 min_max_rec record ;
 line_counter integer ; 
 
 
  
  min_max_pct_rec record ;
 
  cluster_stat_median_rec record ; 	
  current_test_queryid bigint ;
  
  active_speed_correlation DOUBLE PRECISION;
  speed_waitings_correlation DOUBLE PRECISION;
  
  corr_bufferpin DOUBLE PRECISION ; 
  corr_extension DOUBLE PRECISION ; 
  corr_io DOUBLE PRECISION ; 
  corr_ipc DOUBLE PRECISION ; 
  corr_lock DOUBLE PRECISION ; 
  corr_lwlock DOUBLE PRECISION ; 
  corr_timeout DOUBLE PRECISION ; 
  	
  speed_regr_rec record ;
  waitings_regr_rec record ; 
    
  column_count integer ;
  stress_flag BOOLEAN; --TRUE - если отчет составляется по результатам НТ
  min_max_load_rec record ;
  
  current_load_rec record ; 
  
  --Взвешенная корреляция ожиданий (ВКО) 
  pct_wait_event_type numeric ; 
  score_wait_event_type numeric ; 
  score_txt text[];
  --Взвешенная корреляция ожиданий (ВКО) 
  
   correlation_rec record ;
   corr_values text[];
   least_squares_rec record ; 
   report_str text[];
   
   report_str_length integer ;
   
   wait_event_type_array text[7];
   i integer ;
   curr_integral_priority DOUBLE PRECISION;
   wait_event_type_criteria_matrix_rec record ; 
   wait_event_type_criteria_weight_rec record ;

   wait_event_type_Pi_rec record ; 
   ipw_result text ;
BEGIN
	line_count = 1 ;
	
	
	
	IF cluster_performance_finish_timestamp = 'CURRENT_TIMESTAMP'
	THEN 
		SELECT 	date_trunc('minute' ,  to_timestamp( cluster_performance_start_timestamp , 'YYYY-MM-DD HH24:MI' ) ) 
		INTO 	max_timestamp ; 

		min_timestamp = max_timestamp - interval '1 hour'; 	
	ELSE
		SELECT 	date_trunc('minute' ,  to_timestamp( cluster_performance_start_timestamp , 'YYYY-MM-DD HH24:MI' ) ) 
		INTO 	min_timestamp ; 
		
		SELECT 	date_trunc('minute' ,  to_timestamp( cluster_performance_finish_timestamp , 'YYYY-MM-DD HH24:MI' ) ) 
		INTO 	max_timestamp ; 
	END IF ;


	
	result_str[line_count] = '1. СТАТИСТИЧЕСКИЙ АНАЛИЗ ОЖИДАНИЙ СУБД' ; 
	line_count=line_count+1;
	
	TRUNCATE TABLE wait_event_type_criteria_matrix;	
	TRUNCATE TABLE wait_event_type_Pi ; 

	wait_event_type_array[1] = 'BufferPin';
	wait_event_type_array[2] = 'Extension';
	wait_event_type_array[3] = 'IO';
	wait_event_type_array[4] = 'IPC';
	wait_event_type_array[5] = 'Lock';
	wait_event_type_array[6] = 'LWLock';
	wait_event_type_array[7] = 'Timeout';

  	
	
	
	result_str[line_count] = to_char(min_timestamp , 'YYYY-MM-DD HH24:MI') ;
	line_count=line_count+1; 
	result_str[line_count] = to_char(max_timestamp , 'YYYY-MM-DD HH24:MI') ;
	line_count=line_count+1; 

	
	DROP TABLE IF EXISTS tmp_timepoints;
	CREATE TEMPORARY TABLE tmp_timepoints
	(
		curr_timestamp timestamptz  ,   
		curr_timepoint integer 
	);


	INSERT INTO tmp_timepoints
	(
		curr_timestamp ,	
		curr_timepoint 
	)
	SELECT 
		curr_timestamp , 
		row_number() over (order by curr_timestamp) AS x
	FROM
	cluster_stat_median
	WHERE 
		curr_timestamp BETWEEN min_timestamp AND max_timestamp  
	ORDER BY curr_timestamp	;
	
	
	----------------------------------------------------------------------------------------------------------------------------------------------	
	-- Граничные значения и медиана 
	SELECT 
		MIN( cl.curr_op_speed) AS min_curr_op_speed, MAX( cl.curr_op_speed) AS max_curr_op_speed, (percentile_cont(0.5) within group (order by cl.curr_op_speed))::numeric AS median_curr_op_speed , 
		MIN( cl.curr_waitings) AS min_curr_waitings, MAX( cl.curr_waitings) AS max_curr_waitings,  (percentile_cont(0.5) within group (order by cl.curr_waitings))::numeric AS median_curr_waitings , 
		MIN( cl.curr_bufferpin) AS min_curr_bufferpin, MAX( cl.curr_bufferpin) AS max_curr_bufferpin, (percentile_cont(0.5) within group (order by cl.curr_bufferpin))::numeric AS median_curr_bufferpin , 
		MIN( cl.curr_extension) AS min_curr_extension, MAX( cl.curr_extension) AS max_curr_extension, (percentile_cont(0.5) within group (order by cl.curr_extension))::numeric AS median_curr_extension , 
		MIN( cl.curr_io) AS min_curr_io, MAX( cl.curr_io) AS max_curr_io, (percentile_cont(0.5) within group (order by cl.curr_io))::numeric AS median_curr_io , 
		MIN( cl.curr_ipc) AS min_curr_ipc, MAX( cl.curr_ipc) AS max_curr_ipc, (percentile_cont(0.5) within group (order by cl.curr_ipc))::numeric AS median_curr_ipc , 
		MIN( cl.curr_lock) AS min_curr_lock, MAX( cl.curr_lock) AS max_curr_lock, (percentile_cont(0.5) within group (order by cl.curr_lock))::numeric AS median_curr_lock , 
		MIN( cl.curr_lwlock) AS min_curr_lwlock, MAX( cl.curr_lwlock) AS max_curr_lwlock, (percentile_cont(0.5) within group (order by cl.curr_lwlock))::numeric AS median_curr_lwlock , 
		MIN( cl.curr_timeout) AS min_curr_timeout, MAX( cl.curr_timeout) AS max_curr_timeout, (percentile_cont(0.5) within group (order by cl.curr_timeout))::numeric AS median_curr_timeout  
	INTO  	min_max_rec
	FROM 
		cluster_stat_median cl 
	WHERE 	
		cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 	;
	-- Граничные значения
	----------------------------------------------------------------------------------------------------		
    line_count=line_count+1; 
	result_str[line_count] = 	'ГРАНИЧНЫЕ ЗНАЧЕНИЯ И МЕДИАНА|'||
								'№'||'|'||		
								'SPEED '||'|'||
								'WAITINGS' ||'|'||
								'BUFFERPIN ' ||'|'||
								'EXTENSION ' ||'|'||
								'IO ' ||'|'||
								'IPC ' ||'|'||
								'LOCK ' ||'|'||
								'LWLOCK ' ||'|'||
								'TIMEOUT ' ||'|'
									;							
	line_count=line_count+1; 

	result_str[line_count] = 	'MIN'||'|'||
									1 ||'|'||
									REPLACE ( TO_CHAR( ROUND( min_max_rec.min_curr_op_speed::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
									REPLACE ( TO_CHAR( ROUND( min_max_rec.min_curr_waitings::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
									REPLACE ( TO_CHAR( ROUND( min_max_rec.min_curr_bufferpin::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
									REPLACE ( TO_CHAR( ROUND( min_max_rec.min_curr_extension::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
									REPLACE ( TO_CHAR( ROUND( min_max_rec.min_curr_io::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
									REPLACE ( TO_CHAR( ROUND( min_max_rec.min_curr_ipc::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
									REPLACE ( TO_CHAR( ROUND( min_max_rec.min_curr_lock::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
									REPLACE ( TO_CHAR( ROUND( min_max_rec.min_curr_lwlock::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
									REPLACE ( TO_CHAR( ROUND( min_max_rec.min_curr_timeout::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'
									;							
	line_count=line_count+1; 	

	result_str[line_count] = 	'MEDIAN'||'|'||
									' ' ||'|'||
									REPLACE ( TO_CHAR( ROUND( min_max_rec.median_curr_op_speed::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
									REPLACE ( TO_CHAR( ROUND( min_max_rec.median_curr_waitings::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
									REPLACE ( TO_CHAR( ROUND( min_max_rec.median_curr_bufferpin::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
									REPLACE ( TO_CHAR( ROUND( min_max_rec.median_curr_extension::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
									REPLACE ( TO_CHAR( ROUND( min_max_rec.median_curr_io::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
									REPLACE ( TO_CHAR( ROUND( min_max_rec.median_curr_ipc::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
									REPLACE ( TO_CHAR( ROUND( min_max_rec.median_curr_lock::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
									REPLACE ( TO_CHAR( ROUND( min_max_rec.median_curr_lwlock::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
									REPLACE ( TO_CHAR( ROUND( min_max_rec.median_curr_timeout::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'
									;							
	line_count=line_count+1; 	

	
	SELECT 
		count(curr_timestamp)
	INTO line_counter
	FROM 
		cluster_stat_median
	WHERE 	
		curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
	
			
	result_str[line_count] = 	'MAX'||'|'||
									line_counter||'|'||
									REPLACE ( TO_CHAR( ROUND( min_max_rec.max_curr_op_speed::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
									REPLACE ( TO_CHAR( ROUND( min_max_rec.max_curr_waitings::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
									REPLACE ( TO_CHAR( ROUND( min_max_rec.max_curr_bufferpin::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
									REPLACE ( TO_CHAR( ROUND( min_max_rec.max_curr_extension::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
									REPLACE ( TO_CHAR( ROUND( min_max_rec.max_curr_io::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
									REPLACE ( TO_CHAR( ROUND( min_max_rec.max_curr_ipc::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
									REPLACE ( TO_CHAR( ROUND( min_max_rec.max_curr_lock::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
									REPLACE ( TO_CHAR( ROUND( min_max_rec.max_curr_lwlock::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
									REPLACE ( TO_CHAR( ROUND( min_max_rec.max_curr_timeout::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'
									;		

-- Граничные значения
----------------------------------------------------------------------------------------------------------------------------------------------	

	line_count=line_count+2; 

	result_str[line_count] = '1.1 ЛИНИЯ ТРЕНДА ОПЕРАЦИОННОЙ СКОРОСТИ ' ; 
	line_count=line_count+1; 
	result_str[line_count] = 'ПО ЛИНИИ РЕГРЕССИИ вида : Y = a + bt ; ГДЕ t - точка наблюдения.  ' ; 
	line_count=line_count+1; 
	CALL truncate_time_series();
	INSERT INTO first_time_series ( curr_timestamp , curr_value	)
	SELECT cl.curr_timestamp , curr_op_speed 
	FROM   cluster_stat_median cl 
	WHERE	cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
			
	SELECT * INTO least_squares_rec FROM the_line_of_least_squares();	
	result_str[line_count] = 'Коэффициент детерминации R^2 ' ||'|'|| REPLACE ( TO_CHAR( ROUND( least_squares_rec.current_r_squared::numeric , 2 ) , '000000000000D0000') , '.' , ',' ) ;
	line_count=line_count+1; 	
	result_str[line_count] = 'угол наклона  ' ||'|'|| REPLACE ( TO_CHAR( ROUND( least_squares_rec.slope_angle_degrees::numeric , 2 ) , '000000000000D0000') , '.' , ',' ) ;
	line_count=line_count+1; 	
	SELECT interpretation_r2_coefficient( least_squares_rec.current_r_squared::numeric ) 
	INTO corr_values ; 
	result_str[line_count] = corr_values[1] ; 
	line_count=line_count+1;
	result_str[line_count] = corr_values[2] ; 
	line_count=line_count+1;
	result_str[line_count] = corr_values[3] ; 
	line_count=line_count+1;
	


	line_count=line_count+1; 
	result_str[line_count] = '1.2 ЛИНИЯ ТРЕНДА ОЖИДАНИЙ СУБД' ; 
	line_count=line_count+1; 
	result_str[line_count] = 'ПО ЛИНИИ РЕГРЕССИИ вида : Y = a + bt ; ГДЕ t - точка наблюдения.  ' ; 
	line_count=line_count+1; 
	CALL truncate_time_series();
	INSERT INTO first_time_series ( curr_timestamp , curr_value	)
	SELECT cl.curr_timestamp , curr_waitings 
	FROM   cluster_stat_median cl 
	WHERE	cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
			
	SELECT * INTO least_squares_rec FROM the_line_of_least_squares();		
	result_str[line_count] = 'Коэффициент детерминации R^2 ' ||'|'|| REPLACE ( TO_CHAR( ROUND( least_squares_rec.current_r_squared::numeric , 2 ) , '000000000000D0000') , '.' , ',' ) ;
	line_count=line_count+1; 	
	result_str[line_count] = 'угол наклона  ' ||'|'|| REPLACE ( TO_CHAR( ROUND( least_squares_rec.slope_angle_degrees::numeric , 2 ) , '000000000000D0000') , '.' , ',' ) ;
	line_count=line_count+1; 	
	SELECT interpretation_r2_coefficient( least_squares_rec.current_r_squared::numeric ) 
	INTO corr_values ; 
	result_str[line_count] = corr_values[1] ; 
	line_count=line_count+1;
	result_str[line_count] = corr_values[2] ; 
	line_count=line_count+1;
	result_str[line_count] = corr_values[3] ; 
	line_count=line_count+1;
	
	line_count=line_count+1; 
	result_str[line_count] = '1.3 РЕГРЕССИЯ ОПЕРАЦИОННОЙ СКОРОСТИ(Y) по ОЖИДАНИЯМ(X) СУБД' ; 
	line_count=line_count+1; 
	result_str[line_count] = 'ЛИНИЯ РЕГРЕССИИ вида : Y = a + bX ' ; 
	line_count=line_count+1; 
	CALL truncate_time_series();
	INSERT INTO first_time_series ( curr_timestamp , curr_value	)
	SELECT cl.curr_timestamp , curr_op_speed 
	FROM   cluster_stat_median cl 
	WHERE	cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
	
	INSERT INTO second_time_series ( curr_timestamp , curr_value	)
	SELECT cl.curr_timestamp , curr_waitings 
	FROM   cluster_stat_median cl 
	WHERE	cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
			
	SELECT * INTO least_squares_rec FROM Y_X_regression_line();	
	result_str[line_count] = 'Коэффициент детерминации R^2 ' ||'|'|| REPLACE ( TO_CHAR( ROUND( least_squares_rec.current_r_squared::numeric , 2 ) , '000000000000D0000') , '.' , ',' ) ;
	line_count=line_count+1; 
	IF least_squares_rec.current_r_squared >= 0.2 
	THEN 	
		result_str[line_count] = 'угол наклона  ' ||'|'|| REPLACE ( TO_CHAR( ROUND( least_squares_rec.slope_angle_degrees::numeric , 2 ) , '000000000000D0000') , '.' , ',' ) ;
		line_count=line_count+1; 	
		SELECT interpretation_r2_coefficient( least_squares_rec.current_r_squared::numeric ) 
		INTO corr_values ; 
		result_str[line_count] = corr_values[1] ; 
		line_count=line_count+1;
		result_str[line_count] = corr_values[2] ; 
		line_count=line_count+1;
		result_str[line_count] = corr_values[3] ; 
		line_count=line_count+1;
	ELSE	
		result_str[line_count] = 'Модель объясняет менее 20% (вплоть до 0%) вариации.'; 
		line_count=line_count+1;
	END IF;

	----------------------------------------------------------------------------------------------------
	--2. КОРРЕЛЯЦИЯ: ОПЕРАЦИОННАЯ СКОРОСТЬ - ОЖИДАНИЯ СУБД
	-- Быстрая проверка значимости корреляции
	CALL truncate_time_series();
	INSERT INTO first_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , curr_op_speed
	FROM  cluster_stat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
	
	INSERT INTO second_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , curr_waitings
	FROM  cluster_stat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;

	SELECT * INTO correlation_rec FROM quick_significance_check();
	
	line_count=line_count+1;			 
	result_str[line_count] = '2. КОРРЕЛЯЦИЯ: ОПЕРАЦИОННАЯ СКОРОСТЬ - ОЖИДАНИЯ СУБД';
	line_count=line_count+1;	

    SELECT fill_corr_values_for_negative_corr( correlation_rec ) 
	INTO corr_values ; 
	result_str[line_count] = corr_values[1] ; 
	line_count=line_count+1;
	IF corr_values[2] != ' ' THEN result_str[line_count] = corr_values[2] ; line_count=line_count+1; END IF ;
	IF corr_values[3] != ' ' THEN result_str[line_count] = corr_values[3] ; line_count=line_count+1; END IF ;	
	--2. КОРРЕЛЯЦИЯ: ОПЕРАЦИОННАЯ СКОРОСТЬ - ОЖИДАНИЯ СУБД
	----------------------------------------------------------------------------------------------------	
	

------------------------------------------------------------------------------------------------------------------------------------------------------------
	line_count=line_count+2; 	
	result_str[line_count] = '3. КОМПЛЕКСНЫЙ АНАЛИЗ ПО ТИПАМ ОЖИДАНИЙ(wait_event_type) ';
	line_count=line_count+1;	
	
	----------------------------------------------------------------------------------------------------
	--3.1 BufferPin
	CALL truncate_time_series();
	INSERT INTO first_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , curr_waitings
	FROM  cluster_stat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
	
	INSERT INTO second_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , curr_bufferpin
	FROM  cluster_stat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
	
	SELECT * INTO correlation_rec FROM quick_significance_check();

	pct_wait_event_type = 	((min_max_rec.max_curr_bufferpin + min_max_rec.min_curr_bufferpin)/2.0)/
							((min_max_rec.max_curr_waitings + min_max_rec.min_curr_waitings)/2.0)*100.0; 
	score_wait_event_type = ROUND((correlation_rec.correvation_value::numeric * pct_wait_event_type::numeric / 100.0)::numeric , 2) ;
	
	SELECT fill_in_comprehensive_analysis_wait_event_type( '3.1 BufferPin' , 'BufferPin' , score_wait_event_type )
	INTO report_str ; 

	SELECT result_str || report_str
	INTO result_str ; 
	SELECT array_append( result_str , ' ')
	INTO result_str ;
    SELECT array_length( result_str , 1 )
	INTO line_count;	
	
	--3.1 BufferPin'
	----------------------------------------------------------------------------------------------------
	
	----------------------------------------------------------------------------------------------------
	--3.2 Extension
	CALL truncate_time_series();
	INSERT INTO first_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , curr_waitings
	FROM  cluster_stat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
	
	INSERT INTO second_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , curr_extension
	FROM  cluster_stat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
	
	SELECT * INTO correlation_rec FROM quick_significance_check();
	
	pct_wait_event_type = 	((min_max_rec.max_curr_extension + min_max_rec.min_curr_extension)/2.0)/
							((min_max_rec.max_curr_waitings + min_max_rec.min_curr_waitings)/2.0)*100.0; 
	score_wait_event_type = ROUND((correlation_rec.correvation_value::numeric * pct_wait_event_type::numeric / 100.0)::numeric , 2) ;
	
	SELECT fill_in_comprehensive_analysis_wait_event_type( '3.2 Extension' , 'Extension' , score_wait_event_type )
	INTO report_str ; 
	
	SELECT result_str || report_str
	INTO result_str ; 
	SELECT array_append( result_str , ' ')
	INTO result_str ;
    SELECT array_length( result_str , 1 )
	INTO line_count;	
	
	--3.2 Extension'
	----------------------------------------------------------------------------------------------------	
	
	----------------------------------------------------------------------------------------------------
	--3.3 IO
	CALL truncate_time_series();
	INSERT INTO first_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , curr_waitings
	FROM  cluster_stat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
	
	INSERT INTO second_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , curr_io
	FROM  cluster_stat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
	
	SELECT * INTO correlation_rec FROM quick_significance_check();

	pct_wait_event_type = 	((min_max_rec.max_curr_io + min_max_rec.min_curr_io)/2.0)/
							((min_max_rec.max_curr_waitings + min_max_rec.min_curr_waitings)/2.0)*100.0; 
	score_wait_event_type = ROUND((correlation_rec.correvation_value::numeric * pct_wait_event_type::numeric / 100.0)::numeric , 2) ;
	
	SELECT fill_in_comprehensive_analysis_wait_event_type( '3.3 IO' , 'IO' , score_wait_event_type )
	INTO report_str ; 
	
	SELECT result_str || report_str
	INTO result_str ; 
	SELECT array_append( result_str , ' ')
	INTO result_str ;
    SELECT array_length( result_str , 1 )
	INTO line_count;	
	--3.3 IO
	----------------------------------------------------------------------------------------------------		

	----------------------------------------------------------------------------------------------------
	--3.4 IPC
	CALL truncate_time_series();
	INSERT INTO first_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , curr_waitings
	FROM  cluster_stat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
	
	INSERT INTO second_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , curr_ipc
	FROM  cluster_stat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
	
	SELECT * INTO correlation_rec FROM quick_significance_check();

	pct_wait_event_type = 	((min_max_rec.max_curr_ipc + min_max_rec.min_curr_ipc)/2.0)/
							((min_max_rec.max_curr_waitings + min_max_rec.min_curr_waitings)/2.0)*100.0; 
	score_wait_event_type = ROUND((correlation_rec.correvation_value::numeric * pct_wait_event_type::numeric / 100.0)::numeric , 2) ;
	
	SELECT fill_in_comprehensive_analysis_wait_event_type( '3.4 IPC' , 'IPC' , score_wait_event_type )
	INTO report_str ; 

	SELECT result_str || report_str
	INTO result_str ; 
	SELECT array_append( result_str , ' ')
	INTO result_str ;
    SELECT array_length( result_str , 1 )
	INTO line_count;	
	--3.4 IPC'
	----------------------------------------------------------------------------------------------------		
		
	----------------------------------------------------------------------------------------------------
	--3.5 Lock
	CALL truncate_time_series();
	INSERT INTO first_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , curr_waitings
	FROM  cluster_stat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
	
	INSERT INTO second_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , curr_lock
	FROM  cluster_stat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
	
	SELECT * INTO correlation_rec FROM quick_significance_check();

	pct_wait_event_type = 	((min_max_rec.max_curr_lock + min_max_rec.min_curr_lock)/2.0)/
							((min_max_rec.max_curr_waitings + min_max_rec.min_curr_waitings)/2.0)*100.0; 
	score_wait_event_type = ROUND((correlation_rec.correvation_value::numeric * pct_wait_event_type::numeric / 100.0)::numeric , 2) ;
	
	SELECT fill_in_comprehensive_analysis_wait_event_type( '3.5 Lock' , 'Lock' , score_wait_event_type )
	INTO report_str ; 
	
	SELECT result_str || report_str
	INTO result_str ; 
	SELECT array_append( result_str , ' ')
	INTO result_str ;
    SELECT array_length( result_str , 1 )
	INTO line_count;	
	
	--3.5 Lock
	----------------------------------------------------------------------------------------------------	
	
	----------------------------------------------------------------------------------------------------
	--3.6 LWLock
	CALL truncate_time_series();
	INSERT INTO first_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , curr_waitings
	FROM  cluster_stat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
	
	INSERT INTO second_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , curr_lwlock
	FROM  cluster_stat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
	
	SELECT * INTO correlation_rec FROM quick_significance_check();

	pct_wait_event_type = 	((min_max_rec.max_curr_lwlock + min_max_rec.min_curr_lwlock)/2.0)/
							((min_max_rec.max_curr_waitings + min_max_rec.min_curr_waitings)/2.0)*100.0; 
	score_wait_event_type = ROUND((correlation_rec.correvation_value::numeric * pct_wait_event_type::numeric / 100.0)::numeric , 2) ;
	
	SELECT fill_in_comprehensive_analysis_wait_event_type( '3.6 LWLock' , 'LWLock' , score_wait_event_type )
	INTO report_str ; 
	
	SELECT result_str || report_str
	INTO result_str ; 
	SELECT array_append( result_str , ' ')
	INTO result_str ;
    SELECT array_length( result_str , 1 )
	INTO line_count;	
	

	--3.6 LWLock'
	----------------------------------------------------------------------------------------------------		
	
	----------------------------------------------------------------------------------------------------
	--3.7 Timeout
	CALL truncate_time_series();
	INSERT INTO first_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , curr_waitings
	FROM  cluster_stat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
	
	INSERT INTO second_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , curr_timeout
	FROM  cluster_stat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
	
	SELECT * INTO correlation_rec FROM quick_significance_check();

	pct_wait_event_type = 	((min_max_rec.max_curr_timeout + min_max_rec.min_curr_timeout)/2.0)/
							((min_max_rec.max_curr_waitings + min_max_rec.min_curr_waitings)/2.0)*100.0; 
	score_wait_event_type = ROUND((correlation_rec.correvation_value::numeric * pct_wait_event_type::numeric / 100.0)::numeric , 2) ;
	
	SELECT fill_in_comprehensive_analysis_wait_event_type( '3.7 Timeout' , 'Timeout' , score_wait_event_type )
	INTO report_str ; 
	
	SELECT result_str || report_str
	INTO result_str ; 
	SELECT array_append( result_str , ' ')
	INTO result_str ;
    SELECT array_length( result_str , 1 )
	INTO line_count;	
	
	--3.7 Timeout
	----------------------------------------------------------------------------------------------------		
	
	----------------------------------------------------------------------------------------------------			
	--Интегральный приоритет типа ожидания	
	--Расчет нормализованных значений 
		CALL norm_wait_event_type_criteria_matrix();
	--Расчет нормализованных значений 
	
	--Для каждого типа ожидания i интегральный приоритет Pi рассчитывается как 
	--взвешенная сумма нормализованных значений четырёх показателей, 
	--предварительно приведённых к единой шкале и направлению «больше – лучше»:	


	SELECT * 
	INTO wait_event_type_criteria_weight_rec
	FROM wait_event_type_criteria_weight;
	
	FOR i IN 1..7
	LOOP 
		SELECT * 
		INTO wait_event_type_criteria_matrix_rec
		FROM wait_event_type_criteria_matrix
		WHERE wait_event_type = wait_event_type_array[i];
		
		curr_integral_priority = wait_event_type_criteria_weight_rec.curr_value[1] * wait_event_type_criteria_matrix_rec.calculated_r_norm --r Корреляция
									+
								 wait_event_type_criteria_weight_rec.curr_value[2] * wait_event_type_criteria_matrix_rec.calculated_p_norm --p-value
									+
								 wait_event_type_criteria_weight_rec.curr_value[3] * wait_event_type_criteria_matrix_rec.calculated_w_norm --ВКО (w)
									+
								 wait_event_type_criteria_weight_rec.curr_value[4] * wait_event_type_criteria_matrix_rec.calculated_r2_norm ; --ВКО (w)
								 
		INSERT INTO wait_event_type_Pi
		( wait_event_type , integral_priority ) 
		VALUES 
		( wait_event_type_criteria_matrix_rec.wait_event_type , curr_integral_priority );		
	END LOOP ;
	
	line_count=line_count+1;	
	result_str[line_count] = 'РЕЗУЛЬТАТ ОТЧЕТА: ИНТЕГРАЛЬНЫЙ ПРИОРИТЕТ ТИПА ОЖИДАНИЯ';	
	line_count=line_count+1;	
	result_str[line_count] = '№ | WAIT_EVENT_TYPE | ПРИОРИТЕТ ';
	line_count=line_count+1;			

	counter = 1;
	
	FOR wait_event_type_Pi_rec IN 
	SELECT * 
	FROM wait_event_type_Pi
	WHERE integral_priority > 0 
	ORDER BY integral_priority DESC 
	LOOP
		ipw_result = counter ||'|'||wait_event_type_Pi_rec.wait_event_type ||'|'|| REPLACE ( TO_CHAR( ROUND( wait_event_type_Pi_rec.integral_priority::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' );
		IF ipw_result IS NOT NULL 
		THEN 
			result_str[line_count] = ipw_result ; 
			line_count=line_count+1;			
			counter = counter + 1 ;
		END IF ;
	END LOOP ;
	
	--Интегральный приоритет типа ожидания	
	----------------------------------------------------------------------------------------------------			

  return result_str ; 
END
$$ LANGUAGE plpgsql  ;
COMMENT ON FUNCTION report_postgresql_wait_event_type IS 'КОРРЕЛЯЦИЯ ОЖИДАНИЙ СУБД и vmstat';
-- КОРРЕЛЯЦИЯ ОЖИДАНИЙ СУБД и vmstat
--------------------------------------------------------------------------------


--------------------------------------------------------------------------------
-- report_queryid_for_pareto.sql
-- changed 13/03/2026
--------------------------------------------------------------------------------
--
-- report_queryid_for_pareto Диаграмма Парето по queryid
--
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Сформировать диаграмму Парето по queryid
CREATE OR REPLACE FUNCTION report_queryid_for_pareto(  start_timestamp text , finish_timestamp text , with_db BOOLEAN DEFAULT FALSE  ) RETURNS text[] AS $$
DECLARE
 result_str text[] ;
 line_count integer ;
 min_timestamp timestamptz ; 
 max_timestamp timestamptz ;   
 
 wait_event_type_rec record ;
 wait_event_rec record ;
 
 total_wait_event_count bigint ;
 pct_for_80 numeric ;
 
 
 corr_bufferpin DOUBLE PRECISION ; 
 corr_extension DOUBLE PRECISION ; 
 corr_io DOUBLE PRECISION ; 
 corr_ipc DOUBLE PRECISION ; 
 corr_lock DOUBLE PRECISION ; 
 corr_lwlock DOUBLE PRECISION ; 
 corr_timeout DOUBLE PRECISION ; 
 
 wait_event_type_corr_rec  record ; 
  
 tmp_queryid_index bigint ; 
 wait_event_list text ;
 wait_event_list_rec record ;
 
 curr_calls numeric; 

 wait_event_type_counter bigint ; 
 wait_event_type_Pi_rec record ; 

BEGIN
	line_count = 1 ;
	
	result_str[line_count] = '5. ДИАГРАММА ПАРЕТО ПО QUERYID';	
	line_count=line_count+1;
	
	line_count=line_count+1;
	SELECT date_trunc( 'minute' , to_timestamp( start_timestamp , 'YYYY-MM-DD HH24:MI' ) )
	INTO    min_timestamp ; 
  
	SELECT date_trunc( 'minute' , to_timestamp( finish_timestamp , 'YYYY-MM-DD HH24:MI' ) )
	INTO    max_timestamp ; 
	
	result_str[line_count] = to_char(min_timestamp , 'YYYY-MM-DD HH24:MI') ;
	line_count=line_count+1; 
	result_str[line_count] = to_char(max_timestamp , 'YYYY-MM-DD HH24:MI') ;
	line_count=line_count+2; 
	
	result_str[line_count] =' QUERYID - идентификатор SQL выражения  |';
	line_count=line_count+1;
	result_str[line_count] =' CALLS - количество выполнений |';
	line_count=line_count+1;
	result_str[line_count] =' WAITINGS - Ожидания wait_event_type по данному queryid |';
	line_count=line_count+1;
	result_str[line_count] =' PCT - отношение ожиданий wait_event_type по данному queryid |';
	line_count=line_count+1;
	result_str[line_count] =' к общему количество ожиданий wait_event_type |';
	line_count=line_count+1;
	result_str[line_count] =' DBNAME ROLENAME - Наименование БД и Роли  |';
	line_count=line_count+1;
	result_str[line_count] =' WAIT_EVENT LIST - Список событий ожиданий  |';
	line_count=line_count+2;
	
	
	wait_event_type_counter = 1 ;
	FOR wait_event_type_Pi_rec IN 
	SELECT * 
	FROM wait_event_type_Pi
	WHERE integral_priority > 0 
	ORDER BY integral_priority DESC 	
	LOOP
		
		result_str[line_count] = wait_event_type_counter ||'. '||wait_event_type_Pi_rec.wait_event_type ||'. ИНТЕГРАЛЬНЫЙ ПРИОРИТЕТ ТИПА ОЖИДАНИЯ = |'|| REPLACE ( TO_CHAR( ROUND( wait_event_type_Pi_rec.integral_priority::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' );
		line_count=line_count+1;			
		wait_event_type_counter = wait_event_type_counter + 1 ;
		
		IF with_db 
		THEN 		
			result_str[line_count] =' QUERYID  '||'|'||	
									' CALLS '||'|' ||
									' WAITINGS '||'|' ||  --Всего ожидания wait_event_type по данному queryid
									' PCT '||'|' ||       --отношение ожиданий wait_event_type по данному queryid к общему количество ожиданий wait_event_type
									' DBNAME ROLENAME '||'|'||
									' WAIT_EVENT LIST '||'|'
									;	
			line_count=line_count+1;
		ELSE
			result_str[line_count] =' QUERYID  '||'|'||	
									' CALLS '||'|' ||
									' WAITINGS '||'|' ||  --Всего ожидания wait_event_type по данному queryid
									' PCT '||'|' ||       --отношение ожиданий wait_event_type по данному queryid к общему количество ожиданий wait_event_type
									' WAIT_EVENT LIST '||'|'
									;	
			line_count=line_count+1;		
		END IF ;
		
		pct_for_80 = 0;
		
		FOR wait_event_rec IN 
		SELECT 	
			queryid , dbname , username ,
			SUM(curr_value_long) AS count 
		FROM 	
			statement_stat_waitings_median
		WHERE 
			curr_timestamp  BETWEEN min_timestamp AND max_timestamp
			AND wait_event_type = wait_event_type_Pi_rec.wait_event_type 
		GROUP BY 
			queryid , dbname , username 
		ORDER BY 
			4 desc 
		LOOP	
			WITH report_wait_event_for_pareto AS
			(
			SELECT 					
				SUM(curr_value_long) AS counter 
			FROM 	
				statement_stat_waitings_median
			WHERE 
				curr_timestamp  BETWEEN min_timestamp AND max_timestamp
				AND wait_event_type = wait_event_type_Pi_rec.wait_event_type
			GROUP BY 				
				queryid , dbname , username 
			)
			SELECT SUM(counter) 
			INTO total_wait_event_count 
			FROM report_wait_event_for_pareto ; 
			
			IF pct_for_80 = 0 
			THEN 
				pct_for_80 = (wait_event_rec.count::numeric / total_wait_event_count::numeric *100.0)::numeric ; 
			ELSE
			    pct_for_80 = pct_for_80 + (wait_event_rec.count::numeric / total_wait_event_count::numeric *100.0)::numeric ; 
			END IF;
			
			SELECT 
				SUM(calls_long)
			INTO
				curr_calls
			FROM 
				statement_stat_median
			WHERE
				curr_timestamp  BETWEEN min_timestamp AND max_timestamp
				AND queryid = wait_event_rec.queryid  
				AND dbname = wait_event_rec.dbname ; 
				
			IF with_db 
			THEN 		
				result_str[line_count] =  wait_event_rec.queryid  ||'|'||
										  REPLACE ( TO_CHAR( ROUND( curr_calls::numeric , 0 ) , '000000000000D0000') , '.' , ',' ) ||'|'||
										  wait_event_rec.count  ||'|'||
										  REPLACE ( TO_CHAR( ROUND( (wait_event_rec.count::numeric / total_wait_event_count::numeric *100.0)::numeric , 2 ) , '000000000000D0000') , '.' , ',' ) ||'|'||
										  wait_event_rec.dbname||' '||wait_event_rec.username||'|'
										  ;
			ELSE
				result_str[line_count] =  wait_event_rec.queryid  ||'|'||
										  REPLACE ( TO_CHAR( ROUND( curr_calls::numeric , 0 ) , '000000000000D0000') , '.' , ',' ) ||'|'||
										  wait_event_rec.count  ||'|'||
										  REPLACE ( TO_CHAR( ROUND( (wait_event_rec.count::numeric / total_wait_event_count::numeric *100.0)::numeric , 2 ) , '000000000000D0000') , '.' , ',' ) ||'|'
										  ;
				
			END IF ;
									  
			FOR wait_event_list_rec IN 
			SELECT 
				DISTINCT wait_event
			FROM 
				statement_stat_waitings_median
			WHERE 
				curr_timestamp  BETWEEN min_timestamp AND max_timestamp
				AND wait_event_type = wait_event_type_Pi_rec.wait_event_type
				AND queryid = wait_event_rec.queryid 
			LOOP
				result_str[line_count] = result_str[line_count] || wait_event_list_rec.wait_event ||' ';
			END LOOP ;
			result_str[line_count] = result_str[line_count] ||'|';
 
			line_count=line_count+1; 
			
			IF pct_for_80 > 80.0 
			THEN 
				EXIT;
			END IF;
			
		END LOOP ;		
		--FOR wait_event_rec IN 
	

	END LOOP;
	
	
		
		
		

return result_str ; 
END
$$ LANGUAGE plpgsql  ;
COMMENT ON FUNCTION report_queryid_for_pareto IS 'Диаграмма Парето по queryid';
-- Диаграмма Парето по queryid
-------------------------------------------------------------------------------


	

--------------------------------------------------------------------------------
-- report_shared_buffers.sql
--------------------------------------------------------------------------------
--
-- report_shared_buffers Статистика shared_buffers
--
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Статистика shared_buffers
CREATE OR REPLACE FUNCTION report_shared_buffers( start_timestamp text , finish_timestamp text   ) RETURNS text[] AS $$
DECLARE
 result_str text[] ;
 line_count integer ;
 min_timestamp timestamptz ; 
 max_timestamp timestamptz ; 
 
 counter integer ; 
 line_counter integer ; 

 shared_buffers_rec record;

BEGIN
	line_count = 1 ;
	
	
	
	
	IF finish_timestamp = 'CURRENT_TIMESTAMP'
	THEN 
		SELECT 	date_trunc('minute' ,  to_timestamp( start_timestamp , 'YYYY-MM-DD HH24:MI' ) ) 
		INTO 	max_timestamp ; 

		min_timestamp = max_timestamp - interval '1 hour'; 	
	ELSE
		SELECT 	date_trunc('minute' ,  to_timestamp( start_timestamp , 'YYYY-MM-DD HH24:MI' ) ) 
		INTO 	min_timestamp ; 
		
		SELECT 	date_trunc('minute' ,  to_timestamp( finish_timestamp , 'YYYY-MM-DD HH24:MI' ) ) 
		INTO 	max_timestamp ; 
	END IF ;


	
	result_str[line_count] = 'СТАТИСТИКА shared_buffers' ; 
	line_count=line_count+1;
		
	
	result_str[line_count] = to_char(min_timestamp , 'YYYY-MM-DD HH24:MI') ;
	line_count=line_count+1; 
	result_str[line_count] = to_char(max_timestamp , 'YYYY-MM-DD HH24:MI') ;
	line_count=line_count+2; 
	
	result_str[line_count] = 	'timestamp'||'|'||  --1
								'№'||'|'	--2
								'shared_blk_rw_time(s)'||'|' --3
								'shared_blks_hit'||'|' --4
								'shared_blks_read'||'|' --5
								'shared_blks_dirtied'||'|' --6
								'shared_blks_written'||'|' --7												
								;
	line_count = line_count + 1;
	counter = 0 ; 
	FOR shared_buffers_rec IN
	SELECT 
		cls.curr_timestamp , --1
		(cls.curr_shared_blk_read_time+cls.curr_shared_blk_write_time)/1000.0 AS shared_blks_read_write_time , --3
		cls.curr_shared_blks_hit AS shared_blks_hit ,--4
		cls.curr_shared_blks_read AS shared_blks_read ,--5
		cls.curr_shared_blks_dirtied AS shared_blks_dirtied ,--6
		cls.curr_shared_blks_written AS shared_blks_written --7		
	FROM cluster_stat_median cls 
	WHERE 	
		cls.curr_timestamp BETWEEN min_timestamp AND max_timestamp 	
    ORDER BY cls.curr_timestamp 
	LOOP
		counter = counter + 1 ;
		result_str[line_count] =
								to_char( shared_buffers_rec.curr_timestamp , 'YYYY-MM-DD HH24:MI') ||'|'|| --1
								counter ||'|'||  --2
								REPLACE ( TO_CHAR( ROUND( shared_buffers_rec.shared_blks_read_write_time::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|'||  --3
								REPLACE ( TO_CHAR( ROUND( shared_buffers_rec.shared_blks_hit::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|'||  --4
								REPLACE ( TO_CHAR( ROUND( shared_buffers_rec.shared_blks_read::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|'||  --5
								REPLACE ( TO_CHAR( ROUND( shared_buffers_rec.shared_blks_dirtied::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|'||  --6
								REPLACE ( TO_CHAR( ROUND( shared_buffers_rec.shared_blks_written::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|' --7
								;
		
		line_count=line_count+1; 
	END LOOP;


  return result_str ; 
END
$$ LANGUAGE plpgsql  ;
COMMENT ON FUNCTION report_shared_buffers IS 'Статистика shared_buffers';
-- Чек-лист IO
-------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- report_sql_list.sql
--------------------------------------------------------------------------------
--
-- report_sql_list Список SQL выражений за период
--
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Список SQL выражений за период
CREATE OR REPLACE FUNCTION report_sql_list(  start_timestamp text , finish_timestamp text ) RETURNS text[] AS $$
DECLARE
 result_str text[] ;
 line_count integer ;
 min_timestamp timestamptz ; 
 max_timestamp timestamptz ;   
 sql_rec record ; 
 
BEGIN
	line_count = 1 ;
	
	result_str[line_count] = 'СПИСОК SQL ВЫРАЖЕНИЙ';	
	line_count=line_count+1;
	
	line_count=line_count+1;
	SELECT date_trunc( 'minute' , to_timestamp( start_timestamp , 'YYYY-MM-DD HH24:MI' ) )
	INTO    min_timestamp ; 
  
	SELECT date_trunc( 'minute' , to_timestamp( finish_timestamp , 'YYYY-MM-DD HH24:MI' ) )
	INTO    max_timestamp ; 
	
	result_str[line_count] = to_char(min_timestamp , 'YYYY-MM-DD HH24:MI') ;
	line_count=line_count+1; 
	result_str[line_count] = to_char(max_timestamp , 'YYYY-MM-DD HH24:MI') ;
	line_count=line_count+2; 
	
	result_str[line_count] = 'QUERYID | SQL TEXT |';	
	line_count=line_count+1;	
	
	FOR sql_rec IN 
	WITH ssm AS
	(
		SELECT 
			queryid
		FROM
			statement_stat_median
		WHERE 
			curr_timestamp BETWEEN min_timestamp AND max_timestamp
		GROUP BY 
			queryid			
	)
	SELECT 
		sss.* 
	FROM 
		statement_stat_sql sss
		JOIN  ssm ON ( sss.queryid = ssm.queryid )
	LOOP
		result_str[line_count] = sql_rec.queryid||'|'|| sql_rec.query;	
	    line_count=line_count+1;
	END LOOP ;	
	
	
return result_str ; 
END
$$ LANGUAGE plpgsql  ;
COMMENT ON FUNCTION report_queryid_for_pareto IS 'Список SQL выражений за период';
-- Список SQL выражений за период
-------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- report_vm_dirty.sql
--------------------------------------------------------------------------------
--
-- report_vm_dirty Статистика dirty_ratio/dirty_background_ratio
--
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Чек-лист IO
CREATE OR REPLACE FUNCTION report_vm_dirty( ram_all integer , start_timestamp text , finish_timestamp text   ) RETURNS text[] AS $$
DECLARE
 result_str text[] ;
 line_count integer ;
 min_timestamp timestamptz ; 
 max_timestamp timestamptz ; 
 
 counter integer ; 
 line_counter integer ; 

 vm_dirty_rec record;

BEGIN
	line_count = 1 ;
	
	
	
	
	IF finish_timestamp = 'CURRENT_TIMESTAMP'
	THEN 
		SELECT 	date_trunc('minute' ,  to_timestamp( start_timestamp , 'YYYY-MM-DD HH24:MI' ) ) 
		INTO 	max_timestamp ; 

		min_timestamp = max_timestamp - interval '1 hour'; 	
	ELSE
		SELECT 	date_trunc('minute' ,  to_timestamp( start_timestamp , 'YYYY-MM-DD HH24:MI' ) ) 
		INTO 	min_timestamp ; 
		
		SELECT 	date_trunc('minute' ,  to_timestamp( finish_timestamp , 'YYYY-MM-DD HH24:MI' ) ) 
		INTO 	max_timestamp ; 
	END IF ;


	
	result_str[line_count] = 'Статистика dirty_ratio/dirty_background_ratio' ; 
	line_count=line_count+1;
	
	result_str[line_count] = 'RAM (MB)| '||ram_all||'|';
	line_count=line_count+1;		
	
	result_str[line_count] = to_char(min_timestamp , 'YYYY-MM-DD HH24:MI') ;
	line_count=line_count+1; 
	result_str[line_count] = to_char(max_timestamp , 'YYYY-MM-DD HH24:MI') ;
	line_count=line_count+2; 
	
	result_str[line_count] = 	'timestamp'||'|'||  --1
								'№'||'|'	--2
								'dirty (KB)'||'|' --3
								'% от dirty_ratio'||'|' --4
								'% от dirty_background_ratio'||'|' --5
								'free + cached memory'||'|' --6
								;
	line_count = line_count + 1;
	counter = 0 ; 
	FOR vm_dirty_rec IN
	SELECT 
		curr_timestamp , --1
		dirty_kb_long ,  --3
		dirty_percent_long , --4
		dirty_bg_percent_long , --5
		available_mem_mb_long  --6		
	FROM os_stat_vmstat_median cls 
	WHERE 	
		curr_timestamp BETWEEN min_timestamp AND max_timestamp 	
    ORDER BY cls.curr_timestamp 
	LOOP
		counter = counter + 1 ;
		result_str[line_count] =
								to_char( vm_dirty_rec.curr_timestamp , 'YYYY-MM-DD HH24:MI') ||'|'|| --1
								counter ||'|'||  --2
								REPLACE ( TO_CHAR( ROUND( vm_dirty_rec.dirty_kb_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|'||  --3
								REPLACE ( TO_CHAR( ROUND( vm_dirty_rec.dirty_percent_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|'||  --4
								REPLACE ( TO_CHAR( ROUND( vm_dirty_rec.dirty_bg_percent_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|'||  --5
								REPLACE ( TO_CHAR( ROUND( vm_dirty_rec.available_mem_mb_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|'  --6								
								;
		
		line_count=line_count+1; 
	END LOOP;


  return result_str ; 
END
$$ LANGUAGE plpgsql  ;
COMMENT ON FUNCTION report_vm_dirty IS 'Статистика dirty_ratio/dirty_background_ratio';
-- Чек-лист IO
-------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- report_vmstat.sql
--------------------------------------------------------------------------------
--
-- report_vmstat Данные для графиков по VMSTAT
--
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Данные для графиков по VMSTAT
CREATE OR REPLACE FUNCTION report_vmstat(  start_timestamp text , finish_timestamp text   ) RETURNS text[] AS $$
DECLARE
 result_str text[] ;
 line_count integer ;
 min_timestamp timestamptz ; 
 max_timestamp timestamptz ; 
 
 counter integer ; 
 min_max_rec record ;
 line_counter integer ; 
   
  min_max_pct_rec record ;
 
  cluster_stat_median_rec record ; 	
  os_stat_vmstat_median_rec record ; 
  
BEGIN
	line_count = 1 ;
	
	
	
	
	IF finish_timestamp = 'CURRENT_TIMESTAMP'
	THEN 
		SELECT 	date_trunc('minute' ,  to_timestamp( start_timestamp , 'YYYY-MM-DD HH24:MI' ) ) 
		INTO 	max_timestamp ; 

		min_timestamp = max_timestamp - interval '1 hour'; 	
	ELSE
		SELECT 	date_trunc('minute' ,  to_timestamp( start_timestamp , 'YYYY-MM-DD HH24:MI' ) ) 
		INTO 	min_timestamp ; 
		
		SELECT 	date_trunc('minute' ,  to_timestamp( finish_timestamp , 'YYYY-MM-DD HH24:MI' ) ) 
		INTO 	max_timestamp ; 
	END IF ;


	
	result_str[line_count] = 'Данные для графиков по VMSTAT' ; 
	line_count=line_count+1;
	
	
	result_str[line_count] = to_char(min_timestamp , 'YYYY-MM-DD HH24:MI') ;
	line_count=line_count+1; 
	result_str[line_count] = to_char(max_timestamp , 'YYYY-MM-DD HH24:MI') ;
	line_count=line_count+2; 
	
	
	
	DROP TABLE IF EXISTS tmp_timepoints;
	CREATE TEMPORARY TABLE tmp_timepoints
	(
		curr_timestamp timestamptz  ,   
		curr_timepoint integer 
	);


	INSERT INTO tmp_timepoints
	(
		curr_timestamp ,	
		curr_timepoint 
	)
	SELECT 
		curr_timestamp , 
		row_number() over (order by curr_timestamp) AS x
	FROM
	os_stat_vmstat_median cl
	WHERE 
		cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp  
	ORDER BY curr_timestamp	;

	SELECT 
		MIN( cl.procs_r_long) AS min_procs_r_long , MAX( cl.procs_r_long) AS max_procs_r_long , 
		MIN( cl.procs_b_long) AS min_procs_b_long , MAX( cl.procs_b_long) AS max_procs_b_long , 		
		MIN( cl.memory_swpd_long) AS min_memory_swpd_long , MAX( cl.memory_swpd_long) AS max_memory_swpd_long , 
		MIN( cl.memory_free_long) AS min_memory_free_long, MAX( cl.memory_free_long) AS max_memory_free_long , 
		MIN( cl.memory_buff_long) AS min_memory_buff_long , MAX( cl.memory_buff_long) AS max_memory_buff_long , 
		MIN( cl.memory_cache_long) AS min_memory_cache_long , MAX( cl.memory_cache_long) AS max_memory_cache_long , 
		MIN( cl.swap_si_long) AS min_swap_si_long , MAX( cl.swap_si_long) AS max_swap_si_long , 
		MIN( cl.swap_so_long) AS min_swap_so_long , MAX( cl.swap_so_long) AS max_swap_so_long ,
		MIN( cl.io_bi_long) AS min_io_bi_long , MAX( cl.io_bi_long) AS max_io_bi_long ,
		MIN( cl.io_bo_long) AS min_io_bo_long , MAX( cl.io_bo_long) AS max_io_bo_long ,
		MIN( cl.system_in_long) AS min_system_in_long , MAX( cl.system_in_long) AS max_system_in_long ,
		MIN( cl.system_cs_long) AS min_system_cs_long , MAX( cl.system_cs_long) AS max_system_cs_long ,
		MIN( cl.cpu_us_long) AS min_cpu_us_long , MAX( cl.cpu_us_long) AS max_cpu_us_long ,
		MIN( cl.cpu_sy_long) AS min_cpu_sy_long , MAX( cl.cpu_sy_long) AS max_cpu_sy_long ,
		MIN( cl.cpu_id_long) AS min_cpu_id_long , MAX( cl.cpu_id_long) AS max_cpu_id_long ,
		MIN( cl.cpu_wa_long) AS min_cpu_wa_long , MAX( cl.cpu_wa_long) AS max_cpu_wa_long ,
		MIN( cl.cpu_st_long) AS min_cpu_st_long , MAX( cl.cpu_st_long) AS max_cpu_st_long 
	INTO  	min_max_rec
	FROM 
		os_stat_vmstat_median cl 
	WHERE 	
		cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 	;
		

							
	result_str[line_count] = 	'timestamp'||'|'||  --1
								'№'||'|'
								;
	IF min_max_rec.max_procs_r_long > 0 
	THEN 
		result_str[line_count] = result_str[line_count] ||
								'procs_r' ||'|'; --4
	END IF;
	
	IF min_max_rec.max_procs_b_long > 0 
	THEN 
		result_str[line_count] = result_str[line_count] ||
								'procs_b' ||'|'; --5
	END IF;
	
	IF min_max_rec.max_memory_swpd_long > 0 
	THEN 
		result_str[line_count] = result_str[line_count] ||
								'memory_swpd' ||'|'; --6
	END IF;

	IF min_max_rec.max_memory_free_long > 0 
	THEN 
		result_str[line_count] = result_str[line_count] ||
								'memory_free' ||'|'; --7
	END IF;

	IF min_max_rec.max_memory_buff_long > 0 
	THEN 
		result_str[line_count] = result_str[line_count] ||
								'memory_buff' ||'|'; --8
	END IF;
	
	IF min_max_rec.max_memory_cache_long > 0 
	THEN 
		result_str[line_count] = result_str[line_count] ||
								'memory_cache' ||'|'; --9
	END IF;
	
	IF min_max_rec.max_swap_si_long > 0 
	THEN 
		result_str[line_count] = result_str[line_count] ||
								'swap_si' ||'|'; --10
	END IF;
	
	IF min_max_rec.max_swap_so_long > 0 
	THEN 
		result_str[line_count] = result_str[line_count] ||
								'swap_so' ||'|'; --11
	END IF;
	
	IF min_max_rec.max_io_bi_long > 0 
	THEN 
		result_str[line_count] = result_str[line_count] ||
								'io_bi' ||'|'; --12
	END IF;
	
	IF min_max_rec.max_io_bo_long > 0 
	THEN 
		result_str[line_count] = result_str[line_count] ||
								'io_bo' ||'|'; --13
	END IF;
	
	IF min_max_rec.max_system_in_long > 0 
	THEN 
		result_str[line_count] = result_str[line_count] ||
								'system_in' ||'|'; --14
	END IF;
	
	IF min_max_rec.max_system_cs_long > 0 
	THEN 
		result_str[line_count] = result_str[line_count] ||
								'system_cs' ||'|'; --15
	END IF;
	
	IF min_max_rec.max_cpu_us_long > 0 
	THEN 
		result_str[line_count] = result_str[line_count] ||
								'cpu_us' ||'|'; --16
	END IF;
	
	IF min_max_rec.max_cpu_sy_long > 0 
	THEN 
		result_str[line_count] = result_str[line_count] ||
								'cpu_sy' ||'|'; --17
	END IF;
	
	IF min_max_rec.max_cpu_id_long > 0 
	THEN 
		result_str[line_count] = result_str[line_count] ||
								'cpu_id' ||'|'; --18
	END IF;
	
	IF min_max_rec.max_cpu_wa_long > 0 
	THEN 
		result_str[line_count] = result_str[line_count] ||
								'cpu_wa' ||'|'; --19
	END IF;
	
	IF min_max_rec.max_cpu_st_long > 0 
	THEN 
		result_str[line_count] = result_str[line_count] ||
								'cpu_st' ||'|'; --20
	END IF;
								

	line_count=line_count+1; 
	
	counter = 0 ; 
	FOR os_stat_vmstat_median_rec IN
	SELECT 
		cl.curr_timestamp , --1
		cl.procs_r_long ,--4
		cl.procs_b_long ,--5
		cl.memory_swpd_long ,--6
		cl.memory_free_long ,--7
		cl.memory_buff_long ,--8
		cl.memory_cache_long ,--9
		cl.swap_si_long ,--10
		cl.swap_so_long ,--11
		cl.io_bi_long ,--12
		cl.io_bo_long ,--13
		cl.system_in_long ,--14
		cl.system_cs_long ,--15
		cl.cpu_us_long ,--16
		cl.cpu_sy_long ,--17
		cl.cpu_id_long ,--18
		cl.cpu_wa_long ,--19
		cl.cpu_st_long--20
	FROM 
		os_stat_vmstat_median cl 
	WHERE 	
		cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 	
    ORDER BY cl.curr_timestamp 
	LOOP
		counter = counter + 1 ;
		
		result_str[line_count] =
								to_char( os_stat_vmstat_median_rec.curr_timestamp , 'YYYY-MM-DD HH24:MI') ||'|'|| --1
								counter ||'|'
								;
								
		IF min_max_rec.max_procs_r_long > 0 
		THEN 
			result_str[line_count] = result_str[line_count] ||
								REPLACE ( TO_CHAR( ROUND( os_stat_vmstat_median_rec.procs_r_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|' --4
								;
		END IF;
		
		IF min_max_rec.max_procs_b_long > 0 
		THEN 
			result_str[line_count] = result_str[line_count] ||
								REPLACE ( TO_CHAR( ROUND( os_stat_vmstat_median_rec.procs_b_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|' --5
								;
		END IF;
		
		IF min_max_rec.max_memory_swpd_long > 0 
		THEN 
			result_str[line_count] = result_str[line_count] ||
								REPLACE ( TO_CHAR( ROUND( os_stat_vmstat_median_rec.memory_swpd_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|' --6
								;
		END IF;
		
		IF min_max_rec.max_memory_free_long > 0 
		THEN 
			result_str[line_count] = result_str[line_count] ||
								REPLACE ( TO_CHAR( ROUND( os_stat_vmstat_median_rec.memory_free_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|' --7
								;
		END IF;
		
		IF min_max_rec.max_memory_buff_long > 0 
		THEN 
			result_str[line_count] = result_str[line_count] ||
								REPLACE ( TO_CHAR( ROUND( os_stat_vmstat_median_rec.memory_buff_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|' --8
								;
		END IF;
		
		IF min_max_rec.max_memory_cache_long > 0 
		THEN 
			result_str[line_count] = result_str[line_count] ||
								REPLACE ( TO_CHAR( ROUND( os_stat_vmstat_median_rec.memory_cache_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|' --9
								;
		END IF;
		
		IF min_max_rec.max_swap_si_long > 0 
		THEN 
			result_str[line_count] = result_str[line_count] ||
								REPLACE ( TO_CHAR( ROUND( os_stat_vmstat_median_rec.swap_si_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|' --10
								;
		END IF;
		
		IF min_max_rec.max_swap_so_long > 0 
		THEN 
			result_str[line_count] = result_str[line_count] ||
								REPLACE ( TO_CHAR( ROUND( os_stat_vmstat_median_rec.swap_so_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|' --11
								;
		END IF;
		
		IF min_max_rec.max_io_bi_long > 0 
		THEN 
			result_str[line_count] = result_str[line_count] ||
								REPLACE ( TO_CHAR( ROUND( os_stat_vmstat_median_rec.io_bi_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|' --12
								;
		END IF;
		
		IF min_max_rec.max_io_bo_long > 0 
		THEN 
			result_str[line_count] = result_str[line_count] ||
								REPLACE ( TO_CHAR( ROUND( os_stat_vmstat_median_rec.io_bo_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|' --13
								;
		END IF;
		
		IF min_max_rec.max_system_in_long > 0 
		THEN 
			result_str[line_count] = result_str[line_count] ||
								REPLACE ( TO_CHAR( ROUND( os_stat_vmstat_median_rec.system_in_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|' --14
								;
		END IF;
		
		IF min_max_rec.max_system_cs_long > 0 
		THEN 
			result_str[line_count] = result_str[line_count] ||
								REPLACE ( TO_CHAR( ROUND( os_stat_vmstat_median_rec.system_cs_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|' --15
								;
		END IF;
		
		IF min_max_rec.max_cpu_us_long > 0 
		THEN 
			result_str[line_count] = result_str[line_count] ||
								REPLACE ( TO_CHAR( ROUND( os_stat_vmstat_median_rec.cpu_us_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|' --16
								;
		END IF;
		
		IF min_max_rec.max_cpu_sy_long > 0 
		THEN 
			result_str[line_count] = result_str[line_count] ||
								REPLACE ( TO_CHAR( ROUND( os_stat_vmstat_median_rec.cpu_sy_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|' --17
								;
		END IF;
		
		IF min_max_rec.max_cpu_id_long > 0 
		THEN 
			result_str[line_count] = result_str[line_count] ||
								REPLACE ( TO_CHAR( ROUND( os_stat_vmstat_median_rec.cpu_id_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|' --18
								;
		END IF;
		
		IF min_max_rec.max_cpu_wa_long > 0 
		THEN 
			result_str[line_count] = result_str[line_count] ||
								REPLACE ( TO_CHAR( ROUND( os_stat_vmstat_median_rec.cpu_wa_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|' --19
								;
		END IF;
		
		IF min_max_rec.max_cpu_st_long > 0 
		THEN 
			result_str[line_count] = result_str[line_count] ||
								REPLACE ( TO_CHAR( ROUND( os_stat_vmstat_median_rec.cpu_st_long::numeric , 0 ) , '000000000000D0000' ) , '.' , ',' )  ||'|' --20	
								;
		END IF;
		


								
	  line_count=line_count+1; 			
	END LOOP;
	

  return result_str ; 
END
$$ LANGUAGE plpgsql  ;
COMMENT ON FUNCTION report_vmstat IS 'Данные для графиков по VMSTAT';
-- Данные для графиков по VMSTAT
-------------------------------------------------------------------------------


--------------------------------------------------------------------------------
-- report_vmstat_iostat.sql
--------------------------------------------------------------------------------
--
-- report_vmstat_iostat Корреляция метрик vmstat и iopstat
--
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Корреляция метрик vmstat и iopstat
CREATE OR REPLACE FUNCTION report_vmstat_iostat( start_timestamp text , finish_timestamp text , device_name text  ) RETURNS text[] AS $$
DECLARE
 result_str text[] ;
 line_count integer ;
 min_timestamp timestamptz ; 
 max_timestamp timestamptz ; 
 
 counter integer ; 
 min_max_rec record ;
 line_counter integer ; 
 
  min_max_pct_rec record ;
  
  corr_wa_util DOUBLE PRECISION ; -- Корреляция 
  
  corr_buff_rps DOUBLE PRECISION ; -- Корреляция 
  corr_buff_wps DOUBLE PRECISION ; -- Корреляция 
  corr_buff_rmbs DOUBLE PRECISION ; -- Корреляция 
  corr_buff_wmbs DOUBLE PRECISION ; -- Корреляция 
  
  corr_cache_rps DOUBLE PRECISION ; -- Корреляция 
  corr_cache_wps DOUBLE PRECISION ; -- Корреляция 
  corr_cache_rmbs DOUBLE PRECISION ; -- Корреляция 
  corr_cache_wmbs DOUBLE PRECISION ; -- Корреляция 
  
  
  
  os_stat_vmstat_iostat_rec record ; 
  
  util_pct DOUBLE PRECISION ; 
  r_await_pct DOUBLE PRECISION ; 
  w_await_pct DOUBLE PRECISION ; 
  aqu_sz_pct  DOUBLE PRECISION ; 
  
  
  subpart_counter integer ; 
  part integer ; 
  correlation_rec record; 
  reason_casulas_list text[];
  report_str text[];
  
  speed_iops_corr DOUBLE PRECISION ;
  speed_mbps_corr DOUBLE PRECISION ; 
  delta_corr DOUBLE PRECISION ; 

	cpi_matrix_rec record ; 
 
  
BEGIN
	line_count = 1 ;
	
	
	IF finish_timestamp = 'CURRENT_TIMESTAMP'
	THEN 
		SELECT 	date_trunc('minute' ,  to_timestamp( start_timestamp , 'YYYY-MM-DD HH24:MI' ) ) 
		INTO 	max_timestamp ; 

		min_timestamp = max_timestamp - interval '1 hour'; 	
	ELSE
		SELECT 	date_trunc('minute' ,  to_timestamp( start_timestamp , 'YYYY-MM-DD HH24:MI' ) ) 
		INTO 	min_timestamp ; 
		
		SELECT 	date_trunc('minute' ,  to_timestamp( finish_timestamp , 'YYYY-MM-DD HH24:MI' ) ) 
		INTO 	max_timestamp ; 
	END IF ;


    --Очистить таблицу для расчета Индекса Приоритета Корреляции (Correlation Priority Index, CPI) .
	TRUNCATE TABLE cpi_matrix;
	
	result_str[line_count] = '1. СТАТИСТИЧЕСКИЙ АНАЛИЗ ПОДСИСТЕМЫ IO' ; 
	line_count=line_count+1;
	
	result_str[line_count] = to_char(min_timestamp , 'YYYY-MM-DD HH24:MI') ;
	line_count=line_count+1; 
	result_str[line_count] = to_char(max_timestamp , 'YYYY-MM-DD HH24:MI') ;
	line_count=line_count+2; 
	
	result_str[line_count] = 'DEVICE '; 
	line_count=line_count+1;
	result_str[line_count] = device_name ; 
	line_count=line_count+2;
	
	
	
	DROP TABLE IF EXISTS tmp_timepoints;
	CREATE TEMPORARY TABLE tmp_timepoints
	(
		curr_timestamp timestamptz  ,   
		curr_timepoint integer 
	);


	INSERT INTO tmp_timepoints
	(
		curr_timestamp ,	
		curr_timepoint 
	)
	SELECT 
		cl.curr_timestamp , 
		row_number() over (order by cl.curr_timestamp) AS x
	FROM
	os_stat_vmstat_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)	
	WHERE 
		cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp  
	ORDER BY cl.curr_timestamp	;
	
	----------------------------------------------------------------------------------------------------------------------------------------------	
	-- Граничные значения и медиана 
	SELECT 
		MIN( cl.cpu_wa_long) AS min_cpu_wa_long , MAX( cl.cpu_wa_long) AS max_cpu_wa_long , (percentile_cont(0.5) within group (order by cl.cpu_wa_long))::numeric AS median_cpu_wa_long , 
		MIN( cl_io.dev_util_pct_long) AS min_dev_util_pct_long , MAX( cl_io.dev_util_pct_long) AS max_dev_util_pct_long, (percentile_cont(0.5) within group (order by cl_io.dev_util_pct_long))::numeric AS median_dev_util_pct_long , 
		MIN( cl.memory_buff_long) AS min_memory_buff_long , MAX( cl.memory_buff_long) AS max_memory_buff_long, (percentile_cont(0.5) within group (order by cl.memory_buff_long))::numeric AS median_memory_buff_long ,  
		MIN( cl.memory_cache_long) AS min_memory_cache_long , MAX( cl.memory_cache_long) AS max_memory_cache_long, (percentile_cont(0.5) within group (order by cl.memory_cache_long))::numeric AS median_memory_cache_long , 
		MIN( cl_io.dev_rps_long) AS min_dev_rps_long , MAX( cl_io.dev_rps_long) AS max_dev_rps_long, (percentile_cont(0.5) within group (order by cl_io.dev_rps_long))::numeric AS median_dev_rps_long , 
		MIN( cl_io.dev_rmbs_long) AS min_dev_rmbs_long , MAX( cl_io.dev_rmbs_long) AS max_dev_rmbs_long, (percentile_cont(0.5) within group (order by cl_io.dev_rmbs_long))::numeric AS median_dev_rmbs_long , 
		MIN( cl_io.dev_wps_long) AS min_dev_wps_long , MAX( cl_io.dev_wps_long) AS max_dev_wps_long, (percentile_cont(0.5) within group (order by cl_io.dev_wps_long))::numeric AS median_dev_wps_long , 
		MIN( cl_io.dev_wmbps_long) AS min_dev_wmbps_long , MAX( cl_io.dev_wmbps_long) AS max_dev_wmbps_long, (percentile_cont(0.5) within group (order by cl_io.dev_wmbps_long))::numeric AS median_dev_wmbps_long  , 
		MIN( cl_io.dev_r_await_long) AS min_dev_r_await_long , MAX( cl_io.dev_r_await_long) AS max_dev_r_await_long, (percentile_cont(0.5) within group (order by cl_io.dev_r_await_long))::numeric AS median_dev_r_await_long , 
		MIN( cl_io.dev_w_await_long) AS min_dev_w_await_long , MAX( cl_io.dev_w_await_long) AS max_dev_w_await_long, (percentile_cont(0.5) within group (order by cl_io.dev_w_await_long))::numeric AS median_dev_w_await_long , 
		MIN( cl_io.dev_aqu_sz_long) AS min_dev_aqu_sz_long , MAX( cl_io.dev_aqu_sz_long) AS max_dev_aqu_sz_long, (percentile_cont(0.5) within group (order by cl_io.dev_aqu_sz_long))::numeric AS median_dev_aqu_sz_long
	INTO  	min_max_rec
	FROM 
		os_stat_vmstat_median cl 
		JOIN os_stat_iostat_device_median cl_io ON ( cl.curr_timestamp = cl_io.curr_timestamp)
		JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
	WHERE 	
		cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
		AND cl_io.device = device_name ;
	-- Граничные значения и медиана 
	----------------------------------------------------------------------------------------------------------------------------------------------	

		
	SELECT 
		count(curr_timestamp)
	INTO line_counter
	FROM 
		cluster_stat_median cl
	WHERE 	
		cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
		
		
    
	result_str[line_count] = 	'ГРАНИЧНЫЕ ЗНАЧЕНИЯ И МЕДИАНА'||'|'||								
								'cpu_wa ' ||'|' ||
								'device_util ' ||'|'||
                                'memory_buff ' ||'|'||
								'memory_cache ' ||'|'||
								'r/s' ||'|'||
								'rMB/s ' ||'|'||
								'w/s' ||'|'||
								'wMB/s' ||'|'||
								'r_await' ||'|'||
								'w_await' ||'|'||
								'aqu_sz' ||'|'
								;							
	line_count=line_count+1; 
	
	
	result_str[line_count] = 	'MIN '||'|'||								
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_cpu_wa_long::numeric , 4 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_dev_util_pct_long::numeric , 4 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_memory_buff_long::numeric , 4 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_memory_cache_long::numeric , 4 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_dev_rps_long::numeric , 4 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_dev_rmbs_long::numeric , 4 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_dev_wps_long::numeric , 4 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_dev_wmbps_long::numeric , 4 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_dev_r_await_long::numeric , 4 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_dev_w_await_long::numeric , 4 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_dev_aqu_sz_long::numeric , 4 ) , '000000000000D0000') , '.' , ',' )||'|'
								
								;							
	line_count=line_count+1; 

	result_str[line_count] = 	'MEDIAN'||'|'||								
								REPLACE ( TO_CHAR( ROUND( min_max_rec.median_cpu_wa_long::numeric , 4 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.median_dev_util_pct_long::numeric , 4 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.median_memory_buff_long::numeric , 4 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.median_memory_cache_long::numeric , 4 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.median_dev_rps_long::numeric , 4 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.median_dev_rmbs_long::numeric , 4 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.median_dev_wps_long::numeric , 4 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.median_dev_wmbps_long::numeric , 4 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.median_dev_r_await_long::numeric , 4 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.median_dev_w_await_long::numeric , 4 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.median_dev_aqu_sz_long::numeric , 4 ) , '000000000000D0000') , '.' , ',' )||'|'
								
								;							
	line_count=line_count+1; 
	
	
	result_str[line_count] = 	'MAX'||'|'||								
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_cpu_wa_long::numeric , 4 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_dev_util_pct_long::numeric , 4 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_memory_buff_long::numeric , 4 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_memory_cache_long::numeric , 4 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_dev_rps_long::numeric , 4 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_dev_rmbs_long::numeric , 4 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_dev_wps_long::numeric , 4 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_dev_wmbps_long::numeric , 4 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_dev_r_await_long::numeric , 4 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_dev_w_await_long::numeric , 4 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_dev_aqu_sz_long::numeric , 4 ) , '000000000000D0000') , '.' , ',' )||'|'
								;								
	line_count=line_count+2; 	
	


	line_count=line_count+1; 								
	result_str[line_count] = 'ОТНОСИТЕЛЬНЫЕ ПОКАЗАТЕЛИ iostat' ; 
	line_count=line_count+1;		

-----------------------------------------------------------------------------
	--%util Процент загрузки устройства (чем ближе к 100%, тем выше нагрузка) свыше 50%.
	WITH 
	  util_counter AS
	  (
		SELECT count(*) AS total_counter
		FROM 
			os_stat_iostat_device_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
		WHERE				
			cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
			AND dev_util_pct_long > 50
			AND cl.device = device_name
	  ) 
	SELECT 
		(total_counter::DOUBLE PRECISION / line_counter::DOUBLE PRECISION)*100.0 
	INTO
		util_pct
	FROM util_counter ;

	result_str[line_count] = '%util Процент загрузки устройства свыше 50%| '|| REPLACE ( TO_CHAR( ROUND( util_pct::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' )  ; 
	line_count=line_count+1;
	
	IF util_pct < 25.0 
	THEN 
		result_str[line_count] = 'OK: менее 25% тестового периода - загрузки устройства свыше 50%' ; 
		line_count=line_count+1;
	ELSIF util_pct > 25.0 AND util_pct <= 50.0
	THEN 
		result_str[line_count] = 'WARNING: 25-50% тестового периода - загрузки устройства свыше 50%' ; 
		line_count=line_count+1;
	ELSE 
		result_str[line_count] = 'ALARM: более 50% тестового периода - загрузки устройства свыше 50%' ; 
		line_count=line_count+1;
	END IF ;	
	--%util Процент загрузки устройства (чем ближе к 100%, тем выше нагрузка) свыше 50%.
	-----------------------------------------------------------------------------
	
    -----------------------------------------------------------------------------
	--Среднее время выполнения запросов чтения (включая время в очереди) свыше 5мс.
	WITH 
	  r_await_counter AS
	  (
		SELECT count(*) AS total_counter
		FROM 
			os_stat_iostat_device_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
		WHERE				
			cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
			AND dev_r_await_long > 5
			AND cl.device = device_name
	  ) 
	SELECT 
		(total_counter::DOUBLE PRECISION / line_counter::DOUBLE PRECISION)*100.0 
	INTO
		r_await_pct
	FROM r_await_counter ;

    line_count=line_count+1; 
	result_str[line_count] = 'Отклик на чтение свыше 5мс(%тестового периода)| '|| REPLACE ( TO_CHAR( ROUND( r_await_pct::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' )  ; 
	line_count=line_count+1;
	
	IF r_await_pct < 25.0 
	THEN 
		result_str[line_count] = 'OK: менее 25% тестового периода - Отклик на чтение свыше 5мс' ; 
		line_count=line_count+1;
	ELSIF r_await_pct > 25.0 AND r_await_pct <= 50.0
	THEN 
		result_str[line_count] = 'WARNING: 25-50% тестового периода - Отклик на чтение свыше 5мс' ; 
		line_count=line_count+1;
	ELSE 
		result_str[line_count] = 'ALARM: более 50% тестового периода - Отклик на чтение свыше 5мс' ; 
		line_count=line_count+1;
	END IF ;	
	--Среднее время выполнения запросов чтения (включая время в очереди) свыше 5мс.
	-----------------------------------------------------------------------------
	
	-----------------------------------------------------------------------------
	--w_await (мс) Среднее время выполнения запросов записи  свыше 5мс.
	WITH 
	  w_await_counter AS
	  (
		SELECT count(*) AS total_counter
		FROM 
			os_stat_iostat_device_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
		WHERE				
			cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
			AND dev_w_await_long > 5
			AND cl.device = device_name
	  ) 
	SELECT 
		(total_counter::DOUBLE PRECISION / line_counter::DOUBLE PRECISION)*100.0 
	INTO
		w_await_pct
	FROM w_await_counter ;

	line_count=line_count+1;
	result_str[line_count] = 'Отклик на запись свыше 5мс(%тестового периода)| '|| REPLACE ( TO_CHAR( ROUND( w_await_pct::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' )  ; 
	line_count=line_count+1;
	
	IF w_await_pct < 25.0 
	THEN 
		result_str[line_count] = 'OK: менее 25% тестового периода - Отклик на запись свыше 5мс' ; 
		line_count=line_count+1;
	ELSIF w_await_pct > 25.0 AND w_await_pct <= 50.0
	THEN 
		result_str[line_count] = 'WARNING: 25-50% тестового периода - Отклик на запись свыше 5мс' ; 
		line_count=line_count+1;
	ELSE 
		result_str[line_count] = 'ALARM: более 50% тестового периода - Отклик на запись свыше 5мс' ; 
		line_count=line_count+1;
	END IF ;	
	--w_await (мс) Среднее время выполнения запросов записи  свыше 5мс.
	-----------------------------------------------------------------------------
	
	-----------------------------------------------------------------------------
	--aqu_sz Средняя длина очереди запросов (глубина очереди) свыше 1
	WITH 
	  aqu_sz_counter AS
	  (
		SELECT count(*) AS total_counter
		FROM 
			os_stat_iostat_device_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
		WHERE				
			cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
			AND dev_aqu_sz_long > 1
			AND cl.device = device_name
	  ) 
	SELECT 
		(total_counter::DOUBLE PRECISION / line_counter::DOUBLE PRECISION)*100.0 
	INTO
		aqu_sz_pct
	FROM aqu_sz_counter ;

	line_count=line_count+1;
	result_str[line_count] = 'Средняя длина очереди запросов (глубина очереди) свыше 1 (%тестового периода) | '|| REPLACE ( TO_CHAR( ROUND( aqu_sz_pct::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' )  ; 
	line_count=line_count+1;

	IF aqu_sz_pct < 25.0 
	THEN 
		result_str[line_count] = 'OK: менее 25% тестового периода - глубина очереди свыше 1' ; 
		line_count=line_count+1;
	ELSIF aqu_sz_pct > 25.0 AND aqu_sz_pct <= 50.0
	THEN 
		result_str[line_count] = 'WARNING: 25-50% тестового периода - глубина очереди свыше 1' ; 
		line_count=line_count+1;
	ELSE 
		result_str[line_count] = 'ALARM: более 50% тестового периода - глубина очереди) свыше 1' ; 
		line_count=line_count+1;
	END IF ;	
	--aqu_sz Средняя длина очереди запросов (глубина очереди).
	-----------------------------------------------------------------------------
	
	
	-----------------------------------------------------------------------------------------------------------------------
	--КОРРЕЛЯЦИЯ VMSTAT и IOSTAT
	line_count=line_count+1;
	result_str[line_count] = '1. КОРРЕЛЯЦИЯ VMSTAT и IOSTAT' ; 
	line_count=line_count+1;
	part = 0 ;
	
	------------------------------------------------------------
	-- vmstat/wa и iostat/util
	IF 	min_max_rec.min_cpu_wa_long != min_max_rec.max_cpu_wa_long AND 
		min_max_rec.min_dev_util_pct_long != min_max_rec.max_dev_util_pct_long
	THEN
		CALL truncate_time_series();
		INSERT INTO first_time_series ( curr_timestamp , curr_value	)
		SELECT 	cl.curr_timestamp , cpu_wa_long 
		FROM 	os_stat_vmstat_median cl 
		WHERE	cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
		
		INSERT INTO second_time_series ( curr_timestamp , curr_value	)
		SELECT 	cl.curr_timestamp , dev_util_pct_long 
		FROM 	os_stat_iostat_device_median cl 
		WHERE	cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND device = device_name ;
		
		SELECT * INTO correlation_rec FROM quick_significance_check();
		
		reason_casulas_list := '{}'::text[]; 				
		reason_casulas_list[1] = 'ПОСЛЕДСТВИЯ: Влияние на процессы - ожидание IO' ; 
		
		part = part + 1 ;
		SELECT fill_in_comprehensive_analysis_correlation( '1.'||part||'. Корреляция: vmstat/wa(ожидание IO) и iostat/util(Процент загрузки устройства)','vmstat/wa(ожидание IO)', 'iostat/util(Процент загрузки устройства)', 1 , reason_casulas_list ) --Анализируется положительная корреляция
		INTO report_str ; 
		
		SELECT array_append( result_str , ' ')
		INTO result_str ;
		SELECT result_str || report_str
		INTO result_str ; 
		SELECT array_append( result_str , ' ')
		INTO result_str ;
		SELECT array_length( result_str , 1 )
		INTO line_count;
	
	END IF ;
	-- vmstat/wa и iostat/util
	------------------------------------------------------------
	
	line_count=line_count+2;	
	result_str[line_count] = '2. БУФЕРИЗАЦИЯ ВВОДА-ВЫВОДА' ; 
	line_count=line_count+1;		
	result_str[line_count] = 'buff - Буфер памяти хранит данные, которые передаются в хранилище с немедленным доступом и из него.' ; 
	line_count=line_count+1;	
	result_str[line_count] = '  Буфер позволяет процессору и модулю памяти работать независимо, не реагируя на незначительные различия в работе' ; 
	line_count=line_count+1;	
	part = 0 ; 
	----------------------------------------------------------------------------
	-- Корреляция: vmstat/buff(буферы) и iostat/rps(Количество операций чтения в секунду)
	IF min_max_rec.min_memory_buff_long != min_max_rec.max_memory_buff_long AND 
	   min_max_rec.min_dev_rps_long != min_max_rec.max_dev_rps_long
	THEN 	
		CALL truncate_time_series();
		INSERT INTO first_time_series ( curr_timestamp , curr_value	)
		SELECT 	cl.curr_timestamp , memory_buff_long 
		FROM 	os_stat_vmstat_median cl 
		WHERE	cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
		
		INSERT INTO second_time_series ( curr_timestamp , curr_value	)
		SELECT 	cl.curr_timestamp , dev_rps_long 
		FROM 	os_stat_iostat_device_median cl 
		WHERE	cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND device = device_name ;
		
		SELECT * INTO correlation_rec FROM quick_significance_check();
		
		reason_casulas_list := '{}'::text[]; 				
		reason_casulas_list[1] = 'ПОСЛЕДСТВИЯ: Не эффективное использование памяти для снижения нагрузки на диск' ; 
		
		part = part + 1 ;
		SELECT fill_in_comprehensive_analysis_correlation('2.'||part||'. Корреляция: vmstat/buff(буферы) и iostat/rps(Количество операций чтения в секунду)','vmstat/buff(буферы)', 'iostat/rps(Количество операций чтения в секунду)', 1 , reason_casulas_list ) --Анализируется положительная корреляция
		INTO report_str ; 
		
		SELECT array_append( result_str , ' ')
		INTO result_str ;
		SELECT result_str || report_str
		INTO result_str ; 
		SELECT array_append( result_str , ' ')
		INTO result_str ;
		SELECT array_length( result_str , 1 )
		INTO line_count;
	END IF ;
	-- Корреляция: vmstat/buff(буферы) и iostat/rps(Количество операций чтения в секунду)
	----------------------------------------------------------------------------

	----------------------------------------------------------------------------
	-- Корреляция: vmstat/buff(буферы) и iostat/rMBps Скорость чтения (МБ/с)
	IF min_max_rec.min_memory_buff_long != min_max_rec.max_memory_buff_long AND 
	   min_max_rec.min_dev_rmbs_long != min_max_rec.max_dev_rmbs_long
	THEN 	
		CALL truncate_time_series();
		INSERT INTO first_time_series ( curr_timestamp , curr_value	)
		SELECT 	cl.curr_timestamp , memory_buff_long 
		FROM 	os_stat_vmstat_median cl 
		WHERE	cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
		
		INSERT INTO second_time_series ( curr_timestamp , curr_value	)
		SELECT 	cl.curr_timestamp , dev_rmbs_long 
		FROM 	os_stat_iostat_device_median cl 
		WHERE	cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND device = device_name ;
		
		SELECT * INTO correlation_rec FROM quick_significance_check();
		
		reason_casulas_list := '{}'::text[]; 				
		reason_casulas_list[1] = 'ПОСЛЕДСТВИЯ: Не эффективное использование памяти для снижения нагрузки на диск' ; 
		
		part = part + 1 ;
		SELECT fill_in_comprehensive_analysis_correlation('2.'||part||'. Корреляция: vmstat/buff(буферы) и iostat/rMBps Скорость чтения (МБ/с)','vmstat/buff(буферы)', 'iostat/rMBps Скорость чтения (МБ/с)', 1 , reason_casulas_list ) --Анализируется положительная корреляция
		INTO report_str ; 
		
		SELECT array_append( result_str , ' ')
		INTO result_str ;
		SELECT result_str || report_str
		INTO result_str ; 
		SELECT array_append( result_str , ' ')
		INTO result_str ;
		SELECT array_length( result_str , 1 )
		INTO line_count;
	END IF ;
	-- Корреляция: vmstat/buff(буферы) и iostat/rMBps Скорость чтения (МБ/с)
	----------------------------------------------------------------------------
	
	----------------------------------------------------------------------------
	-- Корреляция: vmstat/buff(буферы) и iostat/wps(Количество операций записи в секунду).
	IF min_max_rec.min_memory_buff_long != min_max_rec.max_memory_buff_long AND 
	   min_max_rec.min_dev_wps_long != min_max_rec.max_dev_wps_long
	THEN 	
		CALL truncate_time_series();
		INSERT INTO first_time_series ( curr_timestamp , curr_value	)
		SELECT 	cl.curr_timestamp , memory_buff_long 
		FROM 	os_stat_vmstat_median cl 
		WHERE	cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
		
		INSERT INTO second_time_series ( curr_timestamp , curr_value	)
		SELECT 	cl.curr_timestamp , dev_wps_long 
		FROM 	os_stat_iostat_device_median cl 
		WHERE	cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND device = device_name ;
		
		SELECT * INTO correlation_rec FROM quick_significance_check();
		
		reason_casulas_list := '{}'::text[]; 				
		reason_casulas_list[1] = 'ПОСЛЕДСТВИЯ: Не эффективное использование памяти для снижения нагрузки на диск' ; 
		
		part = part + 1 ;
		SELECT fill_in_comprehensive_analysis_correlation('2.'||part||'. Корреляция: vmstat/buff(буферы) и iostat/wps(Количество операций записи в секунду)','vmstat/buff(буферы)', 'iostat/wps(Количество операций записи в секунду)', 1 , reason_casulas_list ) --Анализируется положительная корреляция
		INTO report_str ; 
		
		SELECT array_append( result_str , ' ')
		INTO result_str ;
		SELECT result_str || report_str
		INTO result_str ; 
		SELECT array_append( result_str , ' ')
		INTO result_str ;
		SELECT array_length( result_str , 1 )
		INTO line_count;
	END IF ;
	-- -- Корреляция: vmstat/buff(буферы) и iostat/wps(Количество операций записи в секунду)
	----------------------------------------------------------------------------

	----------------------------------------------------------------------------
	-- Корреляция: vmstat/buff(буферы) и iostat/wMBps Скорость записи (МБ/с)
	IF min_max_rec.min_memory_buff_long != min_max_rec.max_memory_buff_long AND 
	   min_max_rec.min_dev_wps_long != min_max_rec.max_dev_wps_long
	THEN 	
		CALL truncate_time_series();
		INSERT INTO first_time_series ( curr_timestamp , curr_value	)
		SELECT 	cl.curr_timestamp , memory_buff_long 
		FROM 	os_stat_vmstat_median cl 
		WHERE	cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
		
		INSERT INTO second_time_series ( curr_timestamp , curr_value	)
		SELECT 	cl.curr_timestamp , dev_wmbps_long 
		FROM 	os_stat_iostat_device_median cl 
		WHERE	cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND device = device_name ;
		
		SELECT * INTO correlation_rec FROM quick_significance_check();
		
		reason_casulas_list := '{}'::text[]; 				
		reason_casulas_list[1] = 'ПОСЛЕДСТВИЯ: Не эффективное использование памяти для снижения нагрузки на диск' ; 
		
		part = part + 1 ;
		SELECT fill_in_comprehensive_analysis_correlation('2.'||part||'. Корреляция: vmstat/buff(буферы) и iostat/wMBps Скорость записи (МБ/с)','vmstat/buff(буферы)', 'iostat/wMBps Скорость записи (МБ/с)', 1 , reason_casulas_list ) --Анализируется положительная корреляция
		INTO report_str ; 
		
		SELECT array_append( result_str , ' ')
		INTO result_str ;
		SELECT result_str || report_str
		INTO result_str ; 
		SELECT array_append( result_str , ' ')
		INTO result_str ;
		SELECT array_length( result_str , 1 )
		INTO line_count;
	END IF ;
	-- Корреляция: vmstat/buff(буферы) и iostat/wMBps Скорость записи (МБ/с)
	----------------------------------------------------------------------------
	
	line_count=line_count+2;	
	result_str[line_count] = '3. КЭШИРОВАНИЕ ВВОДА-ВЫВОДА' ; 
	line_count=line_count+1;			
	result_str[line_count] = 'cache - Объем памяти, используемый в качестве кэша страниц.' ; 
	line_count=line_count+1;	
	part = 0 ;

	----------------------------------------------------------------------------
	-- Корреляция: vmstat/cache(кэш) и iostat/rps(Количество операций чтения в секунду)
	IF min_max_rec.min_memory_cache_long != min_max_rec.max_memory_cache_long AND 
	   min_max_rec.min_dev_rps_long != min_max_rec.max_dev_rps_long
	THEN 	
		CALL truncate_time_series();
		INSERT INTO first_time_series ( curr_timestamp , curr_value	)
		SELECT 	cl.curr_timestamp , memory_cache_long 
		FROM 	os_stat_vmstat_median cl 
		WHERE	cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
		
		INSERT INTO second_time_series ( curr_timestamp , curr_value	)
		SELECT 	cl.curr_timestamp , dev_rps_long 
		FROM 	os_stat_iostat_device_median cl 
		WHERE	cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND device = device_name ;
		
		SELECT * INTO correlation_rec FROM quick_significance_check();
		
		reason_casulas_list := '{}'::text[]; 				
		reason_casulas_list[1] = 'ПОСЛЕДСТВИЯ: Не эффективное использование памяти для снижения нагрузки на диск' ; 
		
		part = part + 1 ;
		SELECT fill_in_comprehensive_analysis_correlation( '3.'||part||'. Корреляция: vmstat/cache(кэш) и iostat/rps(Количество операций чтения в секунду)','vmstat/cache(кэш)', 'iostat/rps(Количество операций чтения в секунду)', 1 , reason_casulas_list ) --Анализируется положительная корреляция
		INTO report_str ; 
		
		SELECT array_append( result_str , ' ')
		INTO result_str ;
		SELECT result_str || report_str
		INTO result_str ; 
		SELECT array_append( result_str , ' ')
		INTO result_str ;
		SELECT array_length( result_str , 1 )
		INTO line_count;
	END IF ;
	-- Корреляция: vmstat/cache(кэш) и iostat/rps(Количество операций чтения в секунду)
	----------------------------------------------------------------------------

	----------------------------------------------------------------------------
	-- Корреляция: vmstat/cache(кэш) и iostat/rMBps Скорость чтения (МБ/с)
	IF min_max_rec.min_memory_cache_long != min_max_rec.max_memory_cache_long AND 
	   min_max_rec.min_dev_rmbs_long != min_max_rec.max_dev_rmbs_long
	THEN 	
		CALL truncate_time_series();
		INSERT INTO first_time_series ( curr_timestamp , curr_value	)
		SELECT 	cl.curr_timestamp , memory_cache_long 
		FROM 	os_stat_vmstat_median cl 
		WHERE	cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
		
		INSERT INTO second_time_series ( curr_timestamp , curr_value	)
		SELECT 	cl.curr_timestamp , dev_rmbs_long 
		FROM 	os_stat_iostat_device_median cl 
		WHERE	cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND device = device_name ;
		
		SELECT * INTO correlation_rec FROM quick_significance_check();
		
		reason_casulas_list := '{}'::text[]; 				
		reason_casulas_list[1] = 'ПОСЛЕДСТВИЯ: Не эффективное использование памяти для снижения нагрузки на диск' ; 
		
		part = part + 1 ;
		SELECT fill_in_comprehensive_analysis_correlation( '3.'||part||'. Корреляция: vmstat/cache(кэш) и iostat/rMBps Скорость чтения (МБ/с)','vmstat/cache(кэш)', 'iostat/rMBps Скорость чтения (МБ/с)', 1 , reason_casulas_list ) --Анализируется положительная корреляция
		INTO report_str ; 
		
		SELECT array_append( result_str , ' ')
		INTO result_str ;
		SELECT result_str || report_str
		INTO result_str ; 
		SELECT array_append( result_str , ' ')
		INTO result_str ;
		SELECT array_length( result_str , 1 )
		INTO line_count;
	END IF ;
	-- Корреляция: vmstat/cache(кэш) и iostat/rMBps Скорость чтения (МБ/с)
	----------------------------------------------------------------------------
	
	----------------------------------------------------------------------------
	-- Корреляция: vmstat/cache(кэш) и iostat/wps(Количество операций записи в секунду).
	IF min_max_rec.min_memory_cache_long != min_max_rec.max_memory_cache_long AND 
	   min_max_rec.min_dev_wps_long != min_max_rec.max_dev_wps_long
	THEN 	
		CALL truncate_time_series();
		INSERT INTO first_time_series ( curr_timestamp , curr_value	)
		SELECT 	cl.curr_timestamp , memory_cache_long 
		FROM 	os_stat_vmstat_median cl 
		WHERE	cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
		
		INSERT INTO second_time_series ( curr_timestamp , curr_value	)
		SELECT 	cl.curr_timestamp , dev_wps_long 
		FROM 	os_stat_iostat_device_median cl 
		WHERE	cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND device = device_name ;
		
		SELECT * INTO correlation_rec FROM quick_significance_check();
		
		reason_casulas_list := '{}'::text[]; 				
		reason_casulas_list[1] = 'ПОСЛЕДСТВИЯ: Не эффективное использование памяти для снижения нагрузки на диск' ; 
		
		part = part + 1 ;
		SELECT fill_in_comprehensive_analysis_correlation( '3.'||part||'. Корреляция: vmstat/cache(кэш) и iostat/wps(Количество операций записи в секунду)','vmstat/cache(кэш)', 'iostat/wps(Количество операций записи в секунду)', 1 , reason_casulas_list ) --Анализируется положительная корреляция
		INTO report_str ; 
		
		SELECT array_append( result_str , ' ')
		INTO result_str ;
		SELECT result_str || report_str
		INTO result_str ; 
		SELECT array_append( result_str , ' ')
		INTO result_str ;
		SELECT array_length( result_str , 1 )
		INTO line_count;
	END IF ;
	-- -- Корреляция: vmstat/cache(кэш) и iostat/wps(Количество операций записи в секунду)
	----------------------------------------------------------------------------

	----------------------------------------------------------------------------
	-- Корреляция: vmstat/cache(кэш) и iostat/wMBps Скорость записи (МБ/с)
	IF min_max_rec.min_memory_cache_long != min_max_rec.max_memory_cache_long AND 
	   min_max_rec.min_dev_wps_long != min_max_rec.max_dev_wps_long
	THEN 	
		CALL truncate_time_series();
		INSERT INTO first_time_series ( curr_timestamp , curr_value	)
		SELECT 	cl.curr_timestamp , memory_cache_long 
		FROM 	os_stat_vmstat_median cl 
		WHERE	cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
		
		INSERT INTO second_time_series ( curr_timestamp , curr_value	)
		SELECT 	cl.curr_timestamp , dev_wmbps_long 
		FROM 	os_stat_iostat_device_median cl 
		WHERE	cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND device = device_name ;
		
		SELECT * INTO correlation_rec FROM quick_significance_check();
		
		reason_casulas_list := '{}'::text[]; 				
		reason_casulas_list[1] = 'ПОСЛЕДСТВИЯ: Не эффективное использование памяти для снижения нагрузки на диск' ; 
		
		part = part + 1 ;
		SELECT fill_in_comprehensive_analysis_correlation( '3.'||part||'. Корреляция: vmstat/cache(кэш) и iostat/wMBps Скорость записи (МБ/с)','vmstat/cache(кэш)', 'iostat/wMBps Скорость записи (МБ/с)', 1 , reason_casulas_list ) --Анализируется положительная корреляция
		INTO report_str ; 
		
		SELECT array_append( result_str , ' ')
		INTO result_str ;
		SELECT result_str || report_str
		INTO result_str ; 
		SELECT array_append( result_str , ' ')
		INTO result_str ;
		SELECT array_length( result_str , 1 )
		INTO line_count;
	END IF ;
	-- Корреляция: vmstat/cache(кэш) и iostat/wMBps Скорость записи (МБ/с)
	----------------------------------------------------------------------------	
	
	line_count=line_count+2;	
	result_str[line_count] = '4. КОРРЕЛЯЦИЯ ОПЕРАЦИОННОЙ СКОРОСТИ И МЕТРИК ПРОИЗВОДИТЕЛЬНОСТИ ДИСКОВОГО УСТРОЙСТВА' ; 
	line_count=line_count+1;			
	part = 0 ;
	
	-----------------------------------------------------------------------------
	-- Корреляция операционная скорость - IOPS
	CALL truncate_time_series();
	INSERT INTO first_time_series ( curr_timestamp , curr_value	)
	SELECT 	cl.curr_timestamp , memory_cache_long 
	FROM 	os_stat_vmstat_median cl 
	WHERE	cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
	
	INSERT INTO second_time_series ( curr_timestamp , curr_value	)
	SELECT 	cl.curr_timestamp , dev_rps_long + dev_wps_long 
	FROM 	os_stat_iostat_device_median cl 
	WHERE	cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
			AND device = device_name ;
	
	SELECT * INTO correlation_rec FROM quick_significance_check();
	
	reason_casulas_list := '{}'::text[]; 	
	reason_casulas_list[1] = 'Производительность НЕ ограничена IO.' ; 
	reason_casulas_list[2] = 'Возможны  проблемы с CPU, блокировками, памятью' ; 
	
	part = part + 1 ;
	SELECT fill_in_comprehensive_analysis_correlation( '4.'||part||'. Корреляция: ОПЕРАЦИОННАЯ СКОРОСТЬ и IOPS','ОПЕРАЦИОННАЯ СКОРОСТЬ', 'IOPS', -1 , reason_casulas_list ) --Анализируется отрицательная корреляция
	INTO report_str ; 
	
	SELECT array_append( result_str , ' ')
	INTO result_str ;
	SELECT result_str || report_str
	INTO result_str ; 
	SELECT array_append( result_str , ' ')
	INTO result_str ;
	SELECT array_length( result_str , 1 )
	INTO line_count;	
	
	IF correlation_rec.significance_empirical != 'Незначима' AND correlation_rec.significance_t_test != 'Незначима'
	THEN 	
		speed_iops_corr = correlation_rec.correvation_value ;
		IF correlation_rec.correvation_value < 0 
		THEN 
			result_str[line_count] = 'Производительность НЕ ограничена IO.' ; 
			line_count=line_count+1;	
			result_str[line_count] = 'Возможны  проблемы с CPU, блокировками, памятью' ; 
			line_count=line_count+1;		
		END IF;		
	END IF;
	-- Корреляция операционная скорость - IOPS
	-----------------------------------------------------------------------------
	
	-----------------------------------------------------------------------------
	-- Корреляция операционная скорость - MBps
	CALL truncate_time_series();
	INSERT INTO first_time_series ( curr_timestamp , curr_value	)
	SELECT 	cl.curr_timestamp , memory_cache_long 
	FROM 	os_stat_vmstat_median cl 
	WHERE	cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
	
	INSERT INTO second_time_series ( curr_timestamp , curr_value	)
	SELECT 	cl.curr_timestamp , dev_rmbs_long + dev_wmbps_long 
	FROM 	os_stat_iostat_device_median cl 
	WHERE	cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
			AND device = device_name ;
	
	SELECT * INTO correlation_rec FROM quick_significance_check();
	
	reason_casulas_list := '{}'::text[]; 	
	
	part = part + 1 ;
	SELECT fill_in_comprehensive_analysis_correlation( '4.'||part||'. Корреляция: ОПЕРАЦИОННАЯ СКОРОСТЬ и MBps','ОПЕРАЦИОННАЯ СКОРОСТЬ', 'MBps', 1 , reason_casulas_list ) --Анализируется положительная корреляция
	INTO report_str ; 
	
	SELECT array_append( result_str , ' ')
	INTO result_str ;
	SELECT result_str || report_str
	INTO result_str ; 
	SELECT array_append( result_str , ' ')
	INTO result_str ;
	SELECT array_length( result_str , 1 )
	INTO line_count;	
	
	IF correlation_rec.significance_empirical != 'Незначима' AND correlation_rec.significance_t_test != 'Незначима'
	THEN 	
		speed_mbps_corr = correlation_rec.correvation_value ;
		IF correlation_rec.correvation_value >= 0.7
		THEN 
			result_str[line_count] = 'ALARM: Очень высокая корреляция ОПЕРАЦИОННАЯ СКОРОСТЬ и MBps.' ; 
			line_count=line_count+1;
			result_str[line_count] = 'Производительность ограничена пропускной способностью диска' ; 
			line_count=line_count+1;
		ELSIF correlation_rec.correvation_value >= 0  AND correlation_rec.correvation_value < 0.7
		THEN 
			result_str[line_count] = 'WARNING: Слабая корреляция ОПЕРАЦИОННАЯ СКОРОСТЬ и MBps.' ; 
			line_count=line_count+1;
			result_str[line_count] = 'Нагрузка чувствительна к объему передаваемых данных.' ; 
			line_count=line_count+1;		
		ELSE
			result_str[line_count] = 'INFO: Отрицательная корреляция ОПЕРАЦИОННАЯ СКОРОСТЬ и MBps.' ; 
			line_count=line_count+1;
			result_str[line_count] = 'Проблема не в пропускной способности диска.' ; 
			line_count=line_count+1;			
		END IF;	
	END IF;

	-- Корреляция операционная скорость - IOPS
	-----------------------------------------------------------------------------
	
	-----------------------------------------------------------------------------
	-- Сценарии нагрузки 
	line_count=line_count+1;
	result_str[line_count] = 'ТИП НАГРУЗКИ ' ; 
	line_count=line_count+1;
	--Сценарий 1: Высокая корреляция по обоим показателям (r > 0.7)
	IF speed_iops_corr >= 0.7 AND speed_mbps_corr >= 0.7
	THEN 
		result_str[line_count] = 'Сценарий 1: Высокая корреляция по обоим показателям (r > 0.7)' ; 
		line_count=line_count+1;
		result_str[line_count] = 'Смешанная нагрузка. Дисковая подсистема — узкое место.' ; 
		line_count=line_count+1;
		result_str[line_count] = 'Участки оптимизации:' ; 
		line_count=line_count+1;
		result_str[line_count] = 'Соотношение read/write IOPS' ; 
		line_count=line_count+1;
		result_str[line_count] = 'Размер shared_buffers и кеширование' ; 
		line_count=line_count+1;
		result_str[line_count] = 'Возможно, нужны более производительные диски' ; 
		line_count=line_count+1;	
	--Сценарий 1: Высокая корреляция по обоим показателям (r > 0.7)
	
	--Сценарий 2: Высокий IOPS-корреляция, низкий MB/s-корреляция
	ELSIF speed_iops_corr >= 0.7 AND (speed_mbps_corr < 0.7 AND speed_mbps_corr > 0.2 )
	THEN 
		result_str[line_count] = 'Сценарий 2: Высокая IOPS-корреляция, низкая MB/s-корреляция' ; 
		line_count=line_count+1;
		result_str[line_count] = 'OLTP-нагрузка с множеством мелких операций' ; 
		line_count=line_count+1;
		result_str[line_count] = 'Участки оптимизации:' ; 
		line_count=line_count+1;
		result_str[line_count] = 'Индексы (избыточные, отсутствующие)' ; 
		line_count=line_count+1;
		result_str[line_count] = 'Размер work_mem для сортировок и хэш-операций' ; 
		line_count=line_count+1;
		result_str[line_count] = 'Настройки автовакуума' ; 
		line_count=line_count+1;
		result_str[line_count] = 'SSD с высоким IOPS' ; 
		line_count=line_count+1;
	--Сценарий 2: Высокий IOPS-корреляция, низкий MB/s-корреляция
	
	--Сценарий 3: Низкий IOPS-корреляция, высокий MB/s-корреляция
	ELSIF speed_iops_corr <= 0.25 AND (speed_mbps_corr >= 0.7  )
	THEN 
		result_str[line_count] = 'Сценарий 3: Низкая IOPS-корреляция, высокая MB/s-корреляция' ; 
		line_count=line_count+1;
		result_str[line_count] = 'Аналитическая/ETL нагрузка' ; 
		line_count=line_count+1;
		result_str[line_count] = 'Участки оптимизации:' ; 
		line_count=line_count+1;
		result_str[line_count] = 'Партиционирование больших таблиц' ; 
		line_count=line_count+1;
		result_str[line_count] = 'Настройки work_mem для агрегаций' ; 
		line_count=line_count+1;
		result_str[line_count] = 'Параллельное выполнение запросов' ; 
		line_count=line_count+1;
		result_str[line_count] = 'Диски с высокой пропускной способностью' ; 
		line_count=line_count+1;
	--Сценарий 3: Низкий IOPS-корреляция, высокий MB/s-корреляция
	
	--Сценарий 4: Низкая корреляция по обоим показателям (r < 0.25)
	ELSIF speed_iops_corr <= 0.25 AND (speed_mbps_corr <= 0.25  )
	THEN 
		result_str[line_count] = 'Сценарий 4: Низкая корреляция по обоим показателям (r < 0.25)' ; 
		line_count=line_count+1;
		result_str[line_count] = 'Узкое место не в IO' ; 
		line_count=line_count+1;
		result_str[line_count] = 'Участки оптимизации:' ; 
		line_count=line_count+1;
		result_str[line_count] = 'CPU' ; 
		line_count=line_count+1;
		result_str[line_count] = 'Тяжелые блокировки' ; 
		line_count=line_count+1;
		result_str[line_count] = 'Ожидания СУБД' ; 
		line_count=line_count+1;
		result_str[line_count] = 'Параметры параллелизма' ; 
		line_count=line_count+1;	
	--Сценарий 4: Низкая корреляция по обоим показателям (r < 0.25)
	ELSE
		result_str[line_count] = 'Однозначно не определен.' ; 
		line_count=line_count+1;		
	END IF ;
	line_count=line_count+1;	
	
	
	---------------------------------------------------------------------
	-- Расчитать Матрицу для расчета Индекса Приоритета Корреляции (Correlation Priority Index, CPI)
	CALL calculate_cpi_matrix();
	-- Расчитать Матрицу для расчета Индекса Приоритета Корреляции (Correlation Priority Index, CPI)
	---------------------------------------------------------------------
	
	line_count=line_count+2;
	result_str[line_count] = 'ИНДЕКС ПРИОРИТЕТА КОРРЕЛЯЦИИ (Correlation Priority Index, CPI)';
	line_count=line_count+1;
	result_str[line_count] = 'КОРРЕЛИРУЕМЫЕ ЗНАЧЕНИЯ | CPI ';
	line_count=line_count+1;
	FOR cpi_matrix_rec IN 
	SELECT * FROM cpi_matrix
	ORDER BY curr_value DESC
	LOOP 
		result_str[line_count] = cpi_matrix_rec.current_pair||'|'|| REPLACE ( TO_CHAR( ROUND( cpi_matrix_rec.curr_value::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' );
		line_count=line_count+1;
	END LOOP ;
	
    


  return result_str ; 
END
$$ LANGUAGE plpgsql  ;
COMMENT ON FUNCTION report_vmstat_iostat IS 'Корреляция метрик vmstat и iopstat';
-- Корреляция метрик vmstat и iopstat
-------------------------------------------------------------------------------


--------------------------------------------------------------------------------
-- report_vmstat_performance.sql
--------------------------------------------------------------------------------
-- report_vmstat_performance.sql Статистика производительности vmstat
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Статистика производительности vmstat
CREATE OR REPLACE FUNCTION report_vmstat_performance(  cluster_performance_start_timestamp text , cluster_performance_finish_timestamp text   ) RETURNS text[] AS $$
DECLARE
result_str text[] ;
 line_count integer ;
 min_timestamp timestamptz ; 
 max_timestamp timestamptz ; 
 
 counter integer ; 
 min_max_rec record ;
 line_counter integer ; 
 
 least_squares_rec record ; 
 corr_values   text[] ;
 
 b_norm DOUBLE PRECISION;
 K DOUBLE PRECISION;
 trend_analysis   text[] ;
BEGIN
	line_count = 1 ;
	
	IF cluster_performance_finish_timestamp = 'CURRENT_TIMESTAMP'
	THEN 
		SELECT 	date_trunc('minute' ,  to_timestamp( cluster_performance_start_timestamp , 'YYYY-MM-DD HH24:MI' ) ) 
		INTO 	max_timestamp ; 

		min_timestamp = max_timestamp - interval '1 hour'; 	
	ELSE
		SELECT 	date_trunc('minute' ,  to_timestamp( cluster_performance_start_timestamp , 'YYYY-MM-DD HH24:MI' ) ) 
		INTO 	min_timestamp ; 
		
		SELECT 	date_trunc('minute' ,  to_timestamp( cluster_performance_finish_timestamp , 'YYYY-MM-DD HH24:MI' ) ) 
		INTO 	max_timestamp ; 
	END IF ;
	
	result_str[line_count] = '2.ТРЕНДОВЫЙ АНАЛИЗ ПРОИЗВОДИТЕЛЬНОСТИ vmstat' ; 
	line_count=line_count+1;
	
	result_str[line_count] = to_char(min_timestamp , 'YYYY-MM-DD HH24:MI') ;
	line_count=line_count+1; 
	result_str[line_count] = to_char(max_timestamp , 'YYYY-MM-DD HH24:MI') ;
	line_count=line_count+2; 

	----------------------------------------------------------------------------------------------------------------------------------------------	
	-- Граничные значения и медиана 
	SELECT 
		MIN( cl.procs_r_long) AS min_procs_r_long, MAX( cl.procs_r_long) AS max_procs_r_long, (percentile_cont(0.5) within group (order by cl.procs_r_long))::numeric AS median_procs_r_long , 
		MIN( cl.procs_b_long) AS min_procs_b_long, MAX( cl.procs_b_long) AS max_procs_b_long,  (percentile_cont(0.5) within group (order by cl.procs_b_long))::numeric AS median_procs_b_long , 
		MIN( cl.cpu_wa_long) AS min_cpu_wa_long, MAX( cl.cpu_wa_long) AS max_cpu_wa_long, (percentile_cont(0.5) within group (order by cl.cpu_wa_long))::numeric AS median_cpu_wa_long , 
		MIN( cl.cpu_id_long) AS min_cpu_id_long, MAX( cl.cpu_id_long) AS max_cpu_id_long, (percentile_cont(0.5) within group (order by cl.cpu_id_long))::numeric AS median_cpu_id_long  
	INTO  	min_max_rec
	FROM 
		os_stat_vmstat_median cl 
	WHERE 	
		cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 	;
		
	
	----------------------------------------------------------------------------------------------------		
	--ГРАНИЧНЫЕ ЗНАЧЕНИЯ И МЕДИАНА
    line_count=line_count+1; 
	result_str[line_count] = 	'ГРАНИЧНЫЕ ЗНАЧЕНИЯ И МЕДИАНА|'||
								'№'||'|'||		
								'procs -> r '||'|'||
								'procs -> b' ||'|'||
								'cpu -> wa' ||'|'||
								'cpu -> id' ||'|'
									;							
	line_count=line_count+1; 

	result_str[line_count] = 	'MIN'||'|'||
									1 ||'|'||
									REPLACE ( TO_CHAR( ROUND( min_max_rec.min_procs_r_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
									REPLACE ( TO_CHAR( ROUND( min_max_rec.min_procs_b_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
									REPLACE ( TO_CHAR( ROUND( min_max_rec.min_cpu_wa_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
									REPLACE ( TO_CHAR( ROUND( min_max_rec.min_cpu_id_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'
									;							
	line_count=line_count+1; 	

	result_str[line_count] = 	'MEDIAN'||'|'||
									' ' ||'|'||
									REPLACE ( TO_CHAR( ROUND( min_max_rec.median_procs_r_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
									REPLACE ( TO_CHAR( ROUND( min_max_rec.median_procs_b_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
									REPLACE ( TO_CHAR( ROUND( min_max_rec.median_cpu_wa_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
									REPLACE ( TO_CHAR( ROUND( min_max_rec.median_cpu_id_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'
									;							
	line_count=line_count+1; 	

	
	SELECT 
		count(curr_timestamp)
	INTO line_counter
	FROM 
		cluster_stat_median
	WHERE 	
		curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
	
			
	result_str[line_count] = 	'MAX'||'|'||
									line_counter||'|'||
									REPLACE ( TO_CHAR( ROUND( min_max_rec.max_procs_r_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
									REPLACE ( TO_CHAR( ROUND( min_max_rec.max_procs_b_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
									REPLACE ( TO_CHAR( ROUND( min_max_rec.max_cpu_wa_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
									REPLACE ( TO_CHAR( ROUND( min_max_rec.max_cpu_id_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'
									;	
	line_count=line_count+1;
	result_str[line_count] = 'procs -> r: Количество процессов в очереди на выполнение.';
	line_count=line_count+1;
	result_str[line_count] = 'procs -> b: Количество процессов, находящихся в состоянии';
	line_count=line_count+1;
	result_str[line_count] = '  непрерываемого сна (обычно ожидание IO). ';
	line_count=line_count+1;
	result_str[line_count] = '  Рост b — прямой признак того, что процессы не могут продолжить работу.';
	line_count=line_count+1;
	result_str[line_count] = 'cpu -> wa: Процент простоя CPU в ожидании IO.';
	line_count=line_count+1;
	result_str[line_count] = 'cpu -> id: Процент полного простоя CPU ';
	line_count=line_count+1;	
	result_str[line_count] = '  (если id низкий, а ожиданий много — ';
	line_count=line_count+1;	
	result_str[line_count] = '  значит CPU занят обработкой других задач или переключениями).';
	line_count=line_count+2;	
	
	--ГРАНИЧНЫЕ ЗНАЧЕНИЯ И МЕДИАНА
	----------------------------------------------------------------------------------------------------------------------------------------------	
/*
1. Корректировка направления тренда
В зависимости от того, какое изменение метрики считается ухудшением, введём скорректированный коэффициент наклона b′:
Метрика						Ухудшение		Улучшение		Корректировка
procs r (очередь на CPU)	рост (b>0)		снижение (b<0)	b′=b
procs b (ожидание IO)		рост (b>0)		снижение (b<0)	b′=b
cpu wa (ожидание IO)		рост (b>0)		снижение (b<0)	b′=b
cpu id (простой CPU)		снижение(b<0)	рост (b>0)		b′=−b
Таким образом, положительное значение b′ всегда соответствует ухудшению, отрицательное — улучшению.

2. Учёт силы тренда
Коэффициент детерминации R2 показывает, какая доля дисперсии объясняется линейной моделью, то есть силу (надёжность) тренда. 
Чем выше R2, тем более выражен тренд.

3. Итоговый коэффициент тренда
Комбинируем скорректированный наклон и силу связи:
K=b′×R2
где:
•	K — коэффициент тренда (положительный при ухудшении, отрицательный при улучшении);
•	b′ — скорректированный наклон (в исходных единицах измерения метрики за шаг времени);
•	R2 — коэффициент детерминации (от 0 до 1).
Абсолютная величина ∣K∣∣K∣ характеризует выраженность тренда с учётом его статистической значимости: 
даже большой наклон при низком R2 даст небольшой вклад, и наоборот.

*/


	------------------------------------------------------------------------------
	-- procs -> r
	CALL truncate_time_series();
	INSERT INTO first_time_series ( curr_timestamp , curr_value	)
	SELECT cl.curr_timestamp , procs_r_long 
	FROM   os_stat_vmstat_median cl 
	WHERE	cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
			
	SELECT * INTO least_squares_rec FROM the_line_of_least_squares();	

	line_count=line_count+1;
	result_str[line_count] = 'procs -> r: Количество процессов в очереди на выполнение' ; 
	line_count=line_count+1;
    result_str[line_count] = 'ЛИНИЯ РЕГРЕССИИ по t: Y = a + bt ' ; 
	line_count=line_count+1; 
	result_str[line_count] = 'Коэффициент детерминации R^2 ' ||'|'|| REPLACE ( TO_CHAR( ROUND( least_squares_rec.current_r_squared::numeric , 2 ) , '000000000000D0000') , '.' , ',' ) ;
	line_count=line_count+1; 	
	result_str[line_count] = 'угол наклона  ' ||'|'|| REPLACE ( TO_CHAR( ROUND( least_squares_rec.slope_angle_degrees::numeric , 2 ) , '000000000000D0000') , '.' , ',' ) ;
	line_count=line_count+1; 	
	SELECT interpretation_r2_coefficient( least_squares_rec.current_r_squared::numeric ) 
	INTO corr_values ; 
	result_str[line_count] = corr_values[1] ; 
	line_count=line_count+1;
	result_str[line_count] = corr_values[2] ; 
	line_count=line_count+1;
	result_str[line_count] = corr_values[3] ; 
	line_count=line_count+1;	
	--------------------------------------------
	-- Тренд
		b_norm = least_squares_rec.slope_angle_degrees ;		
		IF least_squares_rec.slope_angle_degrees > 0 
		THEN 
			result_str[line_count] = 'Негативный тренд (ухудшение)';
			line_count=line_count+1;
		ELSIF least_squares_rec.slope_angle_degrees < 0 
		THEN 
			result_str[line_count] = 'Позитивный тренд (улучшение)';
			line_count=line_count+1;			
		ELSE 
			result_str[line_count] = 'Тренд отсутствует';
			line_count=line_count+1;			
			b_norm = 0 ;
		END IF ;
		K = b_norm * least_squares_rec.current_r_squared ;

		SELECT interpretation_K_coefficient( K )		
		INTO trend_analysis;
		
		SELECT result_str || trend_analysis
		INTO result_str ; 
		SELECT array_append( result_str , ' ')
		INTO result_str ;
		SELECT array_length( result_str , 1 )
		INTO line_count;	

		IF K >= 20.0
		THEN 
			result_str[line_count] = 'РЕКОМЕНДУЕМЫЕ МЕРОПРИЯТИЯ' ;
			line_count=line_count+1;	
			result_str[line_count] = 'Проверить загрузку процессора, выявить процессы-потребители, проанализировать планировщик.' ; 
			line_count=line_count+1;			
		END IF ;
	-- Тренд
	--------------------------------------------
	
	
	
-- procs -> r
	------------------------------------------------------------------------------
	
	------------------------------------------------------------------------------
	-- procs -> b
	line_count=line_count+1;	
	CALL truncate_time_series();
	INSERT INTO first_time_series ( curr_timestamp , curr_value	)
	SELECT cl.curr_timestamp , procs_b_long 
	FROM   os_stat_vmstat_median cl 
	WHERE	cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
			
	SELECT * INTO least_squares_rec FROM the_line_of_least_squares();	

	line_count=line_count+1;
	result_str[line_count] = 'procs -> b: Количество процессов, находящихся в состоянии';
	line_count=line_count+1;
	result_str[line_count] = '  непрерываемого сна (обычно ожидание IO). ';	
	line_count=line_count+1; 
    result_str[line_count] = 'ЛИНИЯ РЕГРЕССИИ по t: Y = a + bt ' ; 
	line_count=line_count+1; 
	result_str[line_count] = 'Коэффициент детерминации R^2 ' ||'|'|| REPLACE ( TO_CHAR( ROUND( least_squares_rec.current_r_squared::numeric , 2 ) , '000000000000D0000') , '.' , ',' ) ;
	line_count=line_count+1; 	
	result_str[line_count] = 'угол наклона  ' ||'|'|| REPLACE ( TO_CHAR( ROUND( least_squares_rec.slope_angle_degrees::numeric , 2 ) , '000000000000D0000') , '.' , ',' ) ;
	line_count=line_count+1; 	
	SELECT interpretation_r2_coefficient( least_squares_rec.current_r_squared::numeric ) 
	INTO corr_values ; 
	result_str[line_count] = corr_values[1] ; 
	line_count=line_count+1;
	result_str[line_count] = corr_values[2] ; 
	line_count=line_count+1;
	result_str[line_count] = corr_values[3] ; 
	line_count=line_count+1;	
	--------------------------------------------
	-- Тренд
		b_norm = least_squares_rec.slope_angle_degrees ;		
		IF least_squares_rec.slope_angle_degrees > 0 
		THEN 
			result_str[line_count] = 'Негативный тренд (ухудшение)';
			line_count=line_count+1;
		ELSIF least_squares_rec.slope_angle_degrees < 0 
		THEN 
			result_str[line_count] = 'Позитивный тренд (улучшение)';
			line_count=line_count+1;			
		ELSE 
			result_str[line_count] = 'Тренд отсутствует';
			line_count=line_count+1;			
			b_norm = 0 ;
		END IF ;
		K = b_norm * least_squares_rec.current_r_squared ; 

		SELECT interpretation_K_coefficient( K )		
		INTO trend_analysis;
		
		SELECT result_str || trend_analysis
		INTO result_str ; 
		SELECT array_append( result_str , ' ')
		INTO result_str ;
		SELECT array_length( result_str , 1 )
		INTO line_count;	

		IF K >= 20.0
		THEN 
			result_str[line_count] = 'РЕКОМЕНДУЕМЫЕ МЕРОПРИЯТИЯ' ;
			line_count=line_count+1;	
			result_str[line_count] = 'Исследовать дисковую подсистему (iostat, iotop), проверить наличие медленных устройств, конфликтов ввода-вывода.' ; 
			line_count=line_count+1;			
		END IF ;
		
		
	-- Тренд
	--------------------------------------------
	
	-- procs -> b
	------------------------------------------------------------------------------
	

	------------------------------------------------------------------------------
	-- cpu -> wa
	line_count=line_count+1;	
	CALL truncate_time_series();
	INSERT INTO first_time_series ( curr_timestamp , curr_value	)
	SELECT cl.curr_timestamp , cpu_wa_long 
	FROM   os_stat_vmstat_median cl 
	WHERE	cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
			
	SELECT * INTO least_squares_rec FROM the_line_of_least_squares();	

	line_count=line_count+1;
	result_str[line_count] = 'cpu -> wa: Процент простоя CPU в ожидании IO.';
	line_count=line_count+1;
	result_str[line_count] = 'ЛИНИЯ РЕГРЕССИИ по t: Y = a + bt ' ; 
	line_count=line_count+1; 
	result_str[line_count] = 'Коэффициент детерминации R^2 ' ||'|'|| REPLACE ( TO_CHAR( ROUND( least_squares_rec.current_r_squared::numeric , 2 ) , '000000000000D0000') , '.' , ',' ) ;
	line_count=line_count+1; 	
	result_str[line_count] = 'угол наклона  ' ||'|'|| REPLACE ( TO_CHAR( ROUND( least_squares_rec.slope_angle_degrees::numeric , 2 ) , '000000000000D0000') , '.' , ',' ) ;
	line_count=line_count+1; 	
	SELECT interpretation_r2_coefficient( least_squares_rec.current_r_squared::numeric ) 
	INTO corr_values ; 
	result_str[line_count] = corr_values[1] ; 
	line_count=line_count+1;
	result_str[line_count] = corr_values[2] ; 
	line_count=line_count+1;
	result_str[line_count] = corr_values[3] ; 
	line_count=line_count+1;	
	--------------------------------------------
	-- Тренд
		b_norm = least_squares_rec.slope_angle_degrees ;		
		IF least_squares_rec.slope_angle_degrees > 0 
		THEN 
			result_str[line_count] = 'Негативный тренд (ухудшение)';
			line_count=line_count+1;
		ELSIF least_squares_rec.slope_angle_degrees < 0 
		THEN 
			result_str[line_count] = 'Позитивный тренд (улучшение)';
			line_count=line_count+1;			
		ELSE 
			result_str[line_count] = 'Тренд отсутствует';
			line_count=line_count+1;			
			b_norm = 0 ;
		END IF ;
		K = b_norm * least_squares_rec.current_r_squared ; 
		
		SELECT interpretation_K_coefficient( K )		
		INTO trend_analysis;
		
		SELECT result_str || trend_analysis
		INTO result_str ; 
		SELECT array_append( result_str , ' ')
		INTO result_str ;
		SELECT array_length( result_str , 1 )
		INTO line_count;	
		
		IF K >= 20.0
		THEN 
			result_str[line_count] = 'РЕКОМЕНДУЕМЫЕ МЕРОПРИЯТИЯ' ;
			line_count=line_count+1;	
			result_str[line_count] = 'Исследовать дисковую подсистему (iostat, iotop), проверить наличие медленных устройств, конфликтов ввода-вывода.' ; 
			line_count=line_count+1;		
			result_str[line_count] = 'Дополнительно проверить файловые системы, swap.' ; 
			line_count=line_count+1;		
		END IF ;
		
	-- Тренд
	--------------------------------------------
	
	-- cpu -> wa
	------------------------------------------------------------------------------
	
	------------------------------------------------------------------------------
	-- cpu -> id
	line_count=line_count+1;	
	CALL truncate_time_series();
	INSERT INTO first_time_series ( curr_timestamp , curr_value	)
	SELECT cl.curr_timestamp , cpu_id_long 
	FROM   os_stat_vmstat_median cl 
	WHERE	cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
			
	SELECT * INTO least_squares_rec FROM the_line_of_least_squares();	

	line_count=line_count+1;
	result_str[line_count] = 'cpu -> id: Процент полного простоя CPU.';
	line_count=line_count+1;
	result_str[line_count] = 'ЛИНИЯ РЕГРЕССИИ по t: Y = a + bt ' ; 
	line_count=line_count+1; 
	result_str[line_count] = 'Коэффициент детерминации R^2 ' ||'|'|| REPLACE ( TO_CHAR( ROUND( least_squares_rec.current_r_squared::numeric , 2 ) , '000000000000D0000') , '.' , ',' ) ;
	line_count=line_count+1; 	
	result_str[line_count] = 'угол наклона  ' ||'|'|| REPLACE ( TO_CHAR( ROUND( least_squares_rec.slope_angle_degrees::numeric , 2 ) , '000000000000D0000') , '.' , ',' ) ;
	line_count=line_count+1; 	
	SELECT interpretation_r2_coefficient( least_squares_rec.current_r_squared::numeric ) 
	INTO corr_values ; 
	result_str[line_count] = corr_values[1] ; 
	line_count=line_count+1;
	result_str[line_count] = corr_values[2] ; 
	line_count=line_count+1;
	result_str[line_count] = corr_values[3] ; 
	line_count=line_count+1;	
	--------------------------------------------
	-- Тренд
		b_norm = -least_squares_rec.slope_angle_degrees ;
		IF least_squares_rec.slope_angle_degrees > 0 
		THEN			
			result_str[line_count] = 'Позитивный тренд (улучшение)';
			line_count=line_count+1;
		ELSIF least_squares_rec.slope_angle_degrees < 0 
		THEN 
			result_str[line_count] = 'Негативный тренд (ухудшение)';
			line_count=line_count+1;			
		ELSE 
			result_str[line_count] = 'Тренд отсутствует';
			line_count=line_count+1;			
			b_norm = 0 ;
		END IF ;
		K = b_norm * least_squares_rec.current_r_squared ; 
		
		SELECT interpretation_K_coefficient( K )		
		INTO trend_analysis;
		
		SELECT result_str || trend_analysis
		INTO result_str ; 
		SELECT array_append( result_str , ' ')
		INTO result_str ;
		SELECT array_length( result_str , 1 )
		INTO line_count;
		
		IF K >= 20.0
		THEN 
			result_str[line_count] = 'РЕКОМЕНДУЕМЫЕ МЕРОПРИЯТИЯ' ;
			line_count=line_count+1;	
			result_str[line_count] = 'Проверить, не связано ли с уменьшением полезной работы.' ; 
			line_count=line_count+1;		
		END IF ;
		
		
	-- Тренд
	--------------------------------------------	
	-- cpu -> id
	------------------------------------------------------------------------------
		

  return result_str ; 
END
$$ LANGUAGE plpgsql  ;
COMMENT ON FUNCTION report_vmstat_performance IS 'Статистика производительности vmstat';
-- Статистика производительности vmstat
--------------------------------------------------------------------------------


--------------------------------------------------------------------------------
-- report_wait_event_type_for_pareto.sql
-- changed 13/03/2026
--------------------------------------------------------------------------------
--
-- report_wait_event_type_for_pareto Диаграмма Парето по wait_event_type
--
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Сформировать диаграмму Парето по wait_event
CREATE OR REPLACE FUNCTION report_wait_event_type_for_pareto(  start_timestamp text , finish_timestamp text ) RETURNS text[] AS $$
DECLARE
 result_str text[] ;
 line_count integer ;
 min_timestamp timestamptz ; 
 max_timestamp timestamptz ;   
 wait_event_queryid_rec record ;
 wait_event_type_rec record ;
 wait_event_rec record ;
 
 
 sql_stats_history_rec record ;  
 
 query_min_timestamp timestamptz ; 
 query_max_timestamp timestamptz ; 
 total_wait_event_count bigint ;
 pct_for_80 numeric ;
 
 report_wait_event_for_pareto text[];
 report_wait_event_for_pareto_count bigint ;
 index_for_wait_event bigint ;
 
 corr_bufferpin DOUBLE PRECISION ; 
 corr_extension DOUBLE PRECISION ; 
 corr_io DOUBLE PRECISION ; 
 corr_ipc DOUBLE PRECISION ; 
 corr_lock DOUBLE PRECISION ; 
 corr_lwlock DOUBLE PRECISION ; 
 corr_timeout DOUBLE PRECISION ; 
 
 wait_event_type_corr_rec  record ; 
  
 tmp_wait_event_type_corr_index bigint ; 
 	
 wait_event_type_counter bigint ; 
 wait_event_type_Pi_rec record ; 
BEGIN
	line_count = 1 ;
	
	result_str[line_count] = '4. ДИАГРАММА ПАРЕТО ПО WAIT_EVENT';	
	line_count=line_count+1;
	
	
	SELECT date_trunc( 'minute' , to_timestamp( start_timestamp , 'YYYY-MM-DD HH24:MI' ) )
	INTO    min_timestamp ; 
  
	SELECT date_trunc( 'minute' , to_timestamp( finish_timestamp , 'YYYY-MM-DD HH24:MI' ) )
	INTO    max_timestamp ; 
	
	result_str[line_count] = to_char(min_timestamp , 'YYYY-MM-DD HH24:MI') ;
	line_count=line_count+1; 
	result_str[line_count] = to_char(max_timestamp , 'YYYY-MM-DD HH24:MI') ;
	line_count=line_count+2; 

	result_str[line_count] =' WAIT_EVENT_TYPE  '||'|'||
							' WAIT_EVENT  '||'|'||
							' COUNT '||'|' ||
							' PCT '||'|' 
							;	
	line_count=line_count+1;
	
	wait_event_type_counter = 1 ;
	FOR wait_event_type_Pi_rec IN 
	SELECT * 
	FROM wait_event_type_Pi
	WHERE integral_priority > 0 
	ORDER BY integral_priority DESC 	
	LOOP
		
		

		
		pct_for_80 = 0;
		
		FOR wait_event_rec IN 
		SELECT 	
			wait_event , 
			SUM(curr_value_long) AS count
		FROM 	
			statement_stat_waitings_median
		WHERE 
			curr_timestamp  BETWEEN min_timestamp AND max_timestamp
			AND wait_event_type = wait_event_type_Pi_rec.wait_event_type 
		GROUP BY 
			wait_event
		ORDER BY 
			2 desc 
		LOOP	
			WITH report_wait_event_for_pareto AS
			(
			SELECT 	
				wait_event , 			
				SUM(curr_value_long) AS counter 
			FROM 	
				statement_stat_waitings_median
			WHERE 
				curr_timestamp  BETWEEN min_timestamp AND max_timestamp
				AND wait_event_type = wait_event_type_Pi_rec.wait_event_type
			GROUP BY 				
				wait_event
			)
			SELECT SUM(counter) 
			INTO total_wait_event_count 
			FROM report_wait_event_for_pareto ; 
			
			IF pct_for_80 = 0 
			THEN 
				pct_for_80 = (wait_event_rec.count::numeric / total_wait_event_count::numeric *100.0)::numeric ; 
			ELSE
			    pct_for_80 = pct_for_80 + (wait_event_rec.count::numeric / total_wait_event_count::numeric *100.0)::numeric ; 
			END IF;
			
			result_str[line_count] =  wait_event_type_Pi_rec.wait_event_type  ||'|'||
			                          wait_event_rec.wait_event  ||'|'||
									  wait_event_rec.count  ||'|'||
									  REPLACE ( TO_CHAR( ROUND( (wait_event_rec.count::numeric / total_wait_event_count::numeric *100.0)::numeric , 2 ) , '000000000000D0000') , '.' , ',' ) ||'|'
									  ;
			line_count=line_count+1; 
			
			IF pct_for_80 > 80.0 
			THEN 
				EXIT;
			END IF;
			
		END LOOP ; --FOR wait_event_rec IN 
	END LOOP ; --FOR wait_event_type_Pi_rec IN 
	

  return result_str ; 
END
$$ LANGUAGE plpgsql  ;
COMMENT ON FUNCTION report_wait_event_type_for_pareto IS 'Сформировать диаграмму Парето по wait_event_type';
-- Сформировать диаграмму Парето по wait_event_type
-------------------------------------------------------------------------------

	


	

--------------------------------------------------------------------------------
-- report_wait_event_type_vmstat.sql
--------------------------------------------------------------------------------
-- report_wait_event_type_vmstat.sql КОРРЕЛЯЦИЯ И ПРИЧИННОСТЬ ОЖИДАНИЙ СУБД и vmstat
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Статистика производительности vmstat
CREATE OR REPLACE FUNCTION report_wait_event_type_vmstat(  cpu_count integer ,  ram_all integer  , cluster_performance_start_timestamp text , cluster_performance_finish_timestamp text   ) RETURNS text[] AS $$
DECLARE
result_str text[] ;
 line_count integer ;
 min_timestamp timestamptz ; 
 max_timestamp timestamptz ; 
 
 counter integer ; 
 min_max_rec record ;
 line_counter integer ; 

 wait_event_type_Pi_rec record;
 correlation_rec record ; 
 corr_values text[];
 report_str text[];
 correlation_regression_flags_rec record ; 
 
 subpart_counter integer ; 
 part integer ;
 reason_casulas_list text[];
 
 r_pct DOUBLE PRECISION;
 cs_pct DOUBLE PRECISION;
 sy_pct DOUBLE PRECISION;
 
 us_sy_pct DOUBLE PRECISION ; 
  
 timestamp_counter integer ;
 
 free_pct DOUBLE PRECISION;
 si_pct DOUBLE PRECISION;
 so_pct DOUBLE PRECISION;
 
 b_pct DOUBLE PRECISION;
 wa_pct DOUBLE PRECISION;
 
 shared_blks_read_write_ratio DOUBLE PRECISION ;
 hit_ratio_rec  record ; 
 
 dirty_percent_rec record ; 
 dirty_bg_percent_rec record ; 
 available_mem_mb_rec record ; 
 dirty_kb_long_rec record;
 
 temp_txt text;
 
 cpi_matrix_rec record ; 
 
BEGIN
	line_count = 1 ;
	
	part = 0 ;
 
 
	IF cluster_performance_finish_timestamp = 'CURRENT_TIMESTAMP'
	THEN 
		SELECT 	date_trunc('minute' ,  to_timestamp( cluster_performance_start_timestamp , 'YYYY-MM-DD HH24:MI' ) ) 
		INTO 	max_timestamp ; 

		min_timestamp = max_timestamp - interval '1 hour'; 	
	ELSE
		SELECT 	date_trunc('minute' ,  to_timestamp( cluster_performance_start_timestamp , 'YYYY-MM-DD HH24:MI' ) ) 
		INTO 	min_timestamp ; 
		
		SELECT 	date_trunc('minute' ,  to_timestamp( cluster_performance_finish_timestamp , 'YYYY-MM-DD HH24:MI' ) ) 
		INTO 	max_timestamp ; 
	END IF ;
	
	--Очистить таблицу для расчета Индекса Приоритета Корреляции (Correlation Priority Index, CPI) .
	TRUNCATE TABLE cpi_matrix;

	
	result_str[line_count] = '3. СТАТИСТИЧЕСКИЙ АНАЛИЗ ОЖИДАНИЙ СУБД и МЕТРИК vmstat' ; 
	line_count=line_count+1;
	
	result_str[line_count] = to_char(min_timestamp , 'YYYY-MM-DD HH24:MI') ;
	line_count=line_count+1; 
	result_str[line_count] = to_char(max_timestamp , 'YYYY-MM-DD HH24:MI') ;
	
	line_count=line_count+2; 
	
	result_str[line_count] = 'CPU = '||cpu_count; 
	line_count=line_count+1;
	result_str[line_count] = 'RAM ='||ROUND((ram_all::numeric / 1024::numeric),3)||'(GB)'; 
	line_count=line_count+2;
	
	----------------------------------------------------------------------------------------------------------------------------------------------	
	-- Граничные значения и медиана 
	SELECT 
		MIN( cl.procs_r_long) AS min_procs_r_long , MAX( cl.procs_r_long) AS max_procs_r_long , (percentile_cont(0.5) within group (order by cl.procs_r_long))::numeric AS median_procs_r_long , 
		MIN( cl.procs_b_long) AS min_procs_b_long , MAX( cl.procs_b_long) AS max_procs_b_long , (percentile_cont(0.5) within group (order by cl.procs_b_long))::numeric AS median_procs_b_long , 		
		MIN( cl.memory_swpd_long) AS min_memory_swpd_long , MAX( cl.memory_swpd_long) AS max_memory_swpd_long ,  (percentile_cont(0.5) within group (order by cl.memory_swpd_long))::numeric AS median_memory_swpd_long ,
		MIN( cl.memory_free_long) AS min_memory_free_long, MAX( cl.memory_free_long) AS max_memory_free_long ,  (percentile_cont(0.5) within group (order by cl.memory_free_long))::numeric AS median_memory_free_long ,
		MIN( cl.memory_buff_long) AS min_memory_buff_long , MAX( cl.memory_buff_long) AS max_memory_buff_long ,  (percentile_cont(0.5) within group (order by cl.memory_buff_long))::numeric AS median_memory_buff_long ,
		MIN( cl.memory_cache_long) AS min_memory_cache_long , MAX( cl.memory_cache_long) AS max_memory_cache_long ,  (percentile_cont(0.5) within group (order by cl.memory_cache_long))::numeric AS median_memory_cache_long ,
		MIN( cl.swap_si_long) AS min_swap_si_long , MAX( cl.swap_si_long) AS max_swap_si_long ,  (percentile_cont(0.5) within group (order by cl.swap_si_long))::numeric AS median_swap_si_long ,
		MIN( cl.swap_so_long) AS min_swap_so_long , MAX( cl.swap_so_long) AS max_swap_so_long , (percentile_cont(0.5) within group (order by cl.swap_so_long))::numeric AS median_swap_so_long ,
		MIN( cl.io_bi_long) AS min_io_bi_long , MAX( cl.io_bi_long) AS max_io_bi_long , (percentile_cont(0.5) within group (order by cl.io_bi_long))::numeric AS median_io_bi_long ,
		MIN( cl.io_bo_long) AS min_io_bo_long , MAX( cl.io_bo_long) AS max_io_bo_long , (percentile_cont(0.5) within group (order by cl.io_bo_long))::numeric AS median_io_bo_long ,
		MIN( cl.system_in_long) AS min_system_in_long , MAX( cl.system_in_long) AS max_system_in_long , (percentile_cont(0.5) within group (order by cl.system_in_long))::numeric AS median_system_in_long ,
		MIN( cl.system_cs_long) AS min_system_cs_long , MAX( cl.system_cs_long) AS max_system_cs_long , (percentile_cont(0.5) within group (order by cl.system_cs_long))::numeric AS median_system_cs_long ,
		MIN( cl.cpu_us_long) AS min_cpu_us_long , MAX( cl.cpu_us_long) AS max_cpu_us_long , (percentile_cont(0.5) within group (order by cl.cpu_us_long))::numeric AS median_cpu_us_long ,
		MIN( cl.cpu_sy_long) AS min_cpu_sy_long , MAX( cl.cpu_sy_long) AS max_cpu_sy_long , (percentile_cont(0.5) within group (order by cl.cpu_sy_long))::numeric AS median_cpu_sy_long ,
		MIN( cl.cpu_id_long) AS min_cpu_id_long , MAX( cl.cpu_id_long) AS max_cpu_id_long , (percentile_cont(0.5) within group (order by cl.cpu_id_long))::numeric AS median_cpu_id_long ,
		MIN( cl.cpu_wa_long) AS min_cpu_wa_long , MAX( cl.cpu_wa_long) AS max_cpu_wa_long , (percentile_cont(0.5) within group (order by cl.cpu_wa_long))::numeric AS median_cpu_wa_long ,
		MIN( cl.cpu_st_long) AS min_cpu_st_long , MAX( cl.cpu_st_long) AS max_cpu_st_long , (percentile_cont(0.5) within group (order by cl.cpu_st_long))::numeric AS median_cpu_st_long 
	INTO  	min_max_rec
	FROM 
		os_stat_vmstat_median cl 
	WHERE 	
		cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 	;	

    line_count=line_count+1;
	result_str[line_count] = 	'ГРАНИЧНЫЕ ЗНАЧЕНИЯ И МЕДИАНА|'||	
								'№'||'|'||									
								'procs_r' ||'|'||
								'procs_b ' ||'|'||
								'memory_swpd ' ||'|'||
								'memory_free ' ||'|'||
								'memory_buff ' ||'|'||
								'memory_cache ' ||'|'||
								'swap_si ' ||'|' ||
								'swap_so ' ||'|' ||
								'io_bi ' ||'|' ||
								'io_bo ' ||'|' ||
								'system_in ' ||'|' ||
								'system_cs ' ||'|' ||
								'cpu_us ' ||'|' ||
								'cpu_sy ' ||'|' ||
								'cpu_id ' ||'|' ||
								'cpu_wa ' ||'|' ||
								'cpu_st ' ||'|' 
								;							
	line_count=line_count+1; 
	
	result_str[line_count] = 	'MIN'||'|'||
								1 ||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_procs_r_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_procs_b_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_memory_swpd_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_memory_free_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_memory_buff_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_memory_cache_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_swap_si_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_swap_so_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_io_bi_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_io_bo_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_system_in_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_system_cs_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_cpu_us_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_cpu_sy_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_cpu_id_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_cpu_wa_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_cpu_st_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'
								;							
	line_count=line_count+1; 
	
	result_str[line_count] = 	'MEDIAN'||'|'||
								' ' ||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.median_procs_r_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.median_procs_b_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.median_memory_swpd_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.median_memory_free_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.median_memory_buff_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.median_memory_cache_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.median_swap_si_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.median_swap_so_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.median_io_bi_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.median_io_bo_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.median_system_in_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.median_system_cs_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.median_cpu_us_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.median_cpu_sy_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.median_cpu_id_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.median_cpu_wa_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.median_cpu_st_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'
								;							
	line_count=line_count+1; 
	
	SELECT 
		count(curr_timestamp)
	INTO line_counter
	FROM 
		cluster_stat_median cl
	WHERE 	
		cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
	
	
	result_str[line_count] = 	'MAX'||'|'||
								line_counter||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_procs_r_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_procs_b_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_memory_swpd_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_memory_free_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_memory_buff_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_memory_cache_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_swap_si_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_swap_so_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_io_bi_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_io_bo_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_system_in_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_system_cs_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_cpu_us_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_cpu_sy_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_cpu_id_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_cpu_wa_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_cpu_st_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'
								;	
	line_count=line_count+1; 								
	-- Граничные значения и медиана 		
	----------------------------------------------------------------------------------------------------------------------------------------------	
	
	SELECT 
		count(curr_timestamp)
	INTO line_counter
	FROM 
		cluster_stat_median cl
	WHERE 	
		cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp ;

	line_count=line_count+1; 								
	result_str[line_count] = 'ОТНОСИТЕЛЬНЫЕ ПОКАЗАТЕЛИ vmstat' ; 
	line_count=line_count+1;		
		
		
	---------------------------------------------------------------------------
	--us + sy > 80%	
	WITH 
	  cpu_counter AS
	  (
		SELECT count(*) AS total_counter
		FROM 
			os_stat_vmstat_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
		WHERE				
			cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
			AND (cpu_us_long + cpu_sy_long ) > 80
	  ) 
	SELECT 
		(total_counter::DOUBLE PRECISION / line_counter::DOUBLE PRECISION)*100.0 
	INTO
		us_sy_pct
	FROM cpu_counter ;
	
	result_str[line_count] = 'us(user time) + sy(system time) (% свыше 80%) | '|| REPLACE ( TO_CHAR( ROUND( us_sy_pct::numeric , 2 ) , '000000000000D0000' ) , '.' , ',' )  ; 
	line_count=line_count+1;
	
	IF us_sy_pct >= 25.0 
	THEN 
		IF us_sy_pct >= 50.0 
		THEN 
			result_str[line_count] = 'ALARM: более 50% тестового периода' ; 
			line_count=line_count+1;		
		ELSE
			result_str[line_count] = 'WARNING: 25-50% тестового периода' ; 
			line_count=line_count+1;
		END IF ;
		result_str[line_count] = 'Высокая нагрузка на CPU из-за сложных запросов (агрегации, JOINs).';
		line_count=line_count+1;
		result_str[line_count] = 'Конкуренция за ресурсы CPU (например, из-за параллельных процессов).';
		line_count=line_count+1;
		result_str[line_count] = 'Резкий рост sy может указывать на проблемы с системными вызовами';
		line_count=line_count+1;		
		result_str[line_count] = '(например, частое переключение контекста).';
		line_count=line_count+1;		
	ELSE
		result_str[line_count] = 'OK: менее 25% тестового периода' ; 
		line_count=line_count+1;		
	END IF;
		
	-----------------------------------------------------------------------------
	--r — процессы в run queue (готовы к выполнению)(% превышение CPU)
	WITH 
	  cpu_counter AS
	  (
		SELECT count(*) AS total_counter
		FROM 
			os_stat_vmstat_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
		WHERE				
			cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
			AND procs_r_long > cpu_count
	  ) 
	SELECT 
		(total_counter::DOUBLE PRECISION / line_counter::DOUBLE PRECISION)*100.0 
	INTO
		r_pct
	FROM cpu_counter ;

	line_count=line_count+1;
	result_str[line_count] = 'r — процессы в run queue (готовы к выполнению): % превышения ядер CPU | '|| REPLACE ( TO_CHAR( ROUND( r_pct::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' )  ; 
	line_count=line_count+1;
	
	IF r_pct < 25.0 
	THEN 
		result_str[line_count] = 'OK: менее 25% тестового периода - очередь процессов превышает количество ядер CPU' ; 
		line_count=line_count+1;
	ELSIF r_pct > 25.0 AND r_pct <= 50.0
	THEN 
		result_str[line_count] = 'WARNING: 25-50% тестового периода - очередь процессов превышает количество ядер CPU' ; 
		line_count=line_count+1;
	ELSE 
		result_str[line_count] = 'ALARM: более 50% тестового периода - очередь процессов превышает количество ядер CPU' ; 
		line_count=line_count+1;
	END IF ;	
	--r — процессы в run queue (готовы к выполнению)(% превышение CPU)
	-----------------------------------------------------------------------------

	
	-----------------------------------------------------------------------------
	-- sy — system time(% превышение 30%)
	WITH 
	  sy_counter AS
	  (
		SELECT count(*) AS total_sy_counter
		FROM 
			os_stat_vmstat_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
		WHERE				
			cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
			AND cpu_sy_long > 30
	  ) 
	SELECT 
		(total_sy_counter::DOUBLE PRECISION / line_counter::DOUBLE PRECISION)*100.0 
	INTO
		sy_pct
	FROM sy_counter ;

	line_count=line_count+1;
	result_str[line_count] = 'sy — system time: % превышение 30% | '|| REPLACE ( TO_CHAR( ROUND( sy_pct::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' )  ; 
	line_count=line_count+1;
	
	IF sy_pct < 25.0 
	THEN 
		result_str[line_count] = 'OK: менее 25% тестового периода - доля system time  превышает 30%' ; 
		line_count=line_count+1;
	ELSIF sy_pct > 25.0 AND sy_pct <= 50.0
	THEN 
		result_str[line_count] = 'WARNING: 25-50% тестового периода - доля system time превышает 30%' ; 
		line_count=line_count+1;
	ELSE 
		result_str[line_count] = 'ALARM: более 50% тестового периода - доля system time превышает 30%' ; 
		line_count=line_count+1;
	END IF ;
	-- sy — system time(% превышение 30%)
	----------------------------------------------------------------------------
	
	-----------------------------------------------------------------------------
	--free — свободная RAM менее 5%
	WITH 
	  free_counter AS
	  (
		SELECT count(*) AS total_counter
		FROM 
			os_stat_vmstat_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
		WHERE				
			cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
			AND memory_free_long::numeric < ( ram_all::numeric * 0.05::numeric )
	  ) 
	SELECT 
		(total_counter::DOUBLE PRECISION / line_counter::DOUBLE PRECISION)*100.0 
	INTO
		free_pct
	FROM free_counter ;

	result_str[line_count] = 'free — свободная RAM  (% менее 5%) | '|| REPLACE ( TO_CHAR( ROUND( free_pct::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' )  ; 
	line_count=line_count+1;
	
	IF free_pct < 25.0 
	THEN 
		result_str[line_count] = 'OK: менее 25% тестового периода - свободная RAM менее 5%' ; 
		line_count=line_count+1;
	ELSIF free_pct > 25.0 AND free_pct <= 50.0
	THEN 
		result_str[line_count] = 'WARNING: 25-50% тестового периода - свободная RAM менее 5%' ; 
		line_count=line_count+1;
	ELSE 
		result_str[line_count] = 'ALARM: более 50% тестового периода - свободная RAM менее 5%' ; 
		line_count=line_count+1;
	END IF ;	
	--free — свободная RAM
	-----------------------------------------------------------------------------	
	
	-----------------------------------------------------------------------------
	--swap_si -- si — swap in (из swap в RAM) > 0 
	WITH 
	  si_counter AS
	  (
		SELECT count(*) AS total_counter
		FROM 
			os_stat_vmstat_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
		WHERE				
			cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
			AND swap_si_long > 0 
	  ) 
	SELECT 
		(total_counter::DOUBLE PRECISION / line_counter::DOUBLE PRECISION)*100.0 
	INTO
		si_pct
	FROM si_counter ;

	line_count=line_count+1;
	result_str[line_count] = 'swap in (% тестового периода) | '|| REPLACE ( TO_CHAR( ROUND( si_pct::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' )  ; 
	line_count=line_count+1;
	
	IF si_pct = 0 
	THEN 
		result_str[line_count] = 'ОК : Свопинг в RAM не используется' ; 
		line_count=line_count+1;	
	ELSIF si_pct < 25.0 
	THEN 
		result_str[line_count] = 'INFO: менее 25% тестового периода - используется cвопинг в RAM' ; 
		line_count=line_count+1;
	ELSIF si_pct > 25.0 AND si_pct <= 50.0
	THEN 
		result_str[line_count] = 'WARNING: 25-50% тестового периода - используется cвопинг в RAM' ;
		line_count=line_count+1;
	ELSE 
		result_str[line_count] = 'ALARM : более 50% тестового периода - используется cвопинг в RAM' ;
		line_count=line_count+1;
	END IF ;	
	--swap_si -- si — swap in (из swap в RAM) > 0 
	-----------------------------------------------------------------------------
	
	-----------------------------------------------------------------------------
	--so — swap out (из RAM в swap) > 0
	WITH 
	  so_counter AS
	  (
		SELECT count(*) AS total_counter
		FROM 
			os_stat_vmstat_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
		WHERE				
			cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
			AND swap_so_long > 0 
	  ) 
	SELECT 
		(total_counter::DOUBLE PRECISION / line_counter::DOUBLE PRECISION)*100.0 
	INTO
		so_pct
	FROM so_counter ;

	line_count=line_count+1;
	result_str[line_count] = 'swap out (% тестового периода) | '|| REPLACE ( TO_CHAR( ROUND( so_pct::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' )  ; 
	line_count=line_count+1;
	
	IF so_pct = 0 
	THEN 
		result_str[line_count] = 'ОК : Свопинг из RAM не используется' ; 
		line_count=line_count+1;	
	ELSIF so_pct < 25.0 
	THEN 
		result_str[line_count] = 'INFO: менее 25% тестового периода - используется cвопинг из RAM' ; 
		line_count=line_count+1;
	ELSIF so_pct > 25.0 AND so_pct <= 50.0
	THEN 
		result_str[line_count] = 'WARNING: 25-50% тестового периода - используется cвопинг из RAM' ;
		line_count=line_count+1;
	ELSE 
		result_str[line_count] = 'ALARM : более 50% тестового периода - используется cвопинг из RAM' ;
		line_count=line_count+1;
	END IF ;	
	--so — swap out (из RAM в swap) > 0
	-----------------------------------------------------------------------------	
	
	----------------------------------------------------------------------------
	-- wa(ожидание IO)
	WITH 
	  wa_counter AS
	  (
		SELECT count(*) AS total_wa_counter
		FROM 
			os_stat_vmstat_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)			
		WHERE				
			cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
			AND cpu_wa_long > 10
	  ) 
	SELECT 
		(total_wa_counter::DOUBLE PRECISION / line_counter::DOUBLE PRECISION)*100.0 
	INTO
		wa_pct
	FROM wa_counter ;

	line_count=line_count+1;
	result_str[line_count] = 'wa(ожидание IO) свыше 10% | '|| REPLACE ( TO_CHAR( ROUND( wa_pct::numeric , 2 ) , '000000000000D0000' ) , '.' , ',' )  ; 
	line_count=line_count+1;
	
	IF wa_pct < 25.0 
	THEN 
		result_str[line_count] = 'OK: менее 25% тестового периода - wa > 10%' ; 
		line_count=line_count+1;
	ELSIF wa_pct > 25.0 AND wa_pct <= 50.0
	THEN 
		result_str[line_count] = 'WARNING: 25-50% тестового периода - wa > 10%' ; 
		line_count=line_count+1;
	ELSE 
		result_str[line_count] = 'ALARM: более 50% тестового периода - wa > 10%' ; 
		line_count=line_count+1;
	END IF ;
	-- wa(ожидание IO)
	----------------------------------------------------------------------------

    -----------------------------------------------------------------------------
	--b — процессы в uninterruptible sleep (обычно ждут IO)(% превышение CPU)
	WITH 
	  cpu_counter AS
	  (
		SELECT count(*) AS total_counter
		FROM 
			os_stat_vmstat_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
		WHERE				
			cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
			AND procs_b_long > cpu_count
	  ) 
	SELECT 
		(total_counter::DOUBLE PRECISION / line_counter::DOUBLE PRECISION)*100.0 
	INTO
		b_pct
	FROM cpu_counter ;
	
	line_count=line_count+1;
	result_str[line_count] = 'b(процессы в uninterruptible sleep): % превышения ядер CPU | '|| REPLACE ( TO_CHAR( ROUND( b_pct::numeric , 2 ) , '000000000000D0000' ) , '.' , ',' )  ; 
	line_count=line_count+1;
	
	IF b_pct < 25.0 
	THEN 
		result_str[line_count] = 'OK: менее 25% тестового периода - b(процессы в uninterruptible sleep) превышает количество ядер CPU' ; 
		line_count=line_count+1;
	ELSIF b_pct > 25.0 AND b_pct <= 50.0
	THEN 
		result_str[line_count] = 'WARNING: 25-50% тестового периода - b(процессы в uninterruptible sleep) превышает количество ядер CPU' ; 
		line_count=line_count+1;
	ELSE 
		result_str[line_count] = 'ALARM: более 50% тестового периода - b(процессы в uninterruptible sleep) превышает количество ядер CPU' ; 
		line_count=line_count+1;
	END IF ;	
	--b — процессы в uninterruptible sleep (обычно ждут IO)(% превышение CPU)
	-----------------------------------------------------------------------------	


	-----------------------------------------------------------------------------------------------------------------------
	--ЧАСТЬ-1. АНАЛИЗ ОЖИДАНИЙ СУБД и МЕТРИК vmstat
	
	
	line_count=line_count+1;
	result_str[line_count] = 'ЧАСТЬ-1. АНАЛИЗ ОЖИДАНИЙ СУБД и МЕТРИК vmstat' ; 
	line_count=line_count+1;
	
	FOR wait_event_type_Pi_rec IN 
	SELECT *
	FROM wait_event_type_Pi
	WHERE integral_priority > 0 
	ORDER BY integral_priority DESC	
	LOOP
		part = part + 1 ;	
		result_str[line_count] = '1.'||part||'. '||wait_event_type_Pi_rec.wait_event_type ||'(ИНТЕГРАЛЬНЫЙ ПРИОРИТЕТ) | '|| REPLACE ( TO_CHAR( ROUND( wait_event_type_Pi_rec.integral_priority::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' );
		line_count=line_count+1;
			
		
		---------------------------------------------------------------
		--2х этапный метод статистического анализа 
		CALL truncate_time_series();
		subpart_counter = 0 ;
		
		-------------------------------------------------------
		-- BufferPin
		IF wait_event_type_Pi_rec.wait_event_type = 'BufferPin'
		THEN 
			INSERT INTO first_time_series ( curr_timestamp , curr_value	)
			SELECT curr_timestamp , curr_bufferpin
			FROM  cluster_stat_median 
			WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;		
			
			
			
			result_str[line_count] = 'ПРИРОДА ОЖИДАНИЯ: Процесс ждет доступа к блоку данных, ' ; 
			line_count=line_count+1;
			result_str[line_count] = ' который в данный момент изменяется другим процессом (пингуется). ' ; 
			line_count=line_count+1;
			result_str[line_count] = ' Конкуренция за буферный кеш. Вероятны сбросы "грязных" страниц на диск.' ; 
			line_count=line_count+1;
			
			--bo (Block Out): Блоки, отправленные на дисковые устройства.
				TRUNCATE TABLE second_time_series ; 
				INSERT INTO second_time_series ( curr_timestamp , curr_value	)
				SELECT curr_timestamp , io_bo_long
				FROM  os_stat_vmstat_median 
				WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
				
				reason_casulas_list := '{}'::text[]; --??????
				reason_casulas_list[1] = '*INFO: Всплески этого показателя могут коррелировать с моментом,' ; 
				reason_casulas_list[2] = ' когда фоновые процессы записывают буферы на диск,' ; 
				reason_casulas_list[3] = ' вызывая задержки снятия пинов.' ; 
				
				subpart_counter = subpart_counter + 1 ;
				SELECT fill_in_comprehensive_analysis_correlation( '1.'||part||'.'||subpart_counter||'. Корреляция: BufferPin и bo(блоки, записанные на устройства)' , 'BufferPin', 'bo(блоки, записанные на устройства)', 1 , reason_casulas_list ) --Анализируется положительная корреляция
				INTO report_str ; 
				
				SELECT result_str || report_str
				INTO result_str ; 
				SELECT array_append( result_str , ' ')
				INTO result_str ;
				SELECT array_length( result_str , 1 )
				INTO line_count;	
				

			--bo (Block Out): Блоки, отправленные на дисковые устройства.

			--swpd (Swap Used): Объем используемого свопа.
				TRUNCATE TABLE second_time_series ; 
				INSERT INTO second_time_series ( curr_timestamp , curr_value	)
				SELECT curr_timestamp , memory_swpd_long
				FROM  os_stat_vmstat_median 
				WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
				
				reason_casulas_list := '{}'::text[]; --??????				
				reason_casulas_list[1] = '*INFO: Если памяти не хватает и буферный кеш страдает,' ; 
				reason_casulas_list[2] = '  это косвенно влияет на конкуренцию за буферы.' ; 

				subpart_counter = subpart_counter + 1 ;
				SELECT fill_in_comprehensive_analysis_correlation( '1.'||part||'.'||subpart_counter||'. Корреляция: BufferPin и swpd(объём свопа)' , 'BufferPin', 'swpd(объём свопа)', 1 , reason_casulas_list ) --Анализируется положительная корреляция
				INTO report_str ; 
				
				SELECT result_str || report_str
				INTO result_str ; 
				SELECT array_append( result_str , ' ')
				INTO result_str ;
				SELECT array_length( result_str , 1 )
				INTO line_count;	


			--swpd (Swap Used): Объем используемого свопа.
			
			
		END IF ;
		-- BufferPin
		-------------------------------------------------------
		
		-------------------------------------------------------
		-- Extension
		IF wait_event_type_Pi_rec.wait_event_type = 'Extension'
		THEN 
			INSERT INTO first_time_series ( curr_timestamp , curr_value	)
			SELECT curr_timestamp , curr_extension
			FROM  cluster_stat_median 
			WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
			
			result_str[line_count] = 'ПРИРОДА ОЖИДАНИЯ: Процесс ожидает события, управляемого кодом расширения ' ; 
			line_count=line_count+1;			
			result_str[line_count] = ' (например, выполнения внешнего скрипта, вызова API, блокировки внутри расширения). ' ; 
			line_count=line_count+1;			
			result_str[line_count] = '  Такие ожидания не являются частью ядра PostgreSQL и могут быть вызваны любой активностью,' ; 
			line_count=line_count+1;
			result_str[line_count] = '   которую реализует расширение.' ; 
			line_count=line_count+1;
			
			--cs(переключения контекста)
				TRUNCATE TABLE second_time_series ; 
				INSERT INTO second_time_series ( curr_timestamp , curr_value	)
				SELECT curr_timestamp , system_cs_long
				FROM  os_stat_vmstat_median 
				WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
				
				reason_casulas_list := '{}'::text[]; --??????				
				reason_casulas_list[1] = '*INFO: Если расширение использует межпроцессное взаимодействие ' ; 
				reason_casulas_list[2] = ' (например, расширение, общающееся с внешним сервисом через сокеты)' ; 

				subpart_counter = subpart_counter + 1 ;
				SELECT fill_in_comprehensive_analysis_correlation( '1.'||part||'.'||subpart_counter||'. Корреляция: Extension и cs(переключения контекста)' , 'Extension', 'cs(переключения контекста)', 1 , reason_casulas_list ) --Анализируется положительная корреляция
				INTO report_str ; 
				
				SELECT result_str || report_str
				INTO result_str ; 
				SELECT array_append( result_str , ' ')
				INTO result_str ;
				SELECT array_length( result_str , 1 )
				INTO line_count;	
			--cs(переключения контекста)
			
			--in — прерывания
				TRUNCATE TABLE second_time_series ; 
				INSERT INTO second_time_series ( curr_timestamp , curr_value	)
				SELECT curr_timestamp , system_in_long
				FROM  os_stat_vmstat_median 
				WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
				
				reason_casulas_list := '{}'::text[]; --??????				
				reason_casulas_list[1] = '*INFO: Если расширение использует межпроцессное взаимодействие  ' ; 
				reason_casulas_list[2] = ' (например, расширение, общающееся с внешним сервисом через сокеты)' ; 
				

				subpart_counter = subpart_counter + 1 ;
				SELECT fill_in_comprehensive_analysis_correlation( '1.'||part||'.'||subpart_counter||'. Корреляция: Extension и in(прерывания)' , 'Extension', 'in(прерывания)', 1 , reason_casulas_list ) --Анализируется положительная корреляция
				INTO report_str ; 
				
				SELECT result_str || report_str
				INTO result_str ; 
				SELECT array_append( result_str , ' ')
				INTO result_str ;
				SELECT array_length( result_str , 1 )
				INTO line_count;	
			--in — прерывания		

			--us(user time)
				TRUNCATE TABLE second_time_series ; 
				INSERT INTO second_time_series ( curr_timestamp , curr_value	)
				SELECT curr_timestamp , cpu_us_long
				FROM  os_stat_vmstat_median 
				WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;

				reason_casulas_list := '{}'::text[]; --??????				
				reason_casulas_list[1] = '*INFO: Если расширение потребляет много процессорного времени ' ; 
				reason_casulas_list[2] = '(например, выполнение сложных вычислений внутри расширения)' ; 

				subpart_counter = subpart_counter + 1 ;
				SELECT fill_in_comprehensive_analysis_correlation( '1.'||part||'.'||subpart_counter||'. Корреляция: Extension и us(user time)' , 'Extension', 'us(user time)', 1 , reason_casulas_list ) --Анализируется положительная корреляция
				INTO report_str ; 
				
				SELECT result_str || report_str
				INTO result_str ; 
				SELECT array_append( result_str , ' ')
				INTO result_str ;
				SELECT array_length( result_str , 1 )
				INTO line_count;	

			--us(user time)		

			--sy(system time)
				TRUNCATE TABLE second_time_series ; 
				INSERT INTO second_time_series ( curr_timestamp , curr_value	)
				SELECT curr_timestamp , cpu_sy_long
				FROM  os_stat_vmstat_median 
				WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;

				reason_casulas_list := '{}'::text[]; --??????				
				reason_casulas_list[1] = '*INFO: Если расширение потребляет много процессорного времени ' ; 
				reason_casulas_list[2] = '(например, выполнение сложных вычислений внутри расширения)' ; 

				subpart_counter = subpart_counter + 1 ;
				SELECT fill_in_comprehensive_analysis_correlation( '1.'||part||'.'||subpart_counter||'. Корреляция: Extension и sy(system time)' , 'Extension', 'sy(system time)', 1 , reason_casulas_list ) --Анализируется положительная корреляция
				INTO report_str ; 
				
				SELECT result_str || report_str
				INTO result_str ; 
				SELECT array_append( result_str , ' ')
				INTO result_str ;
				SELECT array_length( result_str , 1 )
				INTO line_count;	
			--us(user time)		
		
		END IF ;
		-- Extension
		-------------------------------------------------------
		
		-------------------------------------------------------
		-- IO
		IF wait_event_type_Pi_rec.wait_event_type = 'IO'
		THEN 			
			INSERT INTO first_time_series ( curr_timestamp , curr_value	)
			SELECT curr_timestamp , curr_io
			FROM  cluster_stat_median 
			WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
			
			result_str[line_count] = 'ПРИРОДА ОЖИДАНИЯ: Процесс ждет завершения операции ввода-вывода' ; 
			line_count=line_count+1;	
			result_str[line_count] = ' (чтения страницы с диска или записи на диск). Наиболее "дисковый" тип ожидания. ' ; 
			line_count=line_count+1;	
			result_str[line_count] = ' Наиболее "дисковый" тип ожидания. ' ; 
			line_count=line_count+1;	
			
			
			--bi (Blocks In): Блоки, поступившие с диска. Рост bi при появлении событий IO указывает на чтение данных, которых не было в кеше.
				TRUNCATE TABLE second_time_series ; 
				INSERT INTO second_time_series ( curr_timestamp , curr_value	)
				SELECT curr_timestamp , io_bi_long
				FROM  os_stat_vmstat_median 
				WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;

				reason_casulas_list := '{}'::text[]; --??????				
				reason_casulas_list[1] = '*INFO: Рост bi указывает на чтение данных, которых не было в кеше. ' ; 
				

				subpart_counter = subpart_counter + 1 ;
				SELECT fill_in_comprehensive_analysis_correlation( '1.'||part||'.'||subpart_counter||'. Корреляция: IO и bi(блоки, считанные с устройств)' , 'IO', 'bi(блоки, считанные с устройств)', 1 , reason_casulas_list ) --Анализируется положительная корреляция
				INTO report_str ; 
				
				SELECT result_str || report_str
				INTO result_str ; 
				SELECT array_append( result_str , ' ')
				INTO result_str ;
				SELECT array_length( result_str , 1 )
				INTO line_count;	

			--bi (Blocks In): Блоки, поступившие с диска. Рост bi при появлении событий IO указывает на чтение данных, которых не было в кеше.
		
			--bo (Blocks Out): Блоки, отправленные на диск. Указывает на сброс "грязных" страниц (checkpointer, bgwriter).
				line_count=line_count+1;	
				TRUNCATE TABLE second_time_series ; 
				INSERT INTO second_time_series ( curr_timestamp , curr_value	)
				SELECT curr_timestamp , io_bo_long
				FROM  os_stat_vmstat_median 
				WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
				
				reason_casulas_list := '{}'::text[]; --??????				
				reason_casulas_list[1] = '*INFO: Указывает на сброс "грязных" страниц (checkpointer, bgwriter).' ; 
				
				
				subpart_counter = subpart_counter + 1 ;
				SELECT fill_in_comprehensive_analysis_correlation( '1.'||part||'.'||subpart_counter||'. Корреляция: IO и bo(блоки, записанные на устройства)' , 'IO', 'bo(блоки, записанные на устройства)', 1 , reason_casulas_list ) --Анализируется положительная корреляция
				INTO report_str ; 
				
				SELECT result_str || report_str
				INTO result_str ; 
				SELECT array_append( result_str , ' ')
				INTO result_str ;
				SELECT array_length( result_str , 1 )
				INTO line_count;	

			--bo (Blocks Out): Блоки, отправленные на диск. Указывает на сброс "грязных" страниц (checkpointer, bgwriter).
			
			--wa: Процент времени CPU, потраченного на ожидание IO
				line_count=line_count+1;	
				TRUNCATE TABLE second_time_series ; 
				INSERT INTO second_time_series ( curr_timestamp , curr_value	)
				SELECT curr_timestamp , cpu_wa_long
				FROM  os_stat_vmstat_median 
				WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
			
				reason_casulas_list := '{}'::text[]; --??????				
				reason_casulas_list[1] = '*INFO: Рост ожиданий завершения операций IO' ; 
			
				subpart_counter = subpart_counter + 1 ;
				SELECT fill_in_comprehensive_analysis_correlation( '1.'||part||'.'||subpart_counter||'. Корреляция: IO и wa(ожидание IO)' , 'IO', 'wa(ожидание IO)', 1 , reason_casulas_list ) --Анализируется положительная корреляция
				INTO report_str ; 
				
				SELECT result_str || report_str
				INTO result_str ; 
				SELECT array_append( result_str , ' ')
				INTO result_str ;
				SELECT array_length( result_str , 1 )
				INTO line_count;	

			--wa: Процент времени CPU, потраченного на ожидание IO
		END IF ;
		-- IO
		-------------------------------------------------------
		
		-------------------------------------------------------
		-- IPC
		IF wait_event_type_Pi_rec.wait_event_type = 'IPC'
		THEN 
			INSERT INTO first_time_series ( curr_timestamp , curr_value	)
			SELECT curr_timestamp , curr_ipc
			FROM  cluster_stat_median 
			WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
			
			result_str[line_count] = 'ПРИРОДА ОЖИДАНИЯ: Межпроцессное взаимодействие (Inter-Process Communication).' ; 
			line_count=line_count+1;	
			result_str[line_count] = ' Обычно это ожидание ответа от фоновых процессов ' ; 
			line_count=line_count+1;	
			result_str[line_count] = ' (например, WalWriter при отправке синхронной репликации) или сигналов.' ; 
			line_count=line_count+1;	
			
			--cs(переключения контекста)
				TRUNCATE TABLE second_time_series ; 
				INSERT INTO second_time_series ( curr_timestamp , curr_value	)
				SELECT curr_timestamp , system_cs_long
				FROM  os_stat_vmstat_median 
				WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
				
				reason_casulas_list := '{}'::text[]; --??????				
				reason_casulas_list[1] = '*INFO: Высокое число cs часто сопровождает интенсивный обмен ' ;
				reason_casulas_list[2] = ' сигналами между процессами PostgreSQL.' ; 
				reason_casulas_list[3] = '  Рост cs на фоне роста IPC ожиданий — маркер проблемы.' ; 

				subpart_counter = subpart_counter + 1 ;
				SELECT fill_in_comprehensive_analysis_correlation( '1.'||part||'.'||subpart_counter||'. Корреляция: IPC и cs(переключения контекста)' , 'IPC', 'cs(переключения контекста)', 1 , reason_casulas_list ) --Анализируется положительная корреляция
				INTO report_str ; 
				
				SELECT result_str || report_str
				INTO result_str ; 
				SELECT array_append( result_str , ' ')
				INTO result_str ;
				SELECT array_length( result_str , 1 )
				INTO line_count;	

			--cs(переключения контекста)

			--in(прерывания)
				TRUNCATE TABLE second_time_series ; 
				INSERT INTO second_time_series ( curr_timestamp , curr_value	)
				SELECT curr_timestamp , system_in_long
				FROM  os_stat_vmstat_median 
				WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
				
				reason_casulas_list := '{}'::text[]; --??????				
				reason_casulas_list[1] = '*INFO:  Может указывать на активность сети или таймеров. ' ;
				
				subpart_counter = subpart_counter + 1 ;
				SELECT fill_in_comprehensive_analysis_correlation( '1.'||part||'.'||subpart_counter||'. Корреляция: IPC и in(прерывания)' , 'IPC', 'in(прерывания)', 1 , reason_casulas_list ) --Анализируется положительная корреляция
				INTO report_str ; 
				
				SELECT result_str || report_str
				INTO result_str ; 
				SELECT array_append( result_str , ' ')
				INTO result_str ;
				SELECT array_length( result_str , 1 )
				INTO line_count;	
			--in(прерывания)			
			
			--sy(system time)
				TRUNCATE TABLE second_time_series ; 
				INSERT INTO second_time_series ( curr_timestamp , curr_value	)
				SELECT curr_timestamp , cpu_sy_long
				FROM  os_stat_vmstat_median 
				WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
				
				reason_casulas_list := '{}'::text[]; --??????				
				reason_casulas_list[1] = '*INFO: Обработка IPC требует системных вызовов, что увеличивает sy.' ;
				

				subpart_counter = subpart_counter + 1 ;
				SELECT fill_in_comprehensive_analysis_correlation( '1.'||part||'.'||subpart_counter||'. Корреляция: IPC и sy(system time)' , 'IPC', 'sy(system time)', 1 , reason_casulas_list ) --Анализируется положительная корреляция
				INTO report_str ; 
				
				SELECT result_str || report_str
				INTO result_str ; 
				SELECT array_append( result_str , ' ')
				INTO result_str ;
				SELECT array_length( result_str , 1 )
				INTO line_count;	
			--in(прерывания)						
		
		END IF ;
		-- IPC
		-------------------------------------------------------
		
		-------------------------------------------------------
		-- Lock
		IF wait_event_type_Pi_rec.wait_event_type = 'Lock'
		THEN 
			INSERT INTO first_time_series ( curr_timestamp , curr_value	)
			SELECT curr_timestamp , curr_lock
			FROM  cluster_stat_median 
			WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;

			result_str[line_count] = 'ПРИРОДА ОЖИДАНИЯ: Ожидание получения обычной блокировки транзакций' ; 
			line_count=line_count+1;	
			result_str[line_count] = ' (например, блокировка строки или таблицы другим процессом). ' ; 
			line_count=line_count+1;	
			result_str[line_count] = '  Это логическая блокировка, а не физическая.' ; 
			line_count=line_count+1;	
			
			--r(процессы в run queue)
				TRUNCATE TABLE second_time_series ; 
				INSERT INTO second_time_series ( curr_timestamp , curr_value	)
				SELECT curr_timestamp , procs_r_long
				FROM  os_stat_vmstat_median 
				WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;

				reason_casulas_list := '{}'::text[]; --??????				
				reason_casulas_list[1] = '*INFO: Количество процессов, ожидающих выполнения на CPU.' ;
				reason_casulas_list[2] = ' Если много процессов заблокированы (Lock) и ждут снятия блокировки, они не потребляют CPU, ' ; 
				reason_casulas_list[3] = ' но висят в очереди. Высокое r при низком us/sy может указывать на то, ' ; 
				reason_casulas_list[4] = ' что потоки простаивают из-за блокировок.' ; 
				

				subpart_counter = subpart_counter + 1 ;
				SELECT fill_in_comprehensive_analysis_correlation( '1.'||part||'.'||subpart_counter||'. Корреляция: Lock и r(процессы в run queue)' , 'Lock', 'r(процессы в run queue)', 1 , reason_casulas_list ) --Анализируется положительная корреляция
				INTO report_str ; 
				
				SELECT result_str || report_str
				INTO result_str ; 
				SELECT array_append( result_str , ' ')
				INTO result_str ;
				SELECT array_length( result_str , 1 )
				INTO line_count;	
			--r(процессы в run queue)

			--us(user time)
				TRUNCATE TABLE second_time_series ; 
				INSERT INTO second_time_series ( curr_timestamp , curr_value	)
				SELECT curr_timestamp , cpu_us_long
				FROM  os_stat_vmstat_median 
				WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;

				reason_casulas_list := '{}'::text[]; --??????				
				reason_casulas_list[1] = '*INFO: Падение us при росте Lock ожиданий — признак простоя.' ;

				subpart_counter = subpart_counter + 1 ;
				SELECT fill_in_comprehensive_analysis_correlation( '1.'||part||'.'||subpart_counter||'. Корреляция: Lock и us(user time)' , 'Lock', 'us(user time)', -1 , reason_casulas_list ) --Анализируется отрицательная корреляция
				INTO report_str ; 
				
				SELECT result_str || report_str
				INTO result_str ; 
				SELECT array_append( result_str , ' ')
				INTO result_str ;
				SELECT array_length( result_str , 1 )
				INTO line_count;	
			--us(user time)
		END IF ;
		-- Lock
		-------------------------------------------------------
		
		-------------------------------------------------------
		-- LWLock
		IF wait_event_type_Pi_rec.wait_event_type = 'LWLock'
		THEN 
			INSERT INTO first_time_series ( curr_timestamp , curr_value	)
			SELECT curr_timestamp , curr_lwlock
			FROM  cluster_stat_median 
			WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;

			result_str[line_count] = 'ПРИРОДА ОЖИДАНИЯ: Легковесная блокировка (Lightweight Lock). ' ; 
			line_count=line_count+1;	
			result_str[line_count] = ' Защита структур данных в общей памяти (например, буферного кеша, clog).' ; 
			line_count=line_count+1;	
			result_str[line_count] = ' Тесно связана с ядром СУБД.' ; 
			line_count=line_count+1;	
			
			--cs(переключения контекста)
				TRUNCATE TABLE second_time_series ; 
				INSERT INTO second_time_series ( curr_timestamp , curr_value	)
				SELECT curr_timestamp , system_cs_long
				FROM  os_stat_vmstat_median 
				WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
				
				reason_casulas_list := '{}'::text[]; --??????				
				reason_casulas_list[1] = '*INFO: Интенсивная борьба за LWLocks приводит к частой смене активных процессов.' ;
				

				subpart_counter = subpart_counter + 1 ;
				SELECT fill_in_comprehensive_analysis_correlation( '1.'||part||'.'||subpart_counter||'. Корреляция: LWLock и cs(переключения контекста)' , 'LWLock', 'cs(переключения контекста)', 1 , reason_casulas_list ) --Анализируется положительная корреляция
				INTO report_str ; 
				
				SELECT result_str || report_str
				INTO result_str ; 
				SELECT array_append( result_str , ' ')
				INTO result_str ;
				SELECT array_length( result_str , 1 )
				INTO line_count;	
			--cs(переключения контекста)
			
			--sy(system time)
				TRUNCATE TABLE second_time_series ; 
				INSERT INTO second_time_series ( curr_timestamp , curr_value	)
				SELECT curr_timestamp , cpu_sy_long
				FROM  os_stat_vmstat_median 
				WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;

				reason_casulas_list := '{}'::text[]; --??????				
				reason_casulas_list[1] = '*INFO: Управление LWLocks осуществляется через системные примитивы синхронизации, что требует времени ядра.' ;

				subpart_counter = subpart_counter + 1 ;
				SELECT fill_in_comprehensive_analysis_correlation( '1.'||part||'.'||subpart_counter||'. Корреляция: LWLock и sy(system time)' , 'LWLock', 'sy(system time)', 1 , reason_casulas_list ) --Анализируется положительная корреляция
				INTO report_str ; 
				
				SELECT result_str || report_str
				INTO result_str ; 
				SELECT array_append( result_str , ' ')
				INTO result_str ;
				SELECT array_length( result_str , 1 )
				INTO line_count;	
				
			--sy(system time)			

			--swap out (из RAM в swap)
				TRUNCATE TABLE second_time_series ; 
				INSERT INTO second_time_series ( curr_timestamp , curr_value	)
				SELECT curr_timestamp , swap_so_long
				FROM  os_stat_vmstat_median 
				WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
				
				reason_casulas_list := '{}'::text[]; --??????				
				reason_casulas_list[1] = '*INFO: Если начинаются проблемы с памятью и структуры в shared buffers начинают вытесняться (своп),' ;
				reason_casulas_list[2] = '  борьба за LWLock может усилиться.' ;
				

				subpart_counter = subpart_counter + 1 ;
				SELECT fill_in_comprehensive_analysis_correlation( '1.'||part||'.'||subpart_counter||'. Корреляция: LWLock и swap out (из RAM в swap)' , 'LWLock', 'swap out (из RAM в swap)', 1 , reason_casulas_list ) --Анализируется положительная корреляция
				INTO report_str ; 
				
				SELECT result_str || report_str
				INTO result_str ; 
				SELECT array_append( result_str , ' ')
				INTO result_str ;
				SELECT array_length( result_str , 1 )
				INTO line_count;	

			--swap out (из RAM в swap)			

			--swap in (из swap в RAM)
				TRUNCATE TABLE second_time_series ; 
				INSERT INTO second_time_series ( curr_timestamp , curr_value	)
				SELECT curr_timestamp , swap_si_long
				FROM  os_stat_vmstat_median 
				WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
				
				reason_casulas_list := '{}'::text[]; --??????				
				reason_casulas_list[1] = '*INFO: Если начинаются проблемы с памятью и структуры в shared buffers начинают вытесняться (своп),' ;
				reason_casulas_list[2] = '  борьба за LWLock может усилиться.' ;

				subpart_counter = subpart_counter + 1 ;
				SELECT fill_in_comprehensive_analysis_correlation( '1.'||part||'.'||subpart_counter||'. Корреляция: LWLock и swap in (из swap в RAM' , 'LWLock', 'swap in (из swap в RAM)', 1 , reason_casulas_list ) --Анализируется положительная корреляция
				INTO report_str ; 
				
				SELECT result_str || report_str
				INTO result_str ; 
				SELECT array_append( result_str , ' ')
				INTO result_str ;
				SELECT array_length( result_str , 1 )
				INTO line_count;	
			--swap in (из swap в RAM)			
		
		END IF ;
		-- LWLock
		-------------------------------------------------------
		
		-------------------------------------------------------
		-- Timeout
		IF wait_event_type_Pi_rec.wait_event_type = 'Timeout'
		THEN 
			INSERT INTO first_time_series ( curr_timestamp , curr_value	)
			SELECT curr_timestamp , curr_timeout
			FROM  cluster_stat_median 
			WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;

			result_str[line_count] = 'ПРИРОДА ОЖИДАНИЯ: Процесс приостановлен на заданный интервал ' ; 
			line_count=line_count+1;	
			result_str[line_count] = ' (например, при выполнении pg_sleep или внутренних периодических проверок). ' ; 
			line_count=line_count+1;
			result_str[line_count] = 'Эти ожидания, как правило, не создают высокой нагрузки на систему, ' ; 
			line_count=line_count+1;
			result_str[line_count] = ' если они не становятся чрезмерно частыми.' ; 
			line_count=line_count+2;			
			
			--in(прерывания)
				TRUNCATE TABLE second_time_series ; 
				INSERT INTO second_time_series ( curr_timestamp , curr_value	)
				SELECT curr_timestamp , system_in_long
				FROM  os_stat_vmstat_median 
				WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
				
				reason_casulas_list := '{}'::text[]; --??????				
				reason_casulas_list[1] = '*INFO: Таймеры генерируют прерывания, поэтому аномальный рост in' ;
				reason_casulas_list[2] = '   может указывать на слишком большое количество запланированных таймеров ' ;
				

				subpart_counter = subpart_counter + 1 ;
				SELECT fill_in_comprehensive_analysis_correlation( '1.'||part||'.'||subpart_counter||'. Корреляция: Timeout и in(прерывания)' , 'Timeout', 'in(прерывания)', 1 , reason_casulas_list ) --Анализируется положительная корреляция
				INTO report_str ; 
				
				SELECT result_str || report_str
				INTO result_str ; 
				SELECT array_append( result_str , ' ')
				INTO result_str ;
				SELECT array_length( result_str , 1 )
				INTO line_count;	
			--in(прерывания)			

			--cs(переключения контекста)
				TRUNCATE TABLE second_time_series ; 
				INSERT INTO second_time_series ( curr_timestamp , curr_value	)
				SELECT curr_timestamp , system_cs_long
				FROM  os_stat_vmstat_median 
				WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
				
				reason_casulas_list := '{}'::text[]; --??????				
				reason_casulas_list[1] = '*INFO: Пробуждение процессов по таймеру увеличивает число переключений контекста.' ;

				subpart_counter = subpart_counter + 1 ;
				SELECT fill_in_comprehensive_analysis_correlation( '1.'||part||'.'||subpart_counter||'. Корреляция: Timeout и cs(переключения контекста)' , 'Timeout', 'cs(переключения контекста)', 1 , reason_casulas_list ) --Анализируется положительная корреляция
				INTO report_str ; 
				
				SELECT result_str || report_str
				INTO result_str ; 
				SELECT array_append( result_str , ' ')
				INTO result_str ;
				SELECT array_length( result_str , 1 )
				INTO line_count;	
			--cs(переключения контекста)			
/*			
			result_str[line_count] = 'Timeout/SpinDelay' ; 
			line_count=line_count+1;
			result_str[line_count] = 'wait_event=SpinDelay (циклическое ожидание блокировки) – наиболее критично для производительности.' ; 
			line_count=line_count+1;
			result_str[line_count] = 'ПРИРОДА ОЖИДАНИЯ: Процесс активно «крутится» в цикле (spin), ожидая освобождения легковесной блокировки (LWLock).' ; 
			line_count=line_count+1;
			result_str[line_count] = ' В отличие от обычного ожидания, при spin-цикле процессор не переключается на другую задачу, а постоянно проверяет состояние блокировки,' ; 
			line_count=line_count+1;
			result_str[line_count] = '  что приводит к высокому потреблению CPU и увеличению времени отклика. ' ; 
			line_count=line_count+1;
			result_str[line_count] = ' Часто возникает при интенсивной конкуренции за общие структуры данных (например, буферный кеш, clog).' ; 
			line_count=line_count+1;
			result_str[line_count] = ' Часто возникает при интенсивной конкуренции за общие структуры данных (например, буферный кеш, clog).' ; 
			line_count=line_count+1;
			result_str[line_count] = ' Почему SpinDelay наиболее критичен?' ; 
			line_count=line_count+1;
			result_str[line_count] = ' В отличие от Timeout, который обычно не создаёт нагрузки, spin-циклы утилизируют процессор,' ; 
			line_count=line_count+1;
			result_str[line_count] = '  снижая пропускную способность системы. Длительные spin-ожидания могут привести к исчерпанию CPU,' ; 
			line_count=line_count+1;
			result_str[line_count] = '  росту очередей и значительному падению производительности СУБД.' ; 
			line_count=line_count+1;
*/			


			--sy(system time)
				TRUNCATE TABLE second_time_series ; 
				INSERT INTO second_time_series ( curr_timestamp , curr_value	)
				SELECT curr_timestamp , cpu_sy_long
				FROM  os_stat_vmstat_median 
				WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
				
				reason_casulas_list := '{}'::text[]; --??????				
				reason_casulas_list[1] = '*INFO: Увеличивается из-за системных вызовов, связанных с синхронизацией.' ;

				subpart_counter = subpart_counter + 1 ;
				SELECT fill_in_comprehensive_analysis_correlation( '1.'||part||'.'||subpart_counter||'. Корреляция: Timeout и sy(system time)' , 'Timeout', 'sy(system time)', 1 , reason_casulas_list ) --Анализируется положительная корреляция
				INTO report_str ; 
				
				SELECT result_str || report_str
				INTO result_str ; 
				SELECT array_append( result_str , ' ')
				INTO result_str ;
				SELECT array_length( result_str , 1 )
				INTO line_count;	
			--sy(system time)		

			--r(процессы в run queue)
				TRUNCATE TABLE second_time_series ; 
				INSERT INTO second_time_series ( curr_timestamp , curr_value	)
				SELECT curr_timestamp , procs_r_long
				FROM  os_stat_vmstat_median 
				WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
				
				reason_casulas_list := '{}'::text[]; --??????				
				reason_casulas_list[1] = '*INFO: Увеличивается из-за системных вызовов, связанных с синхронизацией.' ;
				

				subpart_counter = subpart_counter + 1 ;
				SELECT fill_in_comprehensive_analysis_correlation( '1.'||part||'.'||subpart_counter||'. Корреляция: Timeout и r(процессы в run queue)' , 'Timeout', 'r(процессы в run queue)', 1 , reason_casulas_list ) --Анализируется положительная корреляция
				INTO report_str ; 
				
				SELECT result_str || report_str
				INTO result_str ; 
				SELECT array_append( result_str , ' ')
				INTO result_str ;
				SELECT array_length( result_str , 1 )
				INTO line_count;	
			--r(процессы в run queue)	
		END IF ;
		-- Timeout
		-------------------------------------------------------
		
		--2х этапный метод статистического анализа 
		---------------------------------------------------------------	
		line_count=line_count+1;			
	END LOOP ;
	--ЧАСТЬ-1. АНАЛИЗ ОЖИДАНИЙ СУБД и МЕТРИК vmstat
	-----------------------------------------------------------------------------------------------------------------------


	-----------------------------------------------------------------------------------------------------------------
	--ЧАСТЬ-2. АНАЛИЗ МЕТРИК vmstat
	line_count=line_count+1;
	result_str[line_count] = 'ЧАСТЬ-2. АНАЛИЗ МЕТРИК vmstat' ; 
	line_count=line_count+1;
    part = 0;	
	subpart_counter = 0 ;	
		
	----------------------------------------------------------------------------
	--1. КОРРЕЛЯЦИЯ system_cs system_in		
	IF min_max_rec.min_system_cs_long != min_max_rec.max_system_cs_long AND 
	   min_max_rec.min_system_in_long != min_max_rec.max_system_in_long
	THEN
		CALL truncate_time_series();
		INSERT INTO first_time_series ( curr_timestamp , curr_value	)
		SELECT 	cl.curr_timestamp , system_cs_long 
		FROM 	os_stat_vmstat_median cl 
		WHERE	cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
		
		INSERT INTO second_time_series ( curr_timestamp , curr_value	)
		SELECT 	cl.curr_timestamp , system_in_long 
		FROM 	os_stat_vmstat_median cl 
		WHERE	cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
		
		reason_casulas_list := '{}'::text[]; --??????				
		reason_casulas_list[1] = '*INFO: Переключения контекста могут быть вызваны прерываниями.' ;
		
		part = part + 1 ;
		SELECT fill_in_comprehensive_analysis_correlation( '2.'||part||'. Корреляция cs(переключения контекста) и in(прерывания)' , 'cs(переключения контекста)', 'in(прерывания)', 1 , reason_casulas_list ) --Анализируется положительная корреляция
		INTO report_str ; 
		
		SELECT result_str || report_str
		INTO result_str ; 
		SELECT array_append( result_str , ' ')
		INTO result_str ;
		SELECT array_length( result_str , 1 )
		INTO line_count;	
	END IF;	
	
	----------------------------------------------------------------------------
	--2. КОРРЕЛЯЦИЯ system_cs cpu_us
	line_count=line_count+1;
	IF min_max_rec.min_system_cs_long != min_max_rec.max_system_cs_long AND 
	   min_max_rec.min_cpu_us_long != min_max_rec.max_cpu_us_long
	THEN
		CALL truncate_time_series();
		INSERT INTO first_time_series ( curr_timestamp , curr_value	)
		SELECT 	cl.curr_timestamp , system_cs_long 
		FROM 	os_stat_vmstat_median cl 
		WHERE	cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
		
		INSERT INTO second_time_series ( curr_timestamp , curr_value	)
		SELECT 	cl.curr_timestamp , cpu_us_long 
		FROM 	os_stat_vmstat_median cl 
		WHERE	cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
		
		reason_casulas_list := '{}'::text[]; --??????				
		reason_casulas_list[1] = '*INFO: Возможно проблема в пользовательском приложении(resource contention).' ;
		
		part = part + 1 ;
		SELECT fill_in_comprehensive_analysis_correlation( '2.'||part||'. Корреляция cs(переключения контекста) и us(user time)' , 'cs(переключения контекста)', 'us(user time)', 1 , reason_casulas_list ) --Анализируется положительная корреляция
		INTO report_str ; 
		
		SELECT result_str || report_str
		INTO result_str ; 
		SELECT array_append( result_str , ' ')
		INTO result_str ;
		SELECT array_length( result_str , 1 )
		INTO line_count;	
	END IF;	

	----------------------------------------------------------------------------
	--3. Корреляция cs(переключения контекста) и sy(system time)	
	line_count=line_count+1;
	IF min_max_rec.min_system_cs_long != min_max_rec.max_system_cs_long AND 
	   min_max_rec.min_cpu_sy_long != min_max_rec.max_cpu_sy_long
	THEN	
		CALL truncate_time_series();
		INSERT INTO first_time_series ( curr_timestamp , curr_value	)
		SELECT 	cl.curr_timestamp , system_cs_long 
		FROM 	os_stat_vmstat_median cl 
		WHERE	cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
		
		INSERT INTO second_time_series ( curr_timestamp , curr_value	)
		SELECT 	cl.curr_timestamp , cpu_sy_long 
		FROM 	os_stat_vmstat_median cl 
		WHERE	cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
		
		reason_casulas_list := '{}'::text[]; --??????				
		reason_casulas_list[1] = '*INFO: Ядро тратит много времени на переключение контекста и планирование,' ;
		reason_casulas_list[2] = '  вместо полезной работы.' ;
		
		part = part + 1 ;
		SELECT fill_in_comprehensive_analysis_correlation( '2.'||part||'. Корреляция cs(переключения контекста) и sy(system time)', 'cs(переключения контекста)', 'sy(system time)', 1 , reason_casulas_list ) --Анализируется положительная корреляция
		INTO report_str ; 
		
		SELECT result_str || report_str
		INTO result_str ; 
		SELECT array_append( result_str , ' ')
		INTO result_str ;
		SELECT array_length( result_str , 1 )
		INTO line_count;		
	END IF;
	
	--ЧАСТЬ-2. АНАЛИЗ МЕТРИК vmstat
	-----------------------------------------------------------------------------------------------------------------
	
	-----------------------------------------------------------------------------------------------------------------
	--ЧАСТЬ-3. АНАЛИЗ IO
	line_count=line_count+1;
	result_str[line_count] = 'ЧАСТЬ-3. АНАЛИЗ IO' ; 
	line_count=line_count+1;
    part = 0;	
	
	-----------------------------------------------------------------------------
	-- Отношение прочитанных блоков к записанным(новые+измененные)
	SELECT 
		CASE 
			WHEN SUM(curr_shared_blks_dirtied) > 0
			THEN ROUND(SUM(curr_shared_blks_read+curr_shared_blks_hit)::numeric / SUM(curr_shared_blks_dirtied), 4)
			ELSE NULL -- избегаем деления на ноль, если нет изменений
		END 
	INTO 
		shared_blks_read_write_ratio
	FROM 
		cluster_stat_median
	WHERE 
		curr_timestamp BETWEEN min_timestamp AND max_timestamp ;	
	
	part=part+1;
    result_str[line_count] = '3.'||part||'. Отношение прочитанных блоков shared_buffers ';
	line_count=line_count+1;	
	result_str[line_count] = '    к измененным блокам shared_buffers |' ||REPLACE ( TO_CHAR( ROUND( shared_blks_read_write_ratio::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' ); 


	IF shared_blks_read_write_ratio IS NULL
	THEN 
		result_str[line_count] = 'Только читающая нагрузка.' ; 
		line_count=line_count+1;		 
	ELSE
		result_str[line_count] = 'Эмпирические ориентиры ' ; 
		line_count=line_count+1;
		result_str[line_count] = ' для оценки типа нагрузки(OLAP/OLTP): ' ; 
		line_count=line_count+1;

		IF shared_blks_read_write_ratio >= 200
		THEN 
			result_str[line_count] = 'OLAP сценарий.' ; 
			line_count=line_count+1;		
		ELSE 
			result_str[line_count] = 'OLTP сценарий.' ; 
			line_count=line_count+1;		
		END IF;
	
	END IF ;
	-- Отношение прочитанных блоков к записанным(новые+измененные)
	-----------------------------------------------------------------------------

	-----------------------------------------------------------------------------
	-- Hit Ratio
	line_count=line_count+1;
	WITH 
	hit_ratio AS
	(
		SELECT 
			( curr_shared_blks_hit / NULLIF(curr_shared_blks_hit + curr_shared_blks_read, 0))*100.0 as value 
		FROM 
			cluster_stat_median
		WHERE 
			curr_timestamp BETWEEN min_timestamp AND max_timestamp
	) 
	SELECT 
		MIN(value) as min_hit_ratio ,
		MAX(value) as max_hit_ratio , 
		(percentile_cont(0.5) within group (order by value))::numeric as median_hit_ratio
	INTO 
		hit_ratio_rec
	FROM 
		hit_ratio ;

	part=part+1;
    result_str[line_count] = '3.'||part||'. SHARED_BUFFERS HIT RATIO | MIN | MEDIAN | MAX | ';
	line_count=line_count+1;
	temp_txt = 	REPLACE ( TO_CHAR( ROUND( hit_ratio_rec.min_hit_ratio::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' )||'|'||
				REPLACE ( TO_CHAR( ROUND( hit_ratio_rec.median_hit_ratio::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' )||'|'||
				REPLACE ( TO_CHAR( ROUND( hit_ratio_rec.max_hit_ratio::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' )||'|' ;
	
	IF hit_ratio_rec.median_hit_ratio >= 99.0 
	THEN 
		result_str[line_count] = 'OK : Идеальный результат для OLTP |'||temp_txt ; 
		line_count=line_count+2;
	ELSIF hit_ratio_rec.median_hit_ratio >= 90.0 AND hit_ratio_rec.median_hit_ratio < 99.0
	THEN 
		result_str[line_count] = 'INFO: приемлемо для OLAP, особенно при работе с большими таблицами |'||temp_txt ;
		line_count=line_count+2;
	ELSIF hit_ratio_rec.median_hit_ratio >= 85.0 AND hit_ratio_rec.median_hit_ratio < 90.0
	THEN 
		result_str[line_count] = 'WARNING: низкое значение HIT RATIO |'||temp_txt ;
		line_count=line_count+2;
	ELSE
		result_str[line_count] = 'ALARM: критически низкое значение HIT RATIO |'||temp_txt ;
		line_count=line_count+2;
	END IF;			 		
	-- Hit Ratio
	-----------------------------------------------------------------------------

	-----------------------------------------------------------------------------
	-- Корреляция операционная скорость - прочитанные блоки
	CALL truncate_time_series();
	INSERT INTO first_time_series ( curr_timestamp , curr_value	)
	SELECT 	curr_timestamp , curr_op_speed
	FROM    cluster_stat_median 
	WHERE	curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
	
	INSERT INTO second_time_series ( curr_timestamp , curr_value	)
	SELECT 	curr_timestamp , curr_shared_blks_read
	FROM  	cluster_stat_median 
	WHERE	curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
	
	SELECT * INTO correlation_rec FROM quick_significance_check();
	
	reason_casulas_list := '{}'::text[]; --??????				
	
	part = part + 1 ;
	SELECT fill_in_comprehensive_analysis_correlation( '3.'||part||'. Корреляция: операционная скорость и прочитанные блоки','операционная скорость', 'прочитанные блоки', 1 , reason_casulas_list ) --Анализируется положительная корреляция
	INTO report_str ; 
	
	SELECT array_append( result_str , ' ')
	INTO result_str ;
	SELECT result_str || report_str
	INTO result_str ; 
	SELECT array_append( result_str , ' ')
	INTO result_str ;
	SELECT array_length( result_str , 1 )
	INTO line_count;
    
	IF correlation_rec.significance_empirical != 'Незначима' AND correlation_rec.significance_t_test != 'Незначима'
	THEN 
		IF correlation_rec.correvation_value >= 0.7 
		THEN				
			result_str[line_count] = 'ALARM : Очень высокая корреляция.' ; 
			line_count=line_count+1;
			result_str[line_count] = 'Рост скорости операций напрямую зависит от роста чтений с диска.' ; 
			line_count=line_count+1;
		ELSIF correlation_rec.correvation_value >= 0.5 AND correlation_rec.correvation_value < 0.7
		THEN 
			result_str[line_count] = 'WARNING : Высокая корреляция.' ; 
			line_count=line_count+1;
			result_str[line_count] = 'Производительности IO - недостаточно для данной нагрузки.' ; 
			line_count=line_count+1;
		ELSE
			result_str[line_count] = 'INFO : Слабая корреляция.' ; 
			line_count=line_count+1;		
		END IF ;
	END IF ;	
	-- Корреляция операционная скорость - прочитанные блоки
	-----------------------------------------------------------------------------
	
	-----------------------------------------------------------------------------
	-- Корреляция операционная скорость - записанные блоки
	CALL truncate_time_series();
	INSERT INTO first_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , curr_op_speed
	FROM  cluster_stat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
	
	INSERT INTO second_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , curr_shared_blks_dirtied+curr_shared_blks_written
	FROM  cluster_stat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;

	SELECT * INTO correlation_rec FROM quick_significance_check();

	reason_casulas_list := '{}'::text[]; --??????				
	part = part + 1 ;
	SELECT fill_in_comprehensive_analysis_correlation( '3.'||part||'. Корреляция: операционная скорость и записанные блоки','операционная скорость', 'записанные блоки', 1 , reason_casulas_list ) --Анализируется положительная корреляция
	INTO report_str ; 
	
	SELECT array_append( result_str , ' ')
	INTO result_str ;
	SELECT result_str || report_str
	INTO result_str ; 
	SELECT array_append( result_str , ' ')
	INTO result_str ;
	SELECT array_length( result_str , 1 )
	INTO line_count;
	
	IF correlation_rec.significance_empirical != 'Незначима' AND correlation_rec.significance_t_test != 'Незначима'
	THEN 
		IF correlation_rec.correvation_value >= 0.7 
		THEN				
			result_str[line_count] = 'ALARM : Очень высокая корреляция .' ; 
			line_count=line_count+1;
			result_str[line_count] = 'Система ограничена производительностью записи на диск' ; 
			line_count=line_count+1;
			result_str[line_count] = 'Точки для анализа: скорость дисков, настройки commit_delay, wal_buffers.' ; 
			line_count=line_count+1;
		ELSIF correlation_rec.correvation_value >= 0.5 AND correlation_rec.correvation_value < 0.7
		THEN 
			result_str[line_count] = 'WARNING : Высокая корреляция.' ; 
			line_count=line_count+1;
			result_str[line_count] = 'Рост операций записи приводит к падению общей скорости.' ; 
			line_count=line_count+1;
		ELSE
			result_str[line_count] = 'INFO : Слабая корреляция.' ; 
			line_count=line_count+1;		
		END IF ;
	END IF ;	
	-- Корреляция операционная скорость - записанные блоки
	-----------------------------------------------------------------------------
	
	
	-----------------------------------------------------------------------------
	-- корреляция shared_blks_hit - shared_blks_read
	CALL truncate_time_series();
	INSERT INTO first_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , curr_shared_blks_hit
	FROM  cluster_stat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
	
	INSERT INTO second_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , curr_shared_blks_read
	FROM  cluster_stat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;

	SELECT * INTO correlation_rec FROM quick_significance_check();
	
	reason_casulas_list := '{}'::text[]; --??????				
	part = part + 1 ;
	SELECT fill_in_comprehensive_analysis_correlation( '3.'||part||'. Корреляция: shared_buffers hit и прочитанные блоки','shared_buffers hit', 'прочитанные блоки', -1 , reason_casulas_list ) --Анализируется положительная корреляция
	INTO report_str ; 
	
	SELECT array_append( result_str , ' ')
	INTO result_str ;
	SELECT result_str || report_str
	INTO result_str ; 
	SELECT array_append( result_str , ' ')
	INTO result_str ;
	SELECT array_length( result_str , 1 )
	INTO line_count;
	
	IF correlation_rec.significance_empirical != 'Незначима' AND correlation_rec.significance_t_test != 'Незначима'
	THEN 
		IF correlation_rec.correvation_value <= -0.7 
		THEN 
			result_str[line_count] = 'OK : Эффективное кэширование.' ; 
			line_count=line_count+1;
			result_str[line_count] = 'Высокая предсказуемость.' ; 
			line_count=line_count+1;
			result_str[line_count] = 'Когда нагрузка попадает в кэш, это реально снижает дисковую нагрузку.' ; 
			line_count=line_count+2;
		END IF;
		
		IF correlation_rec.correvation_value  > -0.7 AND correlation_rec.correvation_value <= -0.3
		THEN 
			result_str[line_count] = 'INFO : Нелинейная зависимость от кэша.' ; 
			line_count=line_count+1;
			result_str[line_count] = 'Нелинейная зависимость от кэша.' ; 
			line_count=line_count+1;
			result_str[line_count] = 'Увеличение hit помогает, но не пропорционально.' ; 
			line_count=line_count+1;
			result_str[line_count] = 'Возможные причины' ; 
			line_count=line_count+1;
			result_str[line_count] = 'Фрагментация доступа: Даже при повторных запросах нужно подчитывать новые данные с диска.' ; 
			line_count=line_count+1;
			result_str[line_count] = 'Конкуренция за кэш: OLTP и аналитические запросы "вытесняют" данные друг друга.' ; 
			line_count=line_count+1;
			result_str[line_count] = 'Разные рабочие наборы: Несколько приложений с разными паттернами доступа.' ; 
			line_count=line_count+2;
		END IF;
		
		IF correlation_rec.correvation_value > -0.3 AND correlation_rec.correvation_value < 0 
		THEN 
			result_str[line_count] = 'WARNING :  Кэширование практически не влияет на дисковую нагрузку.' ; 
			line_count=line_count+1;
			result_str[line_count] = 'Производительность определяется дисковыми характеристиками.' ; 
			line_count=line_count+1;
			result_str[line_count] = 'Кэш работает как буфер, но не как ускоритель.' ; 
			line_count=line_count+1;
			result_str[line_count] = 'Нагрузка: Аналитическая или "сканирующая":' ; 
			line_count=line_count+1;
			result_str[line_count] = 'Рабочий набор >> shared_buffers: Данные читаются один раз и вытесняются.' ; 
			line_count=line_count+1;
			result_str[line_count] = 'Случайные большие запросы: Каждый запрос читает уникальные данные.' ; 
			line_count=line_count+1;
			result_str[line_count] = 'Проблемы с эффективностью кэша: Неправильные настройки autovacuum, много мёртвых кортежей.' ; 
			line_count=line_count+1;
		END IF;		
	END IF ;	
	-- корреляция shared_blks_hit - shared_blks_read
	
	--------------------------------------------------------------------------------------
	--Корреляция: прочитанные блоки - swap in(из swap в RAM)
	CALL truncate_time_series();
	INSERT INTO first_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , curr_shared_blks_read
	FROM  cluster_stat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
	
	INSERT INTO second_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , swap_si_long
	FROM  os_stat_vmstat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;

	SELECT * INTO correlation_rec FROM quick_significance_check();
	
	reason_casulas_list := '{}'::text[]; --??????				
	part = part + 1 ;
	SELECT fill_in_comprehensive_analysis_correlation( '3.'||part||'. Корреляция: прочитанные блоки и swap in','прочитанные блоки', 'swap in', 1 , reason_casulas_list ) --Анализируется положительная корреляция
	INTO report_str ; 
	
	SELECT array_append( result_str , ' ')
	INTO result_str ;
	SELECT result_str || report_str
	INTO result_str ; 
	SELECT array_append( result_str , ' ')
	INTO result_str ;
	SELECT array_length( result_str , 1 )
	INTO line_count;
	
	IF correlation_rec.significance_empirical != 'Незначима' AND correlation_rec.significance_t_test != 'Незначима'
	THEN 
		IF correlation_rec.correvation_value >= 0.7
		THEN				
			result_str[line_count] = 'ALARM';
			line_count=line_count+1;
			result_str[line_count] = 'Чем больше PostgreSQL читает данных с диска,';
			line_count=line_count+1;
			result_str[line_count] = 'тем больше система вынуждена подкачивать страницы из раздела свопа.';
			line_count=line_count+1;
			result_str[line_count] = 'Прямой индикатор нехватки оперативной памяти.';
			line_count=line_count+1;
			
			result_str[line_count] = 'ВОЗМОЖНЫЕ ПРИЧИНЫ:';
			line_count=line_count+1;
			result_str[line_count] = 'Размер shared_buffers и общий кэш ОС превышают доступную физическую память.';
			line_count=line_count+1;
			result_str[line_count] = 'На сервере запущены другие процессы, потребляющие много памяти.';
			line_count=line_count+1;
			result_str[line_count] = 'Неадекватно низкие настройки work_mem, ведущие к свопингу.';
			line_count=line_count+1;
			result_str[line_count] = 'РЕКОМЕНДУЕМЫЕ ДЕЙСТВИЯ:';
			line_count=line_count+1;
			result_str[line_count] = 'Увеличить объем физической ОЗУ на сервере.';
			line_count=line_count+1;
			result_str[line_count] = 'Оптимизировать настройки памяти PostgreSQL (shared_buffers, work_mem).';
			line_count=line_count+1;
			result_str[line_count] = 'Проверить и ограничить память других процессов.';
			line_count=line_count+1;
			result_str[line_count] = 'Увеличить параметр ядра vm.swappiness (временное решение).';
			line_count=line_count+1;						
		END IF ;
	END IF ;
	--Корреляция: прочитанные блоки - swap in(из swap в RAM)
	--------------------------------------------------------------------------------------
	
	--------------------------------------------------------------------------------------
	-- Корреляция: грязные блоки и bo(блоки записанные на устройства)
	CALL truncate_time_series();
	INSERT INTO first_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , curr_shared_blks_dirtied
	FROM  cluster_stat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
	
	INSERT INTO second_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , io_bo_long
	FROM  os_stat_vmstat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;

	SELECT * INTO correlation_rec FROM quick_significance_check();

	reason_casulas_list := '{}'::text[]; --??????				
	part = part + 1 ;
	SELECT fill_in_comprehensive_analysis_correlation( '3.'||part||'. Корреляция: грязные блоки и bo(блоки записанные на устройства)','грязные блоки', 'bo(блоки записанные на устройства)', 1 , reason_casulas_list ) --Анализируется положительная корреляция
	INTO report_str ; 
	
	SELECT array_append( result_str , ' ')
	INTO result_str ;
	SELECT result_str || report_str
	INTO result_str ; 
	SELECT array_append( result_str , ' ')
	INTO result_str ;
	SELECT array_length( result_str , 1 )
	INTO line_count;
	
	IF correlation_rec.significance_empirical != 'Незначима' AND correlation_rec.significance_t_test != 'Незначима'
	THEN
		IF correlation_rec.correvation_value > 0.2 AND correlation_rec.correvation_value < 0.9  
		THEN
			result_str[line_count] = 'ВОЗМОЖНЫЕ ПРИЧИНЫ:';
			line_count=line_count+1;
			result_str[line_count] = 'Неоптимальные настройки контрольных точек (checkpoint_timeout, max_wal_size).';
			line_count=line_count+1;
			result_str[line_count] = 'Слишком агрессивные настройки фоновых писателей (bgwriter_delay, bgwriter_lru_maxpages).';
			line_count=line_count+1;
			result_str[line_count] = 'Медленный диск под WAL или табличным пространством.';
			line_count=line_count+1;
			result_str[line_count] = 'РЕКОМЕНДУЕМЫЕ ДЕЙСТВИЯ:';
			line_count=line_count+1;
			result_str[line_count] = 'Настроить контрольные точки, увеличив checkpoint_timeout и max_wal_size для более плавной записи.';
			line_count=line_count+1;
			result_str[line_count] = 'Отрегулировать параметры bgwriter.';
			line_count=line_count+1;
			result_str[line_count] = 'Мониторить buffers_checkpoint, buffers_clean, buffers_backend в pg_stat_bgwriter.';
			line_count=line_count+1;
		ELSIF correlation_rec.correvation_value >= 0.9
		THEN
			result_str[line_count] = 'ALARM';
			line_count=line_count+1;			
			result_str[line_count] = 'Возможна чрезмерная агрессивная запись.';
			line_count=line_count+1;
		ELSIF correlation_rec.correvation_value <= 0.2
		THEN
			result_str[line_count] = 'ALARM';
			line_count=line_count+1;	
			result_str[line_count] = 'Возможна проблема с отложенной записью,';
			line_count=line_count+1;
			result_str[line_count] = 'ведущая к накоплению грязных страниц в памяти.';
			line_count=line_count+1;					
		END IF;
	END IF ;	
	
	-- Корреляция: грязные блоки и bo(блоки записанные на устройства)
	--------------------------------------------------------------------------------------
	
	--------------------------------------------------------------------------------------
	-- Корреляция: записанные блоки и bo(блоки записанные на устройства)
	CALL truncate_time_series();
	INSERT INTO first_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , curr_shared_blks_written
	FROM  cluster_stat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
	
	INSERT INTO second_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , io_bo_long
	FROM  os_stat_vmstat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;

	SELECT * INTO correlation_rec FROM quick_significance_check();
	
	reason_casulas_list := '{}'::text[]; --??????				
	part = part + 1 ;
	SELECT fill_in_comprehensive_analysis_correlation( '3.'||part||'. Корреляция: записанные блоки и bo(блоки записанные на устройства)','записанные блоки', 'bo(блоки записанные на устройства)', 1 , reason_casulas_list ) --Анализируется положительная корреляция
	INTO report_str ; 
	
	SELECT array_append( result_str , ' ')
	INTO result_str ;
	SELECT result_str || report_str
	INTO result_str ; 
	SELECT array_append( result_str , ' ')
	INTO result_str ;
	SELECT array_length( result_str , 1 )
	INTO line_count;
	
	IF correlation_rec.significance_empirical != 'Незначима' AND correlation_rec.significance_t_test != 'Незначима'
	THEN 
		IF correlation_rec.correvation_value > 0.2 AND correlation_rec.correvation_value < 0.9  
		THEN
			result_str[line_count] = 'ВОЗМОЖНЫЕ ПРИЧИНЫ:';
			line_count=line_count+1;
			result_str[line_count] = 'Неоптимальные настройки контрольных точек (checkpoint_timeout, max_wal_size).';
			line_count=line_count+1;
			result_str[line_count] = 'Слишком агрессивные настройки фоновых писателей (bgwriter_delay, bgwriter_lru_maxpages).';
			line_count=line_count+1;
			result_str[line_count] = 'Медленный диск под WAL или табличным пространством.';
			line_count=line_count+1;
			result_str[line_count] = 'РЕКОМЕНДУЕМЫЕ ДЕЙСТВИЯ:';
			line_count=line_count+1;
			result_str[line_count] = 'Настроить контрольные точки, увеличив checkpoint_timeout и max_wal_size для более плавной записи.';
			line_count=line_count+1;
			result_str[line_count] = 'Отрегулировать параметры bgwriter.';
			line_count=line_count+1;
			result_str[line_count] = 'Мониторить buffers_checkpoint, buffers_clean, buffers_backend в pg_stat_bgwriter.';
			line_count=line_count+1;
		ELSIF correlation_rec.correvation_value >= 0.9
		THEN
			result_str[line_count] = 'ALARM';
			line_count=line_count+1;			
			result_str[line_count] = 'Возможна чрезмерная агрессивная запись.';
			line_count=line_count+1;
		ELSIF correlation_rec.correvation_value <= 0.2
		THEN
			result_str[line_count] = 'ALARM';
			line_count=line_count+1;	
			result_str[line_count] = 'Возможна проблема с отложенной записью,';
			line_count=line_count+1;
			result_str[line_count] = 'ведущая к накоплению грязных страниц в памяти.';
			line_count=line_count+1;					
		END IF;
	END IF ;	
	-- Корреляция: записанные блоки и bo(блоки записанные на устройства)
	--------------------------------------------------------------------------------------
	
	--------------------------------------------------------------------------------------
	--Корреляция: hit и us(user time)
	CALL truncate_time_series();
	INSERT INTO first_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , curr_shared_blks_hit
	FROM  cluster_stat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
	
	INSERT INTO second_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , cpu_us_long
	FROM  os_stat_vmstat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;

	SELECT * INTO correlation_rec FROM quick_significance_check();
	
	reason_casulas_list := '{}'::text[]; --??????				
	part = part + 1 ;
	SELECT fill_in_comprehensive_analysis_correlation( '3.'||part||'. Корреляция: hit и us(user time)','hit', 'us(user time)', -1 , reason_casulas_list ) --Анализируется положительная корреляция
	INTO report_str ; 
	
	SELECT array_append( result_str , ' ')
	INTO result_str ;
	SELECT result_str || report_str
	INTO result_str ; 
	SELECT array_append( result_str , ' ')
	INTO result_str ;
	SELECT array_length( result_str , 1 )
	INTO line_count;
	
	IF correlation_rec.significance_empirical != 'Незначима' AND correlation_rec.significance_t_test != 'Незначима'
	THEN 
		IF correlation_rec.correvation_value <= -0.7
		THEN				
			result_str[line_count] = 'ALARM';
			line_count=line_count+1;
			result_str[line_count] = 'Возможно при высокой нагрузке на CPU';
			line_count=line_count+1;
			result_str[line_count] = 'не хватает памяти для кэша (при падении hit ratio).';
			line_count=line_count+1;
			
			result_str[line_count] = 'ВОЗМОЖНЫЕ ПРИЧИНЫ:';
			line_count=line_count+1;
			result_str[line_count] = 'Нехватка оперативной памяти для кэша (shared_buffers + OS cache).';
			line_count=line_count+1;
			result_str[line_count] = 'Очень тяжелые запросы, вытесняющие полезные данные из кэша.';
			line_count=line_count+1;
			result_str[line_count] = 'РЕКОМЕНДУЕМЫЕ ДЕЙСТВИЯ:';
			line_count=line_count+1;
			result_str[line_count] = 'Увеличение shared_buffers';
			line_count=line_count+1;
			result_str[line_count] = 'Оптимизация запросов';
			line_count=line_count+1;
		END IF;
	END IF ;	
	--Корреляция: hit и us(user time)
	--------------------------------------------------------------------------------------
	
	--------------------------------------------------------------------------------------
	--Корреляция: hit и sy(system time)
	CALL truncate_time_series();
	INSERT INTO first_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , curr_shared_blks_hit
	FROM  cluster_stat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
	
	INSERT INTO second_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , cpu_sy_long
	FROM  os_stat_vmstat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;

	SELECT * INTO correlation_rec FROM quick_significance_check();
	
	reason_casulas_list := '{}'::text[]; --??????				
	part = part + 1 ;
	SELECT fill_in_comprehensive_analysis_correlation( '3.'||part||'. Корреляция: hit и sy(system time)','hit', 'sy(system time)', -1 , reason_casulas_list ) --Анализируется положительная корреляция
	INTO report_str ; 
	
	SELECT array_append( result_str , ' ')
	INTO result_str ;
	SELECT result_str || report_str
	INTO result_str ; 
	SELECT array_append( result_str , ' ')
	INTO result_str ;
	SELECT array_length( result_str , 1 )
	INTO line_count;
	
	IF correlation_rec.significance_empirical != 'Незначима' AND correlation_rec.significance_t_test != 'Незначима'
	THEN 
		IF correlation_rec.correvation_value <= -0.7
		THEN				
			result_str[line_count] = 'ALARM';
			line_count=line_count+1;
			result_str[line_count] = 'Возможно при высокой нагрузке на CPU';
			line_count=line_count+1;
			result_str[line_count] = 'не хватает памяти для кэша (при падении hit ratio).';
			line_count=line_count+1;
			
			result_str[line_count] = 'ВОЗМОЖНЫЕ ПРИЧИНЫ:';
			line_count=line_count+1;
			result_str[line_count] = 'Нехватка оперативной памяти для кэша (shared_buffers + OS cache).';
			line_count=line_count+1;
			result_str[line_count] = 'Очень тяжелые запросы, вытесняющие полезные данные из кэша.';
			line_count=line_count+1;
			result_str[line_count] = 'РЕКОМЕНДУЕМЫЕ ДЕЙСТВИЯ:';
			line_count=line_count+1;
			result_str[line_count] = 'Увеличение shared_buffers';
			line_count=line_count+1;
			result_str[line_count] = 'Оптимизация запросов';
			line_count=line_count+1;
		END IF;
	END IF ;	
	--Корреляция: hit и us(user time)
	--------------------------------------------------------------------------------------
	
	--------------------------------------------------------------------------------------
	--Корреляция: грязные блоки и wa(ожидание IO)
	CALL truncate_time_series();
	INSERT INTO first_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , curr_shared_blks_dirtied
	FROM  cluster_stat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
	
	INSERT INTO second_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , cpu_wa_long
	FROM  os_stat_vmstat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;

	SELECT * INTO correlation_rec FROM quick_significance_check();
	
	reason_casulas_list := '{}'::text[]; --??????				
	part = part + 1 ;
	SELECT fill_in_comprehensive_analysis_correlation( '3.'||part||'. Корреляция: грязные блоки и wa(ожидание IO)','грязные блоки', 'wa(ожидание IO)', 1 , reason_casulas_list ) --Анализируется положительная корреляция
	INTO report_str ; 
	
	SELECT array_append( result_str , ' ')
	INTO result_str ;
	SELECT result_str || report_str
	INTO result_str ; 
	SELECT array_append( result_str , ' ')
	INTO result_str ;
	SELECT array_length( result_str , 1 )
	INTO line_count;
	
	IF correlation_rec.significance_empirical != 'Незначима' AND correlation_rec.significance_t_test != 'Незначима'
	THEN 
		IF correlation_rec.correvation_value >= 0.7
		THEN				
			result_str[line_count] = 'ALARM';
			line_count=line_count+1;
			result_str[line_count] = 'Cистема не справляется с записью грязных страниц.';
			line_count=line_count+1;
			result_str[line_count] = 'Фоновые процессы не успевают, и backend-процессы начинают самостоятельно синхронно записывать данные,';
			line_count=line_count+1;
			result_str[line_count] = 'блокируясь на вводе-выводе.';
			line_count=line_count+1;
			
			result_str[line_count] = 'ВОЗМОЖНЫЕ ПРИЧИНЫ:';
			line_count=line_count+1;
			result_str[line_count] = 'Очень медленные диски (особенно если WAL и данные на одном устройстве).';
			line_count=line_count+1;
			result_str[line_count] = 'Слишком частые или интенсивные контрольные точки.';
			line_count=line_count+1;
			result_str[line_count] = 'Всплеск операций UPDATE/INSERT.';
			line_count=line_count+1;
			result_str[line_count] = 'РЕКОМЕНДУЕМЫЕ ДЕЙСТВИЯ:';				
			line_count=line_count+1;
			result_str[line_count] = 'Срочно оптимизировать подсистему ввода-вывода:';
			line_count=line_count+1;
			result_str[line_count] = 'использовать более быстрые SSD, отделить WAL на отдельный диск.';
			line_count=line_count+1;
			result_str[line_count] = 'Пересмотреть настройки контрольных точек.';
			line_count=line_count+1;
			result_str[line_count] = 'Увеличить параметр bgwriter_lru_maxpages.';
			line_count=line_count+1;
		END IF;
	END IF ;	
	--Корреляция: грязные блоки и wa(ожидание IO)
	--------------------------------------------------------------------------------------
	
	line_count=line_count+2;	
	result_str[line_count] = 'Часть-4. СТАТИСТИКА VM_DIRTY*';
	line_count=line_count+1;
	result_str[line_count] = 'dirty_kb/dirty_ratio/dirty_background_ratio | MIN | MEDIAN | MAX |';
	line_count=line_count+1;
	
	--dirty pages size (KB)
	WITH 
	dirty_kb AS
	(
		SELECT 
			dirty_kb_long as value 
		FROM 
			os_stat_vmstat_median
		WHERE 
			curr_timestamp BETWEEN min_timestamp AND max_timestamp
	) 
	SELECT 
		MIN(value) as min_dirty_kb ,
		MAX(value) as max_dirty_kb , 
		(percentile_cont(0.5) within group (order by value))::numeric as median_dirty_kb
	INTO 
		dirty_kb_long_rec
	FROM 
		dirty_kb ;
	
	result_str[line_count] =  'dirty pages size (KB) |'||REPLACE ( TO_CHAR( ROUND( dirty_kb_long_rec.min_dirty_kb::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' )||'|'|| 
							 REPLACE ( TO_CHAR( ROUND( dirty_kb_long_rec.median_dirty_kb::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' )||'|'||
							 REPLACE ( TO_CHAR( ROUND( dirty_kb_long_rec.max_dirty_kb::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' )||'|';
							 
	line_count=line_count+1;
	

   	--vm_dirty_percent
	WITH 
	vm_dirty_percent AS
	(
		SELECT 
			dirty_percent_long as value 
		FROM 
			os_stat_vmstat_median
		WHERE 
			curr_timestamp BETWEEN min_timestamp AND max_timestamp
	) 
	SELECT 
		MIN(value) as min_dirty_percent ,
		MAX(value) as max_dirty_percent , 
		(percentile_cont(0.5) within group (order by value))::numeric as median_dirty_percent
	INTO 
		dirty_percent_rec
	FROM 
		vm_dirty_percent ;
		
	result_str[line_count] =  'dirty_ratio |'||REPLACE ( TO_CHAR( ROUND( dirty_percent_rec.min_dirty_percent::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' )||'|'|| 
							 REPLACE ( TO_CHAR( ROUND( dirty_percent_rec.median_dirty_percent::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' )||'|'||
							 REPLACE ( TO_CHAR( ROUND( dirty_percent_rec.max_dirty_percent::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' )||'|'; 
							 
	line_count=line_count+1;
	
	--vm_dirty_bg_percent
	WITH 
	vm_dirty_bg_percent AS
	(
		SELECT 
			dirty_bg_percent_long as value 
		FROM 
			os_stat_vmstat_median
		WHERE 
			curr_timestamp BETWEEN min_timestamp AND max_timestamp
	) 
	SELECT 
		MIN(value) as min_dirty_bg_percent ,
		MAX(value) as max_dirty_bg_percent , 
		(percentile_cont(0.5) within group (order by value))::numeric as median_dirty_bg_percent
	INTO 
		dirty_bg_percent_rec
	FROM 
		vm_dirty_bg_percent ;		
	
	result_str[line_count] = 'dirty_bg_percent |'||REPLACE ( TO_CHAR( ROUND( dirty_bg_percent_rec.min_dirty_bg_percent::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' )||'|'|| 
							 REPLACE ( TO_CHAR( ROUND( dirty_bg_percent_rec.median_dirty_bg_percent::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' )||'|'||
							 REPLACE ( TO_CHAR( ROUND( dirty_bg_percent_rec.max_dirty_bg_percent::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' )||'|';  
							 
	line_count=line_count+1;
		
	--available_mem_mb
	WITH 
	available_mem_mb AS
	(
		SELECT 
			available_mem_mb_long as value 
		FROM 
			os_stat_vmstat_median
		WHERE 
			curr_timestamp BETWEEN min_timestamp AND max_timestamp
	) 
	SELECT 
		MIN(value) as min_available_mem_mb ,
		MAX(value) as max_available_mem_mb , 
		(percentile_cont(0.5) within group (order by value))::numeric as median_available_mem_mb
	INTO 
		available_mem_mb_rec
	FROM 
		available_mem_mb ;	    
	
	result_str[line_count] = 'available_mem_mb |'||REPLACE ( TO_CHAR( ROUND( available_mem_mb_rec.min_available_mem_mb::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' )||'|'|| 
							 REPLACE ( TO_CHAR( ROUND( available_mem_mb_rec.median_available_mem_mb::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' )||'|'||
							 REPLACE ( TO_CHAR( ROUND( available_mem_mb_rec.max_available_mem_mb::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' )||'|';  
							 
	line_count=line_count+2;	
	
	part = 0 ;
	---------------------------------------------------------------------
	--Корреляция: dirty pages size(KB) и  so(swap-out)
	CALL truncate_time_series();
	INSERT INTO first_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , dirty_kb_long
	FROM  os_stat_vmstat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
	
	INSERT INTO second_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , swap_so_long
	FROM  os_stat_vmstat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;

	SELECT * INTO correlation_rec FROM quick_significance_check();
	
	reason_casulas_list := '{}'::text[]; --??????				
	part = part + 1 ;
	SELECT fill_in_comprehensive_analysis_correlation( '4.'||part||'. Корреляция: dirty pages size(KB) и so(swap-out)','dirty pages size(KB)', 'so(swap-out)', 1 , reason_casulas_list ) --Анализируется положительная корреляция
	INTO report_str ; 
	
	SELECT array_append( result_str , ' ')
	INTO result_str ;
	SELECT result_str || report_str
	INTO result_str ; 
	SELECT array_append( result_str , ' ')
	INTO result_str ;
	SELECT array_length( result_str , 1 )
	INTO line_count;
	
	IF correlation_rec.significance_empirical != 'Незначима' AND correlation_rec.significance_t_test != 'Незначима'
	THEN 
		IF correlation_rec.correvation_value >= 0.5 
		THEN 
			result_str[line_count] = 'ALARM';
			line_count=line_count+1;
			result_str[line_count] = 'Из-за большого объёма отложенной на запись памяти (dirty_pages)';
			line_count=line_count+1;
			result_str[line_count] = 'система начинает вытеснять страницы процесса PostgreSQL в своп.';
			line_count=line_count+1;
			result_str[line_count] = 'Признак нехватки оперативной памяти  ';
			line_count=line_count+1;
			result_str[line_count] = 'и гарантированное катастрофическое падение производительности.';
			line_count=line_count+1;
			
			result_str[line_count] = 'ОПЕРАТИВНЫЕ МЕРЫ:';
			line_count=line_count+1;
			result_str[line_count] = 'Увеличить shared_buffers';
			line_count=line_count+1;
			result_str[line_count] = 'Проверить и уменьшить work_mem для предотвращения избыточного использования';
			line_count=line_count+1;
			result_str[line_count] = 'Настроить параметры ядра: уменьшить vm.dirty_background_ratio и vm.dirty_ratio';
			line_count=line_count+1;
			result_str[line_count] = 'Рассмотреть добавление оперативной памяти';
			line_count=line_count+1;
			result_str[line_count] = 'ДОЛГОСРОЧНЫЕ РЕШЕНИЯ:';
			line_count=line_count+1;
			result_str[line_count] = 'Оптимизировать запросы с большими сортировками/hash';
			line_count=line_count+1;
			result_str[line_count] = 'Внедрить мониторинг OOM-рисков';
			line_count=line_count+1;
			result_str[line_count] = 'Рассмотреть партиционирование больших таблиц';
			line_count=line_count+1;
		END IF ;
	END IF ;
	
	--Корреляция: dirty pages size (KB) и  so(swap-out)
	---------------------------------------------------------------------
	
	---------------------------------------------------------------------
	--Корреляция: dirty pages size(KB) и wa(ожидание IO)
	CALL truncate_time_series();
	INSERT INTO first_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , dirty_kb_long
	FROM  os_stat_vmstat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
	
	INSERT INTO second_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , cpu_wa_long
	FROM  os_stat_vmstat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;

	SELECT * INTO correlation_rec FROM quick_significance_check();	

	reason_casulas_list := '{}'::text[]; --??????				
	part = part + 1 ;
	SELECT fill_in_comprehensive_analysis_correlation( '4.'||part||'. Корреляция: dirty pages size(KB) и wa(ожидание IO)','dirty pages size(KB)', 'wa(ожидание IO)', 1 , reason_casulas_list ) --Анализируется положительная корреляция
	INTO report_str ; 
	
	SELECT array_append( result_str , ' ')
	INTO result_str ;
	SELECT result_str || report_str
	INTO result_str ; 
	SELECT array_append( result_str , ' ')
	INTO result_str ;
	SELECT array_length( result_str , 1 )
	INTO line_count;
	
	

	IF correlation_rec.significance_empirical != 'Незначима' AND correlation_rec.significance_t_test != 'Незначима'
	THEN 
		IF correlation_rec.correvation_value >= 0.7 
		THEN				
			result_str[line_count] = 'ALARM';
			line_count=line_count+1;
			result_str[line_count] = 'Критическая нехватка пропускной способности подсистемы I/O.';
			line_count=line_count+1;
			result_str[line_count] = 'База данных задыхается при записи checkpoint, WAL или данных.';
			line_count=line_count+1;
			
			result_str[line_count] = 'ОПТИМИЗАЦИЯ ОС И ЖЕЛЕЗА:';
			line_count=line_count+1;
			result_str[line_count] = 'Размещение WAL на отдельном быстром диске (NVMe)';
			line_count=line_count+1;
			result_str[line_count] = 'Использование более быстрого RAID-массива';
			line_count=line_count+1;
			result_str[line_count] = 'Настройка elevator noop или deadline для SSD';
			line_count=line_count+1;
			result_str[line_count] = 'Увеличение параметров ядра vm.dirty_writeback_centisecs';
			line_count=line_count+1;
		END IF ;
	END IF ;	

	--Корреляция: dirty pages size(KB) и wa(ожидание IO)	
	---------------------------------------------------------------------
	
	---------------------------------------------------------------------
	--Корреляция: dirty pages size(KB) и b(процессы в uninterruptible sleep)
	CALL truncate_time_series();
	INSERT INTO first_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , dirty_kb_long
	FROM  os_stat_vmstat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
	
	INSERT INTO second_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , procs_b_long
	FROM  os_stat_vmstat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;

	SELECT * INTO correlation_rec FROM quick_significance_check();

	reason_casulas_list := '{}'::text[]; --??????				
	part = part + 1 ;
	SELECT fill_in_comprehensive_analysis_correlation( '4.'||part||'. Корреляция: dirty pages size(KB) и b(процессы в uninterruptible sleep)','dirty pages size(KB)', 'b(процессы в uninterruptible sleep)', 1 , reason_casulas_list ) --Анализируется положительная корреляция
	INTO report_str ; 
	
	SELECT array_append( result_str , ' ')
	INTO result_str ;
	SELECT result_str || report_str
	INTO result_str ; 
	SELECT array_append( result_str , ' ')
	INTO result_str ;
	SELECT array_length( result_str , 1 )
	INTO line_count;

	
	IF correlation_rec.significance_empirical != 'Незначима' AND correlation_rec.significance_t_test != 'Незначима'
	THEN 
		IF correlation_rec.correvation_value >= 0.7 
		THEN
			result_str[line_count] = 'ALARM';
			line_count=line_count+1;
			result_str[line_count] = 'Процессы СУБД (например, backend-процессы) массово блокируются в состоянии I/O wait.';
			line_count=line_count+1;
			result_str[line_count] = 'Подтверждает корреляцию с wa и требует настройки vm.dirty_* параметров и/или улучшения дисков.';
			line_count=line_count+1;
			result_str[line_count] = 'Очередь процессов в состоянии b указывает на системный I/O bottleneck.';
			line_count=line_count+1;				
		END IF ;
	END IF ;
	--Корреляция: dirty pages size(KB) и b(процессы в uninterruptible sleep)
	---------------------------------------------------------------------
	
	---------------------------------------------------------------------
	-- Корреляция: dirty pages size (KB) и free(свободная RAM)
	CALL truncate_time_series();
	INSERT INTO first_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , dirty_kb_long
	FROM  os_stat_vmstat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
	
	INSERT INTO second_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , memory_free_long
	FROM  os_stat_vmstat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;

	SELECT * INTO correlation_rec FROM quick_significance_check();

	reason_casulas_list := '{}'::text[]; --??????				
	part = part + 1 ;
	SELECT fill_in_comprehensive_analysis_correlation( '4.'||part||'. Корреляция: dirty pages size (KB) и free(свободная RAM)','dirty pages size(KB)', 'free(свободная RAM)', -1 , reason_casulas_list ) --Анализируется положительная корреляция
	INTO report_str ; 
	
	SELECT array_append( result_str , ' ')
	INTO result_str ;
	SELECT result_str || report_str
	INTO result_str ; 
	SELECT array_append( result_str , ' ')
	INTO result_str ;
	SELECT array_length( result_str , 1 )
	INTO line_count;

	
	
	IF correlation_rec.significance_empirical != 'Незначима' AND correlation_rec.significance_t_test != 'Незначима'
	THEN 
		IF correlation_rec.correvation_value <= -0.7 
		THEN			
			result_str[line_count] = 'ALARM';
			line_count=line_count+1;
			result_str[line_count] = 'Система агрессивно использует всю доступную память для кэширования,';
			line_count=line_count+1;
			result_str[line_count] = 'практически не оставляя свободного запаса. Это риск перехода в состояние memory pressure.';
			line_count=line_count+1;
		END IF ;
	END IF ;
	-- Корреляция: dirty pages size (KB) и free(свободная RAM)
	---------------------------------------------------------------------
	
	---------------------------------------------------------------------
	-- Корреляция: dirty pages size(KB) и bo(блоки записанные на устройства)
	CALL truncate_time_series();
	INSERT INTO first_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , dirty_kb_long
	FROM  os_stat_vmstat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
	
	INSERT INTO second_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , io_bo_long
	FROM  os_stat_vmstat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;

	SELECT * INTO correlation_rec FROM quick_significance_check();

	reason_casulas_list := '{}'::text[]; --??????				
	part = part + 1 ;
	SELECT fill_in_comprehensive_analysis_correlation( '4.'||part||'. Корреляция: dirty pages size(KB) и bo(блоки записанные на устройства)','dirty pages size(KB)', 'bo(блоки записанные на устройства)', 1 , reason_casulas_list ) --Анализируется положительная корреляция
	INTO report_str ; 
	
	SELECT array_append( result_str , ' ')
	INTO result_str ;
	SELECT result_str || report_str
	INTO result_str ; 
	SELECT array_append( result_str , ' ')
	INTO result_str ;
	SELECT array_length( result_str , 1 )
	INTO line_count;


	IF correlation_rec.significance_empirical != 'Незначима' AND correlation_rec.significance_t_test != 'Незначима'
	THEN 
		IF correlation_rec.correvation_value >= 0.7 
		THEN		
			result_str[line_count] = 'ALARM';
			line_count=line_count+1;
			result_str[line_count] = 'Механизм обратной записи не успевает за генерацией dirty pages. ';
			line_count=line_count+1;
			result_str[line_count] = 'Это может быть как из-за медленного диска, так и из-за агрессивной работы приложения.';
			line_count=line_count+1;				
		END IF ;
	END IF ;
	
	
	-- Корреляция: dirty pages size(KB) и bo(блоки записанные на устройства)
	---------------------------------------------------------------------
	
	---------------------------------------------------------------------
	-- Корреляция: dirty pages size(KB) и sy(system time)
	CALL truncate_time_series();
	INSERT INTO first_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , dirty_kb_long
	FROM  os_stat_vmstat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
	
	INSERT INTO second_time_series ( curr_timestamp , curr_value	)
	SELECT curr_timestamp , cpu_sy_long
	FROM  os_stat_vmstat_median 
	WHERE curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
	
	SELECT * INTO correlation_rec FROM quick_significance_check();
	
	reason_casulas_list := '{}'::text[]; --??????				
	part = part + 1 ;
	SELECT fill_in_comprehensive_analysis_correlation( '4.'||part||'. Корреляция: dirty pages size(KB) и sy(system time)','dirty pages size(KB)', 'sy(system time)', 1 , reason_casulas_list ) --Анализируется положительная корреляция
	INTO report_str ; 
	
	SELECT array_append( result_str , ' ')
	INTO result_str ;
	SELECT result_str || report_str
	INTO result_str ; 
	SELECT array_append( result_str , ' ')
	INTO result_str ;
	SELECT array_length( result_str , 1 )
	INTO line_count;
	

	IF correlation_rec.significance_empirical != 'Незначима' AND correlation_rec.significance_t_test != 'Незначима'
	THEN 
		IF correlation_rec.correvation_value >= 0.6
		THEN	
			result_str[line_count] = 'ALARM';
			line_count=line_count+1;
			result_str[line_count] = 'Высокие накладные расходы ядра ОС ';
			line_count=line_count+1;
			result_str[line_count] = 'на управление памятью и операциями ввода-вывода.';
			line_count=line_count+1;
			result_str[line_count] = 'Ядро тратит значительное время ';
			line_count=line_count+1;				
			result_str[line_count] = 'на обработку страниц памяти, что может снижать общую производительность.';
			line_count=line_count+1;
		END IF ;
	END IF ;
	-- Корреляция: dirty pages size(KB) и sy(system time
	---------------------------------------------------------------------
	
	
	---------------------------------------------------------------------
	-- Расчитать Матрицу для расчета Индекса Приоритета Корреляции (Correlation Priority Index, CPI)
	CALL calculate_cpi_matrix();
	-- Расчитать Матрицу для расчета Индекса Приоритета Корреляции (Correlation Priority Index, CPI)
	---------------------------------------------------------------------
	
	line_count=line_count+2;
	result_str[line_count] = 'ИНДЕКС ПРИОРИТЕТА КОРРЕЛЯЦИИ (Correlation Priority Index, CPI)';
	line_count=line_count+1;
	result_str[line_count] = 'КОРРЕЛИРУЕМЫЕ ЗНАЧЕНИЯ | CPI ';
	line_count=line_count+1;
	FOR cpi_matrix_rec IN 
	SELECT * FROM cpi_matrix
	ORDER BY curr_value DESC
	LOOP 
		result_str[line_count] = cpi_matrix_rec.current_pair||'|'|| REPLACE ( TO_CHAR( ROUND( cpi_matrix_rec.curr_value::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' );
		line_count=line_count+1;
	END LOOP ;
	
	
	
  return result_str ; 
END
$$ LANGUAGE plpgsql  ;
COMMENT ON FUNCTION report_wait_event_type_vmstat IS 'КОРРЕЛЯЦИЯОЖИДАНИЙ СУБД и vmstat';
-- КОРРЕЛЯЦИЯ ОЖИДАНИЙ СУБД и vmstat
--------------------------------------------------------------------------------


--------------------------------------------------------------------------------
-- stats_proсessing_functions.sql
-- updated 26/03/2026
--------------------------------------------------------------------------------
-- Функции для статистической обработки  данных
--------------------------------------------------------------------------------
-- truncate_time_series БЫСТРАЯ ОЧИСТКА ТАБЛИЦ ВРЕМЕННЫХ РЯДОВ
-- quick_significance_check() БЫСТРАЯ ПРОВЕРКА ЗНАЧИМОСТИ КОРРЕЛЯЦИИ 
-- fill_corr_values_for_positive_corr Заполнить значения корреляции для положительного коэффициента
-- fill_corr_values_for_negative_corr Заполнить значения корреляции для отрицательного коэффициента
-- the_line_of_least_squares() ЛИНИЯ НАИМЕНЬШИХ КВАДРАТОВ для линии регрессии вида Y = a + bt
-- Y_X_regression_line()	ЛИНИЯ НАИМЕНЬШИХ КВАДРАТОВ для линии регрессии вида Y = a + bX
-- interpretation_r2_coefficient  Интерпретация коэффициента детерминации
-- fill_in_wce_activities() ЗАПОЛНЕНИЕ ТАБЛИЦЫ activities
-- fill_in_comprehensive_analysis_wait_event_type Заполнить данные по комплексному анализу ожиданий
-- fill_in_comprehensive_analysis_correlation Заполнить данные по комплексному анализу корреляции
-- calculate_cpi_matrix()  Вычислить значение Индекса Приоритета Корреляции (Correlation Priority Index, CPI)
-- get_wce_activities Получить активности по заданному wait_event_type и значению ВКО 
-- calc_wait_event_type_criteria_weight() Расчет весов критериев для wait_event_type
-- norm_wait_event_type_criteria_matrix() Нормализовать значения в матрице критериев 
-- interpretation_K_coefficient Интерпретация коэффициента тренда 

--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- БЫСТРАЯ ОЧИСТКА ТАБЛИЦ ВРЕМЕННЫХ РЯДОВ
CREATE OR REPLACE PROCEDURE truncate_time_series() 
AS $$
BEGIN	
	TRUNCATE TABLE first_time_series ; 
	TRUNCATE TABLE second_time_series ; 
END
$$ LANGUAGE plpgsql; 
COMMENT ON PROCEDURE truncate_time_series IS 'БЫСТРАЯ ОЧИСТКА ТАБЛИЦ ВРЕМЕННЫХ РЯДОВ';
-- БЫСТРАЯ ОЧИСТКА ТАБЛИЦ ВРЕМЕННЫХ РЯДОВ
--------------------------------------------------------------------------------


--------------------------------------------------------------------------------
-- БЫСТРАЯ ПРОВЕРКА ЗНАЧИМОСТИ КОРРЕЛЯЦИИ
CREATE OR REPLACE FUNCTION quick_significance_check()
RETURNS TABLE (
    correvation_value  DOUBLE PRECISION,
    p_value numeric,
    significance_empirical text,
    significance_t_test text
)
AS $$
DECLARE
    diff_sum numeric := 0;
    diff_sq_sum numeric := 0;
    mean_diff numeric;
    sd_diff numeric;
    t_stat numeric;
    df INT;
    p_val numeric;
    i integer;
    total_n integer;
    t_value_rec record;
    variance_num numeric;   -- числитель дисперсии
BEGIN 

    ------------------------------------------------------------------
    -- Расчет p-value (парный t-критерий)
    SELECT COUNT(*) INTO total_n FROM first_time_series;

    -- Вычисление суммы разностей и суммы квадратов разностей
    FOR t_value_rec IN
        SELECT t1.curr_value AS t1_value, t2.curr_value AS t2_value
        FROM first_time_series t1
        JOIN second_time_series t2 ON t1.curr_timestamp = t2.curr_timestamp
        ORDER BY t1.curr_timestamp
    LOOP
        diff_sum := diff_sum + (t_value_rec.t1_value - t_value_rec.t2_value);
        diff_sq_sum := diff_sq_sum + (t_value_rec.t1_value - t_value_rec.t2_value)^2;
    END LOOP;

    IF total_n > 1 THEN
        -- Среднее разностей
        mean_diff := diff_sum / total_n;

        -- Дисперсия (несмещённая)
        variance_num := diff_sq_sum - (diff_sum * diff_sum) / total_n;
        IF variance_num < 0 THEN
            variance_num := 0;   -- защита от погрешностей
        END IF;

        sd_diff := sqrt(variance_num / (total_n - 1));

        IF sd_diff = 0 THEN
            -- Все разности одинаковы → нет значимых отличий
            p_val := 1.0;
        ELSE
            t_stat := mean_diff / (sd_diff / sqrt(total_n));
            df := total_n - 1;
            p_val := 2 * (1 - student_t_cdf(t_stat, df));
        END IF;
    ELSE
        -- Недостаточно наблюдений для расчёта
        p_val := NULL;
    END IF;

    ------------------------------------------------------------------
    RETURN QUERY
    WITH stats AS (
        SELECT
            COALESCE(CORR(v2.curr_value, v1.curr_value), 0) as r_raw,
            COUNT(*) as n
        FROM first_time_series v1
        JOIN second_time_series v2 ON v1.curr_timestamp = v2.curr_timestamp
    ),
    clipped AS (
        SELECT
            r_raw,
            GREATEST(-1, LEAST(1, r_raw)) as r,
            n
        FROM stats
    )
    SELECT
        r_raw AS correvation_value,
        p_val AS p_value,
        CASE
            WHEN n > 30 AND ABS(r) > 2 / SQRT(n) THEN 'Значима (p < ~0.05)'
            WHEN n > 100 AND ABS(r) > 1.65 / SQRT(n) THEN 'Значима (p < ~0.1)'
            WHEN n > 10 AND ABS(r) > 3 / SQRT(n) THEN 'Значима (p < ~0.01)'
            ELSE 'Незначима'
        END as significance_empirical,
        CASE
            WHEN n < 3 OR r IS NULL THEN 'Недостаточно данных'
            WHEN ABS(r) >= 1 - 1e-12 THEN 'Значима (максимальная корреляция)'
            WHEN ABS(r * SQRT((n - 2) / (1 - r*r))) > 1.96 THEN 'Значима (95% уровень)'
            WHEN ABS(r * SQRT((n - 2) / (1 - r*r))) > 1.645 THEN 'Значима (90% уровень)'
            ELSE 'Незначима'
        END as significance_t_test
    FROM clipped;

END
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION quick_significance_check IS 'БЫСТРАЯ ПРОВЕРКА ЗНАЧИМОСТИ КОРРЕЛЯЦИИ';
-- БЫСТРАЯ ПРОВЕРКА ЗНАЧИМОСТИ КОРРЕЛЯЦИИ
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--Вспомогательная функция student_t_cdf и реализация неполной бета-функции:
CREATE OR REPLACE FUNCTION student_t_cdf(t numeric, df INT)
RETURNS numeric
LANGUAGE plpgsql
AS $$
DECLARE
    x numeric;
    a numeric;
    b numeric := 0.5;
    ibeta numeric;
BEGIN
    IF df <= 0 THEN
        --RAISE EXCEPTION 'Степени свободы должны быть положительными';
    END IF;

    IF t >= 0 THEN
        x := df / (df + t^2);
        a := df::numeric / 2;
        ibeta := incomplete_beta(x, a, b);
        RETURN 1 - 0.5 * ibeta;
    ELSE
        x := df / (df + t^2);
        a := df::numeric / 2;
        ibeta := incomplete_beta(x, a, b);
        RETURN 0.5 * ibeta;
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION incomplete_beta(x numeric, a numeric, b numeric)
RETURNS numeric
LANGUAGE plpgsql
AS $$
DECLARE
    max_iter INT := 200;
    tol numeric := 1e-12;
    m INT;
    aa numeric;
    bb numeric;
    c numeric;
    d numeric;
    del numeric;
    h numeric;
    qab numeric;
    qam numeric;
    qap numeric;
BEGIN
    IF x < 0 OR x > 1 THEN
        --RAISE EXCEPTION 'x должен быть в [0,1]';
    END IF;

    IF x = 0 THEN
        RETURN 0;
    ELSIF x = 1 THEN
        RETURN 1;
    END IF;

    qab := a + b;
    qap := a + 1;
    qam := a - 1;
    c := 1;
    d := 1 - qab * x / qap;
    IF abs(d) < 1e-30 THEN
        d := 1e-30;
    END IF;
    d := 1 / d;
    h := d;

    FOR m IN 1..max_iter LOOP
        aa := m * (b - m) * x / ((qam + 2*m) * (a + 2*m));
        d := 1 + aa * d;
        IF abs(d) < 1e-30 THEN
            d := 1e-30;
        END IF;
        c := 1 + aa / c;
        IF abs(c) < 1e-30 THEN
            c := 1e-30;
        END IF;
        d := 1 / d;
        h := h * d * c;

        aa := -(a + m) * (qab + m) * x / ((a + 2*m) * (qap + 2*m));
        d := 1 + aa * d;
        IF abs(d) < 1e-30 THEN
            d := 1e-30;
        END IF;
        c := 1 + aa / c;
        IF abs(c) < 1e-30 THEN
            c := 1e-30;
        END IF;
        d := 1 / d;
        del := d * c;
        h := h * del;

        IF abs(del - 1) < tol THEN
            EXIT;
        END IF;
    END LOOP;

    RETURN h * exp(a * ln(x) + b * ln(1 - x) - (log_gamma(a) + log_gamma(b) - log_gamma(a+b)));
END;
$$;
--Модифицированная функция incomplete_beta 
CREATE OR REPLACE FUNCTION log_gamma(z numeric)
RETURNS numeric
LANGUAGE plpgsql
AS $$
DECLARE
    -- Коэффициенты Lanczos для g=7, n=9
    p numeric[] := ARRAY[
        0.99999999999980993,
        676.5203681218851,
        -1259.1392167224028,
        771.32342877765313,
        -176.61502916214059,
        12.507343278686905,
        -0.13857109526572012,
        9.9843695780195716e-6,
        1.5056327351493116e-7
    ];
    g numeric := 7;
    i INT;
    x numeric := z;
    y numeric;
    t numeric;
    s numeric;
BEGIN
    IF x < 0.5 THEN
        -- Используем формулу отражения для отрицательных/малых аргументов
        RETURN log(pi()) - log(sin(pi() * x)) - log_gamma(1 - x);
    END IF;

    x := x - 1;
    y := x + g + 0.5;
    s := p[1];

    FOR i IN 2..array_length(p,1) LOOP
        s := s + p[i] / (x + i - 1);
    END LOOP;

    t := x + g + 0.5;
    RETURN (x + 0.5) * ln(t) - t + ln(sqrt(2 * pi())) + ln(s);
END;
$$;
/*
Пояснения
Приближение Ланцоша выбрано как стандартный способ вычисления логарифма гамма-функции с высокой точностью. 
Коэффициенты взяты для g=7 и обеспечивают погрешность менее 10⁻¹⁵ для всех z > 0. 
Для аргументов менее 0.5 использована формула отражения, что расширяет область применимости.

Замена lgamma на log_gamma произведена в финальном вычислении фактора factor. Все остальные части алгоритма неполной бета-функции остались без изменений.

Проверка граничных случаев сохранена для обеспечения корректной работы при x=0 и x=1.
*/
/*
Примечания
Функция использует PL/pgSQL и встроенные математические функции (sqrt, power, ln, exp, lgamma – доступна с PostgreSQL 8.4).

Алгоритм неполной бета-функции сходится для всех x, кроме крайних значений; точность контролируется параметрами tol и max_iter.

При очень малых объёмах выборки и экстремальных t-статистиках может потребоваться увеличение числа итераций.

Функция lgamma (логарифм гамма-функции) доступна в PostgreSQL, что обеспечивает численную устойчивость.

Если необходим критерий корреляции Пирсона с p-значением, можно построить аналогичную функцию, 
вычислив коэффициент корреляции r и используя ту же student_t_cdf со статистикой t = r * sqrt((n-2)/(1-r^2)) и степенями свободы n-2.
*/
--Вспомогательная функция student_t_cdf и реализация неполной бета-функции:
--------------------------------------------------------------------------------




-------------------------------------------------------------------------------
-- Заполнить значения корреляции для положительного коэффициента
CREATE OR REPLACE FUNCTION fill_corr_values_for_positive_corr( correlation_rec record  ) RETURNS text[] AS $$
DECLARE
 result_str text[] ;
 line_count integer ;
BEGIN
	line_count = 1 ;
	
	IF  correlation_rec.correvation_value IS NULL OR correlation_rec.correvation_value <=0 
	THEN 
		result_str[line_count] = '  Отрицательная или отсутствует' ; 
		line_count=line_count+1;			 
	ELSE			
		UPDATE correlation_regression_flags SET correlation_flag = TRUE ;
		result_str[line_count] = '  КОЭФФИЦИЕНТ КОРРЕЛЯЦИИ: |'||REPLACE ( TO_CHAR( ROUND( correlation_rec.correvation_value::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' );
		line_count=line_count+1;	
		result_str[line_count] = '  КОЭФФИЦИЕНТ ЗНАЧИМОСТИ ОЦЕНКИ(эмпирическое правило): |'||correlation_rec.significance_empirical;
		line_count=line_count+1;	
		result_str[line_count] = '  КОЭФФИЦИЕНТ ЗНАЧИМОСТИ ОЦЕНКИ(t-критерий): |'||correlation_rec.significance_t_test;
		line_count=line_count+1;
	END IF;
 
  return result_str ; 
END
$$ LANGUAGE plpgsql  ;
COMMENT ON FUNCTION fill_corr_values_for_positive_corr IS 'Заполнить значения корреляции для положительного коэффициента';
-- Заполнить значения корреляции для положительного коэффициента
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Заполнить значения корреляции для отрицательного коэффициента
CREATE OR REPLACE FUNCTION fill_corr_values_for_negative_corr( correlation_rec record  ) RETURNS text[] AS $$
DECLARE
 result_str text[] ;
 line_count integer ;
BEGIN
	line_count = 1 ;
	
	IF  correlation_rec.correvation_value IS NULL OR correlation_rec.correvation_value >=0 
	THEN 
		result_str[line_count] = '  Положительная или отсутствует' ; 
		line_count=line_count+1;			 
	ELSE	
		UPDATE correlation_regression_flags SET correlation_flag = TRUE ;	
		result_str[line_count] = '  КОЭФФИЦИЕНТ КОРРЕЛЯЦИИ: |'||REPLACE ( TO_CHAR( ROUND( correlation_rec.correvation_value::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' );
		line_count=line_count+1;	
		result_str[line_count] = '  КОЭФФИЦИЕНТ ЗНАЧИМОСТИ ОЦЕНКИ(эмпирическое правило): |'||correlation_rec.significance_empirical;
		line_count=line_count+1;	
		result_str[line_count] = '  КОЭФФИЦИЕНТ ЗНАЧИМОСТИ ОЦЕНКИ(t-критерий): |'||correlation_rec.significance_t_test;
		line_count=line_count+1;
	END IF;
 
  return result_str ; 
END
$$ LANGUAGE plpgsql  ;
COMMENT ON FUNCTION fill_corr_values_for_negative_corr IS 'Заполнить значения корреляции для отрицательного коэффициента';
-- Заполнить значения корреляции для положительного коэффициента
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- ЛИНИЯ НАИМЕНЬШИХ КВАДРАТОВ для линии регрессии вида Y = a + bt
CREATE OR REPLACE FUNCTION the_line_of_least_squares()
RETURNS TABLE (
    current_slope DOUBLE PRECISION,
    slope_angle_degrees DOUBLE PRECISION,
    current_r_squared DOUBLE PRECISION
)
AS $$
BEGIN  
    DROP TABLE IF EXISTS tmp_timepoints;
    CREATE TEMPORARY TABLE tmp_timepoints (
        curr_timestamp timestamptz,
        curr_timepoint integer
    );

    INSERT INTO tmp_timepoints (curr_timestamp, curr_timepoint)
    SELECT
        cl.curr_timestamp,
        row_number() OVER (ORDER BY cl.curr_timestamp) AS x
    FROM first_time_series cl
    ORDER BY cl.curr_timestamp;

    BEGIN
        RETURN QUERY
        WITH stats AS (
            SELECT
                AVG(t.curr_timepoint::DOUBLE PRECISION) AS avg1,
                STDDEV(t.curr_timepoint::DOUBLE PRECISION) AS std1,
                AVG(s.curr_value::DOUBLE PRECISION) AS avg2,
                STDDEV(s.curr_value::DOUBLE PRECISION) AS std2
            FROM first_time_series s
            JOIN tmp_timepoints t ON s.curr_timestamp = t.curr_timestamp
        ),
        standardized_data AS (
            SELECT
                (t.curr_timepoint::DOUBLE PRECISION - avg1) / std1 AS x_z,
                (s.curr_value::DOUBLE PRECISION - avg2) / std2 AS y_z
            FROM first_time_series s
            JOIN tmp_timepoints t ON s.curr_timestamp = t.curr_timestamp
            CROSS JOIN stats
        )
        SELECT
            REGR_SLOPE(y_z, x_z) AS slope,
            ATAN(REGR_SLOPE(y_z, x_z)) * 180 / PI() AS slope_angle_degrees,
            REGR_R2(y_z, x_z) AS r_squared
        FROM standardized_data;
    EXCEPTION
        WHEN division_by_zero THEN
            RETURN QUERY
            SELECT
                1.0::DOUBLE PRECISION AS slope,
                0.0::DOUBLE PRECISION AS slope_angle_degrees,
                0.0::DOUBLE PRECISION AS r_squared;
    END;
END
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION the_line_of_least_squares IS 'ЛИНИЯ НАИМЕНЬШИХ КВАДРАТОВ для линии регрессии вида Y = a + bt';
----------------------------------------------------------------------------------
-- 	ЛИНИЯ НАИМЕНЬШИХ КВАДРАТОВ для линии регрессии вида Y = a + bX
CREATE OR REPLACE FUNCTION Y_X_regression_line() RETURNS 
TABLE 
(
	current_slope  DOUBLE PRECISION , 
	slope_angle_degrees DOUBLE PRECISION , 
	current_r_squared numeric 	
)
AS $$ 
BEGIN	
	BEGIN
		RETURN QUERY 
		WITH stats AS 
		(
		  SELECT 
			AVG(X.curr_value::DOUBLE PRECISION) as avg1, 
			STDDEV(X.curr_value::DOUBLE PRECISION) as std1,
			AVG(Y.curr_value::DOUBLE PRECISION) as avg2, 
			STDDEV(Y.curr_value::DOUBLE PRECISION) as std2
		  FROM
			first_time_series Y JOIN second_time_series X ON ( Y.curr_timestamp  = X.curr_timestamp )
		),
		standardized_data AS 
		(
			SELECT 
				(X.curr_value::DOUBLE PRECISION - avg1) / std1 as x_z,
				(Y.curr_value::DOUBLE PRECISION - avg2) / std2 as y_z
			FROM
				first_time_series Y JOIN second_time_series X ON ( Y.curr_timestamp  = X.curr_timestamp ) , stats
		)	
		SELECT
			REGR_SLOPE(y_z, x_z) as slope, --b
			ATAN(REGR_SLOPE(y_z, x_z)) * 180 / PI() as slope_angle_degrees, --угол наклона
			ROUND( REGR_R2(y_z, x_z)::numeric , 2 ) as r_squared -- Коэффициент детерминации
		FROM standardized_data;
	EXCEPTION
	  --STDDEV(Y.op_speed_long::DOUBLE PRECISION) = 0  
	  WHEN division_by_zero THEN  -- Конкретное исключение для деления на ноль
		RETURN QUERY 
		SELECT 
			1.0::DOUBLE PRECISION as slope, --b
			0.0::DOUBLE PRECISION  as slope_angle_degrees, --угол наклона
			0.0::numeric  as r_squared ; -- Коэффициент детерминации
	END;
END
$$ LANGUAGE plpgsql; 
COMMENT ON FUNCTION Y_X_regression_line IS 'ЛИНИЯ НАИМЕНЬШИХ КВАДРАТОВ для линии регрессии вида Y = a + bX';
----------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Интерпретация коэффициента детерминации
CREATE OR REPLACE FUNCTION interpretation_r2_coefficient ( r_squared numeric  ) RETURNS text[] AS $$
DECLARE
 result_str text[] ;
 line_count integer ;
BEGIN
	line_count = 1 ;
	
	IF r_squared >= 0.8 
	THEN 
		result_str[line_count] = '     Качество модели: Очень высокое.' ; 
		line_count=line_count+1;
		result_str[line_count] = '     Интерпретация: Модель объясняет более 80% дисперсии зависимой переменной.' ; 
		line_count=line_count+1;
		result_str[line_count] = '     Вывод: Связь очень сильная, прогнозная способность высокая.' ; 
		line_count=line_count+1;	
	END IF ;
	
	IF r_squared >= 0.6 AND  r_squared < 0.8
	THEN 
		result_str[line_count] = '     Качество модели: Хорошее.' ; 
		line_count=line_count+1;
		result_str[line_count] = '     Интерпретация: Модель объясняет от 60% до 80% вариации.' ; 
		line_count=line_count+1;
		result_str[line_count] = '     Вывод: Достоверная и практически полезная модель.' ; 
		line_count=line_count+1;	
	END IF ;
	
	IF r_squared >= 0.4 AND  r_squared < 0.6
	THEN  
		result_str[line_count] = '     Качество модели: Удовлетворительное.' ; 
		line_count=line_count+1;
		result_str[line_count] = '     Интерпретация: Модель объясняет от 40% до 60% дисперсии.' ; 
		line_count=line_count+1;
		result_str[line_count] = '     Вывод: Модель пригодна для описания и проверки гипотез,' ; 
		line_count=line_count+1;
		result_str[line_count] = '      но для точного прогнозирования нуждается в доработке.' ; 
		line_count=line_count+1;
	END IF ;
	
	IF r_squared >= 0.2 AND  r_squared < 0.4
	THEN 
		result_str[line_count] = '     Качество модели: Слабое.' ; 
		line_count=line_count+1;		
		result_str[line_count] = '     Интерпретация: Модель объясняет менее 40%, но более 20% вариации.' ; 
		line_count=line_count+1;		
		result_str[line_count] = '     Вывод: Влияние факторов подтверждено, но модель ничего не предсказывает. ' ; 
		line_count=line_count+1;			
		result_str[line_count] = '      Годится только для констатации факта наличия связи.' ; 
		line_count=line_count+1;			
	END IF ;
	
	IF r_squared < 0.2 
	THEN 
		result_str[line_count] = '     Качество модели: Неудовлетворительное.' ; 
		line_count=line_count+1;
		result_str[line_count] = '     Интерпретация: Модель объясняет менее 20% (вплоть до 0%) вариации.' ; 
		line_count=line_count+1;
		result_str[line_count] = '     Вывод: Модель бесполезна. Коэффициенты, даже если они значимы, ' ; 
		line_count=line_count+1;	
		result_str[line_count] = '      ничего не объясняют с практической точки зрения.' ; 
		line_count=line_count+1;	
	END IF ;
	
 
  return result_str ; 
END
$$ LANGUAGE plpgsql  ;
COMMENT ON FUNCTION interpretation_r2_coefficient IS 'Интерпретация коэффициента детерминации';
-- Интерпретация коэффицинета детерминации
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Интерпретация коэффициента тренда 
/*
Обоснование границ
•	≥ 30 – соответствует очень высокой скорости изменения (например, рост очереди процессов на 30 за единицу времени) при отличной объясняющей способности модели (R2>0,8). Такие значения редко встречаются и указывают на серьёзные проблемы.
•	20–30 – сильные тренды, характерные для систем с высокой нагрузкой, требуют внимания.
•	10–20 – умеренные тренды, часто связаны с сезонными или постепенными изменениями.
•	5–10 – слабые тренды, могут быть вызваны флуктуациями.
•	< 5 – шум или статистически незначимые изменения.
*/
CREATE OR REPLACE FUNCTION interpretation_K_coefficient ( K_value DOUBLE PRECISION  ) RETURNS text[] AS $$
DECLARE
 result_str text[] ;
 line_count integer ;
BEGIN
	line_count = 1 ;
	
	result_str[line_count] = 'Коэффициента Тренда |'|| REPLACE ( TO_CHAR( ROUND( K_value::numeric , 2 ) , '000000000000D0000') , '.' , ',' ) ;
	line_count=line_count+1;	

	IF K_value >= 30.0
	THEN 
		result_str[line_count] = 'Очень высокая скорость изменения при отличной объясняющей способности модели (R2>0,8).' ; 
		line_count=line_count+1;
		result_str[line_count] = 'Значения редко встречаются и указывают на серьёзные проблемы.' ; 
		line_count=line_count+1;
		result_str[line_count] = 'Создать инцидент, привлечь экспертов.' ; 
		line_count=line_count+1;
	END IF ;

	IF K_value >= 20.0 AND K_value < 30
	THEN 
		result_str[line_count] = 'Cильный тренд, характерный для систем с высокой нагрузкой, требуется внимание' ; 
		line_count=line_count+1;
		result_str[line_count] = 'Плановое реагирование в ближайшее время, анализ первопричин, корректировка конфигурации.' ; 
		line_count=line_count+1;		
	END IF ;

	IF K_value >= 10.0 AND K_value < 20
	THEN 
		result_str[line_count] = 'Умеренный тренд, часто связан с сезонными или постепенными изменениями' ; 
		line_count=line_count+1;
		result_str[line_count] = 'Усиленный мониторинг и проверка, поиск закономерностей.' ; 
		line_count=line_count+1;		
	END IF ;

	IF K_value >= 5.0 AND K_value < 10
	THEN 
		result_str[line_count] = 'Слабый тренд, возможная флуктуация' ; 
		line_count=line_count+1;
		result_str[line_count] = 'Фоновое наблюдение, ежемесячная отчётность, внимание при совпадении с другими сигналами.' ; 
		line_count=line_count+1;		
	END IF ;

	IF K_value < 5
	THEN 
		result_str[line_count] = 'Шум или статистически незначимые изменения' ; 
		line_count=line_count+1;
		result_str[line_count] = 'Игнорировать либо учитывать в долгосрочной статистике без оперативных действий.' ; 
		line_count=line_count+1;		
	END IF ;
	
  return result_str ; 
END
$$ LANGUAGE plpgsql  ;
COMMENT ON FUNCTION interpretation_r2_coefficient IS 'Интерпретация коэффициента детерминации';
-- Интерпретация коэффициента тренда 
-------------------------------------------------------------------------------


--------------------------------------------------------------------------------
-- ЗАПОЛНЕНИЕ ТАБЛИЦЫ activities
CREATE OR REPLACE PROCEDURE fill_in_wce_activities() 
AS $$
DECLARE 
	activities_list text[];
	wce_rec record ;
	score_rec record ; 
	curr_min_score_wait_event_type numeric[5]; 
	curr_max_score_wait_event_type numeric[5]; 
	curr_wait_event_type text[7] ;
	curr_list text[];
	curr_list_length integer ;
	
	wait_event_type_counter integer ;
	score_wait_event_type_counter integer ;
	
BEGIN	

	curr_min_score_wait_event_type[1] = 0; curr_max_score_wait_event_type[1] = 0.01 ; 
	curr_min_score_wait_event_type[2] = 0.01; curr_max_score_wait_event_type[2] = 0.04 ; 
	curr_min_score_wait_event_type[3] = 0.04; curr_max_score_wait_event_type[3] = 0.1 ; 
	curr_min_score_wait_event_type[4] = 0.1; curr_max_score_wait_event_type[4] = 0.2 ; 
	curr_min_score_wait_event_type[5] = 0.2; curr_max_score_wait_event_type[5] = 1 ; 
	
	curr_wait_event_type[1] ='BufferPin';
	curr_wait_event_type[2] ='Extension';
	curr_wait_event_type[3] ='IO';
	curr_wait_event_type[4] ='IPC';
	curr_wait_event_type[5] ='Lock';
	curr_wait_event_type[6] ='LWLock';
	curr_wait_event_type[7] ='Timeout';
	
	FOR wait_event_type_counter IN 1..7 
	LOOP 
		SELECT * 
		INTO wce_rec
		FROM wce
		WHERE wait_event_type = curr_wait_event_type[wait_event_type_counter];
			
		FOR score_rec IN
		SELECT *
		FROM score
		WHERE wait_event_type_id = wce_rec.id  		
		LOOP 
			
			SELECT array_length( curr_list , 1 )
			INTO curr_list_length ;
			
			SELECT trim_array( curr_list , curr_list_length )
			INTO curr_list ; 
			
			-------------------------------------------------------------------------
			-- BufferPin
			IF wce_rec.wait_event_type = 'BufferPin'
			THEN 
				IF score_rec.min_score_wait_event_type = 0
				THEN
					curr_list[1] = 'ВКО < 0.01 : Игнорировать в текущем анализе.';
					INSERT INTO activities ( score_wait_event_type_id , list )
					VALUES ( score_rec.id , curr_list  );					
				END IF ;
				
				IF score_rec.min_score_wait_event_type = 0.01
				THEN
					curr_list[1] = '    Обновление PostgreSQL до версии с улучшенными алгоритмами работы с буферами';		
					curr_list[2] = '    Рассмотрение возможности использования расширений для управления памятью';
					curr_list[3] = '    Документирование случаев возникновения проблем для будущего анализа';
					INSERT INTO activities ( score_wait_event_type_id , list )
					VALUES ( score_rec.id , curr_list  );					
				END IF ;
			
				IF score_rec.min_score_wait_event_type = 0.04
				THEN
					curr_list[1] = '    Реорганизация графика обслуживания БД для выполнения ресурсоемких операций в периоды низкой нагрузки';		
					curr_list[2] = '    Использование табличных пространств на разных дисках для распределения нагрузки';
					curr_list[3] = '    Мониторинг и ограничение количества одновременных операций обслуживания';
					INSERT INTO activities ( score_wait_event_type_id , list )
					VALUES ( score_rec.id , curr_list  );					
				END IF ;
				
				IF score_rec.min_score_wait_event_type = 0.1
				THEN
					curr_list[1] = '    Проверка и оптимизация работы с временными таблицами и большими наборами данных';		
					curr_list[2] = '    Настройка maintenance_work_mem для операций обслуживания (VACUUM, индексация)';
					curr_list[3] = '    Анализ необходимости увеличения shared_buffers для уменьшения конкуренции';
					INSERT INTO activities ( score_wait_event_type_id , list )
					VALUES ( score_rec.id , curr_list  );					
				END IF ;
				
				IF score_rec.min_score_wait_event_type = 0.2
				THEN
					curr_list[1] = '    Анализ и оптимизация запросов, выполняющих длительные операции с буферами (VACUUM, CREATE INDEX CONCURRENTLY)';		
					curr_list[2] = '    Мониторинг блокировок буферов через представление pg_stat_activity с фильтрацией по wait_event_type = BufferPin';
					curr_list[3] = '    Настройка параметра vacuum_cost_delay для уменьшения конкуренции при фоновых операциях';
					INSERT INTO activities ( score_wait_event_type_id , list )
					VALUES ( score_rec.id , curr_list  );					
				END IF ;
			END IF ; 
			-- BufferPin
			-------------------------------------------------------------------------
			
			
			
			-------------------------------------------------------------------------
			-- Extension
			IF wce_rec.wait_event_type = 'Extension'
			THEN 
				IF score_rec.min_score_wait_event_type = 0
				THEN
					curr_list[1] = 'ВКО < 0.01 : Игнорировать в текущем анализе.';
					INSERT INTO activities ( score_wait_event_type_id , list )
					VALUES ( score_rec.id , curr_list  );					
				END IF ;
				
				IF score_rec.min_score_wait_event_type = 0.01
				THEN
					curr_list[1] = '    Рассмотрение альтернативных расширений с аналогичной функциональностью';		
					curr_list[2] = '    Кастомизация кода расширений';
					curr_list[3] = '    Разделение нагрузки между разными экземплярами PostgreSQL';
					INSERT INTO activities ( score_wait_event_type_id , list )
					VALUES ( score_rec.id , curr_list  );					
				END IF ;
			
				IF score_rec.min_score_wait_event_type = 0.04
				THEN
					curr_list[1] = '    Консультации с сообществом или разработчиками проблемных расширений';		
					curr_list[2] = '    Настройка параметров расширений под конкретную нагрузку';
					curr_list[3] = '    Создание индексов для ускорения работы функций расширений';
					INSERT INTO activities ( score_wait_event_type_id , list )
					VALUES ( score_rec.id , curr_list  );					
				END IF ;
				
				IF score_rec.min_score_wait_event_type = 0.1
				THEN
					curr_list[1] = '    Анализ конфигурации расширений на соответствие рекомендациям раз';		
					curr_list[2] = '    Мониторинг производительности расширений в пиковые периоды нагрузки';
					curr_list[3] = '    Оптимизация запросов, использующих функции проблемных расширений';
					INSERT INTO activities ( score_wait_event_type_id , list )
					VALUES ( score_rec.id , curr_list  );					
				END IF ;
				
				IF score_rec.min_score_wait_event_type = 0.2
				THEN
					curr_list[1] = '    Идентификация проблемных расширений через анализ pg_stat_activity и логов';		
					curr_list[2] = '    Обновление расширений до последних стабильных версий';
					curr_list[3] = '    Временное отключение расширений для диагностики причин ожиданий';
					INSERT INTO activities ( score_wait_event_type_id , list )
					VALUES ( score_rec.id , curr_list  );					
				END IF ;
			END IF ; 
			-- Extension
			-------------------------------------------------------------------------
			
			-------------------------------------------------------------------------
			-- IO
			IF wce_rec.wait_event_type = 'IO'
			THEN 
				IF score_rec.min_score_wait_event_type = 0
				THEN
					curr_list[1] = 'ВКО < 0.01 : Игнорировать в текущем анализе.';
					INSERT INTO activities ( score_wait_event_type_id , list )
					VALUES ( score_rec.id , curr_list  );					
				END IF ;
				
				IF score_rec.min_score_wait_event_type = 0.01
				THEN
					curr_list[1] = '    Внедрение мониторинга задержек дискового ввода-вывода на уровне ОС';
					curr_list[2] = '    Рассмотрение использования сжатия на уровне СУБД или файловой системы';
					curr_list[3] = '    Оптимизация файловой системы и параметров монтирования';
					INSERT INTO activities ( score_wait_event_type_id , list )
					VALUES ( score_rec.id , curr_list  );					
				END IF ;
			
				IF score_rec.min_score_wait_event_type = 0.04
				THEN
					curr_list[1] = '    Настройка параметров shared_buffers и work_mem для уменьшения физических чтений';
					curr_list[2] = '    Применение расширения pg_prewarm для предзагрузки часто используемых данных';
					curr_list[3] = '    Оптимизация размера WAL и параметров контрольных точек (checkpoint)';
					INSERT INTO activities ( score_wait_event_type_id , list )
					VALUES ( score_rec.id , curr_list  );					
				END IF ;
				
				IF score_rec.min_score_wait_event_type = 0.1
				THEN
					curr_list[1] = '    Оптимизация autovacuum для горячих таблиц (настройка агрессивности, порогов)';
					curr_list[2] = '    Разделение таблиц и индексов по разным табличным пространствам на разных дисках';
					curr_list[3] = '    Использование табличных пространств на быстрых накопителях для горячих данных';
					INSERT INTO activities ( score_wait_event_type_id , list )
					VALUES ( score_rec.id , curr_list  );					
				END IF ;
				
				IF score_rec.min_score_wait_event_type = 0.2
				THEN
					curr_list[1] = '    Анализ и оптимизация топ-запросов по времени ожидания IO';
					curr_list[2] = '    Проверка и оптимизация индексов (добавление недостающих, удаление неиспользуемых)';
					curr_list[3] = '    Настройка параметров effective_io_concurrency и random_page_cost для используемого оборудования';
					INSERT INTO activities ( score_wait_event_type_id , list )
					VALUES ( score_rec.id , curr_list  );					
				END IF ;
			END IF ; 
			-- IO
			-------------------------------------------------------------------------
			
			-------------------------------------------------------------------------
			-- IPC
			IF wce_rec.wait_event_type = 'IPC'
			THEN 
				IF score_rec.min_score_wait_event_type = 0
				THEN
					curr_list[1] = 'ВКО < 0.01 : Игнорировать в текущем анализе.';
					INSERT INTO activities ( score_wait_event_type_id , list )
					VALUES ( score_rec.id , curr_list  );					
				END IF ;
				
				IF score_rec.min_score_wait_event_type = 0.01
				THEN
					curr_list[1] = '    Обновление до актуальной версии PostgreSQL с улучшенными IPC-механизмами';
					curr_list[2] = '    Внедрение пулеров соединений (pgbouncer, pgpool-II) для уменьшения количества процессов';
					curr_list[3] = '    Разделение БД на логические части с выделением отдельных экземпляров';	
					INSERT INTO activities ( score_wait_event_type_id , list )
					VALUES ( score_rec.id , curr_list  );					
				END IF ;
			
				IF score_rec.min_score_wait_event_type = 0.04
				THEN
					curr_list[1] = '    Настройка параметров shared memory и semaphores на уровне ОС';
					curr_list[2] = '    Оптимизация параметров wal_buffers и commit_delay/commit_siblings';
					curr_list[3] = '    Анализ и устранение конфликтов между сессиями за общие ресурсы';
					INSERT INTO activities ( score_wait_event_type_id , list )
					VALUES ( score_rec.id , curr_list  );					
				END IF ;
				
				IF score_rec.min_score_wait_event_type = 0.1
				THEN					
					curr_list[1] = '    Оптимизация параметров репликации (max_wal_senders, wal_keep_size, max_replication_slots)';
					curr_list[2] = '    Балансировка подключений между экземплярами или использование пулеров соединений';
					curr_list[3] = '    Мониторинг и ограничение количества одновременных подключений';
					INSERT INTO activities ( score_wait_event_type_id , list )
					VALUES ( score_rec.id , curr_list  );					
				END IF ;
				
				IF score_rec.min_score_wait_event_type = 0.2
				THEN
					curr_list[1] = '    Анализ и настройка параллельных запросов (max_parallel_workers_per_gather, max_parallel_workers)';
					curr_list[2] = '    Мониторинг и оптимизация фоновых процессов (autovacuum, background writer, checkpointer)';
					curr_list[3] = '    Настройка параметра shared_buffers для уменьшения конкуренции между процессами';
					INSERT INTO activities ( score_wait_event_type_id , list )
					VALUES ( score_rec.id , curr_list  );					
				END IF ;
			END IF ; 
			-- IPC
			-------------------------------------------------------------------------
			
			-------------------------------------------------------------------------
			-- Lock
			IF wce_rec.wait_event_type = 'Lock'
			THEN 
				IF score_rec.min_score_wait_event_type = 0
				THEN
					curr_list[1] = 'ВКО < 0.01 : Игнорировать в текущем анализе.';
					INSERT INTO activities ( score_wait_event_type_id , list )
					VALUES ( score_rec.id , curr_list  );					
				END IF ;
				
				IF score_rec.min_score_wait_event_type = 0.01
				THEN
					curr_list[1] = '    Изменение архитектуры приложения: внедрение очередей для асинхронной обработки';
					curr_list[2] = '    Пересмотр схемы БД для минимизации точек конкуренции (нормализация/денормализация)';
					curr_list[3] = '    Использование таблиц-очередей на основе SKIP LOCKED';	
					INSERT INTO activities ( score_wait_event_type_id , list )
					VALUES ( score_rec.id , curr_list  );					
				END IF ;
			
				IF score_rec.min_score_wait_event_type = 0.04
				THEN
					curr_list[1] = '    Анализ и оптимизация уровней изоляции транзакций';
					curr_list[2] = '    Внедрение retry-логики в приложении для обработки deadlocks и таймаутов';
					curr_list[3] = '    Использование advisory locks для нестандартных сценариев синхронизации';
					INSERT INTO activities ( score_wait_event_type_id , list )
					VALUES ( score_rec.id , curr_list  );					
				END IF ;
				
				IF score_rec.min_score_wait_event_type = 0.1
				THEN
					curr_list[1] = '    Изменение логики приложения для уменьшения времени удерживания блокировок';
					curr_list[2] = '    Применение стратегий оптимистичной блокировки (version/timestamp поля)';
					curr_list[3] = '    Реструктуризация запросов для минимизации конкуренции за объекты';
					INSERT INTO activities ( score_wait_event_type_id , list )
					VALUES ( score_rec.id , curr_list  );					
				END IF ;
				
				IF score_rec.min_score_wait_event_type = 0.2
				THEN
					curr_list[1] = '    Выявление и устранение блокирующих транзакций';
					curr_list[2] = '    Оптимизация времени выполнения транзакций (разделение больших транзакций на меньшие)';
					curr_list[3] = '    Установка разумных значений lock_timeout и idle_in_transaction_session_timeout';
					INSERT INTO activities ( score_wait_event_type_id , list )
					VALUES ( score_rec.id , curr_list  );					
				END IF ;
			END IF ; 
			-- Lock
			-------------------------------------------------------------------------
			
			
			-------------------------------------------------------------------------
			-- LWLock
			IF wce_rec.wait_event_type = 'LWLock'
			THEN 
				IF score_rec.min_score_wait_event_type = 0
				THEN
					curr_list[1] = 'ВКО < 0.01 : Игнорировать в текущем анализе.';
					INSERT INTO activities ( score_wait_event_type_id , list )
					VALUES ( score_rec.id , curr_list  );					
				END IF ;
				
				IF score_rec.min_score_wait_event_type = 0.01
				THEN
					curr_list[1] = '    Обновление PostgreSQL до версии с улучшенными алгоритмами LWLock';
					curr_list[2] = '    Архитектурные изменения: выделение специализированных инстансов для разных типов нагрузки';
					curr_list[3] = '    Консультации с экспертами PostgreSQL по тонкой настройке';
					INSERT INTO activities ( score_wait_event_type_id , list )
					VALUES ( score_rec.id , curr_list  );					
				END IF ;
			
				IF score_rec.min_score_wait_event_type = 0.04
				THEN
					curr_list[1] = '    Анализ и оптимизация использования prepared transactions.';
					curr_list[2] = '    Мониторинг и ограничение количества одновременных подключений.';
					curr_list[3] = '    Настройка параметров max_connections и superuser_reserved_connections.';
					INSERT INTO activities ( score_wait_event_type_id , list )
					VALUES ( score_rec.id , curr_list  );					
				END IF ;
				
				IF score_rec.min_score_wait_event_type = 0.1
				THEN
					curr_list[1] = '    Оптимизация параллельных операций обслуживания (max_parallel_maintenance_workers)';
					curr_list[2] = '    Балансировка нагрузки во времени для ресурсоемких операций';
					curr_list[3] = '    Тонкая настройка autovacuum для уменьшения конфликтов';
					INSERT INTO activities ( score_wait_event_type_id , list )
					VALUES ( score_rec.id , curr_list  );					
				END IF ;
				
				IF score_rec.min_score_wait_event_type = 0.2
				THEN
					curr_list[1] = '    Мониторинг точек конкуренции';
					curr_list[2] = '    Оптимизация конкурентных DDL-операций (перенос в периоды низкой нагрузки)';
					curr_list[3] = '    Настройка параметров памяти: work_mem, maintenance_work_mem, shared_buffers';	
					INSERT INTO activities ( score_wait_event_type_id , list )
					VALUES ( score_rec.id , curr_list  );					
				END IF ;
			END IF ; 
			-- LWLock
			-------------------------------------------------------------------------
			
			
			-------------------------------------------------------------------------
			-- Timeout
			IF wce_rec.wait_event_type = 'Timeout'
			THEN 
				IF score_rec.min_score_wait_event_type = 0
				THEN
					curr_list[1] = 'ВКО < 0.01 : Игнорировать в текущем анализе.';
					INSERT INTO activities ( score_wait_event_type_id , list )
					VALUES ( score_rec.id , curr_list  );					
				END IF ;
				
				IF score_rec.min_score_wait_event_type = 0.01
				THEN
					curr_list[1] = '    Архитектурные изменения для уменьшения необходимости в длинных транзакциях';
					curr_list[2] = '    Внедрение механизмов ретраев с экспоненциальным откатом в приложении';
					curr_list[3] = '    Обучение разработчиков работе с асинхронными операциями';
					INSERT INTO activities ( score_wait_event_type_id , list )
					VALUES ( score_rec.id , curr_list  );					
				END IF ;
			
				IF score_rec.min_score_wait_event_type = 0.04
				THEN
					curr_list[1] = '    Настройка мониторинга для алертов по таймаутам';
					curr_list[2] = '    Создание дашбордов для отслеживания динамики таймаутов';
					curr_list[3] = '    Разработка скриптов для автоматического анализа причин таймаутов';
					INSERT INTO activities ( score_wait_event_type_id , list )
					VALUES ( score_rec.id , curr_list  );					
				END IF ;
				
				IF score_rec.min_score_wait_event_type = 0.1
				THEN
					curr_list[1] = '    Оптимизация запросов, регулярно превышающих statement_timeout';
					curr_list[2] = '    Ревизия логики приложения на предмет длительных транзакций и блокировок';
					curr_list[3] = '    Внедрение механизмов отслеживания прогресса длительных операций';
					INSERT INTO activities ( score_wait_event_type_id , list )
					VALUES ( score_rec.id , curr_list  );					
				END IF ;
				
				IF score_rec.min_score_wait_event_type = 0.2
				THEN
					curr_list[1] = '    Анализ логов PostgreSQL на предмет сообщений о таймаутах';
					curr_list[2] = '    Определение типов таймаутов (statement_timeout, lock_timeout, idle_timeout) и их источников';
					curr_list[3] = '    Настройка параметров таймаутов в соответствии с требованиями приложения';
					INSERT INTO activities ( score_wait_event_type_id , list )
					VALUES ( score_rec.id , curr_list  );					
				END IF ;
			END IF ; 
			-- Timeout
			-------------------------------------------------------------------------
		END LOOP ;
	END LOOP ;
END
$$ LANGUAGE plpgsql; 
COMMENT ON PROCEDURE fill_in_wce_activities IS 'ЗАПОЛНЕНИЕ ТАБЛИЦЫ activities';
-- ЗАПОЛНЕНИЕ ТАБЛИЦЫ activities
--------------------------------------------------------------------------------

-- Получить активности по заданному wait_event_type и значению ВКО 
CREATE OR REPLACE FUNCTION get_wce_activities( curr_wait_event_type text , curr_score_wait_event_type numeric ) RETURNS text[] AS $$
DECLARE
 result_str text[] ;
 line_count integer ; 
 wce_rec record ;
 score_rec record ; 
 activities_rec record ; 
 list_length integer ; 
BEGIN
	SELECT * 
	INTO wce_rec
	FROM wce
	WHERE wait_event_type = curr_wait_event_type ; 
	
	SELECT * 
	INTO score_rec
	FROM score
	WHERE 
		wait_event_type_id = wce_rec.id 
		AND 
		( min_score_wait_event_type < curr_score_wait_event_type AND curr_score_wait_event_type <= max_score_wait_event_type );
		
	SELECT 	* 
	INTO activities_rec
	FROM activities
	WHERE score_wait_event_type_id = score_rec.id ;
	
	SELECT array_length( activities_rec.list , 1 )
	INTO list_length ; 
	
	FOR line_count IN 1..list_length
	LOOP
		result_str[line_count] = activities_rec.list[line_count];
	END LOOP;
	
  return result_str ; 
END
$$ LANGUAGE plpgsql  ;
COMMENT ON FUNCTION get_wce_activities IS 'Заполнить данные по комплексному анализу ожижданий';
-- Получить активности по заданному wait_event_type и значению ВКО 
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Заполнить данные по комплексному анализу ожиданий
CREATE OR REPLACE FUNCTION fill_in_comprehensive_analysis_wait_event_type( title text , current_wait_type text , score_wait_event_type numeric , id_recomendations_need BOOLEAN DEFAULT TRUE   ) RETURNS text[] AS $$
DECLARE
 result_str text[] ;
 line_count integer ;
 correlation_rec record ; 
 corr_values text[];
 least_squares_rec record ; 
 score_txt text[];
 report_str_length integer ;
 activity_list text[];

BEGIN
	line_count = 1 ;
	
	INSERT INTO wait_event_type_criteria_matrix ( wait_event_type ) VALUES ( current_wait_type );
	
	SELECT * INTO correlation_rec FROM quick_significance_check();
	
	-- Матрица критериев
	-- Заполнить Коэффициент корреляции
	UPDATE wait_event_type_criteria_matrix SET r_value = correlation_rec.correvation_value::DOUBLE PRECISION WHERE wait_event_type = current_wait_type ; 
	IF correlation_rec.correvation_value > 0 
	THEN 		
		UPDATE wait_event_type_criteria_matrix SET p_value = correlation_rec.p_value::DOUBLE PRECISION WHERE wait_event_type = current_wait_type ;
	END IF ;
	-- Заполнить Коэффициент корреляции
	-- Матрица критериев
	
	line_count=line_count+1;			 
	result_str[line_count] = title;
	line_count=line_count+1;
	
	result_str[line_count] = 'Шаг 1. Интерпретация корреляций.';
	
    SELECT fill_corr_values_for_positive_corr( correlation_rec ) 
	INTO corr_values ; 
	SELECT result_str || corr_values
	INTO result_str ; 
	SELECT array_append( result_str , ' ')
	INTO result_str ;
    SELECT array_length( result_str , 1 )
	INTO line_count;
    line_count=line_count+1;	
	

	
    -- Если положительная корреляция между ожиданиями и типом ожидания
	IF correlation_rec.correvation_value > 0 
	THEN 	
	    -- ЕСЛИ ОЦЕНКА ЗНАЧИМОСТИ КОЭФФИЦИЕНТА КОРРЕЛЯЦИИ - ЗНАЧИМА
		IF correlation_rec.significance_empirical != 'Незначима' AND correlation_rec.significance_t_test != 'Незначима'
		THEN 			
			result_str[line_count] = 'Шаг 2. Интерпретация ВКО.';
			line_count=line_count+1;
			result_str[line_count] = '  ВЗВЕШЕННАЯ КОРРЕЛЯЦИЯ ОЖИДАНИЙ(ВКО):' ||'|'|| REPLACE ( TO_CHAR( ROUND( score_wait_event_type, 2 ) , '000000000000D0000') , '.' , ',' )||'|' ; 		
			line_count=line_count+1;
			
			-- Матрица критериев
			-- Заполнить ВКО (w)
				UPDATE wait_event_type_criteria_matrix SET w_value = score_wait_event_type::DOUBLE PRECISION WHERE wait_event_type = current_wait_type ;			
			-- Заполнить ВКО (w)
			-- Матрица критериев

			
			-- ЕСЛИ ВКО > 0.01
			IF score_wait_event_type >= 0.01 
			THEN 
				
				IF score_wait_event_type >= 0.2
				THEN 
					score_txt[1] = '    КРИТИЧЕСКОЕ ЗНАЧЕНИЕ : Немедленный анализ и действие. Основной фокус расследования.';
				ELSIF score_wait_event_type >= 0.1 AND score_wait_event_type < 0.2 
				THEN 
					score_txt[1] = '    ВЫСОКОЕ ЗНАЧЕНИЕ : Глубокий анализ и планирование оптимизации.';
				ELSIF score_wait_event_type >= 0.04 AND score_wait_event_type < 0.1 
				THEN 
					score_txt[1] = '    СРЕДНЕЕ ЗНАЧЕНИЕ : Контекстный анализ и наблюдение. Решение по остаточному принципу.';
				ELSIF score_wait_event_type >= 0.01 AND score_wait_event_type < 0.04
				THEN 
					score_txt[1] = '    НИЗКОЕ ЗНАЧЕНИЕ : Наблюдение и документирование. Действия только при ухудшении.';
				END IF;
				result_str[line_count] = score_txt[1] ; 
				line_count=line_count+1; 
				IF length(score_txt[2]) > 0 
				THEN 
					result_str[line_count] = score_txt[2] ; 
					line_count=line_count+1; 
					result_str[line_count] = score_txt[3] ; 
					line_count=line_count+1; 
					result_str[line_count] = score_txt[4] ; 		
				END IF;
						
				SELECT * INTO least_squares_rec FROM Y_X_regression_line();
				
				result_str[line_count] = 'Шаг 3. Интерпретация коэффициента детерминации R2.';
				line_count=line_count+1;
				
				-- Матрица критериев
				-- Заполнить R2
					UPDATE wait_event_type_criteria_matrix SET r2_value = least_squares_rec.current_r_squared::DOUBLE PRECISION WHERE wait_event_type = current_wait_type ;				
				-- Заполнить R2
				-- Матрица критериев
			
				--R2 >= 0.2
				IF least_squares_rec.current_r_squared >= 0.2  
				THEN 
					result_str[line_count] = ' РЕГРЕССИЯ ОЖИДАНИЯ СУБД(Y) ПО ОЖИДАНИЯМ ТИПА '||current_wait_type||'(X)' ; 
					line_count=line_count+1; 
					result_str[line_count] = ' Коэффициент детерминации R^2 ' ||'|'|| REPLACE ( TO_CHAR( ROUND( least_squares_rec.current_r_squared::numeric , 2 ) , '000000000000D0000') , '.' , ',' ) ;
					line_count=line_count+1;
					result_str[line_count] = ' угол наклона  ' ||'|'|| REPLACE ( TO_CHAR( ROUND( least_squares_rec.slope_angle_degrees::numeric , 2 ) , '000000000000D0000') , '.' , ',' ) ;
					line_count=line_count+1; 		
					SELECT interpretation_r2_coefficient( least_squares_rec.current_r_squared::numeric ) 
					INTO corr_values ; 
					result_str[line_count] = corr_values[1] ; 
					line_count=line_count+1;
					result_str[line_count] = corr_values[2] ; 
					line_count=line_count+1;
					result_str[line_count] = corr_values[3] ; 
					line_count=line_count+1;
					
					IF id_recomendations_need 
					THEN 
						-----------------------------------------------------
						-- РЕКОМЕНДУЕМЫЕ ДЕЙСТВИЯ
							--Получить активности по заданному wait_event_type и значению ВКО 
							activity_list = '{}'::text[]; 
							SELECT get_wce_activities( current_wait_type , score_wait_event_type )
							INTO activity_list;
							
							result_str[line_count] = 'РЕКОМЕНДУЕМЫЕ ДЕЙСТВИЯ:';
							SELECT result_str || activity_list
							INTO result_str ; 
						-- РЕКОМЕНДУЕМЫЕ ДЕЙСТВИЯ
						-----------------------------------------------------
					END IF;
				END IF;
			ELSE
				result_str[line_count] = 'ВКО < 0.01 : Игнорировать в текущем анализе.';
				line_count=line_count+1;			
			END IF;	--IF score_wait_event_type >= 0.01 
			
		
		ELSE 
			result_str[line_count] = 'ОЦЕНКА ЗНАЧИМОСТИ КОЭФФИЦИЕНТА КОРРЕЛЯЦИИ - Незначима';
			line_count=line_count+1;			
		END IF; --IF correlation_rec.significance_empirical != 'Незначима' AND correlation_rec.significance_t_test != 'Незначима'
	END IF; --IF correlation_rec.correvation_value > 0
	--2.1 BufferPin'
	----------------------------------------------------------------------------------------------------	
	
 
  return result_str ; 
END
$$ LANGUAGE plpgsql  ;
COMMENT ON FUNCTION fill_in_comprehensive_analysis_wait_event_type IS 'Заполнить данные по комплексному анализу ожиданий';
-- Заполнить данные по комплексному анализу ожиданий
-------------------------------------------------------------------------------



-------------------------------------------------------------------------------
-- Заполнить данные по комплексному анализу корреляции
CREATE OR REPLACE FUNCTION fill_in_comprehensive_analysis_correlation( title text , current_wait_type text , vmstat_metric text , correlation_sign integer , reason_casulas_list text[] ) RETURNS text[] AS $$
DECLARE
 result_str text[] ;
 line_count integer ;
 correlation_rec record ; 
 corr_values text[];
 least_squares_rec record ; 
 score_txt text[];
 report_str_length integer ;
 correlation_regression_flags_rec record;
 check_time_series_stationarity_rec record ;
 is_series_stationary_flag BOOLEAN;
 test_stationary_flag BOOLEAN ;
 series_counter integer;
 
 r_norm DOUBLE PRECISION ; -- Нормализованное значение коэффициента корреляции
 r2_norm DOUBLE PRECISION ; -- Нормализованное значение коэффициента детерминации
 slope_norm DOUBLE PRECISION ; -- Нормализованное значение угла наклона 
 
BEGIN
	line_count = 1 ;
	
	TRUNCATE TABLE correlation_regression_flags ; 
	INSERT INTO correlation_regression_flags ( correlation_flag, regression_flag ) VALUES ( FALSE , FALSE );
	
	SELECT * INTO correlation_rec FROM quick_significance_check();
	
	line_count=line_count+1;			 
	result_str[line_count] = title;
	line_count=line_count+1;
	
	result_str[line_count] = 'Шаг 1. Интерпретация корреляций.';
	
	--Прямая корреляция
	IF correlation_sign = 1 
	THEN 	
		SELECT fill_corr_values_for_positive_corr( correlation_rec ) 
		INTO corr_values ; 
	--Обратная корреляция
	ELSE
		SELECT fill_corr_values_for_negative_corr( correlation_rec ) 
		INTO corr_values ; 	
	END IF;
	
	SELECT result_str || corr_values
	INTO result_str ; 
	SELECT array_append( result_str , ' ')
	INTO result_str ;
    SELECT array_length( result_str , 1 )
	INTO line_count;
    line_count=line_count+1;

	-- Если направление корреляции совпадает с требуемым	
	IF SIGN( correlation_rec.correvation_value) = correlation_sign
	THEN 
		IF ABS(correlation_rec.correvation_value) >= 0.7 
		THEN
			result_str[line_count] = ' ОЧЕНЬ ВЫСОКАЯ КОРРЕЛЯЦИЯ' ; 
			line_count=line_count+1;
		ELSIF ABS(correlation_rec.correvation_value) >= 0.5 AND ABS(correlation_rec.correvation_value) < 0.7
		THEN 
			result_str[line_count] = ' ВЫСОКАЯ КОРРЕЛЯЦИЯ' ; 
			line_count=line_count+1;	
		ELSE
			result_str[line_count] = ' СЛАБАЯ ИЛИ СРЕДНЯЯ КОРРЕЛЯЦИЯ' ; 
			line_count=line_count+1;		
		END IF;
	ELSE 
		UPDATE correlation_regression_flags SET regression_flag = FALSE  ;
	END IF;
	

	
    -- Если направление корреляции совпадает с требуемым	
	IF ABS(correlation_rec.correvation_value) > 0 AND SIGN( correlation_rec.correvation_value) = correlation_sign
	THEN 	
	    -- ЕСЛИ ОЦЕНКА ЗНАЧИМОСТИ КОЭФФИЦИЕНТА КОРРЕЛЯЦИИ - ЗНАЧИМА
		IF correlation_rec.significance_empirical != 'Незначима' AND correlation_rec.significance_t_test != 'Незначима'
		THEN 	
			UPDATE correlation_regression_flags SET correlation_flag = TRUE ;
			
			SELECT * INTO least_squares_rec FROM Y_X_regression_line();
			
			result_str[line_count] = 'Шаг 2. Интерпретация коэффициента детерминации R2.';
			line_count=line_count+1;
			
			--R2 >= 0.2
			IF least_squares_rec.current_r_squared >= 0.2  
			THEN 
				UPDATE correlation_regression_flags SET regression_flag = TRUE ;	
				result_str[line_count] = ' РЕГРЕССИЯ '||current_wait_type||'(Y) ПО '||vmstat_metric||'(X)' ; 
				line_count=line_count+1; 
				result_str[line_count] = ' Коэффициент детерминации R^2 ' ||'|'|| REPLACE ( TO_CHAR( ROUND( least_squares_rec.current_r_squared::numeric , 2 ) , '000000000000D0000') , '.' , ',' ) ;
				line_count=line_count+1;
				result_str[line_count] = ' угол наклона  ' ||'|'|| REPLACE ( TO_CHAR( ROUND( least_squares_rec.slope_angle_degrees::numeric , 2 ) , '000000000000D0000') , '.' , ',' ) ;
				line_count=line_count+1; 		
				SELECT interpretation_r2_coefficient( least_squares_rec.current_r_squared::numeric ) 
				INTO corr_values ; 
				result_str[line_count] = corr_values[1] ; 
				line_count=line_count+1;
				result_str[line_count] = corr_values[2] ; 
				line_count=line_count+1;
				result_str[line_count] = corr_values[3] ; 
				line_count=line_count+1;
			END IF;
		ELSE 
			UPDATE correlation_regression_flags SET regression_flag = FALSE  ;	
		END IF; --IF correlation_rec.significance_empirical != 'Незначима' AND correlation_rec.significance_t_test != 'Незначима'
	END IF; --IF correlation_rec.correvation_value > 0
	----------------------------------------------------------------------------------------------------	
	
	SELECT * 
	INTO correlation_regression_flags_rec
	FROM correlation_regression_flags;
	
	IF NOT correlation_regression_flags_rec.correlation_flag
	THEN 
		result_str[line_count] = 'ИНТЕРПРЕТАЦИЯ КОРРЕЛЯЦИЙ: КОРРЕЛЯЦИЯ НЕСУЩЕСТВЕННА'; 
		line_count=line_count+1;
	END IF ; 
	
	IF NOT correlation_regression_flags_rec.regression_flag
	THEN 
		result_str[line_count] = 'ИНТЕРПРЕТАЦИЯ КОЭФФИЦИЕНТА ДЕТЕРМИНАЦИИ R2: НЕПРИГОДНАЯ МОДЕЛЬ'; 
		line_count=line_count+1;
	END IF ; 
	
	IF correlation_regression_flags_rec.correlation_flag AND correlation_regression_flags_rec.regression_flag
	THEN 			
		SELECT result_str || reason_casulas_list
		INTO result_str ; 
		
		----------------------------------------
		-- Добавить значение в таблицу cpi_matrix
		INSERT INTO cpi_matrix 	
		( 
			current_pair , 
			r_norm , 
			r2_norm , 
			slope_source )
		VALUES
		( 
			title ,
			ABS(correlation_rec.correvation_value) ,
			least_squares_rec.current_r_squared , 
			least_squares_rec.slope_angle_degrees
		);		
		-- Добавить значение в таблицу cpi_matrix
		----------------------------------------
		
	ELSE
		return result_str ; 
	END IF;

    return result_str ; 
END
$$ LANGUAGE plpgsql  ;
COMMENT ON FUNCTION fill_in_comprehensive_analysis_correlation IS '-- Заполнить данные по комплексному анализу корреляции';
-- Заполнить данные по комплексному анализу ожиданий
-------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Вычислить значение Индекса Приоритета Корреляции (Correlation Priority Index, CPI)
CREATE OR REPLACE PROCEDURE calculate_cpi_matrix()
AS $$
DECLARE
    min_abs_slope DOUBLE PRECISION;
    max_abs_slope DOUBLE PRECISION;
    range_abs_slope DOUBLE PRECISION;
BEGIN
    -- 1. Получаем глобальные минимум и максимум модуля наклона
    SELECT MIN(ABS(slope_source)), MAX(ABS(slope_source))
    INTO min_abs_slope, max_abs_slope
    FROM cpi_matrix
    WHERE slope_source IS NOT NULL;

    -- 2. Защита от деления на ноль (если все значения одинаковы или NULL)
    IF max_abs_slope IS NULL OR max_abs_slope = min_abs_slope THEN
        range_abs_slope := 1;   -- тогда все slope_norm будут (|slope| - min)/1 = 0
    ELSE
        range_abs_slope := max_abs_slope - min_abs_slope;
    END IF;

    -- 3. Нормализуем модуль наклона для всех записей
    UPDATE cpi_matrix
    SET slope_norm = (ABS(slope_source) - min_abs_slope) / range_abs_slope
    WHERE slope_source IS NOT NULL;

    -- 4. Для строк с NULL slope_source устанавливаем 0 (или можно оставить NULL)
    UPDATE cpi_matrix
    SET slope_norm = 0
    WHERE slope_source IS NULL;

    -- 5. Вычисляем CPI как кубический корень из произведения трёх нормированных компонент
    UPDATE cpi_matrix
    SET curr_value = 
        CASE 
            WHEN r_norm IS NOT NULL AND r2_norm IS NOT NULL AND slope_norm IS NOT NULL 
                 AND r_norm >= 0 AND r2_norm >= 0 AND slope_norm >= 0
            THEN power(r_norm * r2_norm * slope_norm, 1.0/3.0)
            ELSE NULL
        END;
END;
$$ LANGUAGE plpgsql;
COMMENT ON PROCEDURE calculate_cpi_matrix IS 'Вычислить значение Индекса Приоритета Корреляции (Correlation Priority Index, CPI)';
-- Вычислить значение Индекса Приоритета Корреляции (Correlation Priority Index, CPI)


--------------------------------------------------------------------------------
-- Добавить значение 
CREATE OR REPLACE PROCEDURE truncate_time_series() 
AS $$
BEGIN	
	TRUNCATE TABLE first_time_series ; 
	TRUNCATE TABLE second_time_series ; 
END
$$ LANGUAGE plpgsql; 
COMMENT ON PROCEDURE truncate_time_series IS 'БЫСТРАЯ ОЧИСТКА ТАБЛИЦ ВРЕМЕННЫХ РЯДОВ';
-- БЫСТРАЯ ОЧИСТКА ТАБЛИЦ ВРЕМЕННЫХ РЯДОВ
--------------------------------------------------------------------------------


--------------------------------------------------------------------------------
-- Расчет весов критериев для wait_event_type
CREATE OR REPLACE PROCEDURE calc_wait_event_type_criteria_weight()
AS $$
DECLARE
    current_criteria_value numeric[][];  -- допускается любая размерность
	lines numeric[4];
	square_root numeric[4];	
	square_root_sum numeric;
	weigth numeric[4];
	i integer ; 
BEGIN
	TRUNCATE TABLE wait_event_type_criteria_weight ; 
    -- Инициализация массива 4x4 (заполнение нулями)
    current_criteria_value := array_fill(0::numeric, ARRAY[4,4]);
	
	-- Инициализация массивов 
    lines := array_fill(0::numeric, ARRAY[4]);
	square_root := array_fill(0::numeric, ARRAY[4]);
	weigth := array_fill(0::numeric, ARRAY[4]);
	
/*
Пример экспертных оценок (могут корректироваться под конкретную задачу):
•	Корреляция Пирсона (rr) и p-value (pp) одинаково важны, так как сила связи без значимости ненадёжна → a12=1a12=1.
•	Взвешенная корреляция (ww) учитывает объёмы, поэтому она несколько важнее, чем просто корреляция → a13=3a13=3 (умеренное превосходство).
•	Коэффициент детерминации (R2R2) даёт представление о вкладе в общую вариабельность, поэтому он важнее корреляции, но чуть менее важен, чем ВКО → a14=2a14=2.
•	P-value (pp) важнее ВКО? Возможно, нет: значимость критична, но ВКО добавляет информацию о весе. Положим a23=1/2a23=1/2 (т.е. ВКО в 2 раза важнее p-value).
•	P-value и R2R2: R2R2 важнее, так как показывает объяснённую дисперсию → a24=1/3a24=1/3 (т.е. R2R2 в 3 раза важнее).
•	ВКО и R2R2: оба показателя важны, но ВКО более специфичен для ожиданий → a34=2a34=2 (ВКО в 2 раза важнее R2R2).
*/
    -- r Корреляция
    current_criteria_value[1][1] := 1;
    current_criteria_value[1][2] := 1;
    current_criteria_value[1][3] := 3;
    current_criteria_value[1][4] := 2;

    -- p-value
    current_criteria_value[2][1] := 1;
    current_criteria_value[2][2] := 1;
    current_criteria_value[2][3] := 1.0 / 2.0;   -- 0.5
    current_criteria_value[2][4] := 1.0 / 3.0;   -- 0.333...

    -- ВКО (w)
    current_criteria_value[3][1] := 1.0 / 3.0;
    current_criteria_value[3][2] := 2;
    current_criteria_value[3][3] := 1;
    current_criteria_value[3][4] := 2;

    -- R2
    current_criteria_value[4][1] := 1.0 / 2.0;
    current_criteria_value[4][2] := 3;
    current_criteria_value[4][3] := 1.0 / 2.0;
    current_criteria_value[4][4] := 1;

	--Произведение элементов каждой строки
	-- Корень 4-й степени из произведений
	FOR i IN 1..4 
	LOOP 
		lines[i] = current_criteria_value[i][1] * current_criteria_value[i][2] * current_criteria_value[i][3] * current_criteria_value[i][4] ;
		square_root[i] = power( lines[i], 1.0 / 4.0);
----RAISE NOTICE 'lines[i] = %', lines[i];		
----RAISE NOTICE 'square_root[i] = %', square_root[i];		
	END LOOP ;
	
	--Сумма корней
	square_root_sum = square_root[1] + square_root[2] + square_root[3] + square_root[4] ;
----RAISE NOTICE 'square_root_sum = %', square_root_sum;			
	
	--Нормировка – веса
	--r корреляция
	weigth[1] = square_root[1] / square_root_sum ; 
----RAISE NOTICE 'weigth[1] = %', weigth[1];				
	-- p-value
	weigth[2] = square_root[2] / square_root_sum ; 
----RAISE NOTICE 'weigth[2] = %', weigth[2];					
	 -- ВКО (w)
	weigth[3] = square_root[3] / square_root_sum ; 
----RAISE NOTICE 'weigth[3] = %', weigth[3];					
	-- R2
	weigth[4] = square_root[4] / square_root_sum ; 
----RAISE NOTICE 'weigth[4] = %', weigth[4];	

----RAISE NOTICE 'sum = %', weigth[1]+weigth[2]+weigth[3]+weigth[4];	
	
	INSERT INTO wait_event_type_criteria_weight(curr_value) VALUES ( weigth );
	
END
$$ LANGUAGE plpgsql;
COMMENT ON PROCEDURE calc_wait_event_type_criteria_weight IS 'Расчет весов критериев для wait_event_type';

-- Нормализовать значения в матрице критериев 
CREATE OR REPLACE PROCEDURE norm_wait_event_type_criteria_matrix()
AS $$
DECLARE
    rec record;
    r_min numeric;
    r_max numeric;
    p_min numeric;
    p_max numeric;
    w_min numeric;
    w_max numeric;
    r2_min numeric;
    r2_max numeric;
    r_norm numeric;
    p_norm numeric;
    w_norm numeric;
    r2_norm numeric;
BEGIN
    -- Однократное вычисление глобальных минимумов и максимумов для каждого критерия
    SELECT
        MIN(ABS(r_value)) FILTER (WHERE r_value IS NOT NULL),
        MAX(ABS(r_value)) FILTER (WHERE r_value IS NOT NULL),
        MIN(-log10(p_value + 1e-10)) FILTER (WHERE p_value IS NOT NULL),
        MAX(-log10(p_value + 1e-10)) FILTER (WHERE p_value IS NOT NULL),
        MIN(w_value) FILTER (WHERE w_value IS NOT NULL),
        MAX(w_value) FILTER (WHERE w_value IS NOT NULL),
        MIN(r2_value) FILTER (WHERE r2_value IS NOT NULL),
        MAX(r2_value) FILTER (WHERE r2_value IS NOT NULL)
    INTO
        r_min, r_max,
        p_min, p_max,
        w_min, w_max,
        r2_min, r2_max
    FROM wait_event_type_criteria_matrix;

    -- Цикл по всем строкам таблицы
    FOR rec IN
        SELECT *
        FROM wait_event_type_criteria_matrix
        ORDER BY wait_event_type
    LOOP
        --RAISE NOTICE '%', rec;

        IF rec.r_value > 0 THEN
            -- Нормализация абсолютного значения корреляции
            IF r_max IS NOT NULL AND r_min IS NOT NULL AND r_max != r_min THEN
                r_norm := (ABS(rec.r_value) - r_min) / (r_max - r_min);
            ELSE
                r_norm := 0;
            END IF;

            -- Нормализация преобразованного p-value
            IF rec.p_value IS NOT NULL THEN
                IF p_max IS NOT NULL AND p_min IS NOT NULL AND p_max != p_min THEN
                    p_norm := (-log10(rec.p_value + 1e-10) - p_min) / (p_max - p_min);
                ELSE
                    p_norm := 0;
                END IF;
            ELSE
                p_norm := NULL;
            END IF;

            -- Нормализация взвешенной корреляции ожиданий
            IF rec.w_value IS NOT NULL THEN
                IF w_max IS NOT NULL AND w_min IS NOT NULL AND w_max != w_min THEN
                    w_norm := (rec.w_value - w_min) / (w_max - w_min);
                ELSE
                    w_norm := 0;
                END IF;
            ELSE
                w_norm := NULL;
            END IF;

            -- Нормализация коэффициента детерминации
            IF rec.r2_value IS NOT NULL THEN
                IF r2_max IS NOT NULL AND r2_min IS NOT NULL AND r2_max != r2_min THEN
                    r2_norm := (rec.r2_value - r2_min) / (r2_max - r2_min);
                ELSE
                    r2_norm := 0;
                END IF;
            ELSE
                r2_norm := NULL;
            END IF;
        ELSE
            -- Для строк с неположительной корреляцией все нормы обнуляются
            r_norm := 0;
            p_norm := 0;
            w_norm := 0;
            r2_norm := 0;
        END IF;

        --RAISE NOTICE 'wait_event_type_criteria_matrix_rec.wait_event_type = %', rec.wait_event_type;
        --RAISE NOTICE 'r_norm = %', r_norm;
        --RAISE NOTICE 'p_norm = %', p_norm;
        --RAISE NOTICE 'w_norm = %', w_norm;
        --RAISE NOTICE 'r2_norm = %', r2_norm;

        -- Сохраняем вычисленные значения в таблице
        UPDATE wait_event_type_criteria_matrix
        SET calculated_r_norm = r_norm,
            calculated_p_norm = p_norm,
            calculated_w_norm = w_norm,
            calculated_r2_norm = r2_norm
        WHERE wait_event_type = rec.wait_event_type;
    END LOOP;
END
$$ LANGUAGE plpgsql;
COMMENT ON PROCEDURE norm_wait_event_type_criteria_matrix IS 'Нормализовать значения в матрице критериев';
--------------------------------------------------------------------------------
-- stats_proсessing_tables.sql
--------------------------------------------------------------------------------
-- Таблицы для статистической обработки  данных
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Первый временной ряд
DROP TABLE IF EXISTS first_time_series;
CREATE UNLOGGED TABLE first_time_series 
(
 	curr_timestamp timestamp with time zone , 
	curr_value DOUBLE PRECISION 
);

COMMENT ON TABLE first_time_series IS 'Первый временной ряд';
COMMENT ON COLUMN first_time_series.curr_timestamp IS 'Точка времени сбора данных ';
COMMENT ON COLUMN first_time_series.curr_timestamp IS 'Значение';
-- Первый временной ряд
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Второй временной ряд
DROP TABLE IF EXISTS second_time_series;
CREATE UNLOGGED TABLE second_time_series 
(
 	curr_timestamp timestamp with time zone , 
	curr_value DOUBLE PRECISION 
);

COMMENT ON TABLE second_time_series IS 'Второй временной ряд';
COMMENT ON COLUMN second_time_series.curr_timestamp IS 'Точка времени сбора данных ';
COMMENT ON COLUMN second_time_series.curr_timestamp IS 'Значение';
-- Второй временной ряд
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- временной ряд для проверки на стационарность для причинности Гренджера
DROP TABLE IF EXISTS test_stationary_series;
CREATE UNLOGGED TABLE test_stationary_series
(
 	curr_timestamp timestamp with time zone , 
	curr_value DOUBLE PRECISION 
);

COMMENT ON TABLE test_stationary_series IS 'временной ряд для проверки на стационарность для причинности Гренджера';
COMMENT ON COLUMN test_stationary_series.curr_timestamp IS 'Точка времени сбора данных ';
COMMENT ON COLUMN test_stationary_series.curr_timestamp IS 'Значение';
-- временной ряд для проверки на стационарность для причинности Гренджера
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- преобразование нестационарного временного ряда в стационарный
DROP TABLE IF EXISTS series_for_make_series_stationary;
CREATE UNLOGGED TABLE series_for_make_series_stationary
(
 	curr_timestamp timestamp with time zone , 
	differenced_value DOUBLE PRECISION ,
	log_value DOUBLE PRECISION ,
	standardized_value DOUBLE PRECISION
);

COMMENT ON TABLE series_for_make_series_stationary IS 'преобразование нестационарного временного ряда в стационарный';
COMMENT ON COLUMN series_for_make_series_stationary.curr_timestamp IS 'Точка времени сбора данных ';
COMMENT ON COLUMN series_for_make_series_stationary.differenced_value IS 'Дифференцирование';
COMMENT ON COLUMN series_for_make_series_stationary.log_value IS 'Логарифмирование';
COMMENT ON COLUMN series_for_make_series_stationary.standardized_value IS 'Стандартизация';
-- преобразованиt нестационарного временного ряда в стационарный
--------------------------------------------------------------------------------



--------------------------------------------------------------------------------
-- временной ряд для проверки на стационарность для причинности Гренджера
DROP TABLE IF EXISTS test_stationary_series;
CREATE UNLOGGED TABLE test_stationary_series
(
 	curr_timestamp timestamp with time zone , 
	curr_value DOUBLE PRECISION 
);

COMMENT ON TABLE test_stationary_series IS 'временной ряд для проверки на стационарность для причинности Гренджера';
COMMENT ON COLUMN test_stationary_series.curr_timestamp IS 'Точка времени сбора данных ';
COMMENT ON COLUMN test_stationary_series.curr_timestamp IS 'Значение';
-- временной ряд для проверки на стационарность для причинности Гренджера
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- СПРАВОЧНИКИ - ЛОГИРУЮТСЯ 
-- мероприятия ВКО по типам ожиданий
-- Weighted Correlation of Expectations
DROP TABLE IF EXISTS wce;
CREATE TABLE wce 
(
	id SERIAL  , 
	wait_event_type text
);

COMMENT ON TABLE wce IS 'мероприятия ВКО по типам ожиданий';
COMMENT ON COLUMN wce.wait_event_type IS 'Тип ожидания';

DROP TABLE IF EXISTS score;
CREATE TABLE score 
(	
	id SERIAL ,
	wait_event_type_id integer  ,
	min_score_wait_event_type numeric , 
	max_score_wait_event_type numeric 
);

COMMENT ON TABLE score IS 'мероприятия ВКО по типам ожиданий';
COMMENT ON COLUMN score.wait_event_type_id IS 'ID - wce';
COMMENT ON COLUMN score.min_score_wait_event_type IS 'Левая граница - значение ВКО для данного типа ожидания';
COMMENT ON COLUMN score.max_score_wait_event_type IS 'Правая граница - значение ВКО для данного типа ожидания';

DROP TABLE IF EXISTS activities;
CREATE TABLE activities 
(	
	score_wait_event_type_id integer ,
	list text[] 
);

COMMENT ON TABLE activities IS 'Мероприятия ВКО по типам ожиданий';
COMMENT ON COLUMN activities.score_wait_event_type_id IS 'ID - score ';
COMMENT ON COLUMN activities.list IS 'Мероприятия ВКО по типам ожиданий по данному типу ожидания для данного значения ВКО';

-------------------------------------------------------------------
-- Заполнение таблиц ВКО
INSERT INTO wce ( wait_event_type ) VALUES ('BufferPin');
INSERT INTO wce ( wait_event_type ) VALUES ('Extension');
INSERT INTO wce ( wait_event_type ) VALUES ('IO');
INSERT INTO wce ( wait_event_type ) VALUES ('IPC');
INSERT INTO wce ( wait_event_type ) VALUES ('Lock');
INSERT INTO wce ( wait_event_type ) VALUES ('LWLock');
INSERT INTO wce ( wait_event_type ) VALUES ('Timeout');

INSERT INTO score ( wait_event_type_id , min_score_wait_event_type , max_score_wait_event_type ) VALUES ( (SELECT id FROM wce WHERE wait_event_type = 'BufferPin') , 0 , 0.01 );
INSERT INTO score ( wait_event_type_id , min_score_wait_event_type , max_score_wait_event_type ) VALUES ( (SELECT id FROM wce WHERE wait_event_type = 'BufferPin') , 0.01 , 0.04 );
INSERT INTO score ( wait_event_type_id , min_score_wait_event_type , max_score_wait_event_type ) VALUES ( (SELECT id FROM wce WHERE wait_event_type = 'BufferPin') , 0.04 , 0.1 );
INSERT INTO score ( wait_event_type_id , min_score_wait_event_type , max_score_wait_event_type ) VALUES ( (SELECT id FROM wce WHERE wait_event_type = 'BufferPin') , 0.1 , 0.2 );
INSERT INTO score ( wait_event_type_id , min_score_wait_event_type , max_score_wait_event_type ) VALUES ( (SELECT id FROM wce WHERE wait_event_type = 'BufferPin') , 0.2 , 1.0 );

INSERT INTO score ( wait_event_type_id , min_score_wait_event_type , max_score_wait_event_type ) VALUES ( (SELECT id FROM wce WHERE wait_event_type = 'Extension') , 0 , 0.01 );
INSERT INTO score ( wait_event_type_id , min_score_wait_event_type , max_score_wait_event_type ) VALUES ( (SELECT id FROM wce WHERE wait_event_type = 'Extension') , 0.01 , 0.04 );
INSERT INTO score ( wait_event_type_id , min_score_wait_event_type , max_score_wait_event_type ) VALUES ( (SELECT id FROM wce WHERE wait_event_type = 'Extension') , 0.04 , 0.1 );
INSERT INTO score ( wait_event_type_id , min_score_wait_event_type , max_score_wait_event_type ) VALUES ( (SELECT id FROM wce WHERE wait_event_type = 'Extension') , 0.1 , 0.2 );
INSERT INTO score ( wait_event_type_id , min_score_wait_event_type , max_score_wait_event_type ) VALUES ( (SELECT id FROM wce WHERE wait_event_type = 'Extension') , 0.2 , 1.0 );

INSERT INTO score ( wait_event_type_id , min_score_wait_event_type , max_score_wait_event_type ) VALUES ( (SELECT id FROM wce WHERE wait_event_type = 'IO') , 0 , 0.01 );
INSERT INTO score ( wait_event_type_id , min_score_wait_event_type , max_score_wait_event_type ) VALUES ( (SELECT id FROM wce WHERE wait_event_type = 'IO') , 0.01 , 0.04 );
INSERT INTO score ( wait_event_type_id , min_score_wait_event_type , max_score_wait_event_type ) VALUES ( (SELECT id FROM wce WHERE wait_event_type = 'IO') , 0.04 , 0.1 );
INSERT INTO score ( wait_event_type_id , min_score_wait_event_type , max_score_wait_event_type ) VALUES ( (SELECT id FROM wce WHERE wait_event_type = 'IO') , 0.1 , 0.2 );
INSERT INTO score ( wait_event_type_id , min_score_wait_event_type , max_score_wait_event_type ) VALUES ( (SELECT id FROM wce WHERE wait_event_type = 'IO') , 0.2 , 1.0 );

INSERT INTO score ( wait_event_type_id , min_score_wait_event_type , max_score_wait_event_type ) VALUES ( (SELECT id FROM wce WHERE wait_event_type = 'IPC') , 0 , 0.01 );
INSERT INTO score ( wait_event_type_id , min_score_wait_event_type , max_score_wait_event_type ) VALUES ( (SELECT id FROM wce WHERE wait_event_type = 'IPC') , 0.01 , 0.04 );
INSERT INTO score ( wait_event_type_id , min_score_wait_event_type , max_score_wait_event_type ) VALUES ( (SELECT id FROM wce WHERE wait_event_type = 'IPC') , 0.04 , 0.1 );
INSERT INTO score ( wait_event_type_id , min_score_wait_event_type , max_score_wait_event_type ) VALUES ( (SELECT id FROM wce WHERE wait_event_type = 'IPC') , 0.1 , 0.2 );
INSERT INTO score ( wait_event_type_id , min_score_wait_event_type , max_score_wait_event_type ) VALUES ( (SELECT id FROM wce WHERE wait_event_type = 'IPC') , 0.2 , 1.0 );

INSERT INTO score ( wait_event_type_id , min_score_wait_event_type , max_score_wait_event_type ) VALUES ( (SELECT id FROM wce WHERE wait_event_type = 'Lock') , 0 , 0.01 );
INSERT INTO score ( wait_event_type_id , min_score_wait_event_type , max_score_wait_event_type ) VALUES ( (SELECT id FROM wce WHERE wait_event_type = 'Lock') , 0.01 , 0.04 );
INSERT INTO score ( wait_event_type_id , min_score_wait_event_type , max_score_wait_event_type ) VALUES ( (SELECT id FROM wce WHERE wait_event_type = 'Lock') , 0.04 , 0.1 );
INSERT INTO score ( wait_event_type_id , min_score_wait_event_type , max_score_wait_event_type ) VALUES ( (SELECT id FROM wce WHERE wait_event_type = 'Lock') , 0.1 , 0.2 );
INSERT INTO score ( wait_event_type_id , min_score_wait_event_type , max_score_wait_event_type ) VALUES ( (SELECT id FROM wce WHERE wait_event_type = 'Lock') , 0.2 , 1.0 );

INSERT INTO score ( wait_event_type_id , min_score_wait_event_type , max_score_wait_event_type ) VALUES ( (SELECT id FROM wce WHERE wait_event_type = 'LWLock') , 0 , 0.01 );
INSERT INTO score ( wait_event_type_id , min_score_wait_event_type , max_score_wait_event_type ) VALUES ( (SELECT id FROM wce WHERE wait_event_type = 'LWLock') , 0.01 , 0.04 );
INSERT INTO score ( wait_event_type_id , min_score_wait_event_type , max_score_wait_event_type ) VALUES ( (SELECT id FROM wce WHERE wait_event_type = 'LWLock') , 0.04 , 0.1 );
INSERT INTO score ( wait_event_type_id , min_score_wait_event_type , max_score_wait_event_type ) VALUES ( (SELECT id FROM wce WHERE wait_event_type = 'LWLock') , 0.1 , 0.2 );
INSERT INTO score ( wait_event_type_id , min_score_wait_event_type , max_score_wait_event_type ) VALUES ( (SELECT id FROM wce WHERE wait_event_type = 'LWLock') , 0.2 , 1.0 );

INSERT INTO score ( wait_event_type_id , min_score_wait_event_type , max_score_wait_event_type ) VALUES ( (SELECT id FROM wce WHERE wait_event_type = 'Timeout') , 0 , 0.01 );
INSERT INTO score ( wait_event_type_id , min_score_wait_event_type , max_score_wait_event_type ) VALUES ( (SELECT id FROM wce WHERE wait_event_type = 'Timeout') , 0.01 , 0.04 );
INSERT INTO score ( wait_event_type_id , min_score_wait_event_type , max_score_wait_event_type ) VALUES ( (SELECT id FROM wce WHERE wait_event_type = 'Timeout') , 0.04 , 0.1 );
INSERT INTO score ( wait_event_type_id , min_score_wait_event_type , max_score_wait_event_type ) VALUES ( (SELECT id FROM wce WHERE wait_event_type = 'Timeout') , 0.1 , 0.2 );
INSERT INTO score ( wait_event_type_id , min_score_wait_event_type , max_score_wait_event_type ) VALUES ( (SELECT id FROM wce WHERE wait_event_type = 'Timeout') , 0.2 , 1.0 );
-- Заполнение таблиц ВКО

--------------------------------------------------------------------------------
-- Веса критериев
DROP TABLE IF EXISTS wait_event_type_criteria_weight  ;
CREATE TABLE wait_event_type_criteria_weight 
(	
	curr_value numeric[4]	
);

COMMENT ON TABLE wait_event_type_criteria_weight IS 'Веса критериев';
COMMENT ON COLUMN wait_event_type_criteria_weight.curr_value IS 'Вес i-критерия';
-- Веса критериев
--------------------------------------------------------------------------------
-- СПРАВОЧНИКИ - ЛОГИРУЮТСЯ 
-------------------------------------------------------------------

-- мероприятия ВКО по типам ожиданий
-- Weighted Correlation of Expectations
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--Сервисная таблица для комплексного корреляционного анализа 
DROP TABLE IF EXISTS correlation_regression_flags ;
CREATE UNLOGGED TABLE correlation_regression_flags 
(	
	correlation_flag BOOLEAN , 	
	regression_flag BOOLEAN ,
	is_series_stationary_flag BOOLEAN 
);

COMMENT ON TABLE correlation_regression_flags IS 'Сервисная таблица для комплексного корреляционного анализа';
COMMENT ON COLUMN correlation_regression_flags.correlation_flag IS 'Шаг 1. Интерпретация корреляций.';
COMMENT ON COLUMN correlation_regression_flags.regression_flag IS 'Шаг 2. Интерпретация коэффициента детерминации R2.';
COMMENT ON COLUMN correlation_regression_flags.is_series_stationary_flag IS 'Шаг 3. Проверка стационарности ряда';
--Сервисная таблица для комплексного корреляционного анализа 



--------------------------------------------------------------------------------
-- Матрица критериев
DROP TABLE IF EXISTS wait_event_type_criteria_matrix  ;
CREATE UNLOGGED TABLE wait_event_type_criteria_matrix 
(	
	wait_event_type text ,
	r_value DOUBLE PRECISION , 
	calculated_r_norm DOUBLE PRECISION , 
	p_value DOUBLE PRECISION , 
	calculated_p_norm DOUBLE PRECISION , 
	w_value DOUBLE PRECISION , 
	calculated_w_norm DOUBLE PRECISION , 
	r2_value DOUBLE PRECISION ,
	calculated_r2_norm DOUBLE PRECISION 	 	
);

COMMENT ON TABLE wait_event_type_criteria_matrix IS 'Матрица критериев';
COMMENT ON COLUMN wait_event_type_criteria_matrix.wait_event_type IS 'тип ожидания';
COMMENT ON COLUMN wait_event_type_criteria_matrix.r_value IS 'r Корреляция';
COMMENT ON COLUMN wait_event_type_criteria_matrix.p_value IS 'p-value';
COMMENT ON COLUMN wait_event_type_criteria_matrix.w_value IS 'ВКО (w)';
COMMENT ON COLUMN wait_event_type_criteria_matrix.r2_value IS 'R2';
COMMENT ON COLUMN wait_event_type_criteria_matrix.calculated_r_norm IS 'Нормализованное значение - r Корреляция';
COMMENT ON COLUMN wait_event_type_criteria_matrix.calculated_p_norm IS 'Нормализованное значение - p-value';
COMMENT ON COLUMN wait_event_type_criteria_matrix.calculated_w_norm IS 'Нормализованное значение - ВКО (w)';
COMMENT ON COLUMN wait_event_type_criteria_matrix.calculated_r2_norm IS 'Нормализованное значение - R2';
-- Матрица критериев
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Матрица для расчета Индекса Приоритета Корреляции (Correlation Priority Index, CPI) .
DROP TABLE IF EXISTS cpi_matrix  ;
CREATE UNLOGGED TABLE cpi_matrix 
(
		current_pair text ,
		r_norm DOUBLE PRECISION , 
        r2_norm DOUBLE PRECISION , 
		slope_source DOUBLE PRECISION , 
		slope_norm DOUBLE PRECISION , 
		curr_value DOUBLE PRECISION 
);
COMMENT ON TABLE cpi_matrix IS 'Матрица для расчета Индекса Приоритета Корреляции (Correlation Priority Index, CPI)';
COMMENT ON COLUMN cpi_matrix.current_pair IS 'Наименования значений для вычисления cpi';
COMMENT ON COLUMN cpi_matrix.r_norm IS 'Нормализованное значение коэфициента корреляции';
COMMENT ON COLUMN cpi_matrix.r2_norm IS 'Нормализованное значение коэфициента детерминации';
COMMENT ON COLUMN cpi_matrix.slope_source IS 'Исходное значение угла наклона линии регрессии';
COMMENT ON COLUMN cpi_matrix.slope_norm IS 'Нормальзованное значение угла наклона линии регрессии';
COMMENT ON COLUMN cpi_matrix.curr_value IS 'Значение Индекса Приоритета Корреляции (Correlation Priority Index, CPI)';

-- Матрица для расчета Индекса Приоритета Корреляции (Correlation Priority Index, CPI) .
--------------------------------------------------------------------------------


--------------------------------------------------------------------------------
--интегральный приоритет Pi для wait_event_type
DROP TABLE IF EXISTS wait_event_type_Pi  ;
CREATE UNLOGGED TABLE wait_event_type_Pi 
(
	wait_event_type text ,
	integral_priority DOUBLE PRECISION		
);
COMMENT ON TABLE wait_event_type_Pi IS 'интегральный приоритет Pi для wait_event_type';
COMMENT ON COLUMN wait_event_type_Pi.wait_event_type IS 'тип ожидания';
COMMENT ON COLUMN wait_event_type_Pi.integral_priority IS 'интегральный приоритет';
--интегральный приоритет Pi для wait_event_type
--------------------------------------------------------------------------------





--------------------------------------------------------------------------------
-- background_processes_logs_tables.sql
-- Updated 19.04.2026
--------------------------------------------------------------------------------
-- Статистика фоновых процессов
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Создание таблицы для хранения событий autovacuum, извлечённых из логов PostgreSQL
CREATE TABLE IF NOT EXISTS autovacuum_log_events (
    id             BIGSERIAL PRIMARY KEY,      -- суррогатный ключ
    curr_timestamp TIMESTAMPTZ NOT NULL,       -- временная метка события из лога
    database_name  TEXT        NOT NULL,       -- имя базы данных
    schema_name    TEXT        NOT NULL,       -- имя схемы
    table_name     TEXT        NOT NULL,       -- имя таблицы
    duration_ms    NUMERIC,                    -- продолжительность операции в миллисекундах
    index_scans    INTEGER,                    -- количество сканирований индексов
    pages_removed  BIGINT,                     -- pages: X removed
    pages_remain   BIGINT                      -- pages: Y remain
);

-- Индексы для ускорения агрегирующих запросов
CREATE INDEX IF NOT EXISTS idx_av_log_curr_timestamp ON autovacuum_log_events (curr_timestamp);
CREATE INDEX IF NOT EXISTS idx_av_log_db_table     ON autovacuum_log_events (database_name, schema_name, table_name);

-- Комментарии к таблице и столбцам
COMMENT ON TABLE autovacuum_log_events IS 'События autovacuum, полученные из логов СУБД PostgreSQL (log_autovacuum_min_duration=0).';

COMMENT ON COLUMN autovacuum_log_events.id IS 'Уникальный идентификатор записи (автоинкремент)';
COMMENT ON COLUMN autovacuum_log_events.curr_timestamp IS 'Временная метка записи в логе (соответствует началу строки в формате log_line_prefix)';
COMMENT ON COLUMN autovacuum_log_events.database_name IS 'Имя базы данных, в которой выполнялся autovacuum';
COMMENT ON COLUMN autovacuum_log_events.schema_name IS 'Имя схемы обработанной таблицы';
COMMENT ON COLUMN autovacuum_log_events.table_name IS 'Имя обработанной таблицы';
COMMENT ON COLUMN autovacuum_log_events.duration_ms IS 'Длительность операции в миллисекундах (из строки "duration: X ms")';
COMMENT ON COLUMN autovacuum_log_events.index_scans IS 'Число сканирований индексов (из "index scans: X")';
COMMENT ON COLUMN autovacuum_log_events.pages_removed IS 'Количество освобождённых страниц (из "pages: X removed")';
COMMENT ON COLUMN autovacuum_log_events.pages_remain IS 'Количество оставшихся страниц (из "pages: Y remain")';
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- markov_chain_tables.sql
--------------------------------------------------------------------------------
-- Таблицы для расчета цепи Маркова 
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- КОНФИГУРАЦИЯ 
/*
Настройка параметров
Параметры alpha и interval_hours можно менять вручную через UPDATE markov_config.
Рекомендации:
Для стабильной нагрузки: alpha = 0.01, интервал 1 час.
Если нагрузка меняется медленно, можно уменьшить alpha до 0.005 и увеличить интервал до 2–4 часов.
При обнаружении дрейфа (методика check_and_forget) можно форсированно увеличить alpha или уменьшить интервал на время, изменив значения в markov_config.
*/
/*
Включить адаптивное забывание (по умолчанию):
UPDATE markov_config SET adaptive_forgetting_enabled = true;

Временно отключить (например, при отладке):
UPDATE markov_config SET adaptive_forgetting_enabled = false;
После отключения вызов SELECT check_and_forget(); будет немедленно возвращать сообщение:
Adaptive forgetting is disabled by markov_config.adaptive_forgetting_enabled = false.
При этом плановое забывание внутри markov_chain_training() продолжит работать с параметром alpha и interval_minute из той же таблицы.
*/
/*
Более частое применение забывания позволяет плавно уменьшать веса, 
не дожидаясь накопления большого объёма «устаревших» переходов. 
При медианном окне в 1 час полчаса – разумный компромисс между адаптивностью и вычислительной нагрузкой.
скорость забывания: период полураспада ≈6.5 часов (при 0.10)
*/

DROP TABLE IF EXISTS markov_config;
CREATE UNLOGGED TABLE IF NOT EXISTS markov_config (
    last_forget_time  TIMESTAMPTZ NOT NULL DEFAULT now(),
    alpha             REAL       NOT NULL DEFAULT 0.1,
    interval_minute   INT        NOT NULL DEFAULT 30 , 
	forecast_log_retention_days  SMALLINT NOT NULL DEFAULT 21 ,
	transition_log_retention_days SMALLINT DEFAULT 21, 
	--CHECK_AND_FORGET
	kl_threshold          REAL DEFAULT 0.4,
    chi2_threshold        REAL DEFAULT 220.0,      -- для 188 степеней свободы при p=0.01
    os_dev_threshold      REAL DEFAULT 0.3,
    brier_threshold       REAL DEFAULT 0.25,
    check_interval_minutes INT DEFAULT 15,         -- период вызова check_and_forget
    forget_alpha_max      REAL DEFAULT 0.5,
    confirmation_cycles   SMALLINT DEFAULT 2 , 
	adaptive_forgetting_enabled BOOLEAN DEFAULT true , 
	archive_retention_days SMALLINT DEFAULT 21 , -- archive_retention_days = 21 – удалять снимки старше 3 недель.
    check_state_retention_days SMALLINT DEFAULT 7 , --check_state_retention_days = 7 – хранить историю проверок для механизма подтверждения, но не более недели.
    forget_log_retention_days SMALLINT DEFAULT 90 , --forget_log_retention_days = 90 – журнал забываний можно хранить дольше для аудита.
	brier_min_observations SMALLINT DEFAULT 10 ,   -- Минимальное количество прогнозов за последние 2 часа для расчёта Brier Score
	--CHECK_AND_FORGET
	--Адаптивное изменение alpha в зависимости от времени, прошедшего с последнего инцидента
	use_adaptive_alpha BOOLEAN DEFAULT false ,
	base_alpha REAL DEFAULT 0.1 ,
	min_alpha REAL DEFAULT 0.01 ,
	incident_half_life_days REAL DEFAULT 7.0 ,
	last_incident_time TIMESTAMPTZ DEFAULT NULL ,
	apply_forgetting_log_retention_days INT DEFAULT 21
	--Адаптивное изменение alpha в зависимости от времени, прошедшего с последнего инцидента

);
COMMENT ON TABLE markov_config IS 'таблица конфигурации, включая управление адаптивным забыванием';
COMMENT ON COLUMN markov_config.last_forget_time IS 'Время последнего забывания';
COMMENT ON COLUMN markov_config.alpha IS 'Скорость забывания';
COMMENT ON COLUMN markov_config.interval_minute IS 'Интервал забывания в минутах';
COMMENT ON COLUMN markov_config.forecast_log_retention_days IS 'Глубина хранения данных в таблице forecast_log ';
COMMENT ON COLUMN markov_config.transition_log_retention_days IS 'Глубина хранения данных в таблице transition_log ';
COMMENT ON COLUMN markov_config.kl_threshold IS 'Порог KL-дивергенции для форсированного забывания';
COMMENT ON COLUMN markov_config.chi2_threshold IS 'Порог χ² для форсированного забывания';
COMMENT ON COLUMN markov_config.os_dev_threshold IS 'Порог отклонения операционной скорости (30%)';
COMMENT ON COLUMN markov_config.brier_threshold IS 'Порог Brier Score (0.25)';
COMMENT ON COLUMN markov_config.check_interval_minutes IS 'Интервал плановой проверки (минуты)';
COMMENT ON COLUMN markov_config.confirmation_cycles IS 'Сколько проверок подряд должен сохраняться признак для срабатывания';
COMMENT ON COLUMN markov_config.adaptive_forgetting_enabled IS 'Если false, функция check_and_forget не выполняет забывание (но плановое забывание в markov_chain_training продолжает работать)';
COMMENT ON COLUMN markov_config.archive_retention_days IS 'Глубина хранения архивных снимков матрицы (markov_probabilities_archive), дней';
COMMENT ON COLUMN markov_config.check_state_retention_days IS 'Глубина хранения истории проверок check_and_forget (check_state), дней';
COMMENT ON COLUMN markov_config.forget_log_retention_days IS 'Глубина хранения журнала форсированных забываний (forget_log), дней';
COMMENT ON COLUMN markov_config.use_adaptive_alpha IS 'Если true, alpha вычисляется динамически на основе времени с последнего инцидента';
COMMENT ON COLUMN markov_config.base_alpha IS 'Базовое значение alpha (при частых инцидентах)';
COMMENT ON COLUMN markov_config.min_alpha IS 'Минимальное значение alpha (при очень редких инцидентах)';
COMMENT ON COLUMN markov_config.incident_half_life_days IS 'Период полураспада веса инцидента (дни)';
COMMENT ON COLUMN markov_config.last_incident_time IS 'Время последнего перехода в аварийное состояние';
COMMENT ON COLUMN markov_config.apply_forgetting_log_retention_days IS 'Срок хранения записей в таблице apply_forgetting_log (дней)';
COMMENT ON COLUMN markov_config.brier_min_observations IS 'Минимальное количество прогнозов за последние 2 часа для расчёта Brier Score';

INSERT INTO markov_config (last_forget_time)
VALUES ( now() )
ON CONFLICT DO NOTHING;

--------------------------------------------------------------------------------
--Основная таблица переходных частот
/*
from_state / to_state — закодированные идентификаторы состояний (например, от 0 до 188 при размерности 189). SMALLINT занимает 2 байта, диапазона ±32 767 достаточно.
frequency — REAL (4 байта) . Для вероятностных расчётов REAL достаточно.
Минимальная строка таблицы: 2+2+4 = 8 байт данных + ≈27 байт служебных полей PostgreSQL ≈ 35 байт. При 35 000 ячеек (189×189) размер таблицы ≈ 1,2 МБ, индексы ещё примерно столько же. 
Это пренебрежимо мало для любой современной СУБД.

Преимущества построчного хранения:
Атомарное обновление одной ячейки: INSERT ... ON CONFLICT DO UPDATE SET frequency = frequency + 1.
Простое применение «забывания»: UPDATE markov_frequencies SET frequency = frequency * (1 - alpha).
Быстрое извлечение строки переходов из состояния: SELECT to_state, frequency FROM markov_frequencies WHERE from_state = $1.
*/
DROP TABLE IF EXISTS markov_frequencies;
CREATE TABLE markov_frequencies (
    from_state  SMALLINT NOT NULL,
    to_state    SMALLINT NOT NULL,
    frequency   REAL     NOT NULL DEFAULT 0.0
);

ALTER TABLE markov_frequencies ADD CONSTRAINT markov_frequencies_pk PRIMARY KEY (from_state, to_state);
COMMENT ON TABLE markov_frequencies IS 'Основная таблица переходных частот Цепи Маркова';
COMMENT ON COLUMN markov_frequencies.from_state IS 'Исходное состояние';
COMMENT ON COLUMN markov_frequencies.to_state IS 'Целевое  состояние';
COMMENT ON COLUMN markov_frequencies.frequency IS 'Частота переходов';
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Таблица журнала переходов
DROP TABLE IF EXISTS transition_log;
CREATE TABLE transition_log (
    id          BIGSERIAL    ,
    ts          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    from_state  SMALLINT     NOT NULL,
    to_state    SMALLINT     NOT NULL
);
ALTER TABLE transition_log ADD CONSTRAINT transition_log_pk PRIMARY KEY (id);
CREATE INDEX idx_transition_log_ts ON transition_log (ts);
-- Индекс для быстрой выборки по исходному состоянию (при необходимости)
CREATE INDEX idx_transition_log_from ON transition_log (from_state);

COMMENT ON TABLE transition_log IS 'Таблица журнала переходов';
COMMENT ON COLUMN transition_log.ts IS 'Точка наблюдения';
COMMENT ON COLUMN transition_log.from_state IS 'Исходное состояние';
COMMENT ON COLUMN transition_log.to_state IS 'Целевое  состояние';

--------------------------------------------------------------------------------
-- Вероятности для цепи Маркова
DROP TABLE IF EXISTS markov_probabilities;
CREATE TABLE markov_probabilities (
    from_state  SMALLINT NOT NULL,
    to_state    SMALLINT NOT NULL,
    probability REAL NOT NULL
);

ALTER TABLE markov_probabilities ADD CONSTRAINT markov_probabilities_pk PRIMARY KEY (from_state, to_state);
COMMENT ON TABLE markov_probabilities IS 'Основная таблица переходных частот Цепи Маркова';
COMMENT ON COLUMN markov_probabilities.from_state IS 'Исходное состояние';
COMMENT ON COLUMN markov_probabilities.to_state IS 'Целевое  состояние';
COMMENT ON COLUMN markov_probabilities.probability IS 'Вероятность перехода';
--
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Таблица для хранения снимка матрицы вероятностей
-- за предыдущую неделю
DROP TABLE IF EXISTS markov_probabilities_prev_week;
CREATE TABLE markov_probabilities_prev_week (
    from_state  SMALLINT NOT NULL,
    to_state    SMALLINT NOT NULL,
    probability REAL    NOT NULL
);
ALTER TABLE markov_probabilities_prev_week ADD CONSTRAINT markov_probabilities_prev_week_pk PRIMARY KEY (from_state, to_state);
COMMENT ON TABLE markov_probabilities_prev_week IS 'Таблица для хранения снимка матрицы вероятностей';
COMMENT ON COLUMN markov_probabilities_prev_week.from_state IS 'Исходное состояние';
COMMENT ON COLUMN markov_probabilities_prev_week.to_state IS 'Целевое  состояние';
COMMENT ON COLUMN markov_probabilities_prev_week.probability IS 'Вероятность перехода';

--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Таблица поглощающей матрицы
DROP TABLE IF EXISTS markov_absorbing;
CREATE TABLE IF NOT EXISTS markov_absorbing (
    from_state  SMALLINT NOT NULL,
    to_state    SMALLINT NOT NULL,
    probability REAL    NOT NULL
);

ALTER TABLE markov_absorbing ADD CONSTRAINT markov_absorbing_pk PRIMARY KEY (from_state, to_state);
COMMENT ON TABLE markov_absorbing IS 'Таблица поглощающей матрицы';
COMMENT ON COLUMN markov_absorbing.from_state IS 'Исходное состояние';
COMMENT ON COLUMN markov_absorbing.to_state IS 'Целевое  состояние';
COMMENT ON COLUMN markov_absorbing.probability IS 'Вероятность перехода';
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Справочник состояний
DROP TABLE IF EXISTS state_descriptions;
CREATE TABLE state_descriptions (
    state_id    SMALLINT ,
    correlation REAL    NOT NULL,   
    os_trend    SMALLINT NOT NULL,  
    wait_trend  SMALLINT NOT NULL   
);
ALTER TABLE state_descriptions ADD CONSTRAINT state_descriptions_pk PRIMARY KEY (state_id);
COMMENT ON TABLE state_descriptions IS 'Справочник состояний';
COMMENT ON COLUMN state_descriptions.correlation IS 'Округлённое значение коэффициента корреляции';
COMMENT ON COLUMN state_descriptions.os_trend IS 'Направление тренда операционной скорости -1, 0, 1';
COMMENT ON COLUMN state_descriptions.wait_trend IS 'Направление тренда ожиданий  -1, 0, 1';
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Таблица для обучения цепи Маркова
DROP TABLE IF EXISTS markov_chain;
CREATE UNLOGGED TABLE markov_chain (
    prev_correlation REAL    ,   
    prev_os_trend    SMALLINT ,  
    prev_wait_trend  SMALLINT , 
    curr_correlation REAL  NOT NULL   ,   
    curr_os_trend    SMALLINT  NOT NULL,  
    curr_wait_trend  SMALLINT  NOT NULL 
);
COMMENT ON TABLE markov_chain IS 'Таблица для обучения цепи Маркова';
COMMENT ON COLUMN markov_chain.prev_correlation IS 'Предыдущее значение :Округлённое значение коэффициента корреляции';
COMMENT ON COLUMN markov_chain.prev_os_trend IS 'Предыдущее значение :Направление тренда операционной скорости -1, 0, 1';
COMMENT ON COLUMN markov_chain.prev_wait_trend IS 'Предыдущее значение :Направление тренда ожиданий  -1, 0, 1';
COMMENT ON COLUMN markov_chain.curr_correlation IS 'Новое значение:Округлённое значение коэффициента корреляции';
COMMENT ON COLUMN markov_chain.curr_os_trend IS 'Новое значение: Направление тренда операционной скорости -1, 0, 1';
COMMENT ON COLUMN markov_chain.curr_wait_trend IS 'Новое значение: Направление тренда ожиданий  -1, 0, 1';
-- Таблица для обучения цепи Маркова
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Таблица журнала прогнозов для оценки точности модели
DROP TABLE IF EXISTS forecast_log;
CREATE UNLOGGED TABLE forecast_log (
    id              BIGSERIAL    ,
    ts              TIMESTAMPTZ  NOT NULL DEFAULT now(),   
    model_train_date DATE        NOT NULL,                
    predicted_risk  REAL         NOT NULL,                
    actual_risk     SMALLINT     NOT NULL CHECK (actual_risk IN (0, 1)),  
    from_state      SMALLINT     ,                        
    to_state        SMALLINT                              
);
ALTER TABLE forecast_log ADD CONSTRAINT forecast_log_pk PRIMARY KEY (id);

-- Индексы для быстрой выборки по дате модели и времени
CREATE INDEX idx_forecast_log_model_date ON forecast_log (model_train_date);
CREATE INDEX idx_forecast_log_ts ON forecast_log (ts);

COMMENT ON TABLE forecast_log IS 'Таблица журнала прогнозов для оценки точности модели';
COMMENT ON COLUMN forecast_log.ts IS '-- момент, когда стал известен фактический исход';
COMMENT ON COLUMN forecast_log.model_train_date IS '-- дата, до которой обучена модель (идентификатор версии)';
COMMENT ON COLUMN forecast_log.predicted_risk IS '-- предсказанная вероятность аварии на 1 шаг';
COMMENT ON COLUMN forecast_log.actual_risk IS '-- 1 если переход был в аварийное состояние';
COMMENT ON COLUMN forecast_log.from_state IS '-- (опционально) исходное состояние для анализа';
COMMENT ON COLUMN forecast_log.to_state IS '-- (опционально) фактическое состояние';

--------------------------------------------------------------------------------



------------------------------------------------------------------
-- Таблица для архивных снимков матрицы вероятностей
/*
train_date (DATE, NOT NULL)
Дата, определяющая версию модели. Обычно соответствует последнему календарному дню, данные за который были включены в обучение матрицы на момент создания снимка.
Позволяет впоследствии выбрать модель по состоянию «на дату» для ретроспективного анализа точности или сравнения моделей разных периодов.

from_state (SMALLINT, NOT NULL)
Кодированный идентификатор исходного состояния марковской цепи. Диапазон значений: 0 … 188 (всего 189 возможных состояний — комбинации коэффициента корреляции и трендов операционной скорости/ожиданий).
Совместно с train_date и to_state формирует уникальный ключ записи.

to_state (SMALLINT, NOT NULL)
Идентификатор целевого состояния, в которое осуществляется переход. Принимает значения из того же диапазона, что и from_state.

probability (REAL, NOT NULL)
Оценка вероятности перехода P(from_state → to_state), вычисленная по частотам переходов на момент создания снимка. Гарантирует, что для каждого from_state сумма вероятностей по всем to_state равна 1 (в пределах точности REAL).
Используется в функциях прогнозирования риска по историческим моделям (например, predict_risk_1min_archived).
*/
DROP TABLE IF EXISTS markov_probabilities_archive;
CREATE TABLE markov_probabilities_archive (
    train_date  DATE     NOT NULL,   
    from_state  SMALLINT NOT NULL,   
    to_state    SMALLINT NOT NULL,   
    probability REAL     NOT NULL   
    
);
ALTER TABLE markov_probabilities_archive ADD CONSTRAINT markov_probabilities_archive_pk PRIMARY KEY (train_date, from_state, to_state);
-- Индексы для ускорения запросов прогноза по конкретной модели
CREATE INDEX idx_archive_date_from ON markov_probabilities_archive (train_date, from_state);

COMMENT ON TABLE markov_probabilities_archive IS 'Таблица для архивных снимков матрицы вероятностей';
COMMENT ON COLUMN markov_probabilities_archive.train_date IS '-- дата, на которую зафиксирована модель (последний день обучающих данных)';
COMMENT ON COLUMN markov_probabilities_archive.from_state IS '-- идентификатор исходного состояния (0..188)';
COMMENT ON COLUMN markov_probabilities_archive.to_state IS '-- идентификатор целевого состояния (0..188)';
COMMENT ON COLUMN markov_probabilities_archive.probability IS '-- оценённая вероятность перехода P(from_state → to_state)';

------------------------------------------------------------------------------------------------------------------------------
-- CHECK_AND_FORGET

-- Эталонное распределение состояний для проверки дрейфа (методика 3.1)
DROP TABLE IF EXISTS state_baseline;
CREATE TABLE state_baseline (
    hour_of_day   SMALLINT NOT NULL,   -- 0..23
    dow           SMALLINT NOT NULL,   -- 1 = понедельник .. 7 = воскресенье
    state_id      SMALLINT NOT NULL,
    probability   REAL NOT NULL,
    last_updated  TIMESTAMPTZ DEFAULT now()
);
ALTER TABLE state_baseline ADD PRIMARY KEY (hour_of_day, dow, state_id);
COMMENT ON TABLE state_baseline IS 'Эталонное распределение состояний для проверки дрейфа (методика 3.1)';

-- Журнал форсированных забываний (методика 4.1)
DROP TABLE IF EXISTS forget_log;
CREATE TABLE forget_log (
    id            BIGSERIAL PRIMARY KEY,
    ts            TIMESTAMPTZ DEFAULT now(),
    alpha         REAL NOT NULL,
    triggered_by  TEXT[],                 -- массив сработавших признаков: {'KL','OS','Brier','Infra','Diurnal'}
    kl_div        REAL,
    chi2_val      REAL,
    brier_score   REAL,
    os_deviation  REAL,
    details       TEXT
);
COMMENT ON TABLE forget_log IS 'Журнал форсированных забываний (методика 4.1)';

--Статистика операционной скорости (для обнаружения аномалий)
--Среднее и стандартное отклонение операционной скорости по часам (последние 20 дней)
DROP TABLE IF EXISTS operational_speed_stats;
CREATE TABLE operational_speed_stats (
    hour_of_day   SMALLINT NOT NULL,
    avg_speed     REAL NOT NULL,
    stddev_speed  REAL NOT NULL,
    last_updated  TIMESTAMPTZ DEFAULT now()
);
ALTER TABLE operational_speed_stats ADD PRIMARY KEY (hour_of_day);
COMMENT ON TABLE operational_speed_stats IS 'Среднее и стандартное отклонение операционной скорости по часам (последние 20 дней)';

-- Таблица для регистрации внешних событий (заполняется через триггеры CI/CD или вручную)
DROP TABLE IF EXISTS infrastructure_events;
CREATE TABLE infrastructure_events (
    id           BIGSERIAL PRIMARY KEY,
    event_time   TIMESTAMPTZ DEFAULT now(),
    event_type   TEXT NOT NULL,   -- 'deploy', 'config_change', 'failover', 'manual'
    description  TEXT,
    processed    BOOLEAN DEFAULT false
);
COMMENT ON TABLE infrastructure_events IS 'Таблица для регистрации внешних событий (заполняется через триггеры CI/CD или вручную)';

-- Таблица для хранения состояния проверок (для механизма подтверждения)
DROP TABLE IF EXISTS check_state;
CREATE TABLE check_state (
    check_time    TIMESTAMPTZ PRIMARY KEY,
    kl_flag       BOOLEAN,
    chi2_flag     BOOLEAN,
    os_flag       BOOLEAN,
    brier_flag    BOOLEAN,
    infra_flag    BOOLEAN,
    diurnal_flag  BOOLEAN
);
COMMENT ON TABLE check_state IS 'Состояние каждой проверки check_and_forget для подтверждения признаков';
-- CHECK_AND_FORGET
------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------
-- Таблица журнала событий apply_forgetting
DROP TABLE IF EXISTS apply_forgetting_log;
CREATE TABLE apply_forgetting_log (
    id                  BIGSERIAL PRIMARY KEY,
    ts                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    effective_alpha     REAL NOT NULL,
    adaptive_used       BOOLEAN NOT NULL,
    days_since_incident REAL,
    alpha_override      REAL,
    details             TEXT
);
COMMENT ON TABLE apply_forgetting_log IS 'Журнал вызовов apply_forgetting (помимо RAISE NOTICE)';
COMMENT ON COLUMN apply_forgetting_log.effective_alpha IS 'Фактически применённый коэффициент забывания';
COMMENT ON COLUMN apply_forgetting_log.adaptive_used IS 'Использовалось ли адаптивное забывание (cfg.use_adaptive_alpha)';
COMMENT ON COLUMN apply_forgetting_log.days_since_incident IS 'Количество дней с последнего инцидента (NULL, если инцидентов не было или адаптивный режим выключен)';
COMMENT ON COLUMN apply_forgetting_log.alpha_override IS 'Значение переданного параметра alpha_override (если не NULL)';
COMMENT ON COLUMN apply_forgetting_log.details IS 'Дополнительная информация (например, значения параметров)';
--------------------------------------------------------------------------------
-- markov_chain_functions.sql
--------------------------------------------------------------------------------
-- ОБУЧЕНИЕ ЦЕПИ МАРКОВА
/*
Рекомендации по эксплуатации
Параметр	Рекомендуемое значение	Примечание
alpha (плановое)	0.01 – 0.02	Для медленной адаптации.
interval_minute	30 – 60	Плановое забывание каждые 30–60 минут.
Порог KL (в check_and_forget)	0.4	Форсированное забывание при превышении.
Порог Brier Score	0.25	Если прогнозы ухудшились.
confirmation_cycles	2	Чтобы избежать ложных срабатываний.
*/
-- markov_chain_training - обучение цепи Маркова
-- evaluate_training_sufficiency Основная функция проверки достаточности обучения 
------------------------------------------------------------------------------------------
-- Прогнозирование
-- predict_risk_1min получить вероятность попасть в аварийную зону 
-- predict_risk_k_diag получить вероятность попасть в аварийную зону за K шагов
------------------------------------------------------------------------------------------
-- Сервисные функции
-- fill_state_descriptions - Функция заполнения справочника состояний
-- get_state_id - Получить state_id для заданных r , OS_trend , wait_trend
-- update_markov_frequency - Обновить основную таблицу переходных частот
-- log_transition_and_update Функция логирования перехода и обновления матрицы частот
-- get_current_os_waiting_correlation_for_markov_chain - получить текущее значение коэффициента корреляции для цепи маркова на окне 1 час 
-- update_markov_probabilities Обновить матрицу вероятностей
-- rebuild_markov_absorbing заполнить матрицу поглощения
-- log_forecast Функция записи прогноза и его фактического исхода
-- predict_risk_1min_archived Вспомогательная функция прогноза по архивной матрице:
-- compare_brier_scores Расчёт и сравнение Brier Score
-- get_stationary_distribution Вспомогательная функция: получение стационарного распределения
-- check_kl_divergence KL-дивергенция стационарного и эмпирического (последняя неделя)
-- archive_markov_probabilities Архивация вероятностей цепи Маркова
-- calculate_kl_divergence Расчёт KL-дивергенции между текущим часом и эталоном
-- calculate_chi_squared Расчёт χ² – критерия
-- get_os_deviation Отклонение операционной скорости (SMA20 vs историческое среднее)
-- emergency_forget Функция экстренного забывания по событию
-- apply_forgetting Функция забывания
-- update_last_incident_time Обновление времени последнего инцидента (через триггер)
-- enable_adaptive_forgetting Функция для обновления конфигурации (включение адаптивного режима)
-- disable_adaptive_forgetting Отключает адаптивное забывание (use_adaptive_alpha = false)
-- get_adaptive_forgetting_status Получение параметров адаптивного режима забывания
-- set_last_incident_time Ручная установка времени последнего инцидента (для тестов или внешних событий)
--
-- get_forget_log Функция получения журнала forget_log за период
-- get_apply_forgetting_log Функция получения журнала apply_forgetting_log за период 
-- 
------------------------------------------------------------------------------------------
-- Сервисные функции по cron
--
-- # Основная процедура check_and_forget
-- */15 * * * * psql -d expecto_db -U expecto_user  -c "SELECT check_and_forget()"
-- # Функция создания/обновления снимка матрицы прошлой недели
-- 5 19 * * 5 psql -d expecto_db -U expecto_user  -c "SELECT snapshot_markov_prev_week();"
-- # Ежедневная очистка forecast_log в 01:30
-- 30 1 * * * psql -d expecto_db -U expecto_user -c "SELECT clean_forecast_log()"
-- # Ежедневная очистка transition_log в 01:15
-- 15 1 * * * psql -d expecto_db -U expecto_user -c "SELECT clean_transition_log()"
-- # Ежедневное обновление эталонного распределения состояний (в 01:00)
-- 0 1 * * * psql -d expecto_db -U expecto_user -c "SELECT update_state_baseline()"
-- # Ежедневное обновление статистики операционной скорости (в 01:30)
-- 30 1 * * * psql -d expecto_db -U expecto_user -c "SELECT refresh_os_stats()"
-- # Очистка архивных снимков матрицы (раз в неделю, в воскресенье в 02:00)
-- 0 2 * * 0 psql -d expecto_db -U expecto_user -c "SELECT clean_markov_probabilities_archive()"
-- # Очистка check_state (ежедневно в 03:00)
-- 0 3 * * * psql -d expecto_db -U expecto_user -c "SELECT clean_check_state()"
-- # Очистка forget_log (раз в месяц, например, 1-го числа в 04:00)
-- 0 4 1 * * psql -d expecto_db -U expecto_user -c "SELECT clean_forget_log()"
-- #  Очистка журнала apply_forgetting_log (каждый день в 02:00):
-- 0 2 * * * psql -d expecto_db -U expecto_user -c "SELECT clean_apply_forgetting_log();"
--
-- check_and_forget Основная процедура check_and_forget
-- snapshot_markov_prev_week() Функция создания/обновления снимка матрицы прошлой недели
-- clean_forecast_log Очистка журнала переходов
-- clean_transition_log Функция очистки transition_log
-- update_state_baseline Обновление эталонных распределений (запускать ежедневно)
-- refresh_os_stats Обновление статистики операционной скорости (запускать ежедневно)
-- clean_markov_probabilities_archive Очистка архивных снимков матрицы вероятностей
-- clean_check_state Очистка истории проверок check_state
-- clean_forget_log Очистка журнала форсированных забываний
-- clean_apply_forgetting_log Функция очистки старых записей apply_forgetting_log (для cron)
--
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--Функция заполнения справочника состояний цепи Маркова
CREATE OR REPLACE FUNCTION fill_state_descriptions() RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    -- Очистка таблицы перед заполнением
    TRUNCATE state_descriptions;

    -- Генерация всех комбинаций и вставка
    INSERT INTO state_descriptions (state_id, correlation, os_trend, wait_trend)
    SELECT
        -- Формула: correlation_index * 9 + (os_trend_index) * 3 + wait_trend_index
        -- где correlation_index = 0..20, os_trend_index = 0..2, wait_trend_index = 0..2
        c_idx * 9 + (os + 1) * 3 + (wt + 1) AS state_id,
        (-1.0 + 0.1 * c_idx)::REAL            AS correlation,
        os::SMALLINT                           AS os_trend,
        wt::SMALLINT                           AS wait_trend
    FROM
        generate_series(0, 20)   AS c_idx,   -- 0 => r=-1.0, 20 => r=+1.0
        generate_series(-1, 1)   AS os,      -- -1,0,1
        generate_series(-1, 1)   AS wt       -- -1,0,1
    ORDER BY state_id;  -- для наглядности, не обязательно
END;
$$;
COMMENT ON FUNCTION fill_state_descriptions IS 'Функция заполнения справочника состояний цепи Маркова.';
--------------------------------------------------------------------------------


--------------------------------------------------------------------------------
-- Получить state_id для заданных r , OS_trend , wait_trend
CREATE OR REPLACE FUNCTION get_state_id(
    r           REAL,
    os_trend    SMALLINT,
    wait_trend  SMALLINT
)
RETURNS SMALLINT
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT (
        (round((round(r::numeric, 1) + 1.0) / 0.1)::int * 9) +
        ((os_trend + 1)::int * 3) +
        (wait_trend + 1)::int
    )::smallint
$$;
COMMENT ON FUNCTION get_state_id IS 'Получить state_id для заданнеых r , OS_trend , wait_trend';
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Обновить основную таблицу переходных частот
CREATE OR REPLACE FUNCTION update_markov_frequency(
    r_from        REAL,
    os_trend_from SMALLINT,
    wait_trend_from SMALLINT,
    r_to          REAL,
    os_trend_to   SMALLINT,
    wait_trend_to SMALLINT
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    from_id SMALLINT;
    to_id   SMALLINT;
BEGIN
    from_id := get_state_id(r_from, os_trend_from, wait_trend_from);
    to_id   := get_state_id(r_to,   os_trend_to,   wait_trend_to);

    INSERT INTO markov_frequencies (from_state, to_state, frequency)
    VALUES (from_id, to_id, 1.0)
    ON CONFLICT (from_state, to_state) DO UPDATE
        SET frequency = markov_frequencies.frequency + 1.0;
END;
$$;
COMMENT ON FUNCTION update_markov_frequency IS 'Обновить основную таблицу переходных частот';
--------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------
-- Функция логирования перехода и обновления матрицы частот
CREATE OR REPLACE FUNCTION log_transition_and_update(
    r_from          REAL,
    os_trend_from   SMALLINT,
    wait_trend_from SMALLINT,
    r_to            REAL,
    os_trend_to     SMALLINT,
    wait_trend_to   SMALLINT
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    from_id SMALLINT;
    to_id   SMALLINT;
BEGIN
    from_id := get_state_id(r_from, os_trend_from, wait_trend_from);
    to_id   := get_state_id(r_to,   os_trend_to,   wait_trend_to);

    -- Запись в журнал
    INSERT INTO transition_log (ts, from_state, to_state)
    VALUES (now(), from_id, to_id);

    -- Обновление матрицы частот (функция создана ранее)
    PERFORM update_markov_frequency(
        r_from, os_trend_from, wait_trend_from,
        r_to,   os_trend_to,   wait_trend_to
    );
END;
$$;
COMMENT ON FUNCTION log_transition_and_update IS 'Обновить основную таблицу переходных частот';
---------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------
/*
Анализ изменений и механизм планового забывания
таблица markov_config, в которой задаются:
alpha (коэффициент забывания)
interval_minute ,
last_forget_time – время последнего применения.

Проверка условия: перед основным циклом обучения функция сравнивает now() - last_forget_time с интервалом. Если порог превышен, вызывается apply_forgetting(alpha), которая:
умножает все частоты в markov_frequencies на (1 - alpha),
удаляет пренебрежимо малые значения,
перестраивает markov_probabilities и markov_absorbing.
После этого last_forget_time обновляется, чтобы следующее забывание произошло через час.

Основной цикл без изменений: после блока забывания функция работает как прежде: получает метрики, сдвигает состояние, вычисляет состояния, логирует прогноз и обновляет матрицу частот.

Настройка параметров
Параметры alpha и interval_minute можно менять вручную через UPDATE markov_config.
Рекомендации:

Для стабильной нагрузки: alpha = 0.01, интервал 1 час.

Если нагрузка меняется медленно, можно уменьшить alpha до 0.005 и увеличить интервал до 2–4 часов.

При обнаружении дрейфа (методика check_and_forget) можно форсированно увеличить alpha или уменьшить интервал на время, изменив значения в markov_config.

Зависимости
Функция ожидает, что уже созданы:
markov_chain (таблица с одной строкой, хранящая предыдущее и текущее состояния),
get_current_os_waiting_correlation_for_markov_chain() – функция, возвращающая текущие r, тренды,
get_state_id, log_transition_and_update, apply_forgetting,
markov_probabilities, forecast_log, state_descriptions.

*/
-- markov_chain_training - обучение цепи Маркова
-- ============================================================
-- 2. Модифицированная функция обучения
-- ============================================================
-- Удаляем старую версию функции
DROP FUNCTION IF EXISTS markov_chain_training();

-- Создаём новую версию (с вызовом apply_forgetting() без аргументов)
CREATE OR REPLACE FUNCTION markov_chain_training()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    new_values_rec   RECORD;
    markov_chain_rec RECORD;
    new_correlation  REAL;
    new_os_trend     SMALLINT;
    new_wait_trend   SMALLINT;
    prev_state       SMALLINT;
    curr_state       SMALLINT;
    risk_pred        REAL;
    actual           SMALLINT;
    last_forget      TIMESTAMPTZ;
    forget_alpha     REAL;
    forget_interval  INTERVAL;
    is_state_descriptions_has_got_data BOOLEAN;
BEGIN
    -- Инициализация справочника состояний
    SELECT EXISTS (SELECT 1 FROM state_descriptions) INTO is_state_descriptions_has_got_data;
    IF NOT is_state_descriptions_has_got_data THEN 
        PERFORM fill_state_descriptions();
    END IF;

    -- ---------------------------------------------------------
    -- Блок планового забывания (модифицирован)
    -- ---------------------------------------------------------
    SELECT last_forget_time, alpha, MAKE_INTERVAL(mins => interval_minute)
        INTO last_forget, forget_alpha, forget_interval
    FROM markov_config
    LIMIT 1;

    IF now() - last_forget >= forget_interval THEN
        PERFORM apply_forgetting();   -- <-- БЕЗ ПАРАМЕТРА
    END IF;
    -- ---------------------------------------------------------

    -- Сбор текущих метрик
    SELECT * INTO new_values_rec
    FROM get_current_os_waiting_correlation_for_markov_chain();

    new_correlation := new_values_rec.current_correlation;
    new_os_trend    := new_values_rec.current_os_trend;
    new_wait_trend  := new_values_rec.current_wait_trend;

    -- Чтение последнего сохранённого состояния
    SELECT * INTO markov_chain_rec FROM markov_chain;

    -- Первое измерение – только сохраняем
    IF markov_chain_rec.prev_correlation IS NULL THEN
        INSERT INTO markov_chain (
            prev_correlation, prev_os_trend, prev_wait_trend,
            curr_correlation, curr_os_trend, curr_wait_trend
        ) VALUES (
            new_correlation, new_os_trend, new_wait_trend,
            new_correlation, new_os_trend, new_wait_trend
        );
        RETURN;
    END IF;

    -- Сдвиг состояния
    UPDATE markov_chain
    SET prev_correlation = markov_chain_rec.curr_correlation,
        prev_os_trend    = markov_chain_rec.curr_os_trend,
        prev_wait_trend  = markov_chain_rec.curr_wait_trend,
        curr_correlation = new_correlation,
        curr_os_trend    = new_os_trend,
        curr_wait_trend  = new_wait_trend;

    -- Перечитываем обновлённую запись
    SELECT * INTO markov_chain_rec FROM markov_chain;

    -- Идентификация состояний
    prev_state := get_state_id(markov_chain_rec.prev_correlation,
                               markov_chain_rec.prev_os_trend,
                               markov_chain_rec.prev_wait_trend);
    curr_state := get_state_id(markov_chain_rec.curr_correlation,
                               markov_chain_rec.curr_os_trend,
                               markov_chain_rec.curr_wait_trend);

    -- Прогноз риска на 1 минуту вперёд
    SELECT COALESCE(SUM(probability), 0.0) INTO risk_pred
    FROM markov_probabilities
    WHERE from_state = prev_state
      AND to_state IN (
          SELECT state_id FROM state_descriptions
          WHERE correlation < 0 AND os_trend = -1
      );

    -- Фактический исход
    SELECT CASE WHEN correlation < 0 AND os_trend = -1 THEN 1 ELSE 0 END INTO actual
    FROM state_descriptions
    WHERE state_id = curr_state;

    -- Логирование прогноза
    IF actual IS NOT NULL THEN 
        INSERT INTO forecast_log (ts, model_train_date, predicted_risk, actual_risk, from_state, to_state)
        VALUES (now(), current_date, risk_pred, actual, prev_state, curr_state);
    END IF;

    -- Обновление матрицы частот
    PERFORM log_transition_and_update(
        markov_chain_rec.prev_correlation,
        markov_chain_rec.prev_os_trend,
        markov_chain_rec.prev_wait_trend,
        markov_chain_rec.curr_correlation,
        markov_chain_rec.curr_os_trend,
        markov_chain_rec.curr_wait_trend
    );
END;
$$;
COMMENT ON FUNCTION markov_chain_training() IS 'Ежеминутное обучение цепи Маркова с плановым забыванием. Вызов apply_forgetting() без параметров позволяет использовать адаптивный alpha (если включён в markov_config.use_adaptive_alpha).';
--markov_chain_training - обучение цепи Маркова
-----------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------------------------------------------
-- get_current_os_waiting_correlation_for_markov_chain - получить текущее значение коэффициента корреляции для цепи маркова на окне 1 час 
CREATE OR REPLACE FUNCTION get_current_os_waiting_correlation_for_markov_chain()
RETURNS TABLE 
(
  current_correlation REAL  ,  
  current_os_trend    SMALLINT  ,
  current_wait_trend  SMALLINT
)
LANGUAGE plpgsql
AS $$
DECLARE
 
 timepoint timestamptz ;
 speed_waitings_correlation DOUBLE PRECISION ;
 regr_slope_value DOUBLE PRECISION;
 speed_regr_slope_value DOUBLE PRECISION;
 waitings_regr_slope_value DOUBLE PRECISION;

 speed_regr_rec record;
 waitings_regr_rec record;

BEGIN
	SELECT MAX(curr_timestamp)
	INTO timepoint 
	FROM cluster_stat_median ; 
	
	-------------------------------------------------------------------
	--КОРРЕЛЯЦИЯ СКОРОСТЬ - ОЖИДАНИЯ
	SELECT COALESCE( corr( curr_op_speed ,  curr_waitings ) , 0 ) AS correlation_value 
	INTO speed_waitings_correlation
	FROM
		 cluster_stat_median
	WHERE 
		curr_timestamp BETWEEN timepoint - interval '1 hour' AND timepoint ; 
	--КОРРЕЛЯЦИЯ АКТИВНЫЕ СЕССИИ - СКОРОСТЬ 
	-------------------------------------------------------------------
	CREATE TEMPORARY TABLE IF NOT EXISTS tmp_timepoints
	(
		curr_timestamp timestamptz  ,   
		curr_timepoint integer 
	);


	INSERT INTO tmp_timepoints
	(
		curr_timestamp ,	
		curr_timepoint 
	)
	SELECT 
		curr_timestamp , 
		row_number() over (order by curr_timestamp) AS x
	FROM
	cluster_stat_median
	WHERE 
		curr_timestamp BETWEEN timepoint - interval '1 hour' AND timepoint 
	ORDER BY curr_timestamp	;
	
	----------------------------------------------------------------------------------------------------
	-- ОПЕРАЦИОННАЯ СКОРОСТЬ
    -- 	линия регрессии  скорости  : Y = a + bX
	BEGIN
		WITH stats AS 
		(
		  SELECT 
			AVG(t.curr_timepoint::DOUBLE PRECISION) as avg1, 
			STDDEV(t.curr_timepoint::DOUBLE PRECISION) as std1,
			AVG(s.curr_op_speed::DOUBLE PRECISION) as avg2, 
			STDDEV(s.curr_op_speed::DOUBLE PRECISION) as std2
		  FROM
			cluster_stat_median s JOIN tmp_timepoints t ON ( s.curr_timestamp  = t.curr_timestamp )
		  WHERE 
			t.curr_timestamp BETWEEN timepoint - interval '1 hour' AND timepoint 
		),
		standardized_data AS 
		(
			SELECT 
				(t.curr_timepoint::DOUBLE PRECISION - avg1) / std1 as x_z,
				(s.curr_op_speed::DOUBLE PRECISION - avg2) / std2 as y_z
			FROM
				cluster_stat_median s JOIN tmp_timepoints t ON ( s.curr_timestamp  = t.curr_timestamp ) , stats
			WHERE 
				t.curr_timestamp BETWEEN timepoint - interval '1 hour' AND timepoint  
		)	
		SELECT
			REGR_SLOPE(y_z, x_z) as slope, --b
			ATAN(REGR_SLOPE(y_z, x_z)) * 180 / PI() as slope_angle_degrees, --угол наклона
			REGR_R2(y_z, x_z) as r_squared -- Коэффициент детерминации
		INTO 
			speed_regr_rec
		FROM standardized_data;
	EXCEPTION
	  --STDDEV(s.curr_op_speed::DOUBLE PRECISION) = 0  
	  WHEN division_by_zero THEN  -- Конкретное исключение для деления на ноль
	    SELECT 
			1.0 as slope, --b
			0.0  as slope_angle_degrees, --угол наклона
			0.0  as r_squared -- Коэффициент детерминации
		INTO 
		speed_regr_rec ;
	END;
	speed_regr_slope_value = SIGN( speed_regr_rec.slope_angle_degrees ); 	
	-- 	линия регрессии  скорости  : Y = a + bX
	-- ОПЕРАЦИОННАЯ СКОРОСТЬ
	-------------------------------------------------------------------
	
	----------------------------------------------------------------------------------------------------
	-- ОЖИДАНИЯ
    -- 	линия регрессии  скорости  : Y = a + bX
	BEGIN 
		WITH stats AS 
		(
		  SELECT 
			AVG(t.curr_timepoint::DOUBLE PRECISION) as avg1, 
			STDDEV(t.curr_timepoint::DOUBLE PRECISION) as std1,
			AVG(s.curr_waitings::DOUBLE PRECISION) as avg2, 
			STDDEV(s.curr_waitings::DOUBLE PRECISION) as std2
		  FROM
			cluster_stat_median s JOIN tmp_timepoints t ON ( s.curr_timestamp  = t.curr_timestamp )
		  WHERE 
			t.curr_timestamp BETWEEN timepoint - interval '1 hour' AND timepoint  
		),
		standardized_data AS 
		(
			SELECT 
				(t.curr_timepoint::DOUBLE PRECISION - avg1) / std1 as x_z,
				(s.curr_waitings::DOUBLE PRECISION - avg2) / std2 as y_z
			FROM
				cluster_stat_median s JOIN tmp_timepoints t ON ( s.curr_timestamp  = t.curr_timestamp ) , stats
			WHERE 
				t.curr_timestamp BETWEEN timepoint - interval '1 hour' AND timepoint  
		)	
		SELECT
			REGR_SLOPE(y_z, x_z) as slope, --b
			ATAN(REGR_SLOPE(y_z, x_z)) * 180 / PI() as slope_angle_degrees, --угол наклона
			REGR_R2(y_z, x_z) as r_squared -- Коэффициент детерминации
		INTO 
			waitings_regr_rec
		FROM standardized_data;
	EXCEPTION
	  --STDDEV(s.curr_waitings::DOUBLE PRECISION) = 0  
	  WHEN division_by_zero THEN  -- Конкретное исключение для деления на ноль
	    SELECT 
			1.0 as slope, --b
			0.0  as slope_angle_degrees, --угол наклона
			0.0  as r_squared -- Коэффициент детерминации
		INTO 
		waitings_regr_rec ;
	END;
	waitings_regr_slope_value = SIGN(  waitings_regr_rec.slope_angle_degrees ); 	
	-- 	линия регрессии  скорости  : Y = a + bX
	-- ОЖИДАНИЯ
	-------------------------------------------------------------------	
	
	DROP TABLE tmp_timepoints;
	
	RETURN QUERY 
	SELECT round(speed_waitings_correlation::numeric,1)::REAL , speed_regr_slope_value::SMALLINT , waitings_regr_slope_value::SMALLINT ; 
		

END;
$$;
COMMENT ON FUNCTION get_current_os_waiting_correlation_for_markov_chain IS 'получить текущее значение коэффициента корреляции для цепи маркова на окне 1 час ';
------------------------------------------------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------
-- update_markov_probabilities Обновить матрицу вероятностей
CREATE OR REPLACE FUNCTION update_markov_probabilities() RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
	-- Удаляем старые вероятности
	TRUNCATE markov_probabilities;

	-- Вставляем свежие, нормализуя построчно
	INSERT INTO markov_probabilities (from_state, to_state, probability)
	SELECT
		from_state,
		to_state,
		frequency / SUM(frequency) OVER (PARTITION BY from_state) AS probability
	FROM markov_frequencies
	WHERE frequency > 0;
	
	--Заполнить матрицу поглощения
	PERFORM rebuild_markov_absorbing();
	
 END;
$$;
COMMENT ON FUNCTION markov_chain_training IS 'Обновить матрицу вероятностей';
--------------------------------------------------------------------

--------------------------------------------------------------------
-- predict_risk_1min получить вероятность попасть в аварийную зону на следующем шаге
/*
risk_1min (REAL) Вероятность перехода в аварийное состояние на следующей минуте. 
Принимает значение:
фактической вероятности (0.0 … 1.0), если состояние известно и есть исторические данные о переходах в аварию;
0.0, если состояние известно, но переходов в аварию ранее не зафиксировано;
0.05 (априорная оценка по умолчанию), если текущее состояние ранее не встречалось в обучении.

situation (TEXT) Диагностическая метка, объясняющая, как получено значение risk_1min:
'risk_calculated' — состояние известно, найден хотя бы один аварийный переход, риск вычислен по матрице вероятностей;
'no_risk' — состояние известно, но ни одного перехода в аварийную зону не зарегистрировано (вероятность принята за 0);
'unknown_state' — текущее состояние отсутствует в таблице markov_probabilities (модель с ним не сталкивалась), использована априорная оценка.

transitions_to_risk (BIGINT) Количество зафиксированных в обучении уникальных аварийных состояний, в которые совершались переходы из текущего состояния.
> 0 при situation = 'risk_calculated';
0 при 'no_risk' или 'unknown_state'.

total_transitions_known (BIGINT) Общее количество уникальных состояний, в которые когда-либо были совершены переходы из текущего состояния (вообще, не только аварийных).
> 0, если состояние известно;
0 при 'unknown_state', что и служит индикатором неизвестности.

Пример использования в мониторинге
Эти четыре поля позволяют не только получить вероятность инцидента, но и оценить надёжность прогноза:
Если situation = 'unknown_state' и risk_1min = 0.05, оператор понимает, что модель «гадает», и стоит присмотреться внимательнее.
Если situation = 'no_risk' с нулевым риском, можно доверять, но помнить, что отсутствие исторических инцидентов не гарантирует их невозможность.
Сочетание высокого risk_1min и situation = 'risk_calculated' с большим transitions_to_risk — наиболее достоверный сигнал тревоги.
*/
CREATE OR REPLACE FUNCTION predict_risk_1min() RETURNS TABLE
(
    current_risk REAL,
    current_situation TEXT,
    current_transitions_to_risk BIGINT,
    current_total_transitions_known BIGINT
)
LANGUAGE plpgsql
AS $$
DECLARE
    result_risk real;
    markov_chain_rec record;
BEGIN
    SELECT *
    INTO markov_chain_rec
    FROM get_current_os_waiting_correlation_for_markov_chain();

RAISE NOTICE '%', markov_chain_rec;	

    RETURN QUERY
    WITH current_state AS (
        SELECT get_state_id AS state_id
        FROM get_state_id(
                 markov_chain_rec.current_correlation,
                 markov_chain_rec.current_os_trend,
                 markov_chain_rec.current_wait_trend
             )
    ),
    risk_calc AS (
        SELECT
            COALESCE(SUM(p.probability), 0.0) AS raw_risk,
            COUNT(p.to_state) AS transitions_to_risk,
            (SELECT COUNT(*) FROM markov_probabilities WHERE from_state = cs.state_id) AS total_transitions_known,
            CASE
                WHEN (SELECT COUNT(*) FROM markov_probabilities WHERE from_state = cs.state_id) = 0 THEN 'unknown_state'
                WHEN COUNT(p.to_state) = 0 THEN 'no_risk'
                ELSE 'risk_calculated'
            END AS situation
        FROM current_state cs
        LEFT JOIN markov_probabilities p
            ON p.from_state = cs.state_id
            AND p.to_state IN (
                SELECT state_id FROM state_descriptions
                WHERE correlation < 0 AND os_trend = -1
            )
        GROUP BY cs.state_id   -- <-- исправление
    )
    SELECT
        CASE
            WHEN situation = 'unknown_state' THEN 0.05
            WHEN situation = 'no_risk' THEN 0.0
            ELSE raw_risk
        END AS current_risk,
		situation AS current_situation,
		transitions_to_risk AS current_transitions_to_risk,
		total_transitions_known AS current_total_transitions_known
    FROM risk_calc;
END;
$$;
COMMENT ON FUNCTION predict_risk_1min IS 'получить вероятность попасть в аварийную зону';
--------------------------------------------------------------------

--------------------------------------------------------------------
/*
Таблица markov_absorbing хранит строки поглощающей цепи, в которой аварийные состояния (отрицательная корреляция и падение операционной скорости) сделаны поглощающими:
 единственный возможный переход из них — остаться в том же состоянии с вероятностью 1.

Функция rebuild_markov_absorbing вызывается после каждого пересчёта markov_probabilities  и формирует матрицу заново.

Логика заполнения:
Для всех неаварийных исходных состояний (from_state) переносятся переходы из markov_probabilities, кроме переходов в аварийные состояния, отличные от себя (такие переходы в поглощающей цепи должны иметь вероятность 0 и исключаются).
Для каждого аварийного состояния вставляется ровно одна строка (state_id, state_id, 1.0).
Состояния, которые ни разу не встречались в обучении, просто не попадают в markov_absorbing; при прогнозе они корректно диагностируются как unknown_state в функции predict_risk_k_diag
*/
CREATE OR REPLACE FUNCTION rebuild_markov_absorbing()
RETURNS void
LANGUAGE sql
AS $$
    TRUNCATE markov_absorbing;

    -- Переходы из неаварийных состояний (включая переходы в аварийные)
    INSERT INTO markov_absorbing (from_state, to_state, probability)
    SELECT p.from_state, p.to_state, p.probability
    FROM markov_probabilities p
    JOIN state_descriptions sd_from ON p.from_state = sd_from.state_id
    WHERE NOT (sd_from.correlation < 0 AND sd_from.os_trend = -1);

    -- Поглощающие петли для аварийных состояний
    INSERT INTO markov_absorbing (from_state, to_state, probability)
    SELECT state_id, state_id, 1.0
    FROM state_descriptions
    WHERE correlation < 0 AND os_trend = -1;
$$;
COMMENT ON FUNCTION rebuild_markov_absorbing IS 'Пересчет таблицы поглощения';
--------------------------------------------------------------------

--------------------------------------------------------------------
--
/*
Возвращаемые параметры
risk — вероятность попасть в любое аварийное состояние хотя бы раз за k минут.
situation — диагностическая метка:
 'risk_calculated' — состояние знакомо, риск ненулевой (прямой или косвенный);
 'no_risk' — состояние знакомо, риск равен 0 (аварийные состояния недостижимы за k шагов);
 'unknown_state' — состояние отсутствует в обученной матрице, риск оценён априорно (5% за шаг).
transitions_to_risk — количество прямых переходов из текущего состояния в аварийные (только для известных состояний).
total_transitions_known — общее количество известных переходов из текущего состояния (0 для неизвестных).

Функция использует предварительно созданную таблицу markov_absorbing, которая преобразует матрицу вероятностей в поглощающую цепь (аварийные состояния делаются поглощающими). 
Её необходимо пересчитывать при каждом обновлении markov_probabilities.
*/
CREATE OR REPLACE FUNCTION predict_risk_k_diag( k INT )
RETURNS TABLE (
    risk REAL,
    situation TEXT,
    transitions_to_risk INT,
    total_transitions_known INT
)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    total_states CONSTANT INT := 189;
    v REAL[];
    v_new REAL[];
    av_states INT[];
    from_s SMALLINT;
    to_s SMALLINT;
    prob REAL;
    step INT;
    base_risk CONSTANT REAL := 0.05;
    _transitions_to_risk INT;
    _total_transitions_known INT;
	markov_chain_rec record;
    current_state_id SMALLINT;
	
BEGIN

	SELECT *
    INTO markov_chain_rec
    FROM get_current_os_waiting_correlation_for_markov_chain();

RAISE NOTICE '%', markov_chain_rec;

    SELECT get_state_id AS state_id
	INTO current_state_id
	FROM get_state_id(
						markov_chain_rec.current_correlation,
						markov_chain_rec.current_os_trend,
						markov_chain_rec.current_wait_trend
					 );
    

    -- Список аварийных (поглощающих) состояний: correlation < 0 и os_trend = -1
    SELECT array_agg(state_id) INTO av_states
    FROM state_descriptions
    WHERE correlation < 0 AND os_trend = -1;

    -- Проверяем, известно ли текущее состояние модели
    SELECT COUNT(*) INTO _total_transitions_known
    FROM markov_probabilities
    WHERE from_state = current_state_id;

    IF _total_transitions_known = 0 THEN
        -- Состояние незнакомо → априорная оценка риска (5% за шаг)
        risk := 1.0 - POWER(1.0 - base_risk, k);
        situation := 'unknown_state';
        transitions_to_risk := 0;
        total_transitions_known := 0;
        RETURN NEXT;
        RETURN;
    END IF;

    -- Число прямых аварийных переходов из текущего состояния
    SELECT COUNT(*) INTO _transitions_to_risk
    FROM markov_probabilities
    WHERE from_state = current_state_id
      AND to_state = ANY(av_states);

    transitions_to_risk := _transitions_to_risk;
    total_transitions_known := _total_transitions_known;

    -- Инициализация вектора вероятностей
    v := array_fill(0.0, ARRAY[total_states]);
    v[current_state_id + 1] := 1.0;

    -- Последовательное умножение вектора на поглощающую матрицу P_abs
    FOR step IN 1..k LOOP
        v_new := array_fill(0.0, ARRAY[total_states]);

        FOR from_s IN 0..188 LOOP
            IF v[from_s + 1] > 0.0 THEN
                FOR to_s, prob IN
                    SELECT m.to_state, m.probability
                    FROM markov_absorbing m
                    WHERE m.from_state = from_s
                LOOP
                    v_new[to_s + 1] := v_new[to_s + 1] + v[from_s + 1] * prob;
                END LOOP;
            END IF;
        END LOOP;

        v := v_new;
    END LOOP;

    -- Суммируем вероятности по всем аварийным состояниям
    SELECT SUM(v[state_id + 1]) INTO risk
    FROM unnest(av_states) AS state_id;

    IF risk IS NULL THEN
        risk := 0.0;
    END IF;

    -- Классификация ситуации
    IF risk = 0.0 THEN
        situation := 'no_risk';
    ELSE
        situation := 'risk_calculated';
    END IF;

    RETURN NEXT;
END;
$$;
COMMENT ON FUNCTION predict_risk_k_diag IS 'получить вероятность попасть в аварийную зону за к шагов';

---------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------
-- Функция создания/обновления снимка матрицы прошлой недели
-- 5 18 * * 5 psql -d expecto_db -U expecto_user  -c "SELECT snapshot_markov_prev_week();"
CREATE OR REPLACE FUNCTION snapshot_markov_prev_week()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    -- Очищаем старый снимок
    TRUNCATE markov_probabilities_prev_week;

    -- Вставляем точную копию текущих вероятностей
    INSERT INTO markov_probabilities_prev_week
    SELECT from_state, to_state, probability
    FROM markov_probabilities;

    -- Архивируем текущую матрицу с пометкой текущей даты обучения
    PERFORM archive_markov_probabilities(current_date);
END;
$$;
COMMENT ON FUNCTION snapshot_markov_prev_week IS 'Функция создания/обновления снимка матрицы прошлой недели';
---------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------
-- Функция записи прогноза и его фактического исхода
/*
Функция log_forecast предназначена для регистрации каждого одношагового прогноза сразу после того, как стал известен фактический переход. 
В зависимости от сценария использования, вызов может осуществляться двумя способами.

Сценарий 1. Непрерывный мониторинг точности (признак 3.3 методики забывания)
В этом режиме модель постоянно обновляется (с забыванием), и мы хотим отслеживать её текущую калибровку. 
Вызов происходит каждую минуту в основном цикле сбора метрик:
На шаге t-1:
Определяется текущее состояние prev_state.
По матрице markov_probabilities вычисляется predicted_risk — вероятность перехода из prev_state в любое аварийное состояние на следующем шаге (см. функцию predict_risk_1min).
Значение predicted_risk и prev_state сохраняются в переменных агента.

На шаге t (следующая минута):
Получаются новые метрики, формируется состояние curr_state.
Определяется actual_risk = 1, если curr_state принадлежит аварийной зоне (correlation < 0 AND os_trend = -1), иначе 0.
Вызывается log_forecast:

sql
SELECT log_forecast(
    p_predicted_risk := <сохранённый_predicted_risk>,
    p_actual_risk    := actual_risk,
    p_model_train_date := current_date,   -- или фиксированная дата версии модели
    p_from_state     := prev_state,
    p_to_state       := curr_state
);
Обновляются prev_state и predicted_risk для следующего цикла.

Такой вызов гарантирует, что каждая запись содержит согласованную пару «прогноз–факт». 
Параметр model_train_date может быть текущей датой (если модель обновляется ежедневно) или датой последнего снимка матрицы (если используются версионированные модели).

Сценарий 2. Оценка моделей с разными периодами обучения (критерий 3 достаточности)
Для проверки того, что добавление 5 дней обучения не улучшает Brier Score более чем на 0.01, необходимо сравнить несколько версий модели на общем тестовом периоде. Процедура:

Создание снимков модели.
В конце каждой недели (или чаще) создаётся копия матрицы markov_probabilities с пометкой даты, например, markov_probabilities_2025_01_15. 
Эта матрица фиксирует состояние обучения на определённый момент.

Прогон на тестовых данных.
Для каждого такого снимка (модели) берутся данные за следующие, например, 2 дня, которые не использовались при обучении этой модели. 
Для каждой минуты этого тестового периода:
Вычисляется predicted_risk с использованием вероятностей из фиксированной матрицы.
Фиксируется фактический исход.
Вызывается log_forecast с параметром p_model_train_date = <дата снимка>.

Расчёт Brier Score.
После накопления логов можно выполнить:
sql
SELECT model_train_date,
       AVG((predicted_risk - actual_risk)^2) AS brier_score
FROM forecast_log
WHERE model_train_date IN ('2025-01-08', '2025-01-15', ...)
GROUP BY model_train_date;
Такой подход позволяет «честно» сравнить модели, обученные на данных до разных дат.

Рекомендации по использованию
Внедряйте непрерывное логирование (сценарий 1) с первого дня работы модели — это даст данные для мониторинга дрейфа точности.
Для периодической проверки достаточности обучения (сценарий 2) запланируйте еженедельный запуск оценки на фиксированных снимках и анализируйте Brier Score, как описано в критерии 3.
*/
CREATE OR REPLACE FUNCTION log_forecast(
    p_predicted_risk    REAL,
    p_actual_risk       SMALLINT,
    p_model_train_date  DATE,
    p_from_state        SMALLINT DEFAULT NULL,
    p_to_state          SMALLINT DEFAULT NULL
)
RETURNS void
LANGUAGE sql
AS $$
    INSERT INTO forecast_log (ts, model_train_date, predicted_risk, actual_risk, from_state, to_state)
    VALUES (now(), p_model_train_date, p_predicted_risk, p_actual_risk, p_from_state, p_to_state);
$$;
COMMENT ON FUNCTION log_forecast IS 'Функция создания/обновления снимка матрицы прошлой недели';
---------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Вспомогательная функция прогноза по архивной матрице
CREATE OR REPLACE FUNCTION predict_risk_1min_archived(
    p_train_date DATE,
    p_from_state SMALLINT
)
RETURNS REAL
LANGUAGE sql
STABLE
AS $$
    SELECT COALESCE(
        (SELECT SUM(probability)
         FROM markov_probabilities_archive
         WHERE train_date = p_train_date
           AND from_state = p_from_state
           AND to_state IN (
               SELECT state_id FROM state_descriptions
               WHERE correlation < 0 AND os_trend = -1
           )),
        0.05  -- априорная оценка для неизвестных состояний
    );
$$;
COMMENT ON FUNCTION predict_risk_1min_archived IS 'Вспомогательная функция прогноза по архивной матрице';
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Расчёт и сравнение Brier Score
/*
Входные параметры:
test_start, test_end — границы тестового периода (дни), за которые вычисляется Brier Score.
model_date_old, model_date_new — даты обучения двух сравниваемых моделей (старой и более новой).

Возвращаемые столбцы:
older_model, newer_model — поданные даты моделей.
older_bs, newer_bs — Brier Score для старой и новой модели.
bs_improvement — разница older_bs - newer_bs (положительное значение означает, что новая модель точнее).

sufficient — TRUE, если улучшение меньше 0.01 (критерий достаточности обучения выполнен).

Логика:
Фильтруем записи forecast_log по model_train_date и временному диапазону.
Вычисляем среднеквадратичную ошибку прогноза (Brier Score).
Сравниваем две модели.

Пример вызова
SELECT * FROM compare_brier_scores(
    '2025-05-12', '2025-05-13',
    '2025-05-09', '2025-05-14'
);
Функция предполагает, что таблица forecast_log уже заполнена прогнозами для обеих моделей на указанном тестовом периоде 
*/
-- Расчёт и сравнение Brier Score
CREATE OR REPLACE FUNCTION compare_brier_scores(
    test_start      DATE,
    test_end        DATE,
    model_date_old  DATE,
    model_date_new  DATE
)
RETURNS TABLE (
    older_model     DATE,
    newer_model     DATE,
    older_bs        REAL,
    newer_bs        REAL,
    bs_improvement  REAL,
    sufficient      BOOLEAN
)
LANGUAGE sql
STABLE
AS $$
    WITH old_preds AS (
        SELECT predicted_risk, actual_risk
        FROM forecast_log
        WHERE model_train_date = model_date_old
          AND ts >= test_start
          AND ts <  test_end + 1  -- включаем весь последний день
    ),
    new_preds AS (
        SELECT predicted_risk, actual_risk
        FROM forecast_log
        WHERE model_train_date = model_date_new
          AND ts >= test_start
          AND ts <  test_end + 1
    ),
    old_bs_val AS (
        SELECT COALESCE(AVG((predicted_risk - actual_risk)^2), 0.0) AS bs
        FROM old_preds
    ),
    new_bs_val AS (
        SELECT COALESCE(AVG((predicted_risk - actual_risk)^2), 0.0) AS bs
        FROM new_preds
    )
    SELECT
        model_date_old,
        model_date_new,
        o.bs,
        n.bs,
        GREATEST(o.bs - n.bs, 0.0),  -- улучшение (старый BS - новый BS)
        (o.bs - n.bs) < 0.01         -- достаточно, если улучшение меньше порога
    FROM old_bs_val o, new_bs_val n;
$$;
COMMENT ON FUNCTION compare_brier_scores IS 'Расчёт и сравнение Brier Score';

--------------------------------------------------------------------------------
-- Вспомогательная функция: получение стационарного распределения
CREATE OR REPLACE FUNCTION get_stationary_distribution(max_iter INT DEFAULT 1000, tol DOUBLE PRECISION DEFAULT 1e-6)
RETURNS DOUBLE PRECISION[]
LANGUAGE plpgsql
AS $$
DECLARE
    n CONSTANT INT := 189;
    v DOUBLE PRECISION[] := array_fill(1.0 / n, ARRAY[n]);
    v_new DOUBLE PRECISION[];
    i INT;
    diff DOUBLE PRECISION;
    to_s RECORD;   -- объявление переменной-записи
BEGIN
    FOR i IN 1..max_iter LOOP
        v_new := array_fill(0.0, ARRAY[n]);
        FOR from_s IN 0..n-1 LOOP
            FOR to_s IN (SELECT to_state, probability FROM markov_probabilities WHERE from_state = from_s) LOOP
                v_new[to_s.to_state + 1] := v_new[to_s.to_state + 1] + v[from_s + 1] * to_s.probability;
            END LOOP;
        END LOOP;
        diff := 0.0;
        FOR j IN 1..n LOOP
            diff := diff + abs(v_new[j] - v[j]);
        END LOOP;
        v := v_new;
        IF diff < tol THEN EXIT; END IF;
    END LOOP;
    RETURN v;
END;
$$;
COMMENT ON FUNCTION get_stationary_distribution IS 'Вспомогательная функция: получение стационарного распределения';--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--KL-дивергенция стационарного и эмпирического (последняя неделя)
/*
Входные параметры: отсутствуют. Анализируется всегда последняя полная неделя.

Возвращаемые столбцы:
kl_value — фактическое значение KL-дивергенции (или NULL, если данных нет).
threshold — пороговое значение (0.1).
passed — TRUE, если критерий выполнен (kl_value < 0.1).

Логика работы:
Вызывается ранее созданная get_stationary_distribution(), которая итеративно вычисляет стационарный вектор на основе текущей матрицы markov_probabilities.
По таблице transition_log подсчитывается, сколько раз система находилась в каждом состоянии за последние 7 дней. Формируется эмпирическое распределение emp_arr.
KL-дивергенция вычисляется по формуле Σ π_i * ln(π_i / emp_i), только для состояний с ненулевыми вероятностями в обоих распределениях.
Граничные случаи:

Если за неделю в transition_log нет записей, passed = FALSE, kl_value = NULL — модель не может быть верифицирована.

Если стационарное распределение содержит состояния, не встречавшиеся в эмпирике (или наоборот), они игнорируются в сумме.

Пример вызова
SELECT * FROM check_kl_divergence();
-- Результат: (kl_value=0.087, threshold=0.1, passed=true)

Этот вызов можно использовать как самостоятельную проверку или внутри составной функции оценки достаточности обучения evaluate_training_sufficiency.
*/
CREATE OR REPLACE FUNCTION check_kl_divergence()
RETURNS TABLE (
    kl_value  DOUBLE PRECISION,
    threshold DOUBLE PRECISION,
    passed    BOOLEAN
)
LANGUAGE plpgsql
AS $$
DECLARE
    pi_arr    DOUBLE PRECISION[];
    emp_arr   DOUBLE PRECISION[] := array_fill(0.0::DOUBLE PRECISION, ARRAY[189]);
    total_obs BIGINT;
    kl        DOUBLE PRECISION := 0.0;
    i         INT;
    emp_val   DOUBLE PRECISION;
BEGIN
    -- 1. Пытаемся получить стационарное распределение; ловим underflow
    BEGIN
        pi_arr := get_stationary_distribution()::DOUBLE PRECISION[];
    EXCEPTION
        WHEN numeric_value_out_of_range THEN
            -- Если внутренняя функция упала с underflow, возвращаем NULL-результат
            kl_value  := NULL;
            threshold := 0.1;
            passed    := FALSE;
            RETURN NEXT;
            RETURN;
    END;

    -- 2. Считаем общее количество наблюдений за последние 7 дней
    SELECT COUNT(*) INTO total_obs
    FROM transition_log
    WHERE ts >= now() - INTERVAL '7 days';

    IF total_obs = 0 THEN
        kl_value  := NULL;
        threshold := 0.1;
        passed    := FALSE;
        RETURN NEXT;
        RETURN;
    END IF;

    -- 3. Эмпирические частоты состояний
    FOR i IN 0..188 LOOP
        SELECT COUNT(*)::DOUBLE PRECISION / total_obs
        INTO emp_val
        FROM transition_log
        WHERE from_state = i
          AND ts >= now() - INTERVAL '7 days';

        emp_arr[i+1] := emp_val;
    END LOOP;

    -- 4. Расчёт KL-дивергенции: sum(pi_i * ln(pi_i / emp_i))
    FOR i IN 1..189 LOOP
        IF pi_arr[i] > 0.0 AND emp_arr[i] > 0.0 THEN
            kl := kl + pi_arr[i] * ln(pi_arr[i] / emp_arr[i]);
        END IF;
    END LOOP;

    kl_value  := kl;
    threshold := 0.1;
    passed    := (kl < 0.1);

    RETURN NEXT;
END;
$$;
COMMENT ON FUNCTION check_kl_divergence IS 'KL-дивергенция стационарного и эмпирического (последняя неделя)';

--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Основная функция проверки достаточности обучения (скорректированная)
/*
Порядок вызова и анализ результатов
1. Регулярный автоматический запуск
Функцию следует выполнять еженедельно в пятницу вечером (после завершения сбора данных за день, после создания снимка markov_probabilities_prev_week и архивирования моделей).
Для критерия 3 необходимо предварительно заполнить forecast_log прогнозами для двух сравниваемых моделей (см. сценарий 2). После этого вызов может быть таким:

SELECT * FROM evaluate_training_sufficiency(
    test_start      => '2025-05-19',   -- последние два рабочих дня до обучения новой модели
    test_end        => '2025-05-20',
    model_date_old  => '2025-05-12',   -- обучение до прошлой пятницы
    model_date_new  => '2025-05-19'    -- обучение до текущей пятницы
);
Если параметры не переданы (все NULL), критерии 1,2,4 будут оценены, а критерий 3 вернёт passed = false с просьбой выполнить ручное сравнение.

2. Интерпретация результатов
Все четыре критерия возвращают passed = true → обучение достаточно. Можно снижать интенсивность планового забывания (уменьшить α), доверять прогнозам.
Если хотя бы один критерий не выполнен:
C1 – увеличить период обучения (меньше 50 переходов для частых состояний).
C2 – вероятности ещё не стабилизировались (продолжить обучение).
C3 – качество прогноза продолжает улучшаться более чем на 0.01 BS (добавить ещё неделю).
C4 – стационарное распределение не соответствует недельной эмпирике (возможно, изменился профиль нагрузки; пересмотреть механизм забывания или обучить отдельные матрицы для разных часов).

3. Автоматизация
Настройте еженедельный джоб (pg_cron или внешний планировщик), который:
Создаёт снимок markov_probabilities_prev_week (пятница 18:05).
Заполняет forecast_log для пары моделей с тестовым периодом (например, среда–четверг текущей недели).
Вызывает evaluate_training_sufficiency с этими датами.
Логирует результат в специальную таблицу training_sufficiency_log для отслеживания динамики.
Такой подход гарантирует объективную оценку зрелости модели без риска ложного оптимизма.
*/
CREATE OR REPLACE FUNCTION evaluate_training_sufficiency(
    test_start      DATE DEFAULT NULL,
    test_end        DATE DEFAULT NULL,
    model_date_old  DATE DEFAULT NULL,
    model_date_new  DATE DEFAULT NULL
)
RETURNS TABLE (
    criterion TEXT,
    value     REAL,
    threshold TEXT,
    passed    BOOLEAN,
    details   TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
    total_transitions BIGINT;
    states_above_1pct INT[];
    n_i_fail INT;
    d_max REAL;
    brier_change REAL;
    kl_result RECORD;
BEGIN
    -- --------------------------------------------------------
    -- Критерий 1: покрытие состояний с частотой >1%
    -- --------------------------------------------------------
    SELECT COUNT(*) INTO total_transitions FROM transition_log;
    IF total_transitions = 0 THEN
        criterion := 'C1: n_i >= 50';
        value := 0;
        threshold := 'n_i>=50 для частых';
        passed := FALSE;
        details := 'transition_log пуст';
        RETURN NEXT;
    ELSE
        WITH state_counts AS (
            SELECT from_state, COUNT(*) AS n_i,
                   COUNT(*)::REAL / total_transitions AS freq
            FROM transition_log
            GROUP BY from_state
        )
        SELECT array_agg(from_state) INTO states_above_1pct
        FROM state_counts
        WHERE freq > 0.01;

        SELECT COUNT(*) INTO n_i_fail
        FROM (
            SELECT from_state, COUNT(*) AS n_i
            FROM transition_log
            WHERE from_state = ANY(states_above_1pct)
            GROUP BY from_state
        ) sub
        WHERE n_i < 50;

        criterion := 'C1: n_i >= 50 (для частых >1%)';
        value := n_i_fail;
        threshold := '0';
        passed := (n_i_fail = 0);
        details := format('Состояний с частотой >1%%: %s, из них с n_i<50: %s',
                          array_length(states_above_1pct,1), n_i_fail);
    END IF;
    RETURN NEXT;

    -- --------------------------------------------------------
    -- Критерий 2: максимальное изменение вероятностей за две недели
    -- --------------------------------------------------------
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'markov_probabilities_prev_week') THEN
        SELECT MAX(abs(COALESCE(p.probability,0) - COALESCE(pw.probability,0))) INTO d_max
        FROM (SELECT DISTINCT from_state, to_state FROM markov_probabilities
              UNION SELECT from_state, to_state FROM markov_probabilities_prev_week) all_trans
        LEFT JOIN markov_probabilities p USING (from_state, to_state)
        LEFT JOIN markov_probabilities_prev_week pw USING (from_state, to_state);
    ELSE
        d_max := NULL;
    END IF;

    criterion := 'C2: max |P_new - P_old|';
    value := COALESCE(d_max, -1);
    threshold := 'D < 0.05';
    passed := (d_max IS NOT NULL AND d_max < 0.05);
    details := CASE WHEN d_max IS NULL THEN 'Нет исторической матрицы за прошлую неделю'
                   ELSE 'D_max = ' || round(d_max::numeric, 4)::text END;
    RETURN NEXT;

    -- --------------------------------------------------------
    -- Критерий 3: Brier Score плато (используем compare_brier_scores)
    -- --------------------------------------------------------
    IF test_start IS NOT NULL AND test_end IS NOT NULL AND model_date_old IS NOT NULL AND model_date_new IS NOT NULL THEN
        SELECT bs_improvement, sufficient INTO brier_change, passed
        FROM compare_brier_scores(test_start, test_end, model_date_old, model_date_new);
        criterion := 'C3: Brier Score изменение < 0.01';
        value := COALESCE(brier_change, -1);
        threshold := '< 0.01';
        details := CASE WHEN brier_change IS NULL THEN 'Нет данных в forecast_log'
                       ELSE 'Изменение BS = ' || round(brier_change::numeric, 4)::text END;
    ELSE
        criterion := 'C3: Brier Score изменение < 0.01';
        value := -1;
        threshold := '< 0.01';
        passed := FALSE;
        details := 'Не заданы параметры тестового периода/моделей. Выполните еженедельное сравнение вручную.';
    END IF;
    RETURN NEXT;

    -- --------------------------------------------------------
    -- Критерий 4: KL-дивергенция (используем готовую функцию)
    -- --------------------------------------------------------
    SELECT * INTO kl_result FROM check_kl_divergence();
    criterion := 'C4: KL(pi || emp) < 0.1';
    value := kl_result.kl_value;
    threshold := '< 0.1';
    passed := kl_result.passed;
    details := CASE WHEN kl_result.kl_value IS NULL THEN 'Нет данных за последнюю неделю'
                   ELSE 'KL = ' || round(kl_result.kl_value::numeric, 4)::text END;
    RETURN NEXT;
END;
$$;
COMMENT ON FUNCTION evaluate_training_sufficiency IS 'Основная функция проверки достаточности обучения (скорректированная)';COMMENT ON FUNCTION get_stationary_distribution IS 'Основная функция проверки достаточности обучения (скорректированная)';


------------------------------------------------------
-- Функция забывания
/*
Проверка и использование
Включите адаптивный режим (если параметры по умолчанию устраивают):
SELECT enable_adaptive_forgetting();

Проверьте конфигурацию:
SELECT use_adaptive_alpha, base_alpha, min_alpha, incident_half_life_days, last_incident_time
FROM markov_config;

Наблюдайте за работой – в логах будут сообщения NOTICE: apply_forgetting: alpha=... с указанием применённого значения.

expecto_db=> SELECT use_adaptive_alpha, base_alpha, min_alpha, incident_half_life_days, last_incident_time
expecto_db-> FROM markov_config;
 use_adaptive_alpha | base_alpha | min_alpha | incident_half_life_days | last_incident_time
--------------------+------------+-----------+-------------------------+--------------------
 f                  |        0.1 |      0.01 |                       7 |
(1 row)

expecto_db=> SELECT enable_adaptive_forgetting();
                          enable_adaptive_forgetting
-------------------------------------------------------------------------------
 Adaptive forgetting enabled: base_alpha=0.1, min_alpha=0.01, half_life=7 days
(1 row)

expecto_db=> SELECT use_adaptive_alpha, base_alpha, min_alpha, incident_half_life_days, last_incident_time
FROM markov_config;
 use_adaptive_alpha | base_alpha | min_alpha | incident_half_life_days | last_incident_time
--------------------+------------+-----------+-------------------------+--------------------
 t                  |        0.1 |      0.01 |                       7 |
(1 row)
*/
CREATE OR REPLACE FUNCTION apply_forgetting(alpha_override REAL DEFAULT NULL)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    cfg RECORD;
    effective_alpha REAL;
    days_since_incident REAL;
    details_text TEXT;
BEGIN
    SELECT use_adaptive_alpha, alpha, base_alpha, min_alpha,
           incident_half_life_days, last_incident_time
    INTO cfg
    FROM markov_config LIMIT 1;

    -- Вычисление эффективного alpha
    IF alpha_override IS NOT NULL THEN
        effective_alpha := alpha_override;
        details_text := format('alpha_override = %s', alpha_override);
    ELSIF cfg.use_adaptive_alpha THEN
        IF cfg.last_incident_time IS NULL THEN
            effective_alpha := cfg.min_alpha;
            days_since_incident := NULL;
            details_text := 'adaptive mode, no incident recorded -> min_alpha';
        ELSE
            days_since_incident := EXTRACT(EPOCH FROM (now() - cfg.last_incident_time)) / 86400.0;
            effective_alpha := cfg.base_alpha * exp(-days_since_incident / cfg.incident_half_life_days);
            effective_alpha := GREATEST(effective_alpha, cfg.min_alpha);
            details_text := format('adaptive mode, days_since_incident = %s, base_alpha = %s, half_life = %s -> effective_alpha = %s',
                                   days_since_incident, cfg.base_alpha, cfg.incident_half_life_days, effective_alpha);
        END IF;
    ELSE
        effective_alpha := cfg.alpha;
        details_text := format('non-adaptive mode, config.alpha = %s', cfg.alpha);
    END IF;

    -- Журналирование в таблицу
    INSERT INTO apply_forgetting_log (effective_alpha, adaptive_used, days_since_incident, alpha_override, details)
    VALUES (effective_alpha, cfg.use_adaptive_alpha, days_since_incident, alpha_override, details_text);

    -- Само забывание
    UPDATE markov_frequencies
    SET frequency = frequency * (1.0 - effective_alpha);

    DELETE FROM markov_frequencies
    WHERE frequency < 1e-6;

    PERFORM update_markov_probabilities();

    UPDATE markov_config SET last_forget_time = now();

    -- Оставляем RAISE NOTICE для совместимости (может быть закомментирован при желании)
    RAISE NOTICE 'apply_forgetting: alpha=%, adaptive=%, days_since_incident=%',
                 effective_alpha, cfg.use_adaptive_alpha, days_since_incident;
END;
$$;
COMMENT ON FUNCTION apply_forgetting(REAL) IS 'Применяет забывание (снижение частот), записывая событие в apply_forgetting_log';
------------------------------------------------------

------------------------------------------------------
-- Архивация вероятностей цепи Маркова
CREATE OR REPLACE FUNCTION archive_markov_probabilities(p_train_date DATE DEFAULT current_date)
RETURNS void
LANGUAGE sql
AS $$
    DELETE FROM markov_probabilities_archive WHERE train_date = p_train_date;
    INSERT INTO markov_probabilities_archive (train_date, from_state, to_state, probability)
    SELECT p_train_date, from_state, to_state, probability
    FROM markov_probabilities;
$$;
COMMENT ON FUNCTION archive_markov_probabilities IS 'Архивация вероятностей цепи Маркова';

------------------------------------------------------
-- Очистка журнала переходов
CREATE OR REPLACE FUNCTION clean_forecast_log()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    retention INT;
BEGIN
    SELECT forecast_log_retention_days INTO retention FROM markov_config;
    DELETE FROM forecast_log
    WHERE ts < now() - (retention || ' days')::INTERVAL;
END;
$$;
COMMENT ON FUNCTION clean_forecast_log IS 'Очистка журнала переходов';

------------------------------------------------------
-- Функция очистки transition_log
CREATE OR REPLACE FUNCTION clean_transition_log()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    retention INT;
BEGIN
    SELECT transition_log_retention_days INTO retention FROM markov_config;
    DELETE FROM transition_log
    WHERE ts < now() - (retention || ' days')::INTERVAL;
END;
$$;
COMMENT ON FUNCTION clean_transition_log IS 'Функция очистки transition_log';

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- CHECK_AND_FORGET
/*
3.1 Периодичность и автоматизация
Плановая проверка – запускать check_and_forget() каждые 15 минут через pg_cron 

3.2 Калибровка порогов (первые 1–2 недели)
Включить режим мониторинга без автоматического забывания – в check_and_forget() временно закомментировать вызов apply_forgetting, но логировать все метрики в отдельную таблицу forget_log_calibration.
Анализировать распределение KL, χ², Brier Score, отклонения скорости. Настроить пороги так, чтобы ложные срабатывания были не чаще 1–2 раз в день.
Для χ² использовать 99-й процентиль распределения с 188 степенями свободы (~220). Если ваша размерность состояний отличается, пересчитать.
Для Brier Score: 0.25 – разумный порог для начала, но после калибровки можно изменить до 0.2 или 0.3.

3.3 Борьба с ложными срабатываниями
Внедрить подтверждение признака: например, хранить в отдельной таблице check_state последние значения KL, os_dev и количество последовательных превышений. Использовать колонку confirmation_cycles в markov_config.
Условие: признак считается истинным, если он превышает порог в 2–3 циклах проверки подряд (30–45 минут). Это снижает чувствительность к кратковременным всплескам.
Пример таблицы для состояния проверок:
CREATE TABLE check_state (
    check_time    TIMESTAMPTZ PRIMARY KEY,
    kl_flag       BOOLEAN,
    os_flag       BOOLEAN,
    brier_flag    BOOLEAN
);
-- В check_and_forget анализировать последние N записей

3.4 Обработка внутридневного дрейфа
Если KL-дивергенция между текущим часом и утренним эталоном (например, 9:00–10:00) превышает 0.2, увеличивайте alpha аддитивно на 0.02 на следующий час.
Альтернатива: перейти к множественным матрицам (разные слоты времени). В текущей реализации можно просто повышать частоту планового забывания в часы пик.

3.5 Интеграция с существующими функциями
apply_forgetting уже реализована и вызывает update_markov_probabilities(), которая перестраивает вероятности и поглощающую матрицу. Ничего дополнительно вызывать не нужно.
forecast_log уже заполняется в markov_chain_training – для расчёта Brier Score используйте её.
check_kl_divergence() из исходных файлов оценивает стационарное распределение за неделю – этот критерий используется в evaluate_training_sufficiency, а не в check_and_forget. Не путайте.

3.6 Логирование и мониторинг
После каждого забывания пишите в forget_log. Настройте алерт, если забывания происходят чаще 6 раз в сутки (можно менять порог).
Еженедельно запускайте evaluate_training_sufficiency() для проверки, что модель стабильна и не требует полного переобучения.

3.7 Ручное управление
Предоставьте администратору функцию SELECT emergency_forget('manual', 0.3); для немедленного «сброса» модели.
Также можно напрямую обновить markov_config.alpha, чтобы временно ускорить плановое забывание.

3.8 Пример вызова для тестирования
-- Ручная проверка (без применения забывания, только логирование)
SELECT check_and_forget(); -- в боевой версии после калибровки раскомментируйте PERFORM apply_forgetting

-- Принудительное забывание после деплоя
SELECT emergency_forget('deploy', 0.4);

4. Заключение
Предложенные таблицы и функции полностью реализуют механизм форсированного забывания, описанный в методике. 
Они интегрируются с существующей реализацией цепи Маркова, используют уже имеющиеся журналы переходов и прогнозов, и не нарушают логику планового забывания. 
Рекомендуется начать с мониторинга метрик в течение 1–2 недель, откалибровать пороги, 
и только затем включить автоматическое применение apply_forgetting внутри check_and_forget.
*/
------------------------------------------------------------------------------------------------------------------------
-- Обновление эталонных распределений (запускать ежедневно)
CREATE OR REPLACE FUNCTION update_state_baseline()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    dow_val SMALLINT;
    hour_val SMALLINT;
BEGIN
    -- Для каждого часа и дня недели вычисляем распределение состояний за последние 7 рабочих дней
    FOR dow_val IN 1..7 LOOP
        FOR hour_val IN 0..23 LOOP
            -- Вставляем / обновляем вероятности состояний для этого слота
            INSERT INTO state_baseline (hour_of_day, dow, state_id, probability)
            SELECT 
                hour_val,
                dow_val,
                from_state,
                COUNT(*)::REAL / SUM(COUNT(*)) OVER () AS prob
            FROM transition_log
            WHERE EXTRACT(DOW FROM ts) = dow_val
              AND EXTRACT(HOUR FROM ts) = hour_val
              AND ts >= now() - INTERVAL '7 days'
            GROUP BY from_state
            ON CONFLICT (hour_of_day, dow, state_id) DO UPDATE
            SET probability = EXCLUDED.probability, last_updated = now();
        END LOOP;
    END LOOP;
END;
$$;
COMMENT ON FUNCTION update_state_baseline IS 'Обновляет эталонные распределения состояний для каждого часа и дня недели';

------------------------------------------------------------------------------------------------------------------------
-- Расчёт KL-дивергенции между текущим часом и эталоном
CREATE OR REPLACE FUNCTION calculate_kl_divergence(
    recent_minutes INT DEFAULT 60,       -- окно последних N минут
    baseline_hour  INT DEFAULT NULL ,                  -- час эталона (если NULL, берётся текущий час)
    baseline_dow   INT DEFAULT NULL      -- день недели эталона (NULL = текущий день)
)
RETURNS REAL
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    recent_dist   REAL[] := array_fill(0.0, ARRAY[189]);
    baseline_dist REAL[] := array_fill(0.0, ARRAY[189]);
    total_recent  INT := 0;
    total_base    INT := 0;
    kl_val        REAL := 0.0;
    i             INT;
    rec           RECORD;
    base_hour     INT;
    base_dow      INT;
BEGIN
    -- Определяем час и день эталона
    base_hour := COALESCE(baseline_hour, EXTRACT(HOUR FROM now())::INT);
    base_dow  := COALESCE(baseline_dow,  EXTRACT(DOW FROM now())::INT);
    
    -- 1. Распределение за последние recent_minutes минут
    FOR rec IN
        SELECT from_state, COUNT(*) AS cnt
        FROM transition_log
        WHERE ts >= now() - (recent_minutes || ' minutes')::INTERVAL
        GROUP BY from_state
    LOOP
        recent_dist[rec.from_state+1] := rec.cnt;
        total_recent := total_recent + rec.cnt;
    END LOOP;
    IF total_recent = 0 THEN RETURN NULL; END IF;
    -- нормализуем
    FOR i IN 1..189 LOOP
        recent_dist[i] := recent_dist[i] / total_recent;
    END LOOP;

    -- 2. Эталонное распределение из state_baseline
    SELECT array_agg(probability ORDER BY state_id) INTO baseline_dist
    FROM state_baseline
    WHERE hour_of_day = base_hour AND dow = base_dow
    ORDER BY state_id;
    IF baseline_dist IS NULL THEN RETURN NULL; END IF;

    -- 3. KL = sum p_i * ln(p_i / q_i) с аддитивным сглаживанием для нулей
    FOR i IN 1..189 LOOP
        IF recent_dist[i] > 0 AND baseline_dist[i] > 0 THEN
            kl_val := kl_val + recent_dist[i] * ln(recent_dist[i] / baseline_dist[i]);
        ELSIF recent_dist[i] > 0 AND baseline_dist[i] = 0 THEN
            -- применяем аддитивное сглаживание (small epsilon)
            kl_val := kl_val + recent_dist[i] * ln(recent_dist[i] / 1e-6);
        END IF;
    END LOOP;
    RETURN kl_val;
END;
$$;
COMMENT ON FUNCTION calculate_kl_divergence IS 'Расчёт KL-дивергенции между текущим часом и эталоном';

------------------------------------------------------------------------------------------------------------------------
-- Расчёт χ² – критерия
CREATE OR REPLACE FUNCTION calculate_chi_squared(
    recent_minutes INT DEFAULT 60,
    baseline_hour  INT DEFAULT NULL,
    baseline_dow   INT DEFAULT NULL
)
RETURNS REAL
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    obs_cnt   INT[];
    exp_cnt   REAL[];
    chi2_val  REAL := 0.0;
    total_obs INT := 0;
    total_exp REAL := 0.0;
    i         INT;
    rec       RECORD;
    base_hour INT;
    base_dow  INT;
BEGIN
    base_hour := COALESCE(baseline_hour, EXTRACT(HOUR FROM now())::INT);
    base_dow  := COALESCE(baseline_dow,  EXTRACT(DOW FROM now())::INT);
    
    obs_cnt := array_fill(0, ARRAY[189]);
    -- наблюдаемые частоты
    FOR rec IN
        SELECT from_state, COUNT(*) AS cnt
        FROM transition_log
        WHERE ts >= now() - (recent_minutes || ' minutes')::INTERVAL
        GROUP BY from_state
    LOOP
        obs_cnt[rec.from_state+1] := rec.cnt;
        total_obs := total_obs + rec.cnt;
    END LOOP;
    IF total_obs = 0 THEN RETURN NULL; END IF;

    -- ожидаемые частоты (из эталона, умноженные на total_obs)
    FOR rec IN
        SELECT state_id, probability
        FROM state_baseline
        WHERE hour_of_day = base_hour AND dow = base_dow
    LOOP
        exp_cnt[rec.state_id+1] := rec.probability * total_obs;
        total_exp := total_exp + exp_cnt[rec.state_id+1];
    END LOOP;
    IF total_exp = 0 THEN RETURN NULL; END IF;

    -- χ² = Σ (O - E)² / E
    FOR i IN 1..189 LOOP
        IF exp_cnt[i] > 0 THEN
            chi2_val := chi2_val + power(obs_cnt[i] - exp_cnt[i], 2) / exp_cnt[i];
        END IF;
    END LOOP;
    RETURN chi2_val;
END;
$$;
COMMENT ON FUNCTION calculate_chi_squared IS 'Расчёт χ² – критерия';

------------------------------------------------------------------------------------------------------------------------
-- Отклонение операционной скорости (SMA20 vs историческое среднее)
CREATE OR REPLACE FUNCTION get_os_deviation()
RETURNS REAL
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    current_hour INT;
    sma20        REAL;
    hist_avg     REAL;
    hist_std     REAL;
    deviation    REAL;
BEGIN
    current_hour := EXTRACT(HOUR FROM now())::INT;
    
    -- SMA20 за последние 20 минут (предполагается таблица cluster_stat_median)
    SELECT AVG(curr_op_speed) INTO sma20
    FROM cluster_stat_median
    WHERE curr_timestamp >= now() - INTERVAL '20 minutes';
    
    -- Исторические среднее и std для этого часа (последние 20 дней)
    SELECT avg_speed, stddev_speed INTO hist_avg, hist_std
    FROM operational_speed_stats
    WHERE hour_of_day = current_hour;
    
    IF hist_avg IS NULL OR hist_std IS NULL OR sma20 IS NULL THEN
        RETURN 0.0;
    END IF;
    
    deviation := abs(sma20 - hist_avg) / NULLIF(hist_avg, 0);
    RETURN deviation;
END;
$$;
COMMENT ON FUNCTION get_os_deviation IS 'Отклонение операционной скорости (SMA20 vs историческое среднее)';

------------------------------------------------------------------------------------------------------------------------
-- Обновление статистики операционной скорости (запускать ежедневно)
CREATE OR REPLACE FUNCTION refresh_os_stats()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    hr INT;
BEGIN
    FOR hr IN 0..23 LOOP
        INSERT INTO operational_speed_stats (hour_of_day, avg_speed, stddev_speed)
        SELECT 
            hr,
            AVG(curr_op_speed),
            STDDEV(curr_op_speed)
        FROM cluster_stat_median
        WHERE EXTRACT(HOUR FROM curr_timestamp) = hr
          AND curr_timestamp >= now() - INTERVAL '20 days'
        ON CONFLICT (hour_of_day) DO UPDATE
        SET avg_speed = EXCLUDED.avg_speed,
            stddev_speed = EXCLUDED.stddev_speed,
            last_updated = now();
    END LOOP;
END;
$$;
COMMENT ON FUNCTION refresh_os_stats IS 'Обновление статистики операционной скорости (запускать ежедневно)';

------------------------------------------------------------------------------------------------------------------------
-- Основная процедура check_and_forget
CREATE OR REPLACE FUNCTION check_and_forget()
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    cfg RECORD;
    kl_val REAL;
    chi2_val REAL;
    os_dev REAL;
    brier_val REAL;
    brier_obs_count INT;
    kl_flag BOOLEAN;
    chi2_flag BOOLEAN;
    os_flag BOOLEAN;
    brier_flag BOOLEAN;
    infra_flag BOOLEAN;
    diurnal_flag BOOLEAN;
    alpha_eff REAL := 0.0;
    reason_list TEXT[] := '{}';
    details_text TEXT := '';
    consecutive_count INT;
    retention_days INT;
BEGIN
    -- 1. Чтение конфигурации (включая check_state_retention_days и brier_min_observations)
    SELECT kl_threshold, chi2_threshold, os_dev_threshold, brier_threshold,
           forget_alpha_max, confirmation_cycles, adaptive_forgetting_enabled,
           check_state_retention_days, brier_min_observations
    INTO cfg
    FROM markov_config LIMIT 1;

    -- 2. Если адаптивное забывание отключено – выходим
    IF NOT cfg.adaptive_forgetting_enabled THEN
        RETURN 'Adaptive forgetting is disabled by markov_config.adaptive_forgetting_enabled = false.';
    END IF;

    -- 3. Очистка устаревших записей check_state с использованием параметра конфигурации
    retention_days := COALESCE(cfg.check_state_retention_days, 7);
    DELETE FROM check_state WHERE check_time < now() - (retention_days || ' days')::INTERVAL;

    -- 4. Расчёт признаков
    kl_val := calculate_kl_divergence(60, NULL, NULL);
    chi2_val := calculate_chi_squared(60, NULL, NULL);
    os_dev := get_os_deviation();

    -- 4.1 Расчёт Brier Score с учётом минимального количества наблюдений
    SELECT AVG((predicted_risk - actual_risk)^2), COUNT(*)
    INTO brier_val, brier_obs_count
    FROM forecast_log
    WHERE ts >= now() - INTERVAL '2 hours';

    -- 5. Определение флагов
    kl_flag := (kl_val IS NOT NULL AND kl_val > cfg.kl_threshold);
    chi2_flag := (chi2_val IS NOT NULL AND chi2_val > cfg.chi2_threshold);
    os_flag := (os_dev > cfg.os_dev_threshold);
    
    -- Brier flag: только если достаточно наблюдений И значение превышает порог
    brier_flag := (brier_obs_count >= COALESCE(cfg.brier_min_observations, 10)
                   AND brier_val > cfg.brier_threshold);
    
    infra_flag := EXISTS (SELECT 1 FROM infrastructure_events
                          WHERE event_time > now() - INTERVAL '1 hour' AND processed = false);
    diurnal_flag := (calculate_kl_divergence(60, EXTRACT(HOUR FROM now())::INT, NULL) > 0.2);

    -- 6. Сохранение результатов проверки
    INSERT INTO check_state (check_time, kl_flag, chi2_flag, os_flag, brier_flag, infra_flag, diurnal_flag)
    VALUES (now(), kl_flag, chi2_flag, os_flag, brier_flag, infra_flag, diurnal_flag);

    -- 7. Подсчёт последовательных срабатываний (любой из флагов)
    WITH consecutive AS (
        SELECT check_time,
               (kl_flag OR chi2_flag OR os_flag OR brier_flag OR infra_flag OR diurnal_flag) AS any_flag
        FROM check_state
        ORDER BY check_time DESC
        LIMIT cfg.confirmation_cycles
    )
    SELECT COUNT(*) INTO consecutive_count
    FROM consecutive
    WHERE any_flag = true;

    -- 8. Если достаточно последовательных срабатываний – вычисляем alpha_eff и применяем забывание
    IF consecutive_count >= cfg.confirmation_cycles THEN
        IF kl_flag THEN
            alpha_eff := alpha_eff + 0.1;
            reason_list := reason_list || 'KL';
            details_text := details_text || format('KL=%.3f; ', kl_val);
        END IF;
        IF chi2_flag THEN
            alpha_eff := alpha_eff + 0.1;
            reason_list := reason_list || 'Chi2';
            details_text := details_text || format('Chi2=%.1f; ', chi2_val);
        END IF;
        IF os_flag THEN
            alpha_eff := alpha_eff + 0.1;
            reason_list := reason_list || 'OS';
            details_text := details_text || format('OS_dev=%.2f; ', os_dev);
        END IF;
        IF brier_flag THEN
            alpha_eff := alpha_eff + 0.1;
            reason_list := reason_list || 'Brier';
            details_text := details_text || format('Brier=%.3f (obs=%d); ', brier_val, brier_obs_count);
        END IF;
        IF infra_flag THEN
            alpha_eff := GREATEST(alpha_eff, 0.3);
            reason_list := reason_list || 'Infra';
            UPDATE infrastructure_events SET processed = true WHERE event_time > now() - INTERVAL '1 hour';
        END IF;
        IF diurnal_flag THEN
            alpha_eff := alpha_eff + 0.02;
            reason_list := reason_list || 'Diurnal';
        END IF;

        IF alpha_eff > 0.0 THEN
            alpha_eff := LEAST(alpha_eff, cfg.forget_alpha_max);
            PERFORM apply_forgetting(alpha_eff);
            INSERT INTO forget_log (alpha, triggered_by, kl_div, chi2_val, brier_score, os_deviation, details)
            VALUES (alpha_eff, reason_list, kl_val, chi2_val, brier_val, os_dev, details_text);
            RETURN format('Forgetting applied with alpha=%.3f, reasons: %s', alpha_eff, array_to_string(reason_list, ','));
        END IF;
    END IF;

    -- Дополнительное диагностическое сообщение при недостатке данных для Brier (опционально)
    IF brier_obs_count < COALESCE(cfg.brier_min_observations, 10) THEN
        RAISE DEBUG 'check_and_forget: insufficient forecast data (%), Brier flag not considered',
                    brier_obs_count;
    END IF;

    RETURN 'No forgetting needed.';
END;
$$;
COMMENT ON FUNCTION check_and_forget() IS 'Основная процедура check_and_forget с корректной обработкой Brier Score (только при достаточном количестве наблюдений) и очисткой check_state по retention_days из конфигурации';

------------------------------------------------------------------------------------------------------------------------
-- emergency_forget Функция экстренного забывания по событию
CREATE OR REPLACE FUNCTION emergency_forget(event_type TEXT, alpha REAL DEFAULT 0.4)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO infrastructure_events (event_type, description) VALUES (event_type, 'Manual or automated trigger');
    PERFORM apply_forgetting(LEAST(alpha, 0.5));
    INSERT INTO forget_log (alpha, triggered_by, details)
    VALUES (LEAST(alpha, 0.5), ARRAY['Emergency_'||event_type], 'Forced by external event');
END;
$$;
COMMENT ON FUNCTION emergency_forget IS 'emergency_forget Функция экстренного забывания по событию';

------------------------------------------------------------------------------------------------------------------------
-- Очистка архивных снимков матрицы вероятностей
CREATE OR REPLACE FUNCTION clean_markov_probabilities_archive()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    retention INT;
BEGIN
    SELECT archive_retention_days INTO retention FROM markov_config;
    DELETE FROM markov_probabilities_archive
    WHERE train_date < current_date - retention;
END;
$$;
COMMENT ON FUNCTION clean_markov_probabilities_archive() IS 'Удаляет архивные снимки матрицы старше заданного числа дней';

------------------------------------------------------------------------------------------------------------------------
-- Очистка истории проверок check_state
CREATE OR REPLACE FUNCTION clean_check_state()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    retention INT;
BEGIN
    SELECT check_state_retention_days INTO retention FROM markov_config;
    DELETE FROM check_state
    WHERE check_time < now() - (retention || ' days')::INTERVAL;
END;
$$;
COMMENT ON FUNCTION clean_check_state() IS 'Удаляет старые записи check_state (история проверок)';

------------------------------------------------------------------------------------------------------------------------
-- Очистка журнала форсированных забываний
CREATE OR REPLACE FUNCTION clean_forget_log()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    retention INT;
BEGIN
    SELECT forget_log_retention_days INTO retention FROM markov_config;
    DELETE FROM forget_log
    WHERE ts < now() - (retention || ' days')::INTERVAL;
END;
$$;
COMMENT ON FUNCTION clean_forget_log() IS 'Удаляет старые записи forget_log';

------------------------------------------------------------------------------------------------------------------------
-- Обновление времени последнего инцидента (через триггер)
CREATE OR REPLACE FUNCTION update_last_incident_time()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM state_descriptions
        WHERE state_id = NEW.to_state
          AND correlation < 0
          AND os_trend = -1
    ) THEN
        UPDATE markov_config SET last_incident_time = NEW.ts;
    END IF;
    RETURN NEW;
END;
$$;
COMMENT ON FUNCTION update_last_incident_time() IS 'Обновление времени последнего инцидента (через триггер)';

DROP TRIGGER IF EXISTS trigger_update_incident_time ON transition_log;
CREATE TRIGGER trigger_update_incident_time
    AFTER INSERT ON transition_log
    FOR EACH ROW
    EXECUTE FUNCTION update_last_incident_time();
-- Обновление времени последнего инцидента (через триггер)	
------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------
-- Функция для обновления конфигурации (включение адаптивного режима)
-- SELECT enable_adaptive_forgetting(0.1, 0.01, 7.0);
CREATE OR REPLACE FUNCTION enable_adaptive_forgetting(
    p_base_alpha REAL DEFAULT 0.1,
    p_min_alpha REAL DEFAULT 0.01,
    p_half_life_days REAL DEFAULT 7.0
)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE markov_config SET
        use_adaptive_alpha = true,
        base_alpha = p_base_alpha,
        min_alpha = p_min_alpha,
        incident_half_life_days = p_half_life_days;

    -- Опционально: обнуляем время последнего инцидента
    UPDATE markov_config SET last_incident_time = NULL;

    RETURN format('Adaptive forgetting enabled: base_alpha=%s, min_alpha=%s, half_life=%s days',
                  p_base_alpha, p_min_alpha, p_half_life_days);
END;
$$;
COMMENT ON FUNCTION enable_adaptive_forgetting(REAL, REAL, REAL) IS 'Включает адаптивное забывание и задаёт параметры';

------------------------------------------------------------------------------------------------------------------------
-- Получение параметров адаптивного режима забывания
CREATE OR REPLACE FUNCTION get_adaptive_forgetting_status()
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
 markov_config_rec record ;
BEGIN
	SELECT 
		use_adaptive_alpha ,
        base_alpha ,
        min_alpha ,
        incident_half_life_days 
	INTO 
		markov_config_rec
	FROM 
		markov_config ; 

    RETURN format('Adaptive forgetting enabled=%s,  base_alpha=%s, min_alpha=%s, half_life=%s days',
                  markov_config_rec.use_adaptive_alpha , markov_config_rec.base_alpha , markov_config_rec.min_alpha , markov_config_rec.incident_half_life_days   );
END;
$$;
COMMENT ON FUNCTION enable_adaptive_forgetting(REAL, REAL, REAL) IS 'Включает адаптивное забывание и задаёт параметры';


-- Функция получения журнала forget_log за период
-- select unnest(get_forget_log());
CREATE OR REPLACE FUNCTION get_forget_log(
    p_start TIMESTAMPTZ DEFAULT now() - INTERVAL '7 days',
    p_end   TIMESTAMPTZ DEFAULT now()
)
RETURNS TEXT[]
LANGUAGE plpgsql
AS $$
DECLARE
    result_array TEXT[] := '{}';
    rec RECORD;
BEGIN
    FOR rec IN
        SELECT id,
               ts,
               alpha,
               triggered_by,
               kl_div,
               chi2_val,
               brier_score,
               os_deviation,
               details
        FROM forget_log
        WHERE ts BETWEEN p_start AND p_end
        ORDER BY ts DESC
    LOOP
        result_array := result_array || format(
            'id=%s, ts=%s, alpha=%s, triggered_by={%s}, kl_div=%s, chi2_val=%s, brier_score=%s, os_deviation=%s, details=%s',
            rec.id,
            rec.ts,
            rec.alpha,
            array_to_string(rec.triggered_by, ','),
            rec.kl_div,
            rec.chi2_val,
            rec.brier_score,
            rec.os_deviation,
            rec.details
        );
    END LOOP;

    RETURN result_array;
END;
$$;

COMMENT ON FUNCTION get_forget_log(TIMESTAMPTZ, TIMESTAMPTZ) IS
'Возвращает массив строк журнала forget_log за указанный период.
Параметры:
  p_start – начало периода (по умолчанию 7 дней назад от now())
  p_end   – конец периода (по умолчанию now())
Каждая строка содержит id, ts, alpha, triggered_by, kl_div, chi2_val, brier_score, os_deviation, details.';

--------------------------------------------------------------------------------
-- Функция получения журнала apply_forgetting_log за период (текстовый массив)
--------------------------------------------------------------------------------
-- Примеры использования
--------------------------------------------------------------------------------
/*
-- Получить журнал за последние 7 дней (по умолчанию)
SELECT get_apply_forgetting_log();

-- Получить журнал за конкретный интервал
SELECT get_apply_forgetting_log('2025-05-01', '2025-05-10');

-- Очистка записей старше 30 дней (значение из конфигурации)
SELECT clean_apply_forgetting_log();

-- Очистка с явным указанием срока (например, 60 дней)
SELECT clean_apply_forgetting_log(60);

-- Очистка журнала apply_forgetting_log (каждый день в 02:00):
-- 0 2 * * * psql -d expecto_db -U expecto_user -c "SELECT clean_apply_forgetting_log();"
*/

CREATE OR REPLACE FUNCTION get_apply_forgetting_log(
    p_start TIMESTAMPTZ DEFAULT now() - INTERVAL '7 days',
    p_end   TIMESTAMPTZ DEFAULT now()
)
RETURNS TEXT[]
LANGUAGE plpgsql
AS $$
DECLARE
    result_array TEXT[] := '{}';
    rec RECORD;
BEGIN
    FOR rec IN
        SELECT id, ts, effective_alpha, adaptive_used, days_since_incident,
               alpha_override, details
        FROM apply_forgetting_log
        WHERE ts BETWEEN p_start AND p_end
        ORDER BY ts DESC
    LOOP
        result_array := result_array || format(
            'id=%s, ts=%s, effective_alpha=%s, adaptive_used=%s, days_since_incident=%s, alpha_override=%s, details=%s',
            rec.id, rec.ts, rec.effective_alpha, rec.adaptive_used,
            rec.days_since_incident, rec.alpha_override, rec.details
        );
    END LOOP;
    RETURN result_array;
END;
$$;
COMMENT ON FUNCTION get_apply_forgetting_log(TIMESTAMPTZ, TIMESTAMPTZ) IS
'Возвращает массив строк журнала apply_forgetting_log за указанный период.
Параметры:
  p_start – начало периода (по умолчанию 7 дней назад)
  p_end   – конец периода (по умолчанию текущий момент)';

--------------------------------------------------------------------------------  
-- Функция очистки старых записей apply_forgetting_log (для cron)
CREATE OR REPLACE FUNCTION clean_apply_forgetting_log(p_retention_days INT DEFAULT NULL)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    retention INT;
    deleted_rows BIGINT;
BEGIN
    -- Если параметр не передан, берём значение из конфигурации
    IF p_retention_days IS NULL THEN
        SELECT apply_forgetting_log_retention_days INTO retention FROM markov_config LIMIT 1;
        IF retention IS NULL THEN
            retention := 30; -- Значение по умолчанию
        END IF;
    ELSE
        retention := p_retention_days;
    END IF;

    DELETE FROM apply_forgetting_log
    WHERE ts < now() - (retention || ' days')::INTERVAL;

    GET DIAGNOSTICS deleted_rows = ROW_COUNT;
    RETURN format('Deleted %s rows from apply_forgetting_log older than %s days', deleted_rows, retention);
END;
$$;
COMMENT ON FUNCTION clean_apply_forgetting_log(INT) IS
'Удаляет записи из apply_forgetting_log старше заданного количества дней.
Параметр p_retention_days – срок хранения в днях (если NULL, берётся из markov_config.apply_forgetting_log_retention_days).';

--------------------------------------------------------------------------------  
--Отключает адаптивное забывание (use_adaptive_alpha = false)
CREATE OR REPLACE FUNCTION disable_adaptive_forgetting()
RETURNS TEXT
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE markov_config SET use_adaptive_alpha = false;
    RETURN 'Adaptive forgetting disabled. Non-adaptive alpha from config.alpha will be used.';
END;
$$;
COMMENT ON FUNCTION disable_adaptive_forgetting() IS 'Отключает адаптивное забывание (use_adaptive_alpha = false)';

--------------------------------------------------------------------------------  
--Ручная установка времени последнего инцидента (для тестов или внешних событий)
CREATE OR REPLACE FUNCTION set_last_incident_time(p_time TIMESTAMPTZ DEFAULT now())
RETURNS TEXT
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE markov_config SET last_incident_time = p_time;
    RETURN format('last_incident_time set to %s', p_time);
END;
$$;
COMMENT ON FUNCTION set_last_incident_time(TIMESTAMPTZ) IS 'Ручная установка времени последнего инцидента (для тестов или внешних событий)';
