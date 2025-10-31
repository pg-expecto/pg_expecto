--------------------------------------------------------------------------------
-- core_cluster_functions.sql
-- version 2.0
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
  -- ОЖИДАНИЯ
  --------------------------------------------
	
	max_timestamp timestamptz ; 
		
	wait_stats_rec	record ;
	
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
			WHEN wait_stats_rec.event_type = 'LWLock' THEN current_lwlock = COALESCE( wait_stats_rec.event_type_count , 0 );
			WHEN wait_stats_rec.event_type = 'Timeout' THEN current_timeout = COALESCE( wait_stats_rec.event_type_count , 0 );
		END CASE ;		
	END LOOP;
	-- ОЖИДАНИЯ		
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
		curr_timeout
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
		current_timeout 
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
		curr_timeout
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
		timeout_long 
		
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
			CALL start_incident(3);
		END IF ;
		
		--Сильная корреляция
		IF ABS(speed_waitings_correlation) >= 0.7 AND SIGN(speed_waitings_correlation) = -1 
		THEN 
			speed_degradation_indicator = -100 ;
			CALL start_incident(4);
		END IF ;
	ELSE
		speed_degradation_indicator = 0 ;
		CALL stop_incidents();
	END IF ;
	--version 27.0
	-------------------------------------------------------------------
	
	
    -- операционная скорость | ожидания |  индикатор 
	result_str= ROUND( cluster_stat_median_rec.curr_op_speed::numeric , 2 )||'|'||  --1
				ROUND( cluster_stat_median_rec.curr_waitings::numeric , 2 )||'|'||  --2
				speed_degradation_indicator ||'|'  --3
				;
	
	
	
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
		priority <= curr_priority AND 
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
-- version 2.0
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
	curr_timeout bigint
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
--Текущая статистика
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--Скользящие медианы
DROP TABLE IF EXISTS cluster_stat_median;
CREATE UNLOGGED TABLE cluster_stat_median 
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
	curr_timeout numeric
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
--Скользящие медианы
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Инциденты производительности
DROP TABLE IF EXISTS performance_incident ;
CREATE UNLOGGED TABLE performance_incident 
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
-- version 3.0
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
-- version 1.0
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
		cpu_st 	  --  st — stolen (украдено гипервизором)
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
		current_cpu_st 	
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
		cpu_st_long 	  --  st — stolen (украдено гипервизором)
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
		curr_cpu_st_long    --  st — stolen (украдено гипервизором)		
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
----------------------------------------------------------------------------------------------------------------------------------------------------------------
-- statement_stat_tables.sql
-- version 1.0
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
	cpu_st integer	  --  st — stolen (украдено гипервизором)
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
-- Метрики vmstat
--------------------------------------------------------------------------------


--------------------------------------------------------------------------------
--Скользящие медианы по метрикам vmstat
DROP TABLE IF EXISTS os_stat_vmstat_median;
CREATE UNLOGGED TABLE os_stat_vmstat_median 
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
	io_bo_long numeric, -- bo — записанные на устройства 
	
	system_in_long numeric, -- in — прерывания
	system_cs_long numeric, -- cs — переключения контекста
	
	cpu_us_long numeric , -- us — user time
	cpu_sy_long numeric , -- sy — system time
	cpu_id_long numeric , -- id — idle
	cpu_wa_long numeric , --  wa — ожидание IO
	cpu_st_long numeric  	  --  st — stolen (украдено гипервизором)
	
);
ALTER TABLE os_stat_vmstat_median ADD CONSTRAINT os_stat_vmstat_median_pk PRIMARY KEY (id);
CREATE INDEX os_stat_vmstat_median_idx ON os_stat_vmstat_median ( curr_timestamp );

COMMENT ON TABLE os_stat_vmstat_median IS 'Скользящие медианы по метрикам vmstat';
COMMENT ON COLUMN os_stat_vmstat.curr_timestamp IS 'Точка времени сбора данных ';	
COMMENT ON COLUMN os_stat_vmstat.procs_r IS 'Скользящая медиана по метрике r — процессы в run queue (готовы к выполнению) ';
COMMENT ON COLUMN os_stat_vmstat.procs_b IS 'Скользящая медиана по метрике b — процессы в uninterruptible sleep (обычно ждут IO) ';
COMMENT ON COLUMN os_stat_vmstat.memory_swpd IS 'Скользящая медиана по метрике swpd — объём свопа ';
COMMENT ON COLUMN os_stat_vmstat.memory_free IS 'Скользящая медиана по метрике free — свободная RAM ';
COMMENT ON COLUMN os_stat_vmstat.memory_buff IS 'Скользящая медиана по метрике buff — буферы';
COMMENT ON COLUMN os_stat_vmstat.memory_cache IS 'Скользящая медиана по метрике cache — кэш';
COMMENT ON COLUMN os_stat_vmstat.swap_si IS 'Скользящая медиана по метрике si — swap in (из swap в RAM)';
COMMENT ON COLUMN os_stat_vmstat.swap_so IS 'Скользящая медиана по метрике si — so — swap out (из RAM в swap)';
COMMENT ON COLUMN os_stat_vmstat.io_bi IS 'Скользящая медиана по метрике bi — блоки, считанные с устройств';
COMMENT ON COLUMN os_stat_vmstat.io_bo IS 'Скользящая медиана по метрике bo — записанные на устройства';
COMMENT ON COLUMN os_stat_vmstat.system_in IS 'Скользящая медиана по метрике in — прерывания';
COMMENT ON COLUMN os_stat_vmstat.system_cs IS 'Скользящая медиана по метрике cs — переключения контекста';
COMMENT ON COLUMN os_stat_vmstat.cpu_us IS 'Скользящая медиана по метрике us — user time';
COMMENT ON COLUMN os_stat_vmstat.cpu_sy IS 'Скользящая медиана по метрике sy — system time';
COMMENT ON COLUMN os_stat_vmstat.cpu_id IS 'Скользящая медиана по метрике id — idle';
COMMENT ON COLUMN os_stat_vmstat.cpu_wa IS 'Скользящая медиана по метрике wa — ожидание IO';
COMMENT ON COLUMN os_stat_vmstat.cpu_st IS 'Скользящая медиана по метрике st — stolen (украдено гипервизором)';
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
CREATE UNLOGGED TABLE os_stat_iostat_device_median 
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
-- version 1.0.1
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
-- version 1.0
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
CREATE UNLOGGED TABLE statement_stat_sql
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
CREATE UNLOGGED TABLE statement_stat_median 
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
CREATE UNLOGGED TABLE statement_stat_waitings_median 
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
-- version 1.0
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
-- version 1.0
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Функции обеспечения нагрузочного теста
-- load_test_new_test() Начать новый тест
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
-- load_test_set_max_load( new_max_load integer ) --Установить максимальное  количество подключений для pgbench
-- load_test_is_test_could_be_finished()  --Если тест может быть остановлен 
--
-- load_test_has_the_first_hour_passed() --ЕСЛИ идет первый час работы
--
-- load_test_increment_pass_counter() --УВЕЛИЧИТЬ СЧЕТЧИК ИТЕРАЦИЙ
--
-- load_test_set_scenario_queryid --Установить quaryid для сценариев 
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
CREATE OR REPLACE FUNCTION load_test_set_load() RETURNS integer AS $$
DECLARE
 current_test_id integer ;
 load_test_rec record ; 
 load_test_pass_rec record ; 
 current_load_connections DOUBLE PRECISION ;
 result_load_connections integer  ;
 has_the_first_hour_passed integer ; 
 current_test_pass_id integer ; 
BEGIN
  SELECT load_test_get_current_test_id()
  INTO current_test_id;

  SELECT *
  INTO load_test_rec
  FROM load_test 
  WHERE test_id = current_test_id;
  
  SELECT load_test_get_current_test_pass_id()
  INTO current_test_pass_id ; 
  
  SELECT * 
  INTO load_test_pass_rec
  FROM load_test_pass
  WHERE id = current_test_pass_id ; 
  

  ---------------------------------------------------------------------------------------------------
  -- STRESS 
  ---------------------------------------------------------------------
	-- РОСТ НАГРУЗКИ НАЧИНАЕТСЯ СО ВТОРОГО ЧАСА 
	SELECT load_test_has_the_first_hour_passed()
	INTO has_the_first_hour_passed ; 
		
	IF has_the_first_hour_passed = 0 
	THEN 
		UPDATE load_test_pass
		SET load_connections = load_test_rec.base_load_connections
		WHERE test_id = current_test_id AND pass_counter = load_test_rec.pass_counter; 
	
		return load_test_rec.base_load_connections ;
	END IF ;
	-- РОСТ НАГРУЗКИ НАЧИНАЕТСЯ СО ВТОРОГО ЧАСА 
	---------------------------------------------------------------------
    
	
	current_load_connections  = power( 1.6::numeric , (load_test_rec.pass_counter::numeric - load_test_rec.base_load_connections::numeric )/2.0 )*1.6 + load_test_rec.base_load_connections::numeric ; 
	SELECT CEIL( current_load_connections )
	INTO result_load_connections ;  
	
	UPDATE 
		load_test_pass 
	SET 
		load_connections = current_load_connections
	WHERE  
		test_id = current_test_id AND 
		pass_counter = load_test_rec.pass_counter ;


	return result_load_connections  ;

END
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION load_test_set_load IS 'Установить текущую нагрузку connections';
--Установить текущую нагрузку connections
------------------------------------------------------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Текущее количество подключений для pgbench для заданного сценария
CREATE OR REPLACE FUNCTION load_test_get_load_by_scenario( current_scenario integer ) RETURNS integer AS $$
DECLARE 
  total_load integer ;
  result_load integer ;
  current_load_connections DOUBLE PRECISION ;
BEGIN
-------------------------------------------
-- Текущий сценарий 
-- сценарий 1 - select only    : 50%
-- сценарий 2 - select + update: 35%
-- сценарий 3 - insert only    : 15%
-------------------------------------------

 SELECT load_test_get_load()
 INTO total_load ; 
 
 CASE 
	WHEN current_scenario = 1 THEN current_load_connections = total_load::DOUBLE PRECISION * 0.5 ;
	WHEN current_scenario = 2 THEN current_load_connections = total_load::DOUBLE PRECISION * 0.35 ;
	WHEN current_scenario = 3 THEN current_load_connections = total_load::DOUBLE PRECISION * 0.15 ;	
 END CASE ;
 
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
BEGIN
	
	SELECT load_test_get_current_test_id()
	INTO current_test_id ;
	
	SELECT scenario_1_queryid
	INTO curr_scenario_queryid
	FROM load_test ;
		
	IF curr_scenario_queryid IS NULL 
	THEN 
		-------------------------------------------------------
		--SCENARIO_1_QUERYID
		SELECT 
			queryid
		INTO 
			curr_scenario_queryid
		FROM 
			pg_stat_statements
		WHERE 
			query like 'select scenario1%' ;
					
		UPDATE 	
			load_test
		SET
			scenario_1_queryid = curr_scenario_queryid
		WHERE 
			test_id = current_test_id;
		--SCENARIO_1_QUERYID
		-------------------------------------------------------
	END IF;
		
	SELECT scenario_2_queryid
	INTO curr_scenario_queryid
	FROM load_test ;
		
	IF curr_scenario_queryid IS NULL 
	THEN 
		-------------------------------------------------------
		--SCENARIO_2_QUERYID
		SELECT 
			queryid
		INTO 
			curr_scenario_queryid
		FROM 
			pg_stat_statements
		WHERE 
			query like 'select scenario2%' ;
					
		UPDATE 	
			load_test
		SET
			scenario_2_queryid = curr_scenario_queryid
		WHERE 
			test_id = current_test_id;
		--SCENARIO_2_QUERYID
		-------------------------------------------------------
	END IF;

	SELECT scenario_3_queryid
	INTO curr_scenario_queryid
	FROM load_test ;
		
	IF curr_scenario_queryid IS NULL 
	THEN 
		--SCENARIO_3_QUERYID
		SELECT 
			queryid
		INTO 
			curr_scenario_queryid
		FROM 
			pg_stat_statements
		WHERE 
			query like 'select scenario3%' ;
				
		UPDATE 	
			load_test
		SET
			scenario_3_queryid = curr_scenario_queryid
		WHERE 
			test_id = current_test_id;
		--SCENARIO_3_QUERYID
		-------------------------------------------------------
	END IF ;			

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
	
  	
  IF current_load > load_test_rec.max_load
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

--------------------------------------------------------------------------------
-- load_test_tables.sql
-- version 1.0
--------------------------------------------------------------------------------
--Таблицы для анализа нагрузочного тестирования
-----------------------------------------------------------------------------------

-----------------------------------------------------------------------------------
--Нагрузочный тест 
DROP TABLE IF EXISTS load_test CASCADE;
CREATE UNLOGGED TABLE load_test
(
  test_id SERIAL , 
  base_load_connections DOUBLE PRECISION DEFAULT 5, -- Базовое количество соединений pgbench
  max_load integer DEFAULT 100 , -- Максимальная нагрука  соединений pgbench
  test_started timestamp with time zone , 
  test_finished timestamp with time zone ,
  pass_counter integer DEFAULT 0 ,   -- Счетчик проходов теста
  scenario_1_queryid bigint , 
  scenario_2_queryid bigint , 
  scenario_3_queryid bigint   
);
ALTER TABLE load_test ADD CONSTRAINT load_test_pk PRIMARY KEY (test_id);

COMMENT ON TABLE load_test IS 'Нагрузочный тест ';
COMMENT ON COLUMN load_test.base_load_connections IS 'Базовое количество соединений pgbench';
COMMENT ON COLUMN load_test.max_load IS 'Максимальная нагрука  соединений pgbench';
COMMENT ON COLUMN load_test.test_started IS 'Начало теста';
COMMENT ON COLUMN load_test.test_finished IS 'Окончание теста';
COMMENT ON COLUMN load_test.test_finished IS 'Количество итераций теста';
COMMENT ON COLUMN load_test.scenario_1_queryid IS 'SQL запрос сценария-1';
COMMENT ON COLUMN load_test.scenario_2_queryid IS 'SQL запрос сценария-2';
COMMENT ON COLUMN load_test.scenario_3_queryid IS 'SQL запрос сценария-3';
--Нагрузочный тест 
-----------------------------------------------------------------------------------

-----------------------------------------------------------------------------------
-- Итерация нагрузочного теста 
DROP TABLE IF EXISTS load_test_pass CASCADE;
CREATE UNLOGGED TABLE load_test_pass
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



--------------------------------------------------------------------------------
-- report_queryid_for_pareto.sql
-- version 3.0
--------------------------------------------------------------------------------
--
-- report_queryid_for_pareto Диаграмма Парето по queryid
--
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Сформировать диаграмму Парето по queryid
CREATE OR REPLACE FUNCTION report_queryid_for_pareto(  start_timestamp text , finish_timestamp text ) RETURNS text[] AS $$
DECLARE
 result_str text[] ;
 line_count integer ;
 min_timestamp timestamptz ; 
 max_timestamp timestamptz ;   
 
 wait_event_type_rec record ;
 wait_event_rec record ;
 
 total_wait_event_count integer ;
 pct_for_80 numeric ;
 
 
 corr_bufferpin DOUBLE PRECISION ; 
 corr_extension DOUBLE PRECISION ; 
 corr_io DOUBLE PRECISION ; 
 corr_ipc DOUBLE PRECISION ; 
 corr_lock DOUBLE PRECISION ; 
 corr_lwlock DOUBLE PRECISION ; 
 corr_timeout DOUBLE PRECISION ; 
 
 wait_event_type_corr_rec  record ; 
  
 tmp_queryid_index integer ; 
 wait_event_list text ;
 wait_event_list_rec record ;
 
 curr_calls numeric; 
BEGIN
	line_count = 1 ;
	
	result_str[line_count] = 'ОТЧЕТ ПО QUERYID ДЛЯ ДИАГРАММЫ ПАРЕТО';	
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
	
	result_str[line_count] = 'SQL ВЫРАЖЕНИЯ, ВЫЗЫВАЮЩИЕ 80% ОЖИДАНИЙ,';	
	line_count=line_count+1;	
	result_str[line_count] = 'C КОЭФФИЦИЕНТОМ КОРРЕЛЯЦИЯ МЕЖДУ ';
	line_count=line_count+1;	
	result_str[line_count] = 'ТИПОМ ОЖИДАНИЯ И ОЖИДАНИЯМИ СУБД';	
	line_count=line_count+1;	
	result_str[line_count] = '0.7 и ВЫШЕ';	
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
	
	
	DROP TABLE IF EXISTS wait_event_type_corr;
	CREATE TEMPORARY TABLE wait_event_type_corr
	(
		wait_event_type text  ,   
		corr_value DOUBLE PRECISION 
	);
	
	----------------------------------------------
	-- ВНУТРЕННЯЯ ТАБЛИЦА ОТЧЕТА 
	TRUNCATE TABLE tmp_queryid_for_pareto;
	-- ВНУТРЕННЯЯ ТАБЛИЦА ОТЧЕТА 
	----------------------------------------------	


	tmp_queryid_index = 0 ; 
	----------------------------------------------------------------------------------------------------
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ ОЖИДАНИЯ - bufferpin
		WITH 
		waitings AS
		(
			SELECT 
				curr_timestamp , curr_waitings  AS curr_waitings  
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp
				AND curr_waitings > 0
		) ,
		bufferpin_waitings AS
		(
			SELECT 
				curr_timestamp , curr_bufferpin  AS curr_bufferpin 
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND curr_bufferpin > 0 
		) 
		SELECT COALESCE( corr( curr_waitings , curr_bufferpin ) , 0 ) AS correlation_value 
		INTO corr_bufferpin
		FROM
			waitings os JOIN bufferpin_waitings  w ON ( os.curr_timestamp = w.curr_timestamp ) ;	
			
		INSERT INTO wait_event_type_corr ( wait_event_type , corr_value )
		VALUES ( 'BufferPin' , corr_bufferpin );
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ ОЖИДАНИЯ - bufferpin 
	----------------------------------------------------------------------------------------------------
	
	----------------------------------------------------------------------------------------------------
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ ОЖИДАНИЯ - extension
		WITH 
		waitings AS
		(
			SELECT 
				curr_timestamp , curr_waitings  AS curr_waitings  
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp
				AND curr_waitings > 0
		) ,
		extension_waitings AS
		(
			SELECT 
				curr_timestamp , curr_extension  AS curr_extension 
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND curr_extension > 0 
		) 
		SELECT COALESCE( corr( curr_waitings , curr_extension ) , 0 ) AS correlation_value 
		INTO corr_extension
		FROM
			waitings os JOIN extension_waitings  w ON ( os.curr_timestamp = w.curr_timestamp ) ;
		
		INSERT INTO wait_event_type_corr ( wait_event_type , corr_value )
		VALUES ( 'Extension' , corr_extension );			
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ ОЖИДАНИЯ - extension 
	----------------------------------------------------------------------------------------------------
	
	----------------------------------------------------------------------------------------------------
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ ОЖИДАНИЯ - io
		WITH 
		waitings AS
		(
			SELECT 
				curr_timestamp , curr_waitings  AS curr_waitings  
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp
				AND curr_waitings > 0
		) ,
		io_waitings AS
		(
			SELECT 
				curr_timestamp , curr_io  AS curr_io 
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND curr_io > 0 
		) 
		SELECT COALESCE( corr( curr_waitings , curr_io ) , 0 ) AS correlation_value 
		INTO corr_io
		FROM
			waitings os JOIN io_waitings  w ON ( os.curr_timestamp = w.curr_timestamp ) ;	
			
		INSERT INTO wait_event_type_corr ( wait_event_type , corr_value )
		VALUES ( 'IO' , corr_io );			
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ ОЖИДАНИЯ - io 
	----------------------------------------------------------------------------------------------------
	
	----------------------------------------------------------------------------------------------------
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ ОЖИДАНИЯ - ipc
		WITH 
		waitings AS
		(
			SELECT 
				curr_timestamp , curr_waitings  AS curr_waitings  
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp
				AND curr_waitings > 0
		) ,
		ipc_waitings AS
		(
			SELECT 
				curr_timestamp , curr_ipc  AS curr_ipc 
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND curr_ipc > 0 
		) 
		SELECT COALESCE( corr( curr_waitings , curr_ipc ) , 0 ) AS correlation_value 
		INTO corr_ipc
		FROM
			waitings os JOIN ipc_waitings  w ON ( os.curr_timestamp = w.curr_timestamp ) ;
	
		INSERT INTO wait_event_type_corr ( wait_event_type , corr_value )
		VALUES ( 'IPC' , corr_ipc );
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ ОЖИДАНИЯ - ipc 
	----------------------------------------------------------------------------------------------------
	
	----------------------------------------------------------------------------------------------------
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ ОЖИДАНИЯ - lock
		WITH 
		waitings AS
		(
			SELECT 
				curr_timestamp , curr_waitings  AS curr_waitings  
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp
				AND curr_waitings > 0
		) ,
		lock_waitings AS
		(
			SELECT 
				curr_timestamp , curr_lock  AS curr_lock 
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND curr_lock > 0 
		) 
		SELECT COALESCE( corr( curr_waitings , curr_lock ) , 0 ) AS correlation_value 
		INTO corr_lock
		FROM
			waitings os JOIN lock_waitings  w ON ( os.curr_timestamp = w.curr_timestamp ) ;

		INSERT INTO wait_event_type_corr ( wait_event_type , corr_value )
		VALUES ( 'Lock' , corr_lock );			
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ ОЖИДАНИЯ - lock 
	----------------------------------------------------------------------------------------------------
	
	----------------------------------------------------------------------------------------------------
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ ОЖИДАНИЯ - lwlock
		WITH 
		waitings AS
		(
			SELECT 
				curr_timestamp , curr_waitings  AS curr_waitings  
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND curr_waitings > 0				
		) ,
		lwlock_waitings AS
		(
			SELECT 
				curr_timestamp , curr_lwlock  AS curr_lwlock 
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND curr_lwlock > 0 
		) 
		SELECT COALESCE( corr( curr_waitings , curr_lwlock ) , 0 ) AS correlation_value 
		INTO corr_lwlock
		FROM
			waitings os JOIN lwlock_waitings  w ON ( os.curr_timestamp = w.curr_timestamp ) ;
			
		INSERT INTO wait_event_type_corr ( wait_event_type , corr_value )
		VALUES ( 'LWLock' , corr_lwlock );
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ ОЖИДАНИЯ - lwlock 
	----------------------------------------------------------------------------------------------------
	
	----------------------------------------------------------------------------------------------------
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ ОЖИДАНИЯ - timeout
		WITH 
		waitings AS
		(
			SELECT 
				curr_timestamp , curr_waitings  AS curr_waitings  
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND curr_waitings > 0				
		) ,
		timeout_waitings AS
		(
			SELECT 
				curr_timestamp , curr_timeout  AS curr_timeout 
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND curr_timeout > 0 
		) 
		SELECT COALESCE( corr( curr_waitings , curr_timeout ) , 0 ) AS correlation_value 
		INTO corr_timeout
		FROM
			waitings os JOIN timeout_waitings  w ON ( os.curr_timestamp = w.curr_timestamp ) ;
		
		INSERT INTO wait_event_type_corr ( wait_event_type , corr_value )
		VALUES ( 'Timeout' , corr_timeout );
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ ОЖИДАНИЯ - timeout 
	----------------------------------------------------------------------------------------------------
	
	
	-----------------------------------------------------------------------------
	
	
	FOR wait_event_type_rec IN 
	SELECT 	
		wait_event_type 
	FROM 	
		statement_stat_waitings_median
	WHERE 
		curr_timestamp  BETWEEN min_timestamp AND max_timestamp
	GROUP BY 
		wait_event_type 
	ORDER BY 
		wait_event_type 
	LOOP 
		SELECT *
		INTO wait_event_type_corr_rec
		FROM wait_event_type_corr
		WHERE wait_event_type = wait_event_type_rec.wait_event_type ;
		
		IF wait_event_type_corr_rec.corr_value < 0.7 
		THEN 
			CONTINUE;
		END IF ; 
		
	    line_count=line_count+1;
		result_str[line_count] =' WAIT_EVENT_TYPE = '||wait_event_type_rec.wait_event_type||'|';
		line_count=line_count+1;	
		
		
		result_str[line_count] =' QUERYID  '||'|'||	
								' CALLS '||'|' ||
								' WAITINGS '||'|' ||  --Всего ожидания wait_event_type по данному queryid
								' PCT '||'|' ||       --отношение ожиданий wait_event_type по данному queryid к общему количество ожиданий wait_event_type
								' DBNAME ROLENAME '||'|'||
								' WAIT_EVENT LIST '||'|'
								;	
		line_count=line_count+1;
		
		pct_for_80 = 0;
		
		FOR wait_event_rec IN 
		SELECT 	
			queryid , dbname , username ,
			SUM(curr_value_long) AS count 
		FROM 	
			statement_stat_waitings_median
		WHERE 
			curr_timestamp  BETWEEN min_timestamp AND max_timestamp
			AND wait_event_type = wait_event_type_rec.wait_event_type 
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
				AND wait_event_type = wait_event_type_rec.wait_event_type
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
			
			result_str[line_count] =  wait_event_rec.queryid  ||'|'||
									  REPLACE ( TO_CHAR( ROUND( curr_calls::numeric , 0 ) , '000000000000D0000') , '.' , ',' ) ||'|'||
									  wait_event_rec.count  ||'|'||
									  REPLACE ( TO_CHAR( ROUND( (wait_event_rec.count::numeric / total_wait_event_count::numeric *100.0)::numeric , 2 ) , '000000000000D0000') , '.' , ',' ) ||'|'||
									  wait_event_rec.dbname||' '||wait_event_rec.username||'|'
									  ;
									  
			FOR wait_event_list_rec IN 
			SELECT 
				DISTINCT wait_event
			FROM 
				statement_stat_waitings_median
			WHERE 
				curr_timestamp  BETWEEN min_timestamp AND max_timestamp
				AND wait_event_type = wait_event_type_rec.wait_event_type
				AND queryid = wait_event_rec.queryid 
			LOOP
				result_str[line_count] = result_str[line_count] || wait_event_list_rec.wait_event ||' ';
			END LOOP ;
			result_str[line_count] = result_str[line_count] ||'|';
 
			line_count=line_count+1; 
			
			tmp_queryid_index = tmp_queryid_index + 1 ;
			INSERT INTO  tmp_queryid_for_pareto 
			( id , wait_event_type , queryid )
			VALUES 
			( tmp_queryid_index ,  wait_event_type_rec.wait_event_type , wait_event_rec.queryid );
			
			IF pct_for_80 > 80.0 
			THEN 
				EXIT;
			END IF;
			
		END LOOP ;		
		--FOR wait_event_rec IN 
	END LOOP;
	--FOR wait_event_type_rec IN 

return result_str ; 
END
$$ LANGUAGE plpgsql  ;
COMMENT ON FUNCTION report_queryid_for_pareto IS 'Диаграмма Парето по queryid';
-- Диаграмма Парето по queryid
-------------------------------------------------------------------------------


	
--------------------------------------------------------------------------------
-- report_sql_list.sql
-- version 1.0
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
-- reports_cluster_report_4graph.sql
-- version 1.0
--------------------------------------------------------------------------------
-- Данные для построения графиков по производительности и ожиданиям  СУБД
--
-- reports_cluster_report_4graph Данные для построения графиков по производительности и ожиданиям  СУБД
--
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Данные для построения графиков по производительности и ожиданиям  СУБД
CREATE OR REPLACE FUNCTION reports_cluster_report_4graph(  cluster_performance_start_timestamp text , cluster_performance_finish_timestamp text   ) RETURNS text[] AS $$
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


	
	result_str[line_count] = 'ПРОИЗВОДИТЕЛЬНОСТЬ И ОЖИДАНИЯ СУБД' ; 
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
COMMENT ON FUNCTION reports_cluster_report_4graph IS 'Данные для построения графиков по производительности и ожиданиям  СУБД';
-- Данные для построения графиков по производительности и ожиданиям  СУБД
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- reports_cluster_report_meta.sql
-- version 1.0
--------------------------------------------------------------------------------
-- Метаданные для отчета по производительности и ожиданиям на уровне СУБД
--
-- reports_cluster_report_meta Метаданные для отчета по производительности и ожиданиям на уровне СУБД
--
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Отчет по производительности и ожиданиям на уровне СУБД
CREATE OR REPLACE FUNCTION reports_cluster_report_meta(  cluster_performance_start_timestamp text , cluster_performance_finish_timestamp text   ) RETURNS text[] AS $$
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
  current_test_id integer ;
  current_load_rec record ; 
BEGIN
	line_count = 1 ;
	
	SELECT load_test_get_current_test_id()
	INTO current_test_id; 
	
	
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


	
	result_str[line_count] = 'ПРОИЗВОДИТЕЛЬНОСТЬ И ОЖИДАНИЯ СУБД' ; 
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


--------------------------------------
-- Cтандартизация z-score
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
			t.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
		),
		standardized_data AS 
		(
			SELECT 
				(t.curr_timepoint::DOUBLE PRECISION - avg1) / std1 as x_z,
				(s.curr_op_speed::DOUBLE PRECISION - avg2) / std2 as y_z
			FROM
				cluster_stat_median s JOIN tmp_timepoints t ON ( s.curr_timestamp  = t.curr_timestamp ) , stats
			WHERE 
				t.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
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
-- 	линия регрессии  скорости  : Y = a + bX

-- 	линия регрессии  ожиданий  : Y = a + bX
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
			t.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
		),
		standardized_data AS 
		(
			SELECT 
				(t.curr_timepoint::DOUBLE PRECISION - avg1) / std1 as x_z,
				(s.curr_waitings::DOUBLE PRECISION - avg2) / std2 as y_z
			FROM
				cluster_stat_median s JOIN tmp_timepoints t ON ( s.curr_timestamp  = t.curr_timestamp ) , stats
			WHERE 
				t.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
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
-- 	линия регрессии  ожиданий  : Y = a + bX

-- Cтандартизация z-score
--------------------------------------
		
	result_str[line_count] = 'ЛИНИЯ РЕГРЕССИИ: Y = a + bX ' ; 
	line_count=line_count+1; 
	result_str[line_count] = 'ОПЕРАЦИОННАЯ СКОРОСТЬ' ; 
	line_count=line_count+1; 
	result_str[line_count] = 'Коэффициент детерминации R^2 ' ||'|'|| REPLACE ( TO_CHAR( ROUND( speed_regr_rec.r_squared::numeric , 2 ) , '000000000000D0000') , '.' , ',' ) ;
	line_count=line_count+1; 	
	result_str[line_count] = 'угол наклона ' ||'|'|| REPLACE ( TO_CHAR( ROUND( speed_regr_rec.slope_angle_degrees::numeric , 2 ) , '000000000000D0000') , '.' , ',' ) ;
	line_count=line_count+1; 
	line_count=line_count+1; 
	
	
	result_str[line_count] = 'ОЖИДАНИЯ СУБД' ; 
	line_count=line_count+1; 
	result_str[line_count] = 'Коэффициент детерминации R^2 ' ||'|'|| REPLACE ( TO_CHAR( ROUND( waitings_regr_rec.r_squared::numeric , 2 ) , '000000000000D0000') , '.' , ',' ) ;
	line_count=line_count+1; 	
	result_str[line_count] = 'угол наклона ' ||'|'|| REPLACE ( TO_CHAR( ROUND( waitings_regr_rec.slope_angle_degrees::numeric , 2 ) , '000000000000D0000') , '.' , ',' ) ;
	line_count=line_count+1; 
	line_count=line_count+1; 

	

	----------------------------------------------------------------------------------------------------
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ СКОРОСТЬ - ОЖИДАНИЯ		
		WITH 
		operating_speed AS
		(
			SELECT 
				curr_timestamp , curr_op_speed  AS operating_speed_long  
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp 						
		) ,
		waitings AS
		(
			SELECT 
				curr_timestamp , curr_waitings  AS curr_curr_waitings 
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND curr_waitings > 0 
		) 
		SELECT COALESCE( corr( operating_speed_long , curr_curr_waitings ) , 0 ) AS correlation_value 
		INTO speed_waitings_correlation
		FROM
			operating_speed os JOIN waitings  w ON ( os.curr_timestamp = w.curr_timestamp ) ;	
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ СКОРОСТЬ - ОЖИДАНИЯ 
	----------------------------------------------------------------------------------------------------
	
	----------------------------------------------------------------------------------------------------
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ ОЖИДАНИЯ - bufferpin
		WITH 
		waitings AS
		(
			SELECT 
				curr_timestamp , curr_waitings  AS curr_waitings  
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp
				AND curr_waitings > 0
		) ,
		bufferpin_waitings AS
		(
			SELECT 
				curr_timestamp , curr_bufferpin  AS curr_bufferpin 
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND curr_bufferpin > 0 
		) 
		SELECT COALESCE( corr( curr_waitings , curr_bufferpin ) , 0 ) AS correlation_value 
		INTO corr_bufferpin
		FROM
			waitings os JOIN bufferpin_waitings  w ON ( os.curr_timestamp = w.curr_timestamp ) ;	
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ ОЖИДАНИЯ - bufferpin 
	----------------------------------------------------------------------------------------------------
	
	----------------------------------------------------------------------------------------------------
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ ОЖИДАНИЯ - extension
		WITH 
		waitings AS
		(
			SELECT 
				curr_timestamp , curr_waitings  AS curr_waitings  
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp
				AND curr_waitings > 0
		) ,
		extension_waitings AS
		(
			SELECT 
				curr_timestamp , curr_extension  AS curr_extension 
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND curr_extension > 0 
		) 
		SELECT COALESCE( corr( curr_waitings , curr_extension ) , 0 ) AS correlation_value 
		INTO corr_extension
		FROM
			waitings os JOIN extension_waitings  w ON ( os.curr_timestamp = w.curr_timestamp ) ;	
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ ОЖИДАНИЯ - extension 
	----------------------------------------------------------------------------------------------------
	
	----------------------------------------------------------------------------------------------------
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ ОЖИДАНИЯ - io
		WITH 
		waitings AS
		(
			SELECT 
				curr_timestamp , curr_waitings  AS curr_waitings  
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp
				AND curr_waitings > 0
		) ,
		io_waitings AS
		(
			SELECT 
				curr_timestamp , curr_io  AS curr_io 
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND curr_io > 0 
		) 
		SELECT COALESCE( corr( curr_waitings , curr_io ) , 0 ) AS correlation_value 
		INTO corr_io
		FROM
			waitings os JOIN io_waitings  w ON ( os.curr_timestamp = w.curr_timestamp ) ;	
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ ОЖИДАНИЯ - io 
	----------------------------------------------------------------------------------------------------
	
	----------------------------------------------------------------------------------------------------
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ ОЖИДАНИЯ - ipc
		WITH 
		waitings AS
		(
			SELECT 
				curr_timestamp , curr_waitings  AS curr_waitings  
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp
				AND curr_waitings > 0
		) ,
		ipc_waitings AS
		(
			SELECT 
				curr_timestamp , curr_ipc  AS curr_ipc 
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND curr_ipc > 0 
		) 
		SELECT COALESCE( corr( curr_waitings , curr_ipc ) , 0 ) AS correlation_value 
		INTO corr_ipc
		FROM
			waitings os JOIN ipc_waitings  w ON ( os.curr_timestamp = w.curr_timestamp ) ;	
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ ОЖИДАНИЯ - ipc 
	----------------------------------------------------------------------------------------------------
	
	----------------------------------------------------------------------------------------------------
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ ОЖИДАНИЯ - lock
		WITH 
		waitings AS
		(
			SELECT 
				curr_timestamp , curr_waitings  AS curr_waitings  
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp
				AND curr_waitings > 0
		) ,
		lock_waitings AS
		(
			SELECT 
				curr_timestamp , curr_lock  AS curr_lock 
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND curr_lock > 0 
		) 
		SELECT COALESCE( corr( curr_waitings , curr_lock ) , 0 ) AS correlation_value 
		INTO corr_lock
		FROM
			waitings os JOIN lock_waitings  w ON ( os.curr_timestamp = w.curr_timestamp ) ;	
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ ОЖИДАНИЯ - lock 
	----------------------------------------------------------------------------------------------------
	
	----------------------------------------------------------------------------------------------------
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ ОЖИДАНИЯ - lwlock
		WITH 
		waitings AS
		(
			SELECT 
				curr_timestamp , curr_waitings  AS curr_waitings  
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND curr_waitings > 0				
		) ,
		lwlock_waitings AS
		(
			SELECT 
				curr_timestamp , curr_lwlock  AS curr_lwlock 
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND curr_lwlock > 0 
		) 
		SELECT COALESCE( corr( curr_waitings , curr_lwlock ) , 0 ) AS correlation_value 
		INTO corr_lwlock
		FROM
			waitings os JOIN lwlock_waitings  w ON ( os.curr_timestamp = w.curr_timestamp ) ;	
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ ОЖИДАНИЯ - lwlock 
	----------------------------------------------------------------------------------------------------
	
	----------------------------------------------------------------------------------------------------
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ ОЖИДАНИЯ - timeout
		WITH 
		waitings AS
		(
			SELECT 
				curr_timestamp , curr_waitings  AS curr_waitings  
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND curr_waitings > 0				
		) ,
		timeout_waitings AS
		(
			SELECT 
				curr_timestamp , curr_timeout  AS curr_timeout 
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND curr_timeout > 0 
		) 
		SELECT COALESCE( corr( curr_waitings , curr_timeout ) , 0 ) AS correlation_value 
		INTO corr_timeout
		FROM
			waitings os JOIN timeout_waitings  w ON ( os.curr_timestamp = w.curr_timestamp ) ;	
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ ОЖИДАНИЯ - timeout 
	----------------------------------------------------------------------------------------------------
	
	result_str[line_count] = 'КОЭФФИЦИЕНТЫ КОРРЕЛЯЦИИ' ; 
	line_count=line_count+1; 
	
	result_str[line_count] = 'SPEED - WAITINGS' ||'|'|| REPLACE ( TO_CHAR( ROUND( speed_waitings_correlation::numeric , 2 ) , '000000000000D0000') , '.' , ',' ) ;
	line_count=line_count+1; 
	result_str[line_count] = 'WAITINGS - BUFFERPIN' ||'|'|| REPLACE ( TO_CHAR( ROUND( corr_bufferpin::numeric , 2 ) , '000000000000D0000') , '.' , ',' ) ;
	line_count=line_count+1; 
	result_str[line_count] = 'WAITINGS - EXTENSION' ||'|'|| REPLACE ( TO_CHAR( ROUND( corr_extension::numeric , 2 ) , '000000000000D0000') , '.' , ',' ) ;
	line_count=line_count+1; 
	result_str[line_count] = 'WAITINGS - IO' ||'|'|| REPLACE ( TO_CHAR( ROUND( corr_io::numeric , 2 ) , '000000000000D0000') , '.' , ',' ) ;
	line_count=line_count+1; 
	result_str[line_count] = 'WAITINGS - IPC' ||'|'|| REPLACE ( TO_CHAR( ROUND( corr_ipc::numeric , 2 ) , '000000000000D0000') , '.' , ',' ) ;
	line_count=line_count+1; 
	result_str[line_count] = 'WAITINGS - LOCK' ||'|'|| REPLACE ( TO_CHAR( ROUND( corr_lock::numeric , 2 ) , '000000000000D0000') , '.' , ',' ) ;
	line_count=line_count+1; 
	result_str[line_count] = 'WAITINGS - LWLOCK' ||'|'|| REPLACE ( TO_CHAR( ROUND( corr_lwlock::numeric , 2 ) , '000000000000D0000') , '.' , ',' ) ;
	line_count=line_count+1; 
	result_str[line_count] = 'WAITINGS - TIMEOUT' ||'|'|| REPLACE ( TO_CHAR( ROUND( corr_timeout::numeric , 2 ) , '000000000000D0000') , '.' , ',' ) ;
	line_count=line_count+2; 
	
	
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
		

	result_str[line_count] = 	' '||'|'||
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

  return result_str ; 
END
$$ LANGUAGE plpgsql  ;
COMMENT ON FUNCTION reports_cluster_report_meta IS 'Метаданные для отчета по производительности и ожиданиям на уровне СУБД';
-- Метаданные для отчета по производительности и ожиданиям на уровне СУБД
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- reports_iostat_device_4graph.sql
-- version 1.0
--------------------------------------------------------------------------------
--
-- reports_iostat_device_4graph Данные для графиков по IOSTAT
--
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Данные для графиков по IOSTAT
CREATE OR REPLACE FUNCTION reports_iostat_device_4graph(  start_timestamp text , finish_timestamp text  , device_name text  ) RETURNS text[] AS $$
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


	
	result_str[line_count] = 'Метаданные IOSTAT' ; 
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
COMMENT ON FUNCTION reports_iostat_device_4graph IS 'Данные для графиков по IOSTAT';
-- Данные для графиков по IOSTAT
-------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- reports_iostat_device_meta.sql
-- version 1.0
--------------------------------------------------------------------------------
--
-- reports_iostat_device_meta Метаданные IOSTAT
--
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Метаданные IOSTAT
CREATE OR REPLACE FUNCTION reports_iostat_device_meta(  start_timestamp text , finish_timestamp text  , device_name text  ) RETURNS text[] AS $$
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
	
	

	result_str[line_count] = 	' '||'|'||
								'№'||'|'||		
								'r/s' ||'|'||
								'rMB/s' ||'|'||
								'rrqm/s' ||'|'||
								'%rrqm' ||'|'||
								'r_await' ||'|'||
								'rareq_sz' ||'|'||
								'w/s' ||'|'||
								'wMB/s' ||'|'||
								'wrqm/s' ||'|'||
								'%wrqm' ||'|'||
								'w_await' ||'|'||
								'wareq_sz' ||'|'||
								'd/s' ||'|'||
								'dMB/s' ||'|'||
								'drqm/s' ||'|'||
								'%drqm' ||'|'||
								'd_await' ||'|'||
								'dareq_sz' ||'|'||
								'aqu_sz' ||'|'||
								'%util' ||'|'||
								'f/s' ||'|'||
								'f_await' ||'|'
								;							
	line_count=line_count+1; 
	
	result_str[line_count] = 	'MIN'||'|'||
								1 ||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_dev_rps_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_dev_rmbs_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_dev_rrqmps_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_dev_rrqm_pct_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_dev_r_await_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_dev_rareq_sz_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_dev_wps_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_dev_wmbps_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_dev_wrqmps_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_dev_wrqm_pct_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_dev_w_await_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_dev_wareq_sz_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_dev_dps_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_dev_dmbps_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_dev_drqmps_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_dev_drqm_pct_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_dev_d_await_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_dev_dareq_sz_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_dev_aqu_sz_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_dev_util_pct_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_dev_fps_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.min_dev_f_await_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'
								;							
	line_count=line_count+1; 	
	
	SELECT 
		count(curr_timestamp)
	INTO line_counter
	FROM 
		os_stat_iostat_device_median
	WHERE 	
		curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
	
	result_str[line_count] = 	'MAX'||'|'||
								line_counter||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_dev_rps_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_dev_rmbs_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_dev_rrqmps_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_dev_rrqm_pct_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_dev_r_await_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_dev_rareq_sz_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_dev_wps_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_dev_wmbps_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_dev_wrqmps_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_dev_wrqm_pct_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_dev_w_await_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_dev_wareq_sz_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_dev_dps_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_dev_dmbps_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_dev_drqmps_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_dev_drqm_pct_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_dev_d_await_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_dev_dareq_sz_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_dev_aqu_sz_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_dev_util_pct_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_dev_fps_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'||
								REPLACE ( TO_CHAR( ROUND( min_max_rec.max_dev_f_await_long::numeric , 0 ) , '000000000000D0000') , '.' , ',' )||'|'
								;							
	

return result_str ; 	
END
$$ LANGUAGE plpgsql  ;
COMMENT ON FUNCTION reports_iostat_device_meta IS 'Метаданные IOSTAT';
-- Метаданные IOSTAT
-------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- reports_load_test.sql
-- version 1.0
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Отчет по нагрузочному тестированию
--
-- reports_load_test() Отчет по нагрузочному тестированию
--
------------------------------------------------------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Сформировать отчет по нагрузочному тестированию
CREATE OR REPLACE FUNCTION reports_load_test() RETURNS text[] AS $$
DECLARE
 result_str text[] ;
 line_count integer ;

 current_test_id bigint;
 load_test_pass_rec record ;
 
 current_speed_short DOUBLE PRECISION;
 current_speed_long DOUBLE PRECISION;
 current_median_short DOUBLE PRECISION;
 current_median_long DOUBLE PRECISION;
 
 scenario_queryid_rec record ;
 
 min_max_rec record ; 
BEGIN
    line_count = 1 ;	
	
	result_str[line_count] = 'ОТЧЕТ ПО РЕЗУЛЬТАТАМ НАГРУЗОЧНОГО ТЕСТИРОВАНИЯ ' ; 
	line_count=line_count+2; 
	
	------------------------------------------------------
	-- ТЕСТОВЫЕ ЗАПРОСЫ
	SELECT load_test_get_current_test_id()
	INTO current_test_id; 		

	SELECT 
	  scenario_1_queryid , scenario_2_queryid  , scenario_3_queryid  
	INTO 
	  scenario_queryid_rec 
	FROM 
	  load_test 
	WHERE 
	   test_id = current_test_id;
	
	result_str[line_count] = 'СЦЕНАРИЙ-1: SELECT ONLY '; 
	line_count=line_count+1; 
	result_str[line_count] = scenario_queryid_rec.scenario_1_queryid ; 
	line_count=line_count+2; 

	result_str[line_count] = 'СЦЕНАРИЙ-2: SELECT+UPDATE'; 
	line_count=line_count+1; 
	result_str[line_count] = scenario_queryid_rec.scenario_2_queryid ; 
	line_count=line_count+2; 

	
	result_str[line_count] = 'СЦЕНАРИЙ-3: INSERT ONLY'; 
	line_count=line_count+1; 
	result_str[line_count] = scenario_queryid_rec.scenario_3_queryid ; 
	line_count=line_count+1; 	
	-- ТЕСТОВЫЕ ЗАПРОСЫ
	------------------------------------------------------  

	SELECT 
	  MIN(c.curr_op_speed) , 
	  MAX(c.curr_op_speed)  
	INTO 
		min_max_rec 
	FROM cluster_stat_median c
	WHERE 
	curr_timestamp between 
	(SELECT MIN(p.start_timestamp) 
	 FROM   load_test_pass p
	 WHERE  p.test_id = 1 AND
	 p.pass_counter >= 6) 
	AND 
	(SELECT MAX(p.finish_timestamp) 
	 FROM   load_test_pass p
	 WHERE  p.test_id = current_test_id AND
	 p.pass_counter >= 6) 
	;

    line_count=line_count+1;
	result_str[line_count] = '| MIN SPEED |' || REPLACE ( TO_CHAR( ROUND( (min_max_rec.min::numeric)::numeric , 4 ) , '000000000000D0000') , '.' , ',' ) ||'|'; 
	line_count=line_count+1; 
	result_str[line_count] = '| MIN SPEED |' || REPLACE ( TO_CHAR( ROUND( (min_max_rec.max::numeric)::numeric , 4 ) , '000000000000D0000') , '.' , ',' ) ||'|'; 
	line_count=line_count+2; 

	result_str[line_count] = 	'start timestamp'||'|'||
								'finish timestamp'||'|'||
								'PASS'||'|'||
								'LOAD'||'|'||								
								'open'||'|'||
								'high'||'|'||
								'low'||'|'||
								'close'||'|'								
								;							
	line_count=line_count+1; 

	
	FOR load_test_pass_rec IN 
	SELECT 		
		p.pass_counter , 
		p.load_connections ,
		p.start_timestamp ,
		p.finish_timestamp , 
		(SELECT c.curr_op_speed 
		FROM cluster_stat_median c
		WHERE 
		curr_timestamp =  
		 ( SELECT MIN(pc.curr_timestamp) 
		   FROM cluster_stat_median pc
		   WHERE pc.curr_timestamp between p.start_timestamp AND p.finish_timestamp         
		 )   
		) AS "open" ,
		(SELECT MAX(c.curr_op_speed) 
		FROM cluster_stat_median c
		WHERE 
		curr_timestamp between p.start_timestamp AND p.finish_timestamp
		) AS "high" ,
		(SELECT MIN(c.curr_op_speed) 
		FROM cluster_stat_median c
		WHERE 
		curr_timestamp between p.start_timestamp AND p.finish_timestamp
		) AS "low" ,
		(SELECT c.curr_op_speed 
		FROM cluster_stat_median c
		WHERE 
		curr_timestamp =  
		 ( SELECT MAX(pc.curr_timestamp) 
		   FROM cluster_stat_median pc
		   WHERE pc.curr_timestamp between p.start_timestamp AND p.finish_timestamp         
		 )   
		) AS "close" 
		FROM   load_test_pass p
		WHERE  p.test_id = current_test_id AND
		p.pass_counter >= 6
		order by 1  
	LOOP
		  
		result_str[line_count] = 	to_char( load_test_pass_rec.start_timestamp , 'YYYY-MM-DD HH24:MI') ||'|'||
									to_char( load_test_pass_rec.finish_timestamp , 'YYYY-MM-DD HH24:MI') ||'|'||
									load_test_pass_rec.pass_counter ||'|'||
									ROUND( load_test_pass_rec.load_connections::numeric , 0 ) ||'|'||
									REPLACE ( TO_CHAR( ROUND( (load_test_pass_rec.open::numeric)::numeric , 0 ) , '000000000000D0000') , '.' , ',' ) ||'|'||
									REPLACE ( TO_CHAR( ROUND( (load_test_pass_rec.high::numeric)::numeric , 0 ) , '000000000000D0000') , '.' , ',' ) ||'|'||
									REPLACE ( TO_CHAR( ROUND( (load_test_pass_rec.low::numeric)::numeric , 0 ) , '000000000000D0000') , '.' , ',' ) ||'|'||
									REPLACE ( TO_CHAR( ROUND( (load_test_pass_rec.close::numeric)::numeric , 0 ) , '000000000000D0000') , '.' , ',' ) ||'|'
								;	
		
	  line_count=line_count+1; 	

	
	END LOOP ;		
	
  return result_str ; 
END
$$ LANGUAGE plpgsql STABLE ;
COMMENT ON FUNCTION reports_load_test IS 'Сформировать отчет по нагрузочному тестированию';
--Сформировать отчет по нагрузочному тестированию
------------------------------------------------------------------------------------------------------------------------------------------------------------------------


------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- reports_load_test_loading.sql
-- version 1.0
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- График изменения нагрузки в ходе нагрузочного тестирования
--
-- reports_load_test_loading() Отчет по нагрузочному тестированию
--
------------------------------------------------------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- График изменения нагрузки в ходе нагрузочного тестирования
CREATE OR REPLACE FUNCTION reports_load_test_loading() RETURNS text[] AS $$
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
COMMENT ON FUNCTION reports_load_test_loading IS ' График изменения нагрузки в ходе нагрузочного тестирования';
-- График изменения нагрузки в ходе нагрузочного тестирования
------------------------------------------------------------------------------------------------------------------------------------------------------------------------


--------------------------------------------------------------------------------
-- reports_queryid_stat.sql
-- version 1.0.1
--------------------------------------------------------------------------------
-- Статистика по отдельному SQL запросу
--
-- reports_queryid_stat История выполения и ожиданий по отдельному SQL запросу
--
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--История выполения и ожиданий по отдельному SQL запросу
CREATE OR REPLACE FUNCTION reports_queryid_stat(  current_queryid bigint  , current_wait_event_type text , start_timestamp text , finish_timestamp text  ) RETURNS text[] AS $$
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
COMMENT ON FUNCTION reports_queryid_stat IS 'История выполения и ожиданий по отдельному SQL запросу';
--История выполения и ожиданий по отдельному SQL запросу
--------------------------------------------------------------------------------
	
	
--------------------------------------------------------------------------------
-- reports_vmstat_4graph.sql
-- version 1.0
--------------------------------------------------------------------------------
--
-- reports_vmstat_4graph Данные для графиков по VMSTAT
--
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Данные для графиков по VMSTAT
CREATE OR REPLACE FUNCTION reports_vmstat_4graph(  start_timestamp text , finish_timestamp text   ) RETURNS text[] AS $$
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
COMMENT ON FUNCTION reports_vmstat_4graph IS 'Данные для графиков по VMSTAT';
-- Данные для графиков по VMSTAT
-------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- reports_vmstat_cpu.sql
-- version 1.0
--------------------------------------------------------------------------------
--
-- reports_vmstat_cpu Чек-лист CPU
--
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Чек-лист CPU
CREATE OR REPLACE FUNCTION reports_vmstat_cpu(  cpu_count integer , start_timestamp text , finish_timestamp text   ) RETURNS text[] AS $$
DECLARE
 result_str text[] ;
 line_count integer ;
 min_timestamp timestamptz ; 
 max_timestamp timestamptz ; 
 
 counter integer ; 
 min_max_rec record ;
 line_counter integer ;  
 	
  
  r_pct DOUBLE PRECISION;
  cs_pct DOUBLE PRECISION;
  sy_pct DOUBLE PRECISION;
  
  corr_cs_in DOUBLE PRECISION ; -- Корреляция cs-in
  corr_cs_us DOUBLE PRECISION ; -- Корреляция cs-us
  corr_cs_sy DOUBLE PRECISION ; -- Корреляция cs-sy
  
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


	
	result_str[line_count] = 'ЧЕК-ЛИСТ CPU' ; 
	line_count=line_count+1;
	
	result_str[line_count] = 'CPU | '||cpu_count||'|' ; 
	line_count=line_count+2;
	
	
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
	os_stat_vmstat_median cl
	WHERE 
		cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp  
	ORDER BY curr_timestamp	;

	SELECT 
		MIN( cl.procs_r_long) AS min_procs_r_long , MAX( cl.procs_r_long) AS max_procs_r_long , 
		MIN( cl.system_cs_long) AS min_system_cs_long , MAX( cl.system_cs_long) AS max_system_cs_long ,		
		MIN( cl.system_in_long) AS min_system_in_long , MAX( cl.system_in_long) AS max_system_in_long ,
		MIN( cl.cpu_us_long) AS min_cpu_us_long , MAX( cl.cpu_us_long) AS max_cpu_us_long ,
		MIN( cl.cpu_sy_long) AS min_cpu_sy_long , MAX( cl.cpu_sy_long) AS max_cpu_sy_long
	INTO  	min_max_rec
	FROM 
		os_stat_vmstat_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
	WHERE 	
		cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 	;
		
    	
	SELECT 
		count(curr_timestamp)
	INTO line_counter
	FROM 
		cluster_stat_median cl
	WHERE 	
		cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
		
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
		result_str[line_count] = 'OK: менее 25% наблюдений - очередь процессов превышает количество ядер CPU' ; 
		line_count=line_count+1;
	ELSIF r_pct > 25.0 AND r_pct <= 50.0
	THEN 
		result_str[line_count] = 'WARNING: 25-50% наблюдений - очередь процессов превышает количество ядер CPU' ; 
		line_count=line_count+1;
	ELSE 
		result_str[line_count] = 'ALARM: более 50% наблюдений - очередь процессов превышает количество ядер CPU' ; 
		line_count=line_count+1;
	END IF ;	
	--r — процессы в run queue (готовы к выполнению)(% превышение CPU)
	-----------------------------------------------------------------------------

	
	----------------------------------------------------------------------------
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
		result_str[line_count] = 'OK: менее 25% наблюдений - доля system time  превышает 30%' ; 
		line_count=line_count+1;
	ELSIF sy_pct > 25.0 AND sy_pct <= 50.0
	THEN 
		result_str[line_count] = 'WARNING: 25-50% наблюдений - доля system time превышает 30%' ; 
		line_count=line_count+1;
	ELSE 
		result_str[line_count] = 'ALARM: более 50% наблюдений - доля system time превышает 30%' ; 
		line_count=line_count+1;
	END IF ;
	-- sy — system time(% превышение 30%)
	----------------------------------------------------------------------------
	----------------------------------------------------------------------------
	-- КОРРЕЛЯЦИЯ system_cs system_in	
	IF min_max_rec.min_system_cs_long != min_max_rec.max_system_cs_long AND 
	   min_max_rec.min_system_in_long != min_max_rec.max_system_in_long
	THEN	
		WITH 
		system_cs_values AS
		(
			SELECT 
				cl.curr_timestamp , system_cs_long  AS system_cs_long 
			FROM 
				os_stat_vmstat_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
			WHERE				
				cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND system_cs_long > 0 
		) ,
		system_in_values AS
		(
			SELECT 
				cl.curr_timestamp , system_in_long  AS system_in_long 
			FROM 
				os_stat_vmstat_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
			WHERE				
				cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND system_in_long > 0 
		) 
		SELECT COALESCE( corr( v1.system_cs_long , v2.system_in_long ) , 0 ) AS correlation_value 
		INTO corr_cs_in
		FROM
			system_cs_values v1 JOIN system_in_values v2 ON ( v1.curr_timestamp = v2.curr_timestamp ) ;
	ELSE 
		corr_cs_in = 0 ;
	END IF;

    result_str[line_count] = 'Корреляция переключений контекста и прерываний(cs - in) | ' ||
	REPLACE ( TO_CHAR( ROUND( corr_cs_in::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' ); 
	line_count=line_count+1;	

	IF corr_cs_in <= 0 
	THEN 
	    result_str[line_count] = 'OK: Корреляция (cs - in) - отрицательная или отсутствует' ; 
		line_count=line_count+1;	
	ELSIF corr_cs_in > 0.7 
	THEN 
		result_str[line_count] = 'ALARM : Очень высокая корреляция (cs - in) - переключения контекста могут быть вызваны прерываниями.' ; 
		line_count=line_count+1;
	ELSIF corr_cs_in > 0.5 AND corr_cs_in <= 0.7
	THEN 
		result_str[line_count] = 'WARNING : Высокая корреляция (cs - in) - переключения контекста могут быть вызваны прерываниями.' ; 
		line_count=line_count+1;
	ELSE
		result_str[line_count] = 'INFO : Слабая или средняя корреляция (cs - in) - переключения контекста могут быть вызваны прерываниями.' ; 
		line_count=line_count+1;
	
	END IF;
	-- КОРРЕЛЯЦИЯ system_cs system_in	
	----------------------------------------------------------------------------

	----------------------------------------------------------------------------
	-- КОРРЕЛЯЦИЯ system_cs cpu_us	
	IF min_max_rec.min_system_cs_long != min_max_rec.max_system_cs_long AND 
	   min_max_rec.min_cpu_us_long != min_max_rec.max_cpu_us_long
	THEN 
	WITH 
		system_cs_values AS
		(
			SELECT 
				cl.curr_timestamp , system_cs_long  AS system_cs_long 
			FROM 
				os_stat_vmstat_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
			WHERE				
				cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND system_cs_long > 0 
		) ,
		cpu_us_values AS
		(
			SELECT 
				cl.curr_timestamp , cpu_us_long  AS cpu_us_long 
			FROM 
				os_stat_vmstat_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
			WHERE				
				cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND cpu_us_long > 0 
		) 
		SELECT COALESCE( corr( v1.system_cs_long , v2.cpu_us_long ) , 0 ) AS correlation_value 
		INTO corr_cs_us
		FROM
			system_cs_values v1 JOIN cpu_us_values v2 ON ( v1.curr_timestamp = v2.curr_timestamp ) ;
	ELSE
		corr_cs_us = 0 ;
	END IF ;

	line_count=line_count+1;
	result_str[line_count] = 'Корреляция переключений контекста и user time(cs - us) | ' ||
	REPLACE ( TO_CHAR( ROUND( corr_cs_us::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' ); 
	line_count=line_count+1;	


	IF corr_cs_us <= 0 
	THEN 
	    result_str[line_count] = 'OK: Корреляция (cs - us) - отрицательная или отсутствует' ; 
		line_count=line_count+1;	
	ELSIF corr_cs_us > 0.7 
	THEN 
		result_str[line_count] = 'ALARM : Очень высокая корреляция (cs - us) - возможно проблема в пользовательском приложении(resource contention).' ; 
		line_count=line_count+1;
	ELSIF corr_cs_us > 0.5 AND corr_cs_us <= 0.7
	THEN 
		result_str[line_count] = 'WARNING : Высокая корреляция (cs - us) - возможно проблема в пользовательском приложении(resource contention).' ; 
		line_count=line_count+1;
	ELSE
		result_str[line_count] = 'INFO : Слабая или средняя (cs - us) - возможно проблема в пользовательском приложении(resource contention).' ; 
		line_count=line_count+1;
	
	END IF;
	-- КОРРЕЛЯЦИЯ system_cs cpu_us	
	----------------------------------------------------------------------------

	----------------------------------------------------------------------------
	-- КОРРЕЛЯЦИЯ system_cs system_sy
	IF min_max_rec.min_system_cs_long != min_max_rec.max_system_cs_long AND 
	   min_max_rec.min_cpu_sy_long != min_max_rec.max_cpu_sy_long
	THEN 
	WITH 
		system_cs_values AS
		(
			SELECT 
				cl.curr_timestamp , system_cs_long  AS system_cs_long 
			FROM 
				os_stat_vmstat_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
			WHERE				
				cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND system_cs_long > 0 
		) ,
		system_sy_values AS
		(
			SELECT 
				cl.curr_timestamp , cpu_sy_long  AS cpu_sy_long 
			FROM 
				os_stat_vmstat_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
			WHERE				
				cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND cpu_sy_long > 0 
		) 
		SELECT COALESCE( corr( v1.system_cs_long , v2.cpu_sy_long ) , 0 ) AS correlation_value 
		INTO corr_cs_sy
		FROM
			system_cs_values v1 JOIN system_sy_values v2 ON ( v1.curr_timestamp = v2.curr_timestamp ) ;
	ELSE 
		corr_cs_sy = 0 ;
	END IF;
			
	line_count=line_count+1;
	result_str[line_count] = 'Корреляция переключений контекста и system time(cs - sy) | ' ||
	REPLACE ( TO_CHAR( ROUND( corr_cs_sy::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' ); 
	line_count=line_count+1;		

	IF corr_cs_sy <= 0 
	THEN 
	    result_str[line_count] = 'OK: Корреляция (cs - sy) - отрицательная или отсутствует' ; 
		line_count=line_count+1;	
	ELSIF corr_cs_sy > 0.7 
	THEN 
		result_str[line_count] = 'ALARM : Очень высокая корреляция (cs - sy) - ядро тратит много времени на переключение контекста и планирование, вместо полезной работы.' ; 
		line_count=line_count+1;
	ELSIF corr_cs_sy > 0.5 AND corr_cs_sy <= 0.7
	THEN 
		result_str[line_count] = 'WARNING : Высокая корреляция(cs - sy) - ядро тратит много времени на переключение контекста и планирование, вместо полезной работы.' ; 
		line_count=line_count+1;
	ELSE
		result_str[line_count] = 'INFO : Слабая или средняя корреляция (cs - sy) - ядро тратит много времени на переключение контекста и планирование, вместо полезной работы.' ; 
		line_count=line_count+1;
	
	END IF;
	-- КОРРЕЛЯЦИЯ system_cs system_sy	
	----------------------------------------------------------------------------

  return result_str ; 
END
$$ LANGUAGE plpgsql  ;
COMMENT ON FUNCTION reports_vmstat_cpu IS 'Чек-лист CPU';
-- Чек-лист CPU
---------------------------------------------------------------------------------------------------------------------------------------------------------------
-- reports_vmstat_io.sql
-- version 1.0
--------------------------------------------------------------------------------
--
-- reports_vmstat_io Чек-лист IO
--
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Чек-лист IO
CREATE OR REPLACE FUNCTION reports_vmstat_io( cpu_count integer , start_timestamp text , finish_timestamp text   ) RETURNS text[] AS $$
DECLARE
 result_str text[] ;
 line_count integer ;
 min_timestamp timestamptz ; 
 max_timestamp timestamptz ; 
 
 counter integer ; 
 min_max_rec record ;
 line_counter integer ; 
  	
  
  b_pct DOUBLE PRECISION;
  wa_pct DOUBLE PRECISION;
  
  b_reg_slope DOUBLE PRECISION; -- угол наклона линии регрессии b — процессы в uninterruptible sleep (обычно ждут IO)
  slope DOUBLE PRECISION; -- угол наклона линии регрессии 
  
  b_regr_rec record ;
  
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


	
	result_str[line_count] = 'ЧЕК-ЛИСТ IO' ; 
	line_count=line_count+1;
		
	result_str[line_count] = 'CPU | '||cpu_count||'|' ;  
	line_count=line_count+2;
	
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
		cl.curr_timestamp , 
		row_number() over (order by cl.curr_timestamp) AS x
	FROM
	os_stat_vmstat_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)	
	WHERE 
		cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp  
	ORDER BY cl.curr_timestamp	;

	SELECT 
		MIN( cl.procs_b_long) AS min_procs_b_long , MAX( cl.procs_b_long) AS max_procs_b_long , 
		MIN( cl.cpu_wa_long) AS min_cpu_wa_long , MAX( cl.cpu_wa_long) AS max_cpu_wa_long 
	INTO  	min_max_rec
	FROM 
		os_stat_vmstat_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
	WHERE 	
		cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 	;
		
    		
	SELECT 
		count(curr_timestamp)
	INTO line_counter
	FROM 
		cluster_stat_median cl
	WHERE 	
		cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
		
	

	----------------------------------------------------------------------------
	-- wa — ожидание IO
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

	result_str[line_count] = 'wa — ожидание IO (% свыше 10%) | '|| REPLACE ( TO_CHAR( ROUND( wa_pct::numeric , 2 ) , '000000000000D0000' ) , '.' , ',' )  ; 
	line_count=line_count+1;
	
	IF wa_pct < 25.0 
	THEN 
		result_str[line_count] = 'OK: менее 25% наблюдений - wa > 10%' ; 
		line_count=line_count+1;
	ELSIF wa_pct > 25.0 AND wa_pct <= 50.0
	THEN 
		result_str[line_count] = 'WARNING: 25-50% наблюдений - wa > 10%' ; 
		line_count=line_count+1;
	ELSE 
		result_str[line_count] = 'ALARM: более 50% наблюдений - wa > 10%' ; 
		line_count=line_count+1;
	END IF ;
	-- wa — ожидание IO
	----------------------------------------------------------------------------
	
	-----------------------------------------------------------------------------
	-- УГОЛ НАКЛОНА ЛИНИИ НАИМЕНЬШИХ КВАДРАТОВ b — процессы в uninterruptible sleep (обычно ждут IO)
	-- 	линия регрессии  скорости  : Y = a + bX
	BEGIN
		WITH stats AS 
		(
		  SELECT 
			AVG(t.curr_timepoint::DOUBLE PRECISION) as avg1, 
			STDDEV(t.curr_timepoint::DOUBLE PRECISION) as std1,
			AVG(s.procs_b_long::DOUBLE PRECISION) as avg2, 
			STDDEV(s.procs_b_long::DOUBLE PRECISION) as std2
		  FROM
			os_stat_vmstat_median s JOIN tmp_timepoints t ON ( s.curr_timestamp  = t.curr_timestamp )
		  WHERE 
			t.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
		),
		standardized_data AS 
		(
			SELECT 
				(t.curr_timepoint::DOUBLE PRECISION - avg1) / std1 as x_z,
				(s.procs_b_long::DOUBLE PRECISION - avg2) / std2 as y_z
			FROM
				os_stat_vmstat_median s JOIN tmp_timepoints t ON ( s.curr_timestamp  = t.curr_timestamp ) , stats
			WHERE 
				t.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
		)	
		SELECT
			REGR_SLOPE(y_z, x_z) as slope, --b
			ATAN(REGR_SLOPE(y_z, x_z)) * 180 / PI() as slope_angle_degrees, --угол наклона
			REGR_R2(y_z, x_z) as r_squared -- Коэффициент детерминации
		INTO 
			b_regr_rec
		FROM standardized_data;
	EXCEPTION
	  --STDDEV(s.curr_op_speed::DOUBLE PRECISION) = 0  
	  WHEN division_by_zero THEN  -- Конкретное исключение для деления на ноль
	    SELECT 
			1.0 as slope, --b
			0.0  as slope_angle_degrees, --угол наклона
			0.0  as r_squared -- Коэффициент детерминации
		INTO 
		b_regr_rec ;
	END;
-- 	линия регрессии  скорости  : Y = a + bX

    line_count=line_count+1;
    result_str[line_count] = 'ЛИНИЯ РЕГРЕССИИ: Y = a + bX ' ; 
	line_count=line_count+1; 
	result_str[line_count] = 'b — процессы в uninterruptible sleep (обычно ждут IO)' ; 
	line_count=line_count+1; 
	result_str[line_count] = 'Коэффициент детерминации R^2 ' ||'|'|| REPLACE ( TO_CHAR( ROUND( b_regr_rec.r_squared::numeric , 2 ) , '000000000000D0000') , '.' , ',' ) ;
	line_count=line_count+1; 	
	result_str[line_count] = 'угол наклона  ' ||'|'|| REPLACE ( TO_CHAR( ROUND( b_regr_rec.slope_angle_degrees::numeric , 2 ) , '000000000000D0000') , '.' , ',' ) ;
	line_count=line_count+1; 	
	
/*	
	Хотя не существует строгих порогов, часто используют следующую качественную шкалу:

	0.8 ≤ R² ≤ 1.0: Очень сильная объясняющая способность модели.

	0.6 ≤ R² < 0.8: Сильная объясняющая способность.

	0.4 ≤ R² < 0.6: Умеренная объясняющая способность.

	0.2 ≤ R² < 0.4: Слабая объясняющая способность.

	0.0 ≤ R² < 0.2: Объясняющая способность отсутствует или крайне мала.	
*/	
	IF ROUND( b_regr_rec.slope_angle_degrees::numeric , 2 ) = 0 OR ROUND( b_regr_rec.r_squared::numeric , 2 ) < 0.6 
	THEN 
		result_str[line_count] = 'OK: количество процессов в uninterruptible sleep - не возрастает, либо незначительно' ; 
		line_count=line_count+1;
	ELSIF wa_pct < 25.0
	THEN 
		result_str[line_count] = 'WARNING: количество процессов в uninterruptible sleep возрастает, но wa > 10% менее 25% наблюдений.' ; 
		line_count=line_count+1;		
	ELSE
		result_str[line_count] = 'ALARM: количество процессов в uninterruptible sleep возрастает, и wa > 10% более 25% наблюдений.' ; 
		line_count=line_count+1;	
	END IF ;
	line_count=line_count+1; 
	
	-- УГОЛ НАКЛОНА ЛИНИИ НАИМЕНЬШИХ КВАДРАТОВ b — процессы в uninterruptible sleep (обычно ждут IO)
	-----------------------------------------------------------------------------

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

	result_str[line_count] = 'b — процессы в uninterruptible sleep (обычно ждут IO): % превышения ядер CPU | '|| REPLACE ( TO_CHAR( ROUND( b_pct::numeric , 2 ) , '000000000000D0000' ) , '.' , ',' )  ; 
	line_count=line_count+1;
	
	IF b_pct < 25.0 
	THEN 
		result_str[line_count] = 'OK: менее 25% наблюдений - процессы в uninterruptible sleep превышают количество ядер CPU' ; 
		line_count=line_count+1;
	ELSIF b_pct > 25.0 AND b_pct <= 50.0
	THEN 
		result_str[line_count] = 'WARNING: 25-50% наблюдений - процессы в uninterruptible sleep превышают количество ядер CPU' ; 
		line_count=line_count+1;
	ELSE 
		result_str[line_count] = 'ALARM: более 50% наблюдений - процессы в uninterruptible sleep превышают количество ядер CPU' ; 
		line_count=line_count+1;
	END IF ;	
	--b — процессы в uninterruptible sleep (обычно ждут IO)(% превышение CPU)
	-----------------------------------------------------------------------------


  return result_str ; 
END
$$ LANGUAGE plpgsql  ;
COMMENT ON FUNCTION reports_vmstat_io IS 'Чек-лист IO';
-- Чек-лист IO
---------------------------------------------------------------------------------------------------------------------------------------------------------------
-- reports_vmstat_iostat.sql
-- version 1.0
--------------------------------------------------------------------------------
--
-- reports_vmstat_iostat Корреляция метрик vmstat и iopstat
--
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Корреляция метрик vmstat и iopstat
CREATE OR REPLACE FUNCTION reports_vmstat_iostat( start_timestamp text , finish_timestamp text , device_name text  ) RETURNS text[] AS $$
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


	
	result_str[line_count] = 'КОРРЕЛЯЦИЯ МЕТРИК VMSTAT И IOPSTAT' ; 
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
	
	SELECT 
		MIN( cl.cpu_wa_long) AS min_cpu_wa_long , MAX( cl.cpu_wa_long) AS max_cpu_wa_long , 
		MIN( cl_io.dev_util_pct_long) AS min_dev_util_pct_long , MAX( cl_io.dev_util_pct_long) AS max_dev_util_pct_long,
		MIN( cl.memory_buff_long) AS min_memory_buff_long , MAX( cl.memory_buff_long) AS max_memory_buff_long , 
		MIN( cl.memory_cache_long) AS min_memory_cache_long , MAX( cl.memory_cache_long) AS max_memory_cache_long , 
		MIN( cl_io.dev_rps_long) AS min_dev_rps_long , MAX( cl_io.dev_rps_long) AS max_dev_rps_long,
		MIN( cl_io.dev_rmbs_long) AS min_dev_rmbs_long , MAX( cl_io.dev_rmbs_long) AS max_dev_rmbs_long,
		MIN( cl_io.dev_wps_long) AS min_dev_wps_long , MAX( cl_io.dev_wps_long) AS max_dev_wps_long,
		MIN( cl_io.dev_wmbps_long) AS min_dev_wmbps_long , MAX( cl_io.dev_wmbps_long) AS max_dev_wmbps_long ,
		MIN( cl_io.dev_r_await_long) AS min_dev_r_await_long , MAX( cl_io.dev_r_await_long) AS max_dev_r_await_long ,
		MIN( cl_io.dev_w_await_long) AS min_dev_w_await_long , MAX( cl_io.dev_w_await_long) AS max_dev_w_await_long ,
		MIN( cl_io.dev_aqu_sz_long) AS min_dev_aqu_sz_long , MAX( cl_io.dev_aqu_sz_long) AS max_dev_aqu_sz_long  		
	INTO  	min_max_rec
	FROM 
		os_stat_vmstat_median cl 
		JOIN os_stat_iostat_device_median cl_io ON ( cl.curr_timestamp = cl_io.curr_timestamp)
		JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
	WHERE 	
		cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
		AND cl_io.device = device_name ;
		
	SELECT 
		count(curr_timestamp)
	INTO line_counter
	FROM 
		cluster_stat_median cl
	WHERE 	
		cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp ;
		
		
    
	result_str[line_count] = 	' '||'|'||								
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
	
	
	result_str[line_count] = 	'MIN'||'|'||								
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
	
		
----------------------------------------------------------------------------
	-- КОРРЕЛЯЦИЯ wa util
	IF min_max_rec.min_cpu_wa_long != min_max_rec.max_cpu_wa_long AND 
	   min_max_rec.min_dev_util_pct_long != min_max_rec.max_dev_util_pct_long
	THEN 
	WITH 
		vmstat_wa_values AS
		(
			SELECT 
				cl.curr_timestamp , cpu_wa_long  AS cpu_wa_long 
			FROM 
				os_stat_vmstat_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
			WHERE				
				cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND cpu_wa_long > 0 
		) ,
		iostat_util_values AS
		(
			SELECT 
				cl.curr_timestamp , dev_util_pct_long  AS dev_util_pct_long 
			FROM 
				os_stat_iostat_device_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
			WHERE				
				cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND dev_util_pct_long > 0 
				AND cl.device = device_name
		) 
		SELECT COALESCE( corr( v1.cpu_wa_long , v2.dev_util_pct_long ) , 0 ) AS correlation_value 
		INTO corr_wa_util
		FROM
			vmstat_wa_values v1 JOIN iostat_util_values v2 ON ( v1.curr_timestamp = v2.curr_timestamp ) ;
	ELSE
	 corr_wa_util = 0 ;
	END IF;

    result_str[line_count] = 'Корреляция ожидания процессором IO и загруженности диска (wa - util) | ' ||
	REPLACE ( TO_CHAR( ROUND( corr_wa_util::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' ); 
	line_count=line_count+1;	

	IF corr_wa_util <= 0 
	THEN 
	    result_str[line_count] = 'OK: Корреляция (wa - util)  - отрицательная или отсутствует' ; 
		line_count=line_count+1;	
	ELSIF corr_wa_util > 0.7 
	THEN 
		result_str[line_count] = 'ALARM : Очень высокая корреляция (wa - util)' ; 
		line_count=line_count+1;
		result_str[line_count] = 'процессы не могут работать, потому что ждут диск' ; 
		line_count=line_count+1;
		result_str[line_count] = 'медленный диск, слишком много запросов' ; 
		line_count=line_count+1;
	ELSIF corr_wa_util > 0.5 AND corr_wa_util <= 0.7
	THEN 
		result_str[line_count] = 'WARNING : Высокая корреляция (wa - util)' ; 
		line_count=line_count+1;
        result_str[line_count] = 'процессы не могут работать, потому что ждут диск' ; 
		line_count=line_count+1;
		result_str[line_count] = 'медленный диск, слишком много запросов' ; 
		line_count=line_count+1;		
	ELSE
		result_str[line_count] = 'INFO : Слабая или средняя корреляция (wa - util)' ; 
		line_count=line_count+1;		
		result_str[line_count] = 'процессы не могут работать, потому что ждут диск' ; 
		line_count=line_count+1;
		result_str[line_count] = 'медленный диск, слишком много запросов' ; 
		line_count=line_count+1;		
	END IF;
	line_count=line_count+1;
	-- КОРРЕЛЯЦИЯ wa util	
	----------------------------------------------------------------------------	

--------------------------------------------------------------------------------
--БУФЕРИЗОВАННЫЙ ВВОД-ВЫВОД
    ----------------------------------------------------------------------------
	-- КОРРЕЛЯЦИЯ buff_rps
	IF min_max_rec.min_memory_buff_long != min_max_rec.max_memory_buff_long AND 
	   min_max_rec.min_dev_rps_long != min_max_rec.max_dev_rps_long
	THEN 	
	WITH 
		vmstat_buff_values AS
		(
			SELECT 
				cl.curr_timestamp , memory_buff_long  AS memory_buff_long 
			FROM 
				os_stat_vmstat_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
			WHERE				
				cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND memory_buff_long > 0 
		) ,
		iostat_rps_values AS
		(
			SELECT 
				cl.curr_timestamp , dev_rps_long  AS dev_rps_long 
			FROM 
				os_stat_iostat_device_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
			WHERE				
				cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND dev_rps_long > 0 
				AND cl.device = device_name
		) 
		SELECT COALESCE( corr( v1.memory_buff_long , v2.dev_rps_long ) , 0 ) AS correlation_value 
		INTO corr_buff_rps
		FROM
			vmstat_buff_values v1 JOIN iostat_rps_values v2 ON ( v1.curr_timestamp = v2.curr_timestamp ) ;
	ELSE
		corr_buff_rps = 0 ;
	END IF ;

    result_str[line_count] = 'Корреляция: объем памяти, используемой для буферов и количество операций чтения с диска (buff - r/s) | ' ||
	REPLACE ( TO_CHAR( ROUND( corr_buff_rps::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' ); 
	line_count=line_count+1;	
	result_str[line_count] = 'Высокое значение - признак не эффективного использование памяти для снижения нагрузки на диск' ; 
	line_count=line_count+1;	
	

	IF corr_buff_rps <= 0 
	THEN 
	    result_str[line_count] = 'OK: Корреляция (buff - r/s)  - отрицательная или отсутствует' ; 
		line_count=line_count+1;	
	ELSIF corr_buff_rps > 0.7 
	THEN 
		result_str[line_count] = 'ALARM : Очень высокая корреляция (buff - r/s)' ; 
		line_count=line_count+1;		
	ELSIF corr_buff_rps > 0.5 AND corr_buff_rps <= 0.7
	THEN 
		result_str[line_count] = 'WARNING : Высокая корреляция (buff - r/s)' ; 
		line_count=line_count+1;		
	ELSE
		result_str[line_count] = 'INFO : Слабая или средняя корреляция (buff - r/s)' ; 
		line_count=line_count+1;			
	END IF;
	line_count=line_count+1;
	-- КОРРЕЛЯЦИЯ buff_rps	
	----------------------------------------------------------------------------

    ----------------------------------------------------------------------------
	-- КОРРЕЛЯЦИЯ buff_rmbs
	IF min_max_rec.min_memory_buff_long != min_max_rec.max_memory_buff_long AND 
	   min_max_rec.min_dev_rmbs_long != min_max_rec.max_dev_rmbs_long
	THEN 
	WITH 
		vmstat_buff_values AS
		(
			SELECT 
				cl.curr_timestamp , memory_buff_long  AS memory_buff_long 
			FROM 
				os_stat_vmstat_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
			WHERE				
				cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND memory_buff_long > 0 
		) ,
		iostat_rmbs_values AS
		(
			SELECT 
				cl.curr_timestamp , dev_rmbs_long  AS dev_rmbs_long 
			FROM 
				os_stat_iostat_device_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
			WHERE				
				cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND dev_rmbs_long > 0 
				AND cl.device = device_name
		) 
		SELECT COALESCE( corr( v1.memory_buff_long , v2.dev_rmbs_long ) , 0 ) AS correlation_value 
		INTO corr_buff_rmbs
		FROM
			vmstat_buff_values v1 JOIN iostat_rmbs_values v2 ON ( v1.curr_timestamp = v2.curr_timestamp ) ;
	ELSE
		corr_buff_rmbs = 0 ;
	END IF ;

    result_str[line_count] = 'Корреляция: объем памяти, используемой для буферов  и объём чтения с диска (buff - rMB/s)  | ' ||
	REPLACE ( TO_CHAR( ROUND( corr_buff_rmbs::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' ); 
	line_count=line_count+1;	
	result_str[line_count] = 'Высокое значение - признак не эффективного использование памяти для снижения нагрузки на диск' ; 
	line_count=line_count+1;	

	IF corr_buff_rmbs <= 0 
	THEN 
	    result_str[line_count] = 'OK: Корреляция (buff - rMB/s)- отрицательная или отсутствует' ; 
		line_count=line_count+1;	
	ELSIF corr_buff_rmbs > 0.7 
	THEN 
		result_str[line_count] = 'ALARM : Очень высокая корреляция (buff - rMB/s)' ; 
		line_count=line_count+1;		
	ELSIF corr_buff_rmbs > 0.5 AND corr_buff_rmbs <= 0.7
	THEN 
		result_str[line_count] = 'WARNING : Высокая корреляция (buff - rMB/s)' ; 
		line_count=line_count+1;		
	ELSE
		result_str[line_count] = 'INFO : Слабая или средняя корреляция (buff - rMB/s)' ; 
		line_count=line_count+1;			
	END IF;
	line_count=line_count+1;
	-- КОРРЕЛЯЦИЯ buff_rmbs	
	----------------------------------------------------------------------------		
	
	----------------------------------------------------------------------------
	-- КОРРЕЛЯЦИЯ buff_wps
	IF min_max_rec.min_memory_buff_long != min_max_rec.max_memory_buff_long AND 
	   min_max_rec.min_dev_wps_long != min_max_rec.max_dev_wps_long
	THEN 
	WITH 
		vmstat_buff_values AS
		(
			SELECT 
				cl.curr_timestamp , memory_buff_long  AS memory_buff_long 
			FROM 
				os_stat_vmstat_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
			WHERE				
				cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND memory_buff_long > 0 
		) ,
		iostat_wps_values AS
		(
			SELECT 
				cl.curr_timestamp , dev_wps_long  AS dev_wps_long 
			FROM 
				os_stat_iostat_device_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
			WHERE				
				cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND dev_wps_long > 0 
				AND cl.device = device_name
		) 
		SELECT COALESCE( corr( v1.memory_buff_long , v2.dev_wps_long ) , 0 ) AS correlation_value 
		INTO corr_buff_wps
		FROM
			vmstat_buff_values v1 JOIN iostat_wps_values v2 ON ( v1.curr_timestamp = v2.curr_timestamp ) ;
	ELSE 
		corr_buff_wps = 0 ;
	END IF;

    result_str[line_count] = 'Корреляция: объем памяти, используемой для буферов  и количество операций записи на диск (buff - w/s) | ' ||
	REPLACE ( TO_CHAR( ROUND( corr_buff_wps::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' ); 
	line_count=line_count+1;	
	result_str[line_count] = 'Высокое значение - признак не эффективного использование памяти для снижения нагрузки на диск' ; 
	line_count=line_count+1;	
	

	IF corr_buff_wps <= 0 
	THEN 
	    result_str[line_count] = 'OK: Корреляция (buff - w/s) - отрицательная или отсутствует' ; 
		line_count=line_count+1;	
	ELSIF corr_buff_wps > 0.7 
	THEN 
		result_str[line_count] = 'ALARM : Очень высокая корреляция (buff - r/s)' ; 
		line_count=line_count+1;		
	ELSIF corr_buff_wps > 0.5 AND corr_buff_wps <= 0.7
	THEN 
		result_str[line_count] = 'WARNING : Высокая корреляция (buff - r/s)' ; 
		line_count=line_count+1;		
	ELSE
		result_str[line_count] = 'INFO : Слабая или средняя корреляция (buff - r/s)' ; 
		line_count=line_count+1;			
	END IF;
	line_count=line_count+1;
	-- КОРРЕЛЯЦИЯ buff_wps	
	----------------------------------------------------------------------------

	----------------------------------------------------------------------------
	-- КОРРЕЛЯЦИЯ buff_wmbs
	IF min_max_rec.min_memory_buff_long != min_max_rec.max_memory_buff_long AND 
	   min_max_rec.min_dev_wmbps_long != min_max_rec.max_dev_wmbps_long
	THEN 
	WITH 
		vmstat_buff_values AS
		(
			SELECT 
				cl.curr_timestamp , memory_buff_long  AS memory_buff_long 
			FROM 
				os_stat_vmstat_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
			WHERE				
				cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND memory_buff_long > 0 
		) ,
		iostat_wmbps_values AS
		(
			SELECT 
				cl.curr_timestamp , dev_wmbps_long  AS dev_wmbps_long 
			FROM 
				os_stat_iostat_device_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
			WHERE				
				cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND dev_wmbps_long > 0 
				AND cl.device = device_name
		) 
		SELECT COALESCE( corr( v1.memory_buff_long , v2.dev_wmbps_long ) , 0 ) AS correlation_value 
		INTO corr_buff_wmbs
		FROM
			vmstat_buff_values v1 JOIN iostat_wmbps_values v2 ON ( v1.curr_timestamp = v2.curr_timestamp ) ;
	ELSE
		corr_buff_wmbs = 0 ;
	END IF ;

    result_str[line_count] = 'Корреляция: объем памяти, используемой для буферов  и объем запись на диск (buff - wMB/s)  | ' ||
	REPLACE ( TO_CHAR( ROUND( corr_buff_wmbs::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' ); 
	line_count=line_count+1;
    result_str[line_count] = 'Высокое значение - признак не эффективного использование памяти для снижения нагрузки на диск' ; 
	line_count=line_count+1;	

	IF corr_buff_wmbs <= 0 
	THEN 
	    result_str[line_count] = 'OK: Корреляция (buff - wMB/s)- отрицательная или отсутствует' ; 
		line_count=line_count+1;	
	ELSIF corr_buff_wmbs > 0.7 
	THEN 
		result_str[line_count] = 'ALARM : Очень высокая корреляция (buff - wMB/s)' ; 
		line_count=line_count+1;		
	ELSIF corr_buff_wmbs > 0.5 AND corr_buff_wmbs <= 0.7
	THEN 
		result_str[line_count] = 'WARNING : Высокая корреляция (buff - wMB/s)' ; 
		line_count=line_count+1;		
	ELSE
		result_str[line_count] = 'INFO : Слабая или средняя корреляция (buff - wMB/s)' ; 
		line_count=line_count+1;			
	END IF;
	line_count=line_count+1;
	-- КОРРЕЛЯЦИЯ buff_wps	
	----------------------------------------------------------------------------	
--БУФЕРИЗОВАННЫЙ ВВОД-ВЫВОД
--------------------------------------------------------------------------------
	
	
--------------------------------------------------------------------------------
--КЭШИРОВАНИЕ ВВОД-ВЫВОД

    ----------------------------------------------------------------------------
	-- КОРРЕЛЯЦИЯ cache_rps
	IF min_max_rec.min_memory_cache_long != min_max_rec.max_memory_cache_long AND 
	   min_max_rec.min_dev_rps_long != min_max_rec.max_dev_rps_long
	THEN 
	WITH 
		vmstat_cache_values AS
		(
			SELECT 
				cl.curr_timestamp , memory_cache_long  AS memory_cache_long 
			FROM 
				os_stat_vmstat_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
			WHERE				
				cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND memory_cache_long > 0 
		) ,
		iostat_rps_values AS
		(
			SELECT 
				cl.curr_timestamp , dev_rps_long  AS dev_rps_long 
			FROM 
				os_stat_iostat_device_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
			WHERE				
				cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND dev_rps_long > 0 
				AND cl.device = device_name
		) 
		SELECT COALESCE( corr( v1.memory_cache_long , v2.dev_rps_long ) , 0 ) AS correlation_value 
		INTO corr_cache_rps
		FROM
			vmstat_cache_values v1 JOIN iostat_rps_values v2 ON ( v1.curr_timestamp = v2.curr_timestamp ) ;
	ELSE
		corr_cache_rps = 0 ; 
	END IF;

    result_str[line_count] = 'Корреляция: объем памяти, используемой для кэширования и количество операций чтения с диска (cache - r/s) | ' ||
	REPLACE ( TO_CHAR( ROUND( corr_cache_rps::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' ); 
	line_count=line_count+1;	
	result_str[line_count] = 'Высокое значение - признак не эффективного использование памяти для снижения нагрузки на диск' ; 
	line_count=line_count+1;	
	

	IF corr_cache_rps <= 0 
	THEN 
	    result_str[line_count] = 'OK: Корреляция (cache - r/s)  - отрицательная или отсутствует' ; 
		line_count=line_count+1;	
	ELSIF corr_cache_rps > 0.7 
	THEN 
		result_str[line_count] = 'ALARM : Очень высокая корреляция (cache - r/s)' ; 
		line_count=line_count+1;		
	ELSIF corr_cache_rps > 0.5 AND corr_cache_rps <= 0.7
	THEN 
		result_str[line_count] = 'WARNING : Высокая корреляция (cache - r/s)' ; 
		line_count=line_count+1;		
	ELSE
		result_str[line_count] = 'INFO : Слабая или средняя корреляция (cache - r/s)' ; 
		line_count=line_count+1;			
	END IF;
	line_count=line_count+1;
	-- КОРРЕЛЯЦИЯ cache_rps	
	----------------------------------------------------------------------------

    ----------------------------------------------------------------------------
	-- КОРРЕЛЯЦИЯ cache_rmbs
	IF min_max_rec.min_memory_cache_long != min_max_rec.max_memory_cache_long AND 
	   min_max_rec.min_dev_rmbs_long != min_max_rec.max_dev_rmbs_long
	THEN
	WITH 
		vmstat_cache_values AS
		(
			SELECT 
				cl.curr_timestamp , memory_cache_long  AS memory_cache_long 
			FROM 
				os_stat_vmstat_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
			WHERE				
				cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND memory_cache_long > 0 
		) ,
		iostat_rmbs_values AS
		(
			SELECT 
				cl.curr_timestamp , dev_rmbs_long  AS dev_rmbs_long 
			FROM 
				os_stat_iostat_device_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
			WHERE				
				cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND dev_rmbs_long > 0 
				AND cl.device = device_name
		) 
		SELECT COALESCE( corr( v1.memory_cache_long , v2.dev_rmbs_long ) , 0 ) AS correlation_value 
		INTO corr_cache_rmbs
		FROM
			vmstat_cache_values v1 JOIN iostat_rmbs_values v2 ON ( v1.curr_timestamp = v2.curr_timestamp ) ;
	ELSE
		corr_cache_rmbs = 0 ;
	END IF;

    result_str[line_count] = 'Корреляция: объем памяти, используемой для кэширования  и объём чтения с диска (cache - rMB/s)  | ' ||
	REPLACE ( TO_CHAR( ROUND( corr_cache_rmbs::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' ); 
	line_count=line_count+1;	
	result_str[line_count] = 'Высокое значение - признак не эффективного использование памяти для снижения нагрузки на диск' ; 
	line_count=line_count+1;	

	IF corr_cache_rmbs <= 0 
	THEN 
	    result_str[line_count] = 'OK: Корреляция (cache - rMB/s)- отрицательная или отсутствует' ; 
		line_count=line_count+1;	
	ELSIF corr_cache_rmbs > 0.7 
	THEN 
		result_str[line_count] = 'ALARM : Очень высокая корреляция (cache - rMB/s)' ; 
		line_count=line_count+1;		
	ELSIF corr_cache_rmbs > 0.5 AND corr_cache_rmbs <= 0.7
	THEN 
		result_str[line_count] = 'WARNING : Высокая корреляция (cache - rMB/s)' ; 
		line_count=line_count+1;		
	ELSE
		result_str[line_count] = 'INFO : Слабая или средняя корреляция (cache - rMB/s)' ; 
		line_count=line_count+1;			
	END IF;
	line_count=line_count+1;
	-- КОРРЕЛЯЦИЯ cache_rmbs	
	----------------------------------------------------------------------------		
	
	----------------------------------------------------------------------------
	-- КОРРЕЛЯЦИЯ cache_wps
	IF min_max_rec.min_memory_cache_long != min_max_rec.max_memory_cache_long AND 
	   min_max_rec.min_dev_wps_long != min_max_rec.max_dev_wps_long
	THEN
	WITH 
		vmstat_cache_values AS
		(
			SELECT 
				cl.curr_timestamp , memory_cache_long  AS memory_cache_long 
			FROM 
				os_stat_vmstat_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
			WHERE				
				cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND memory_cache_long > 0 
		) ,
		iostat_wps_values AS
		(
			SELECT 
				cl.curr_timestamp , dev_wps_long  AS dev_wps_long 
			FROM 
				os_stat_iostat_device_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
			WHERE				
				cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND dev_wps_long > 0 
				AND cl.device = device_name
		) 
		SELECT COALESCE( corr( v1.memory_cache_long , v2.dev_wps_long ) , 0 ) AS correlation_value 
		INTO corr_cache_wps
		FROM
			vmstat_cache_values v1 JOIN iostat_wps_values v2 ON ( v1.curr_timestamp = v2.curr_timestamp ) ;
	ELSE 
		corr_cache_wps = 0 ;
	END IF;

    result_str[line_count] = 'Корреляция: объем памяти, используемой для кэширования  и количество операций записи на диск (cache - w/s) | ' ||
	REPLACE ( TO_CHAR( ROUND( corr_cache_wps::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' ); 
	line_count=line_count+1;	
	result_str[line_count] = 'Высокое значение - признак не эффективного использование памяти для снижения нагрузки на диск' ; 
	line_count=line_count+1;	
	

	IF corr_cache_wps <= 0 
	THEN 
	    result_str[line_count] = 'OK: Корреляция (cache - w/s) - отрицательная или отсутствует' ; 
		line_count=line_count+1;	
	ELSIF corr_cache_wps > 0.7 
	THEN 
		result_str[line_count] = 'ALARM : Очень высокая корреляция (cache - r/s)' ; 
		line_count=line_count+1;		
	ELSIF corr_cache_wps > 0.5 AND corr_cache_wps <= 0.7
	THEN 
		result_str[line_count] = 'WARNING : Высокая корреляция (cache - r/s)' ; 
		line_count=line_count+1;		
	ELSE
		result_str[line_count] = 'INFO : Слабая или средняя корреляция (cache - r/s)' ; 
		line_count=line_count+1;			
	END IF;
	line_count=line_count+1;
	-- КОРРЕЛЯЦИЯ cache_wps	
	----------------------------------------------------------------------------

	----------------------------------------------------------------------------
	-- КОРРЕЛЯЦИЯ cache_wmbs
	IF min_max_rec.min_memory_cache_long != min_max_rec.max_memory_cache_long AND 
	   min_max_rec.min_dev_wmbps_long != min_max_rec.max_dev_wmbps_long
	THEN
	WITH 
		vmstat_cache_values AS
		(
			SELECT 
				cl.curr_timestamp , memory_cache_long  AS memory_cache_long 
			FROM 
				os_stat_vmstat_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
			WHERE				
				cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND memory_cache_long > 0 
		) ,
		iostat_wmbps_values AS
		(
			SELECT 
				cl.curr_timestamp , dev_wmbps_long  AS dev_wmbps_long 
			FROM 
				os_stat_iostat_device_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
			WHERE				
				cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND dev_wmbps_long > 0 
				AND cl.device = device_name
		) 
		SELECT COALESCE( corr( v1.memory_cache_long , v2.dev_wmbps_long ) , 0 ) AS correlation_value 
		INTO corr_cache_wmbs
		FROM
			vmstat_cache_values v1 JOIN iostat_wmbps_values v2 ON ( v1.curr_timestamp = v2.curr_timestamp ) ;
	ELSE
	 corr_cache_wmbs = 0 ;
	END IF;

    result_str[line_count] = 'Корреляция: объем памяти, используемой для кэширования  и объем запись на диск (cache - wMB/s)  | ' ||
	REPLACE ( TO_CHAR( ROUND( corr_cache_wmbs::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' ); 
	line_count=line_count+1;
    result_str[line_count] = 'Высокое значение - признак не эффективного использование памяти для снижения нагрузки на диск' ; 
	line_count=line_count+1;	

	IF corr_cache_wmbs <= 0 
	THEN 
	    result_str[line_count] = 'OK: Корреляция (cache - wMB/s)- отрицательная или отсутствует' ; 
		line_count=line_count+1;	
	ELSIF corr_cache_wmbs > 0.7 
	THEN 
		result_str[line_count] = 'ALARM : Очень высокая корреляция (cache - wMB/s)' ; 
		line_count=line_count+1;		
	ELSIF corr_cache_wmbs > 0.5 AND corr_cache_wmbs <= 0.7
	THEN 
		result_str[line_count] = 'WARNING : Высокая корреляция (cache - wMB/s)' ; 
		line_count=line_count+1;		
	ELSE
		result_str[line_count] = 'INFO : Слабая или средняя корреляция (cache - wMB/s)' ; 
		line_count=line_count+1;			
	END IF;
	line_count=line_count+1;
	-- КОРРЕЛЯЦИЯ cache_wps	
	----------------------------------------------------------------------------	
--КЭШИРОВАНИЕ ВВОД-ВЫВОД
--------------------------------------------------------------------------------	
	
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
		result_str[line_count] = 'OK: менее 25% наблюдений - загрузки устройства свыше 50%' ; 
		line_count=line_count+1;
	ELSIF util_pct > 25.0 AND util_pct <= 50.0
	THEN 
		result_str[line_count] = 'WARNING: 25-50% наблюдений - загрузки устройства свыше 50%' ; 
		line_count=line_count+1;
	ELSE 
		result_str[line_count] = 'ALARM: более 50% наблюдений - загрузки устройства свыше 50%' ; 
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
	result_str[line_count] = 'Отклик на чтение свыше 5мс(%наблюдений)| '|| REPLACE ( TO_CHAR( ROUND( r_await_pct::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' )  ; 
	line_count=line_count+1;
	
	IF r_await_pct < 25.0 
	THEN 
		result_str[line_count] = 'OK: менее 25% наблюдений - Отклик на чтение свыше 5мс' ; 
		line_count=line_count+1;
	ELSIF r_await_pct > 25.0 AND r_await_pct <= 50.0
	THEN 
		result_str[line_count] = 'WARNING: 25-50% наблюдений - Отклик на чтение свыше 5мс' ; 
		line_count=line_count+1;
	ELSE 
		result_str[line_count] = 'ALARM: более 50% наблюдений - Отклик на чтение свыше 5мс' ; 
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
	result_str[line_count] = 'Отклик на запись свыше 5мс(%наблюдений)| '|| REPLACE ( TO_CHAR( ROUND( w_await_pct::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' )  ; 
	line_count=line_count+1;
	
	IF w_await_pct < 25.0 
	THEN 
		result_str[line_count] = 'OK: менее 25% наблюдений - Отклик на запись свыше 5мс' ; 
		line_count=line_count+1;
	ELSIF w_await_pct > 25.0 AND w_await_pct <= 50.0
	THEN 
		result_str[line_count] = 'WARNING: 25-50% наблюдений - Отклик на запись свыше 5мс' ; 
		line_count=line_count+1;
	ELSE 
		result_str[line_count] = 'ALARM: более 50% наблюдений - Отклик на запись свыше 5мс' ; 
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
	result_str[line_count] = 'Средняя длина очереди запросов (глубина очереди) свыше 1 (%наблюдений) | '|| REPLACE ( TO_CHAR( ROUND( aqu_sz_pct::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' )  ; 
	line_count=line_count+1;

	IF aqu_sz_pct < 25.0 
	THEN 
		result_str[line_count] = 'OK: менее 25% наблюдений - глубина очереди свыше 1' ; 
		line_count=line_count+1;
	ELSIF aqu_sz_pct > 25.0 AND aqu_sz_pct <= 50.0
	THEN 
		result_str[line_count] = 'WARNING: 25-50% наблюдений - глубина очереди свыше 1' ; 
		line_count=line_count+1;
	ELSE 
		result_str[line_count] = 'ALARM: более 50% наблюдений - глубина очереди) свыше 1' ; 
		line_count=line_count+1;
	END IF ;	
	--aqu_sz Средняя длина очереди запросов (глубина очереди).
	-----------------------------------------------------------------------------



  return result_str ; 
END
$$ LANGUAGE plpgsql  ;
COMMENT ON FUNCTION reports_vmstat_iostat IS 'Корреляция метрик vmstat и iopstat';
-- Корреляция метрик vmstat и iopstat
-------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- reports_vmstat_meta.sql
-- version 1.0
--------------------------------------------------------------------------------
--
-- reports_vmstat_meta Метаданные по VMSTAT
--
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Метаданные по VMSTAT
CREATE OR REPLACE FUNCTION reports_vmstat_meta(  start_timestamp text , finish_timestamp text   ) RETURNS text[] AS $$
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

	result_str[line_count] = 'Метаданные по VMSTAT' ; 
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
		
	
	line_count=line_count+1;
	result_str[line_count] = 	' '||'|'||
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

  return result_str ; 
END
$$ LANGUAGE plpgsql  ;
COMMENT ON FUNCTION reports_vmstat_meta IS 'Метаданные по VMSTAT';
-- Метаданные по VMSTAT
-------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- reports_vmstat_ram.sql
-- version 1.0
--------------------------------------------------------------------------------
--
-- reports_vmstat_ram Чек-лист RAM
--
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Чек-лист RAM
CREATE OR REPLACE FUNCTION reports_vmstat_ram(  ram_all integer , start_timestamp text , finish_timestamp text   ) RETURNS text[] AS $$
DECLARE
 result_str text[] ;
 line_count integer ;
 min_timestamp timestamptz ; 
 max_timestamp timestamptz ; 
 
 counter integer ; 
  line_counter integer ; 
 
  
  free_pct DOUBLE PRECISION;
  si_pct DOUBLE PRECISION;
  so_pct DOUBLE PRECISION;
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


	
	result_str[line_count] = 'ЧЕК-ЛИСТ RAM' ; 
	line_count=line_count+1;
	
	result_str[line_count] = 'RAM (MB)| '||ram_all||'|';
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
		count(curr_timestamp)
	INTO line_counter
	FROM 
		cluster_stat_median cl
	WHERE 	
		cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp ;	
		
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
		result_str[line_count] = 'OK: менее 25% наблюдений - свободная RAM менее 5%' ; 
		line_count=line_count+1;
	ELSIF free_pct > 25.0 AND free_pct <= 50.0
	THEN 
		result_str[line_count] = 'WARNING: 25-50% наблюдений - свободная RAM менее 5%' ; 
		line_count=line_count+1;
	ELSE 
		result_str[line_count] = 'ALARM: более 50% наблюдений - свободная RAM менее 5%' ; 
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
	result_str[line_count] = 'swap in (% наблюдений) | '|| REPLACE ( TO_CHAR( ROUND( si_pct::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' )  ; 
	line_count=line_count+1;
	
	IF si_pct = 0 
	THEN 
		result_str[line_count] = 'ОК : Свопинг в RAM не используется' ; 
		line_count=line_count+1;	
	ELSIF si_pct < 25.0 
	THEN 
		result_str[line_count] = 'INFO: менее 25% наблюдений - используется cвопинг в RAM' ; 
		line_count=line_count+1;
	ELSIF si_pct > 25.0 AND si_pct <= 50.0
	THEN 
		result_str[line_count] = 'WARNING: 25-50% наблюдений - используется cвопинг в RAM' ;
		line_count=line_count+1;
	ELSE 
		result_str[line_count] = 'ALARM : более 50% наблюдений - используется cвопинг в RAM' ;
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
	result_str[line_count] = 'swap out (% наблюдений) | '|| REPLACE ( TO_CHAR( ROUND( so_pct::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' )  ; 
	line_count=line_count+1;
	
	IF so_pct = 0 
	THEN 
		result_str[line_count] = 'ОК : Свопинг из RAM не используется' ; 
		line_count=line_count+1;	
	ELSIF so_pct < 25.0 
	THEN 
		result_str[line_count] = 'INFO: менее 25% наблюдений - используется cвопинг из RAM' ; 
		line_count=line_count+1;
	ELSIF so_pct > 25.0 AND so_pct <= 50.0
	THEN 
		result_str[line_count] = 'WARNING: 25-50% наблюдений - используется cвопинг из RAM' ;
		line_count=line_count+1;
	ELSE 
		result_str[line_count] = 'ALARM : более 50% наблюдений - используется cвопинг из RAM' ;
		line_count=line_count+1;
	END IF ;	
	--so — swap out (из RAM в swap) > 0
	-----------------------------------------------------------------------------
					

  return result_str ; 
END
$$ LANGUAGE plpgsql  ;
COMMENT ON FUNCTION reports_vmstat_ram IS 'Чек-лист RAM';
-- Чек-лист RAM
-------------------------------------------------------------------------------


--------------------------------------------------------------------------------
-- report_wait_event_for_pareto.sql
-- version 3.0
--------------------------------------------------------------------------------
--
-- report_wait_event_for_pareto Диаграмма Парето по wait_event
--
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Сформировать диаграмму Парето по wait_event
CREATE OR REPLACE FUNCTION report_wait_event_for_pareto(  start_timestamp text , finish_timestamp text ) RETURNS text[] AS $$
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
 total_wait_event_count integer ;
 pct_for_80 numeric ;
 
 report_wait_event_for_pareto text[];
 report_wait_event_for_pareto_count integer ;
 index_for_wait_event integer ;
 
 corr_bufferpin DOUBLE PRECISION ; 
 corr_extension DOUBLE PRECISION ; 
 corr_io DOUBLE PRECISION ; 
 corr_ipc DOUBLE PRECISION ; 
 corr_lock DOUBLE PRECISION ; 
 corr_lwlock DOUBLE PRECISION ; 
 corr_timeout DOUBLE PRECISION ; 
 
 wait_event_type_corr_rec  record ; 
  
 tmp_wait_event_type_corr_index integer ; 
 
BEGIN
	line_count = 1 ;
	
	result_str[line_count] = 'ДИАГРАММА ПАРЕТО ПО WAIT_EVENT';	
	line_count=line_count+1;
	
	
	SELECT date_trunc( 'minute' , to_timestamp( start_timestamp , 'YYYY-MM-DD HH24:MI' ) )
	INTO    min_timestamp ; 
  
	SELECT date_trunc( 'minute' , to_timestamp( finish_timestamp , 'YYYY-MM-DD HH24:MI' ) )
	INTO    max_timestamp ; 
	
	result_str[line_count] = to_char(min_timestamp , 'YYYY-MM-DD HH24:MI') ;
	line_count=line_count+1; 
	result_str[line_count] = to_char(max_timestamp , 'YYYY-MM-DD HH24:MI') ;
	line_count=line_count+2; 
	
	result_str[line_count] ='80% ОЖИДАНИЙ,';	
	line_count=line_count+1;	
	result_str[line_count] = 'C КОЭФФИЦИЕНТОМ КОРРЕЛЯЦИЯ МЕЖДУ ';
	line_count=line_count+1;	
	result_str[line_count] = 'ТИПОМ ОЖИДАНИЯ И ОЖИДАНИЯМИ СУБД';	
	line_count=line_count+1;	
	result_str[line_count] = '0.7 и ВЫШЕ';	
	line_count=line_count+1;	
	
	DROP TABLE IF EXISTS wait_event_type_corr;
	CREATE TEMPORARY TABLE wait_event_type_corr
	(
		wait_event_type text  ,   
		corr_value DOUBLE PRECISION 
	);
	
	----------------------------------------------	
	-- ВНУТРЕННЯЯ ТАБЛИЦА ОТЧЕТА 	
	TRUNCATE TABLE tmp_wait_events ;
	-- ВНУТРЕННЯЯ ТАБЛИЦА ОТЧЕТА 
	----------------------------------------------	
	
	tmp_wait_event_type_corr_index = 0 ; 
	
	----------------------------------------------------------------------------------------------------
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ ОЖИДАНИЯ - bufferpin
		WITH 
		waitings AS
		(
			SELECT 
				curr_timestamp , curr_waitings  AS curr_waitings  
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp
				AND curr_waitings > 0
		) ,
		bufferpin_waitings AS
		(
			SELECT 
				curr_timestamp , curr_bufferpin  AS curr_bufferpin 
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND curr_bufferpin > 0 
		) 
		SELECT COALESCE( corr( curr_waitings , curr_bufferpin ) , 0 ) AS correlation_value 
		INTO corr_bufferpin
		FROM
			waitings os JOIN bufferpin_waitings  w ON ( os.curr_timestamp = w.curr_timestamp ) ;	
			
		INSERT INTO wait_event_type_corr ( wait_event_type , corr_value )
		VALUES ( 'BufferPin' , corr_bufferpin );
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ ОЖИДАНИЯ - bufferpin 
	----------------------------------------------------------------------------------------------------
	
	----------------------------------------------------------------------------------------------------
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ ОЖИДАНИЯ - extension
		WITH 
		waitings AS
		(
			SELECT 
				curr_timestamp , curr_waitings  AS curr_waitings  
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp
				AND curr_waitings > 0
		) ,
		extension_waitings AS
		(
			SELECT 
				curr_timestamp , curr_extension  AS curr_extension 
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND curr_extension > 0 
		) 
		SELECT COALESCE( corr( curr_waitings , curr_extension ) , 0 ) AS correlation_value 
		INTO corr_extension
		FROM
			waitings os JOIN extension_waitings  w ON ( os.curr_timestamp = w.curr_timestamp ) ;
		
		INSERT INTO wait_event_type_corr ( wait_event_type , corr_value )
		VALUES ( 'Extension' , corr_extension );			
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ ОЖИДАНИЯ - extension 
	----------------------------------------------------------------------------------------------------
	
	----------------------------------------------------------------------------------------------------
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ ОЖИДАНИЯ - io
		WITH 
		waitings AS
		(
			SELECT 
				curr_timestamp , curr_waitings  AS curr_waitings  
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp
				AND curr_waitings > 0
		) ,
		io_waitings AS
		(
			SELECT 
				curr_timestamp , curr_io  AS curr_io 
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND curr_io > 0 
		) 
		SELECT COALESCE( corr( curr_waitings , curr_io ) , 0 ) AS correlation_value 
		INTO corr_io
		FROM
			waitings os JOIN io_waitings  w ON ( os.curr_timestamp = w.curr_timestamp ) ;	
			
		INSERT INTO wait_event_type_corr ( wait_event_type , corr_value )
		VALUES ( 'IO' , corr_io );			
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ ОЖИДАНИЯ - io 
	----------------------------------------------------------------------------------------------------
	
	----------------------------------------------------------------------------------------------------
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ ОЖИДАНИЯ - ipc
		WITH 
		waitings AS
		(
			SELECT 
				curr_timestamp , curr_waitings  AS curr_waitings  
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp
				AND curr_waitings > 0
		) ,
		ipc_waitings AS
		(
			SELECT 
				curr_timestamp , curr_ipc  AS curr_ipc 
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND curr_ipc > 0 
		) 
		SELECT COALESCE( corr( curr_waitings , curr_ipc ) , 0 ) AS correlation_value 
		INTO corr_ipc
		FROM
			waitings os JOIN ipc_waitings  w ON ( os.curr_timestamp = w.curr_timestamp ) ;
	
		INSERT INTO wait_event_type_corr ( wait_event_type , corr_value )
		VALUES ( 'IPC' , corr_ipc );
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ ОЖИДАНИЯ - ipc 
	----------------------------------------------------------------------------------------------------
	
	----------------------------------------------------------------------------------------------------
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ ОЖИДАНИЯ - lock
		WITH 
		waitings AS
		(
			SELECT 
				curr_timestamp , curr_waitings  AS curr_waitings  
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp
				AND curr_waitings > 0
		) ,
		lock_waitings AS
		(
			SELECT 
				curr_timestamp , curr_lock  AS curr_lock 
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND curr_lock > 0 
		) 
		SELECT COALESCE( corr( curr_waitings , curr_lock ) , 0 ) AS correlation_value 
		INTO corr_lock
		FROM
			waitings os JOIN lock_waitings  w ON ( os.curr_timestamp = w.curr_timestamp ) ;

		INSERT INTO wait_event_type_corr ( wait_event_type , corr_value )
		VALUES ( 'Lock' , corr_lock );			
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ ОЖИДАНИЯ - lock 
	----------------------------------------------------------------------------------------------------
	
	----------------------------------------------------------------------------------------------------
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ ОЖИДАНИЯ - lwlock
		WITH 
		waitings AS
		(
			SELECT 
				curr_timestamp , curr_waitings  AS curr_waitings  
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND curr_waitings > 0				
		) ,
		lwlock_waitings AS
		(
			SELECT 
				curr_timestamp , curr_lwlock  AS curr_lwlock 
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND curr_lwlock > 0 
		) 
		SELECT COALESCE( corr( curr_waitings , curr_lwlock ) , 0 ) AS correlation_value 
		INTO corr_lwlock
		FROM
			waitings os JOIN lwlock_waitings  w ON ( os.curr_timestamp = w.curr_timestamp ) ;
			
		INSERT INTO wait_event_type_corr ( wait_event_type , corr_value )
		VALUES ( 'LWLock' , corr_lwlock );
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ ОЖИДАНИЯ - lwlock 
	----------------------------------------------------------------------------------------------------
	
	----------------------------------------------------------------------------------------------------
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ ОЖИДАНИЯ - timeout
		WITH 
		waitings AS
		(
			SELECT 
				curr_timestamp , curr_waitings  AS curr_waitings  
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND curr_waitings > 0				
		) ,
		timeout_waitings AS
		(
			SELECT 
				curr_timestamp , curr_timeout  AS curr_timeout 
			FROM cluster_stat_median
			WHERE				
				curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				AND curr_timeout > 0 
		) 
		SELECT COALESCE( corr( curr_waitings , curr_timeout ) , 0 ) AS correlation_value 
		INTO corr_timeout
		FROM
			waitings os JOIN timeout_waitings  w ON ( os.curr_timestamp = w.curr_timestamp ) ;
		
		INSERT INTO wait_event_type_corr ( wait_event_type , corr_value )
		VALUES ( 'Timeout' , corr_timeout );
	--КОРРЕЛЯЦИЯ ОПЕРАЦИОННАЯ ОЖИДАНИЯ - timeout 
	----------------------------------------------------------------------------------------------------
	
	
	-----------------------------------------------------------------------------
	
	
	FOR wait_event_type_rec IN 
	SELECT 	
		wait_event_type 
	FROM 	
		statement_stat_waitings_median
	WHERE 
		curr_timestamp  BETWEEN min_timestamp AND max_timestamp
	GROUP BY 
		wait_event_type 
	ORDER BY 
		wait_event_type 
	LOOP 
		SELECT *
		INTO wait_event_type_corr_rec
		FROM wait_event_type_corr
		WHERE wait_event_type = wait_event_type_rec.wait_event_type ;
		
		IF wait_event_type_corr_rec.corr_value < 0.7 
		THEN 
			CONTINUE;
		END IF ; 
		
		line_count=line_count+2;
		result_str[line_count] =' WAIT_EVENT_TYPE = '|| wait_event_type_rec.wait_event_type ||'|';
		line_count=line_count+1;

		
		result_str[line_count] =' WAIT_EVENT  '||'|'||								
								' COUNT '||'|' ||
								' PCT '||'|' 
								;	
		line_count=line_count+1;
		
		pct_for_80 = 0;
		
		FOR wait_event_rec IN 
		SELECT 	
			wait_event , 
			SUM(curr_value_long) AS count
		FROM 	
			statement_stat_waitings_median
		WHERE 
			curr_timestamp  BETWEEN min_timestamp AND max_timestamp
			AND wait_event_type = wait_event_type_rec.wait_event_type 
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
				AND wait_event_type = wait_event_type_rec.wait_event_type
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
			
			result_str[line_count] =  wait_event_rec.wait_event  ||'|'||
									  wait_event_rec.count  ||'|'||
									  REPLACE ( TO_CHAR( ROUND( (wait_event_rec.count::numeric / total_wait_event_count::numeric *100.0)::numeric , 2 ) , '000000000000D0000') , '.' , ',' ) ||'|'
									  ;
			line_count=line_count+1; 
			
			SELECT report_wait_event_for_pareto || wait_event_rec.wait_event
			INTO report_wait_event_for_pareto ; 
	
			tmp_wait_event_type_corr_index = tmp_wait_event_type_corr_index + 1 ;
			INSERT INTO  tmp_wait_events 
			( id , wait_event_type , wait_event )
			VALUES 
			( tmp_wait_event_type_corr_index , wait_event_type_rec.wait_event_type , wait_event_rec.wait_event );
			
			IF pct_for_80 > 80.0 
			THEN 
				EXIT;
			END IF;
			
		END LOOP ;		
		--FOR wait_event_rec IN 
	END LOOP;
	--FOR wait_event_type_rec IN 

  return result_str ; 
END
$$ LANGUAGE plpgsql  ;
COMMENT ON FUNCTION report_queryid_for_pareto IS 'Сформировать диаграмму Парето по wait_event';
-- Сформировать диаграмму Парето по wait_event
-------------------------------------------------------------------------------

	


	
-------------------------------------------------------------------------------
-- reports_waitings_os_corr.sql
-- version 1.0
--------------------------------------------------------------------------------
--
-- reports_reports_waitings_os_corr Корреляция ожиданий СУБД и метрик vmstat
--
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Корреляция ожиданий СУБД и метрик vmstat
CREATE OR REPLACE FUNCTION reports_waitings_os_corr( start_timestamp text , finish_timestamp text ) RETURNS text[] AS $$
DECLARE
 result_str text[] ;
 line_count integer ;
 min_timestamp timestamptz ; 
 max_timestamp timestamptz ; 
 
 counter integer ; 
 min_max_rec record ;
 line_counter integer ; 
 
  min_max_pct_rec record ;
  
  corr_io_wa DOUBLE PRECISION ; -- Корреляция 
  corr_io_b DOUBLE PRECISION ; -- Корреляция 
  corr_io_si DOUBLE PRECISION ; -- Корреляция 
  corr_io_so DOUBLE PRECISION ; -- Корреляция 
  corr_io_bi DOUBLE PRECISION ; -- Корреляция 
  corr_io_bo DOUBLE PRECISION ; -- Корреляция 
  corr_lwlock_us DOUBLE PRECISION ; -- Корреляция 
  corr_lwlock_sy DOUBLE PRECISION ; -- Корреляция 
  
  bi_regr_rec record ;
  bo_regr_rec record ;
  
  timestamp_counter integer ;
  us_sy_pct DOUBLE PRECISION ; 
  us_regr_rec record ;
  sy_regr_rec record ;
  
  
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

	SELECT 
		count(curr_timestamp)
	INTO timestamp_counter
	FROM 
		cluster_stat_median cl
	WHERE 	
		cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp ;

		
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
	
	result_str[line_count] = 'КОРРЕЛЯЦИЯ ОЖИДАНИЙ СУБД И МЕТРИК vmstat' ; 
	line_count=line_count+1;
	
	
	result_str[line_count] = to_char(min_timestamp , 'YYYY-MM-DD HH24:MI') ;
	line_count=line_count+1; 
	result_str[line_count] = to_char(max_timestamp , 'YYYY-MM-DD HH24:MI') ;
	line_count=line_count+2;  
	
	
	SELECT 
		MIN( cl_w.curr_io) AS min_vmstat_curr_io , MAX( cl_w.curr_io) AS max_curr_io , 
		MIN( cl_w.curr_lwlock) AS min_curr_lwlock , MAX( cl_w.curr_lwlock) AS max_curr_lwlock , 		
		MIN( cl.cpu_wa_long) AS min_cpu_wa_long , MAX( cl.cpu_wa_long) AS max_cpu_wa_long ,
		MIN( cl.procs_b_long) AS min_procs_b_long , MAX( cl.procs_b_long) AS max_procs_b_long , 
		MIN( cl.swap_si_long) AS min_swap_si_long , MAX( cl.swap_si_long) AS max_swap_si_long , 
		MIN( cl.swap_so_long) AS min_swap_so_long , MAX( cl.swap_so_long) AS max_swap_so_long , 
		MIN( cl.io_bi_long) AS min_io_bi_long , MAX( cl.io_bi_long) AS max_io_bi_long ,
		MIN( cl.io_bo_long) AS min_io_bo_long , MAX( cl.io_bo_long) AS max_io_bo_long ,
		MIN( cl.cpu_us_long) AS min_cpu_us_long , MAX( cl.cpu_us_long) AS max_cpu_us_long ,
		MIN( cl.cpu_sy_long) AS min_cpu_sy_long , MAX( cl.cpu_sy_long) AS max_cpu_sy_long		
	INTO  	min_max_rec
	FROM 
		os_stat_vmstat_median cl 
		JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
	WHERE 	
		cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp;
		
		
	---------------------------------------------------------------------------
	--1.Проблема в ОС (по vmstat): Высокий wa, b (медленный/перегруженный диск)	
	--IO - wa	 
		IF min_max_rec.min_vmstat_curr_io != min_max_rec.max_curr_io AND 
		   min_max_rec.min_cpu_wa_long != min_max_rec.max_cpu_wa_long
		THEN 
			WITH 
			io_values AS
			(
				SELECT 
					cl.curr_timestamp , curr_io  AS curr_io 
				FROM 
					os_stat_vmstat_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
				WHERE				
					cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
					AND curr_io > 0 
			) ,
			cpu_wa_values AS
			(
				SELECT 
					cl.curr_timestamp , cpu_wa_long  AS cpu_wa_long 
				FROM 
					os_stat_vmstat_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
				WHERE				
					cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
					AND cpu_wa_long > 0 					
			) 
			SELECT COALESCE( corr( v1.curr_io , v2.cpu_wa_long ) , 0 ) AS correlation_value 
			INTO corr_io_wa
			FROM
				io_values v1 JOIN cpu_wa_values v2 ON ( v1.curr_timestamp = v2.curr_timestamp ) ;
		ELSE
			corr_io_wa = 0 ;
		END IF;
		
		result_str[line_count] = 'Корреляция ожиданий IO и wa(I/O wait): IO-wa | ' ||
		REPLACE ( TO_CHAR( ROUND( corr_io_wa::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' ); 
		line_count=line_count+1;	

		IF corr_io_wa <= 0 
		THEN 
			result_str[line_count] = 'OK: Корреляция (IO-wa)  - отрицательная или отсутствует' ; 
			line_count=line_count+1;	
		ELSIF corr_io_wa > 0.7 
		THEN 
			result_str[line_count] = 'ALARM : Очень высокая корреляция (IO-wa)' ; 
			line_count=line_count+1;
			
			-----------------------------------------------------------------------------------------------------------
			result_str[line_count] = 'Процесс переходит в состояние непрерываемого сна (D), ожидая ответа от диска.' ; 
			line_count=line_count+1;
			result_str[line_count] = 'Даже простые операции чтения/записи (WAL, сброс буферов) начинают занимать неприемлемо много времени,';
			line_count=line_count+1;
			result_str[line_count] = 'так как диск не успевает обрабатывать запросы.';
			line_count=line_count+1;
			result_str[line_count] = 'Возможные причины и последствия ';
			line_count=line_count+1;
			result_str[line_count] = '1. Неоптимальные настройки I/O подсистемы (например, неверный sheduler, низкие лимиты IOPS/пропускной способности)';
			line_count=line_count+1;
			result_str[line_count] = 'Медленный диск или неправильная настройка контроллера/ФС приводят к тому, что даже обычные операции записи WAL ';
			line_count=line_count+1;
			result_str[line_count] = 'или сброса "грязных" страниц на диск начинают занимать много времени.';
			line_count=line_count+1;	
			result_str[line_count] = '2. Неверные настройки виртуальной памяти (например, vm.dirty_*)';
			line_count=line_count+1;	
			result_str[line_count] = 'Слишком большие значения vm.dirty_background_bytes и vm.dirty_bytes приводят к тому, что ОС копит много "грязных" данных в кеше,';
			line_count=line_count+1;
			result_str[line_count] = 'а затем сбрасывает их на диск одним большим взрывом. Это вызывает длительные ожидания записи ';
			line_count=line_count+1;
			result_str[line_count] = 'и может провоцировать длительные проверки контрольных точек (checkpoint).';
			line_count=line_count+1;
			-----------------------------------------------------------------------------------------------------------
			
		ELSIF corr_io_wa > 0.5 AND corr_io_wa <= 0.7
		THEN 
			result_str[line_count] = 'WARNING : Высокая корреляция (IO-wa)' ; 
			line_count=line_count+1;
			-----------------------------------------------------------------------------------------------------------
			result_str[line_count] = 'Процесс переходит в состояние непрерываемого сна (D), ожидая ответа от диска.' ; 
			line_count=line_count+1;
			result_str[line_count] = 'Даже простые операции чтения/записи (WAL, сброс буферов) начинают занимать неприемлемо много времени,';
			line_count=line_count+1;
			result_str[line_count] = 'так как диск не успевает обрабатывать запросы.';
			line_count=line_count+1;
			result_str[line_count] = 'Возможные причины и последствия ';
			line_count=line_count+1;
			result_str[line_count] = '1. Неоптимальные настройки I/O подсистемы (например, неверный sheduler, низкие лимиты IOPS/пропускной способности)';
			line_count=line_count+1;
			result_str[line_count] = 'Медленный диск или неправильная настройка контроллера/ФС приводят к тому, что даже обычные операции записи WAL ';
			line_count=line_count+1;
			result_str[line_count] = 'или сброса "грязных" страниц на диск начинают занимать много времени.';
			line_count=line_count+1;	
			result_str[line_count] = '2. Неверные настройки виртуальной памяти (например, vm.dirty_*)';
			line_count=line_count+1;	
			result_str[line_count] = 'Слишком большие значения vm.dirty_background_bytes и vm.dirty_bytes приводят к тому, что ОС копит много "грязных" данных в кеше,';
			line_count=line_count+1;
			result_str[line_count] = 'а затем сбрасывает их на диск одним большим взрывом. Это вызывает длительные ожидания записи ';
			line_count=line_count+1;
			result_str[line_count] = 'и может провоцировать длительные проверки контрольных точек (checkpoint).';
			line_count=line_count+1;
			-----------------------------------------------------------------------------------------------------------
						
		ELSE
			result_str[line_count] = 'INFO : Слабая или средняя корреляция (IO-wa)' ; 
			line_count=line_count+1;		
			-----------------------------------------------------------------------------------------------------------
			result_str[line_count] = 'Процесс переходит в состояние непрерываемого сна (D), ожидая ответа от диска.' ; 
			line_count=line_count+1;
			result_str[line_count] = 'Даже простые операции чтения/записи (WAL, сброс буферов) начинают занимать неприемлемо много времени,';
			line_count=line_count+1;
			result_str[line_count] = 'так как диск не успевает обрабатывать запросы.';
			line_count=line_count+1;
			result_str[line_count] = 'Возможные причины и последствия ';
			line_count=line_count+1;
			result_str[line_count] = '1. Неоптимальные настройки I/O подсистемы (например, неверный sheduler, низкие лимиты IOPS/пропускной способности)';
			line_count=line_count+1;
			result_str[line_count] = 'Медленный диск или неправильная настройка контроллера/ФС приводят к тому, что даже обычные операции записи WAL ';
			line_count=line_count+1;
			result_str[line_count] = 'или сброса "грязных" страниц на диск начинают занимать много времени.';
			line_count=line_count+1;	
			result_str[line_count] = '2. Неверные настройки виртуальной памяти (например, vm.dirty_*)';
			line_count=line_count+1;	
			result_str[line_count] = 'Слишком большие значения vm.dirty_background_bytes и vm.dirty_bytes приводят к тому, что ОС копит много "грязных" данных в кеше,';
			line_count=line_count+1;
			result_str[line_count] = 'а затем сбрасывает их на диск одним большим взрывом. Это вызывает длительные ожидания записи ';
			line_count=line_count+1;
			result_str[line_count] = 'и может провоцировать длительные проверки контрольных точек (checkpoint).';
			line_count=line_count+1;
			-----------------------------------------------------------------------------------------------------------		
								
		END IF;
		line_count=line_count+1;
		--IO - wa
		--------------------------------------------------------------------------------------------------------------------------
		--IO - b
		IF min_max_rec.min_vmstat_curr_io != min_max_rec.max_curr_io AND 
		   min_max_rec.min_procs_b_long != min_max_rec.max_procs_b_long
		THEN 
			WITH 
			io_values AS
			(
				SELECT 
					cl.curr_timestamp , curr_io  AS curr_io 
				FROM 
					os_stat_vmstat_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
				WHERE				
					cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
					AND curr_io > 0 
			) ,
			procs_b_values AS
			(
				SELECT 
					cl.curr_timestamp , procs_b_long  AS procs_b_long 
				FROM 
					os_stat_vmstat_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
				WHERE				
					cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
					AND procs_b_long > 0 					
			) 
			SELECT COALESCE( corr( v1.curr_io , v2.procs_b_long ) , 0 ) AS correlation_value 
			INTO corr_io_b
			FROM
				io_values v1 JOIN procs_b_values v2 ON ( v1.curr_timestamp = v2.curr_timestamp ) ;
		ELSE
			corr_io_b = 0 ;
		END IF;
		
		result_str[line_count] = 'Корреляция ожиданий IO и b(blocked) процессы, находящихся в состоянии непрерываемого сна: IO-b | ' ||
		REPLACE ( TO_CHAR( ROUND( corr_io_b::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' ); 
		line_count=line_count+1;	

		IF corr_io_b <= 0 
		THEN 
			result_str[line_count] = 'OK: Корреляция (IO-b)  - отрицательная или отсутствует' ; 
			line_count=line_count+1;	
		ELSIF corr_io_b > 0.7 
		THEN 
			result_str[line_count] = 'ALARM : Очень высокая корреляция (IO-b)' ; 
			line_count=line_count+1;
			-----------------------------------------------------------------------------------------------------------
			result_str[line_count] = 'Процесс переходит в состояние непрерываемого сна (D), ожидая ответа от диска.' ; 
			line_count=line_count+1;
			result_str[line_count] = 'Даже простые операции чтения/записи (WAL, сброс буферов) начинают занимать неприемлемо много времени,';
			line_count=line_count+1;
			result_str[line_count] = 'так как диск не успевает обрабатывать запросы.';
			line_count=line_count+1;
			result_str[line_count] = 'Возможные причины и последствия ';
			line_count=line_count+1;
			result_str[line_count] = '1. Неоптимальные настройки I/O подсистемы (например, неверный sheduler, низкие лимиты IOPS/пропускной способности)';
			line_count=line_count+1;
			result_str[line_count] = 'Медленный диск или неправильная настройка контроллера/ФС приводят к тому, что даже обычные операции записи WAL ';
			line_count=line_count+1;
			result_str[line_count] = 'или сброса "грязных" страниц на диск начинают занимать много времени.';
			line_count=line_count+1;	
			result_str[line_count] = '2. Неверные настройки виртуальной памяти (например, vm.dirty_*)';
			line_count=line_count+1;	
			result_str[line_count] = 'Слишком большие значения vm.dirty_background_bytes и vm.dirty_bytes приводят к тому, что ОС копит много "грязных" данных в кеше,';
			line_count=line_count+1;
			result_str[line_count] = 'а затем сбрасывает их на диск одним большим взрывом. Это вызывает длительные ожидания записи ';
			line_count=line_count+1;
			result_str[line_count] = 'и может провоцировать длительные проверки контрольных точек (checkpoint).';
			line_count=line_count+1;
			-----------------------------------------------------------------------------------------------------------		
								
		ELSIF corr_io_b > 0.5 AND corr_io_b <= 0.7
		THEN 
			result_str[line_count] = 'WARNING : Высокая корреляция (IO-b)' ; 
			line_count=line_count+1;
			-----------------------------------------------------------------------------------------------------------
			result_str[line_count] = 'Процесс переходит в состояние непрерываемого сна (D), ожидая ответа от диска.' ; 
			line_count=line_count+1;
			result_str[line_count] = 'Даже простые операции чтения/записи (WAL, сброс буферов) начинают занимать неприемлемо много времени,';
			line_count=line_count+1;
			result_str[line_count] = 'так как диск не успевает обрабатывать запросы.';
			line_count=line_count+1;
			result_str[line_count] = 'Возможные причины и последствия ';
			line_count=line_count+1;
			result_str[line_count] = '1. Неоптимальные настройки I/O подсистемы (например, неверный sheduler, низкие лимиты IOPS/пропускной способности)';
			line_count=line_count+1;
			result_str[line_count] = 'Медленный диск или неправильная настройка контроллера/ФС приводят к тому, что даже обычные операции записи WAL ';
			line_count=line_count+1;
			result_str[line_count] = 'или сброса "грязных" страниц на диск начинают занимать много времени.';
			line_count=line_count+1;	
			result_str[line_count] = '2. Неверные настройки виртуальной памяти (например, vm.dirty_*)';
			line_count=line_count+1;	
			result_str[line_count] = 'Слишком большие значения vm.dirty_background_bytes и vm.dirty_bytes приводят к тому, что ОС копит много "грязных" данных в кеше,';
			line_count=line_count+1;
			result_str[line_count] = 'а затем сбрасывает их на диск одним большим взрывом. Это вызывает длительные ожидания записи ';
			line_count=line_count+1;
			result_str[line_count] = 'и может провоцировать длительные проверки контрольных точек (checkpoint).';
			line_count=line_count+1;
			-----------------------------------------------------------------------------------------------------------
					
		ELSE
			result_str[line_count] = 'INFO : Слабая или средняя корреляция (IO-b)' ; 
			line_count=line_count+1;		
			-----------------------------------------------------------------------------------------------------------
			result_str[line_count] = 'Процесс переходит в состояние непрерываемого сна (D), ожидая ответа от диска.' ; 
			line_count=line_count+1;
			result_str[line_count] = 'Даже простые операции чтения/записи (WAL, сброс буферов) начинают занимать неприемлемо много времени,';
			line_count=line_count+1;
			result_str[line_count] = 'так как диск не успевает обрабатывать запросы.';
			line_count=line_count+1;
			result_str[line_count] = 'Возможные причины и последствия ';
			line_count=line_count+1;
			result_str[line_count] = '1. Неоптимальные настройки I/O подсистемы (например, неверный sheduler, низкие лимиты IOPS/пропускной способности)';
			line_count=line_count+1;
			result_str[line_count] = 'Медленный диск или неправильная настройка контроллера/ФС приводят к тому, что даже обычные операции записи WAL ';
			line_count=line_count+1;
			result_str[line_count] = 'или сброса "грязных" страниц на диск начинают занимать много времени.';
			line_count=line_count+1;	
			result_str[line_count] = '2. Неверные настройки виртуальной памяти (например, vm.dirty_*)';
			line_count=line_count+1;	
			result_str[line_count] = 'Слишком большие значения vm.dirty_background_bytes и vm.dirty_bytes приводят к тому, что ОС копит много "грязных" данных в кеше,';
			line_count=line_count+1;
			result_str[line_count] = 'а затем сбрасывает их на диск одним большим взрывом. Это вызывает длительные ожидания записи ';
			line_count=line_count+1;
			result_str[line_count] = 'и может провоцировать длительные проверки контрольных точек (checkpoint).';
			line_count=line_count+1;
			-----------------------------------------------------------------------------------------------------------	
						
		END IF;
		line_count=line_count+1;
		
		--IO - b
		--------------------------------------------------------------------------------------------------------------------------
	--1.Проблема в ОС (по vmstat): Высокий wa, b (медленный/перегруженный диск)	
    ---------------------------------------------------------------------------
	
	-----------------------------------------------------------------------------
	--Высокие значения bi(blocks in) 
	IF min_max_rec.min_vmstat_curr_io != min_max_rec.max_curr_io AND 
		   min_max_rec.min_io_bi_long != min_max_rec.max_io_bi_long
	THEN 
		-- УГОЛ НАКЛОНА ЛИНИИ НАИМЕНЬШИХ КВАДРАТОВ bi
		-- 	линия регрессии  скорости  : Y = a + bX
			BEGIN
				WITH stats AS 
				(
				  SELECT 
					AVG(t.curr_timepoint::DOUBLE PRECISION) as avg1, 
					STDDEV(t.curr_timepoint::DOUBLE PRECISION) as std1,
					AVG(s.io_bi_long::DOUBLE PRECISION) as avg2, 
					STDDEV(s.io_bi_long::DOUBLE PRECISION) as std2
				  FROM
					os_stat_vmstat_median s JOIN tmp_timepoints t ON ( s.curr_timestamp  = t.curr_timestamp )
				  WHERE 
					t.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				),
				standardized_data AS 
				(
					SELECT 
						(t.curr_timepoint::DOUBLE PRECISION - avg1) / std1 as x_z,
						(s.io_bi_long::DOUBLE PRECISION - avg2) / std2 as y_z
					FROM
						os_stat_vmstat_median s JOIN tmp_timepoints t ON ( s.curr_timestamp  = t.curr_timestamp ) , stats
					WHERE 
						t.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				)	
				SELECT
					REGR_SLOPE(y_z, x_z) as slope, --bi
					ATAN(REGR_SLOPE(y_z, x_z)) * 180 / PI() as slope_angle_degrees, --угол наклона
					REGR_R2(y_z, x_z) as r_squared -- Коэффициент детерминации
				INTO 
					bi_regr_rec
				FROM standardized_data;
			EXCEPTION
			  --STDDEV(s.op_speed_long::DOUBLE PRECISION) = 0  
			  WHEN division_by_zero THEN  -- Конкретное исключение для деления на ноль
				SELECT 
					1.0 as slope, --b
					0.0  as slope_angle_degrees, --угол наклона
					0.0  as r_squared -- Коэффициент детерминации
				INTO 
				bi_regr_rec ;
			END;
		-- УГОЛ НАКЛОНА ЛИНИИ НАИМЕНЬШИХ КВАДРАТОВ bi
		-- 	линия регрессии  скорости  : Y = a + bX
			
			IF  bi_regr_rec.slope_angle_degrees <= 0 
			THEN 
				result_str[line_count] = 'Количество bi(блоки, считанные с устройств) - не растёт | ' ;
				line_count=line_count+1;	
			ELSE 
				WITH 
				io_values AS
				(
					SELECT 
						cl.curr_timestamp , curr_io  AS curr_io 
					FROM 
						os_stat_vmstat_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
					WHERE				
						cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
						AND curr_io > 0 
				) ,
				io_bi_values AS
				(
					SELECT 
						cl.curr_timestamp , io_bi_long  AS io_bi_long 
					FROM 
						os_stat_vmstat_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
					WHERE				
						cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
						AND io_bi_long > 0 					
				) 
				SELECT COALESCE( corr( v1.curr_io , v2.io_bi_long ) , 0 ) AS correlation_value 
				INTO corr_io_bi
				FROM
					io_values v1 JOIN io_bi_values v2 ON ( v1.curr_timestamp = v2.curr_timestamp ) ;  
			END IF;
	ELSE
		corr_io_bi = 0 ;
	END IF;

	result_str[line_count] = 'Корреляция ожиданий IO и bi(blocks in):| ' ||
	REPLACE ( TO_CHAR( ROUND( corr_io_bi::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' ); 
	line_count=line_count+1;	

	IF corr_io_bi <= 0 
	THEN 
		result_str[line_count] = 'OK: Корреляция IO и bi(blocks in) - отрицательная или отсутствует' ; 
		line_count=line_count+1;				
	ELSIF corr_io_bi > 0.7 
	THEN 
		result_str[line_count] = 'ALARM : Очень высокая корреляция (IO и bi)' ; 
		line_count=line_count+1;
		---------------------------------------------------------------------------------------------
		result_str[line_count] = 'Причины и последствия: ' ; 
		line_count=line_count+1;
		result_str[line_count] = 'Активная работа с большими объемами данных  ' ;
		line_count=line_count+1;
		result_str[line_count] = '(например, полное сканирование таблиц, интенсивная запись). ' ;
		line_count=line_count+1;
		result_str[line_count] = 'Неэффективные запросы, вызывающие чрезмерный I/O.	 ' ;
		line_count=line_count+1;
		
	ELSIF corr_io_bi > 0.5 AND corr_io_bi <= 0.7
	THEN 	
		result_str[line_count] = 'WARNING : Высокая корреляция (IO и bi)' ; 
		line_count=line_count+1;
		---------------------------------------------------------------------------------------------
		result_str[line_count] = 'Причины и последствия: ' ; 
		line_count=line_count+1;
		result_str[line_count] = 'Активная работа с большими объемами данных  ' ;
		line_count=line_count+1;
		result_str[line_count] = '(например, полное сканирование таблиц, интенсивная запись). ' ;
		line_count=line_count+1;
		result_str[line_count] = 'Неэффективные запросы, вызывающие чрезмерный I/O.	 ' ;
		line_count=line_count+1;	
	ELSE
		result_str[line_count] = 'INFO : Слабая или средняя корреляция (IO и bi)' ; 
		line_count=line_count+1;
		---------------------------------------------------------------------------------------------
		result_str[line_count] = 'Причины и последствия: ' ; 
		line_count=line_count+1;
		result_str[line_count] = 'Активная работа с большими объемами данных  ' ;
		line_count=line_count+1;
		result_str[line_count] = '(например, полное сканирование таблиц, интенсивная запись). ' ;
		line_count=line_count+1;
		result_str[line_count] = 'Неэффективные запросы, вызывающие чрезмерный I/O.	 ' ;
		line_count=line_count+1;
	END IF ; 
	line_count=line_count+1;	
	--Высокие значения bi(blocks in) 
	-------------------------------------------------------
	
	-----------------------------------------------------------------------------
	--Высокие значения bo(blocks out) 
	IF min_max_rec.min_vmstat_curr_io != min_max_rec.max_curr_io AND 
		   min_max_rec.min_io_bo_long != min_max_rec.max_io_bo_long
	THEN 
		-- УГОЛ НАКЛОНА ЛИНИИ НАИМЕНЬШИХ КВАДРАТОВ bo
		-- 	линия регрессии  скорости  : Y = a + bX
			BEGIN
				WITH stats AS 
				(
				  SELECT 
					AVG(t.curr_timepoint::DOUBLE PRECISION) as avg1, 
					STDDEV(t.curr_timepoint::DOUBLE PRECISION) as std1,
					AVG(s.io_bo_long::DOUBLE PRECISION) as avg2, 
					STDDEV(s.io_bo_long::DOUBLE PRECISION) as std2
				  FROM
					os_stat_vmstat_median s JOIN tmp_timepoints t ON ( s.curr_timestamp  = t.curr_timestamp )
				  WHERE 
					t.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				),
				standardized_data AS 
				(
					SELECT 
						(t.curr_timepoint::DOUBLE PRECISION - avg1) / std1 as x_z,
						(s.io_bo_long::DOUBLE PRECISION - avg2) / std2 as y_z
					FROM
						os_stat_vmstat_median s JOIN tmp_timepoints t ON ( s.curr_timestamp  = t.curr_timestamp ) , stats
					WHERE 
						t.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				)	
				SELECT
					REGR_SLOPE(y_z, x_z) as slope, --bo
					ATAN(REGR_SLOPE(y_z, x_z)) * 180 / PI() as slope_angle_degrees, --угол наклона
					REGR_R2(y_z, x_z) as r_squared -- Коэффициент детерминации
				INTO 
					bo_regr_rec
				FROM standardized_data;
			EXCEPTION
			  --STDDEV(s.op_speed_long::DOUBLE PRECISION) = 0  
			  WHEN division_by_zero THEN  -- Конкретное исключение для деления на ноль
				SELECT 
					1.0 as slope, --b
					0.0  as slope_angle_degrees, --угол наклона
					0.0  as r_squared -- Коэффициент детерминации
				INTO 
				bo_regr_rec ;
			END;
		-- УГОЛ НАКЛОНА ЛИНИИ НАИМЕНЬШИХ КВАДРАТОВ bo
		-- 	линия регрессии  скорости  : Y = a + bX
			
			IF  bo_regr_rec.slope_angle_degrees <= 0 
			THEN 
				result_str[line_count] = 'Количество bo(записанные на устройства ) - не растёт | ' ;
				line_count=line_count+1;	
			ELSE 
				WITH 
				io_values AS
				(
					SELECT 
						cl.curr_timestamp , curr_io  AS curr_io 
					FROM 
						os_stat_vmstat_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
					WHERE				
						cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
						AND curr_io > 0 
				) ,
				io_bo_values AS
				(
					SELECT 
						cl.curr_timestamp , io_bo_long  AS io_bo_long 
					FROM 
						os_stat_vmstat_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
					WHERE				
						cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
						AND io_bo_long > 0 					
				) 
				SELECT COALESCE( corr( v1.curr_io , v2.io_bo_long ) , 0 ) AS correlation_value 
				INTO corr_io_bo
				FROM
					io_values v1 JOIN io_bo_values v2 ON ( v1.curr_timestamp = v2.curr_timestamp ) ;  
			END IF;
	ELSE
		corr_io_bo = 0 ;
	END IF;

	result_str[line_count] = 'Корреляция ожиданий IO и bo(blocks out):| ' ||
	REPLACE ( TO_CHAR( ROUND( corr_io_bo::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' ); 
	line_count=line_count+1;	

	IF corr_io_bo <= 0 
	THEN 
		result_str[line_count] = 'OK: Корреляция IO и bo(blocks out) - отрицательная или отсутствует' ; 
		line_count=line_count+1;				
	ELSIF corr_io_bo > 0.7 
	THEN 
		result_str[line_count] = 'ALARM : Очень высокая корреляция (IO и bo)' ; 
		line_count=line_count+1;
		---------------------------------------------------------------------------------------------
		result_str[line_count] = 'Причины и последствия: ' ; 
		line_count=line_count+1;
		result_str[line_count] = 'Активная работа с большими объемами данных  ' ;
		line_count=line_count+1;
		result_str[line_count] = '(например, полное сканирование таблиц, интенсивная запись). ' ;
		line_count=line_count+1;
		result_str[line_count] = 'Неэффективные запросы, вызывающие чрезмерный I/O.	 ' ;
		line_count=line_count+1;
		
	ELSIF corr_io_bo > 0.5 AND corr_io_bo <= 0.7
	THEN 	
		result_str[line_count] = 'WARNING : Высокая корреляция (IO и bo)' ; 
		line_count=line_count+1;
		---------------------------------------------------------------------------------------------
		result_str[line_count] = 'Причины и последствия: ' ; 
		line_count=line_count+1;
		result_str[line_count] = 'Активная работа с большими объемами данных  ' ;
		line_count=line_count+1;
		result_str[line_count] = '(например, полное сканирование таблиц, интенсивная запись). ' ;
		line_count=line_count+1;
		result_str[line_count] = 'Неэффективные запросы, вызывающие чрезмерный I/O.	 ' ;
		line_count=line_count+1;	
	ELSE
		result_str[line_count] = 'INFO : Слабая или средняя корреляция (IO и bo)' ; 
		line_count=line_count+1;
		---------------------------------------------------------------------------------------------
		result_str[line_count] = 'Причины и последствия: ' ; 
		line_count=line_count+1;
		result_str[line_count] = 'Активная работа с большими объемами данных  ' ;
		line_count=line_count+1;
		result_str[line_count] = '(например, полное сканирование таблиц, интенсивная запись). ' ;
		line_count=line_count+1;
		result_str[line_count] = 'Неэффективные запросы, вызывающие чрезмерный I/O.	 ' ;
		line_count=line_count+1;
	END IF ; 
	line_count=line_count+1;	
	--Высокие значения bo(blocks out) 
	-------------------------------------------------------
	
	---------------------------------------------------------------------------
	--Низкое значение id (idle time) при высоком us (user time) или sy (system time)
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
		(total_counter::DOUBLE PRECISION / timestamp_counter::DOUBLE PRECISION)*100.0 
	INTO
		us_sy_pct
	FROM cpu_counter ;
	
	result_str[line_count] = 'us(user time) + sy(system time) (% свыше 80%) | '|| REPLACE ( TO_CHAR( ROUND( us_sy_pct::numeric , 2 ) , '000000000000D0000' ) , '.' , ',' )  ; 
	line_count=line_count+1;
	
	IF us_sy_pct < 25.0 
	THEN 
		result_str[line_count] = 'OK: менее 25% наблюдений - wa > 10%' ; 
		line_count=line_count+1;		
	ELSIF us_sy_pct > 25.0 AND us_sy_pct <= 50.0
	THEN 
		result_str[line_count] = 'WARNING: 25-50% наблюдений - wa > 10%' ; 
		line_count=line_count+1;
		result_str[line_count] = 'Высокая нагрузка на CPU из-за сложных запросов (агрегации, JOINs).';
		line_count=line_count+1;
		result_str[line_count] = 'Конкуренция за ресурсы CPU (например, из-за параллельных процессов).';
		line_count=line_count+1;
		result_str[line_count] = 'Резкий рост sy может указывать на проблемы с системными вызовами (например, частое переключение контекста).';
		line_count=line_count+1;
	ELSE 
		result_str[line_count] = 'ALARM: более 50% наблюдений - wa > 10%' ; 
		line_count=line_count+1;
		result_str[line_count] = 'Высокая нагрузка на CPU из-за сложных запросов (агрегации, JOINs).';
		line_count=line_count+1;
		result_str[line_count] = 'Конкуренция за ресурсы CPU (например, из-за параллельных процессов).';
		line_count=line_count+1;
		result_str[line_count] = 'Резкий рост sy может указывать на проблемы с системными вызовами (например, частое переключение контекста).';
		line_count=line_count+1;
	END IF ;
	
	--------------------------------------------------------------------
	-- us — user time
	IF min_max_rec.min_curr_lwlock != min_max_rec.max_curr_lwlock AND 
	   min_max_rec.min_cpu_us_long != min_max_rec.max_cpu_us_long 
	THEN 
		-- УГОЛ НАКЛОНА ЛИНИИ НАИМЕНЬШИХ КВАДРАТОВ bi
		-- 	линия регрессии  скорости  : Y = a + bX
			BEGIN
				WITH stats AS 
				(
				  SELECT 
					AVG(t.curr_timepoint::DOUBLE PRECISION) as avg1, 
					STDDEV(t.curr_timepoint::DOUBLE PRECISION) as std1,
					AVG(s.cpu_us_long::DOUBLE PRECISION) as avg2, 
					STDDEV(s.cpu_us_long::DOUBLE PRECISION) as std2
				  FROM
					os_stat_vmstat_median s JOIN tmp_timepoints t ON ( s.curr_timestamp  = t.curr_timestamp )
				  WHERE 
					t.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				),
				standardized_data AS 
				(
					SELECT 
						(t.curr_timepoint::DOUBLE PRECISION - avg1) / std1 as x_z,
						(s.cpu_us_long::DOUBLE PRECISION - avg2) / std2 as y_z
					FROM
						os_stat_vmstat_median s JOIN tmp_timepoints t ON ( s.curr_timestamp  = t.curr_timestamp ) , stats
					WHERE 
						t.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				)	
				SELECT
					REGR_SLOPE(y_z, x_z) as slope, --bi
					ATAN(REGR_SLOPE(y_z, x_z)) * 180 / PI() as slope_angle_degrees, --угол наклона
					REGR_R2(y_z, x_z) as r_squared -- Коэффициент детерминации
				INTO 
					us_regr_rec
				FROM standardized_data;
			EXCEPTION
			  --STDDEV(s.op_speed_long::DOUBLE PRECISION) = 0  
			  WHEN division_by_zero THEN  -- Конкретное исключение для деления на ноль
				SELECT 
					1.0 as slope, --b
					0.0  as slope_angle_degrees, --угол наклона
					0.0  as r_squared -- Коэффициент детерминации
				INTO 
				us_regr_rec ;
			END;
		-- УГОЛ НАКЛОНА ЛИНИИ НАИМЕНЬШИХ КВАДРАТОВ bi
		-- 	линия регрессии  скорости  : Y = a + bX
			
			IF  us_regr_rec.slope_angle_degrees <= 0 
			THEN 
				result_str[line_count] = 'us (user time)- не растёт | ' ;
				line_count=line_count+1;	
			ELSE 
				WITH 
				lwlock_values AS
				(
					SELECT 
						cl.curr_timestamp , curr_lwlock  AS curr_lwlock 
					FROM 
						os_stat_vmstat_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
					WHERE				
						cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
						AND curr_lwlock > 0 
				) ,
				lwlock_us_values AS
				(
					SELECT 
						cl.curr_timestamp , cpu_us_long  AS cpu_us_long 
					FROM 
						os_stat_vmstat_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
					WHERE				
						cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
						AND cpu_us_long > 0 					
				) 
				SELECT COALESCE( corr( v1.curr_lwlock , v2.cpu_us_long ) , 0 ) AS correlation_value 
				INTO corr_lwlock_us
				FROM
					lwlock_values v1 JOIN lwlock_us_values v2 ON ( v1.curr_timestamp = v2.curr_timestamp ) ; 			
			END IF;
			
	ELSE
		corr_lwlock_us = 0 ;
	END IF;

	line_count=line_count+1;	
	result_str[line_count] = 'Корреляция LWLock и us(user time):| ' ||
	REPLACE ( TO_CHAR( ROUND( corr_lwlock_us::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' ); 
	line_count=line_count+1;	

	IF corr_lwlock_us <= 0 
	THEN 
		result_str[line_count] = 'OK: Корреляция LWLock и us(user time) - отрицательная или отсутствует' ; 
		line_count=line_count+1;				
	ELSIF corr_lwlock_us > 0.7 
	THEN 
		result_str[line_count] = 'ALARM : Очень высокая корреляция (LWLock-us)' ; 
		line_count=line_count+1;
		---------------------------------------------------------------------------------------------
		result_str[line_count] = 'Причины и последствия: ' ; 
		line_count=line_count+1;
		result_str[line_count] = 'Высокая нагрузка на CPU из-за сложных запросов (агрегации, JOINs).' ;
		line_count=line_count+1;
		result_str[line_count] = 'Конкуренция за ресурсы CPU (например, из-за параллельных процессов).' ;
		line_count=line_count+1;		
	ELSIF corr_lwlock_us > 0.5 AND corr_lwlock_us <= 0.7
	THEN 	
		result_str[line_count] = 'WARNING : Высокая корреляция (LWLock-us)' ; 
		line_count=line_count+1;
		---------------------------------------------------------------------------------------------
		result_str[line_count] = 'Причины и последствия: ' ; 
		line_count=line_count+1;
		result_str[line_count] = 'Высокая нагрузка на CPU из-за сложных запросов (агрегации, JOINs).' ;
		line_count=line_count+1;
		result_str[line_count] = 'Конкуренция за ресурсы CPU (например, из-за параллельных процессов).' ;
		line_count=line_count+1;	
	ELSE
		result_str[line_count] = 'INFO : Слабая или средняя корреляция (LWLock-us)' ; 
		line_count=line_count+1;
		---------------------------------------------------------------------------------------------
		result_str[line_count] = 'Причины и последствия: ' ; 
		line_count=line_count+1;
		result_str[line_count] = 'Высокая нагрузка на CPU из-за сложных запросов (агрегации, JOINs).' ;
		line_count=line_count+1;
		result_str[line_count] = 'Конкуренция за ресурсы CPU (например, из-за параллельных процессов).' ;
		line_count=line_count+1;
	END IF ; 
	line_count=line_count+1;	
	-- us — user time
	--------------------------------------------------------------------
	
	--------------------------------------------------------------------
	-- sy — system time
	IF min_max_rec.min_curr_lwlock != min_max_rec.max_curr_lwlock AND 
	   min_max_rec.min_cpu_sy_long != min_max_rec.max_cpu_sy_long 
	THEN 
		-- УГОЛ НАКЛОНА ЛИНИИ НАИМЕНЬШИХ КВАДРАТОВ bi
		-- 	линия регрессии  скорости  : Y = a + bX
			BEGIN
				WITH stats AS 
				(
				  SELECT 
					AVG(t.curr_timepoint::DOUBLE PRECISION) as avg1, 
					STDDEV(t.curr_timepoint::DOUBLE PRECISION) as std1,
					AVG(s.cpu_sy_long::DOUBLE PRECISION) as avg2, 
					STDDEV(s.cpu_sy_long::DOUBLE PRECISION) as std2
				  FROM
					os_stat_vmstat_median s JOIN tmp_timepoints t ON ( s.curr_timestamp  = t.curr_timestamp )
				  WHERE 
					t.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				),
				standardized_data AS 
				(
					SELECT 
						(t.curr_timepoint::DOUBLE PRECISION - avg1) / std1 as x_z,
						(s.cpu_sy_long::DOUBLE PRECISION - avg2) / std2 as y_z
					FROM
						os_stat_vmstat_median s JOIN tmp_timepoints t ON ( s.curr_timestamp  = t.curr_timestamp ) , stats
					WHERE 
						t.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
				)	
				SELECT
					REGR_SLOPE(y_z, x_z) as slope, --bi
					ATAN(REGR_SLOPE(y_z, x_z)) * 180 / PI() as slope_angle_degrees, --угол наклона
					REGR_R2(y_z, x_z) as r_squared -- Коэффициент детерминации
				INTO 
					sy_regr_rec
				FROM standardized_data;
			EXCEPTION
			  --STDDEV(s.op_speed_long::DOUBLE PRECISION) = 0  
			  WHEN division_by_zero THEN  -- Конкретное исключение для деления на ноль
				SELECT 
					1.0 as slope, --b
					0.0  as slope_angle_degrees, --угол наклона
					0.0  as r_squared -- Коэффициент детерминации
				INTO 
				sy_regr_rec ;
			END;
		-- УГОЛ НАКЛОНА ЛИНИИ НАИМЕНЬШИХ КВАДРАТОВ bi
		-- 	линия регрессии  скорости  : Y = a + bX
			
			IF  sy_regr_rec.slope_angle_degrees <= 0 
			THEN 
				result_str[line_count] = 'sy (system time)- не растёт | ' ;
				line_count=line_count+1;	
			ELSE 
				WITH 
				lwlock_values AS
				(
					SELECT 
						cl.curr_timestamp , curr_lwlock  AS curr_lwlock 
					FROM 
						os_stat_vmstat_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
					WHERE				
						cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
						AND curr_lwlock > 0 
				) ,
				lwlock_sy_values AS
				(
					SELECT 
						cl.curr_timestamp , cpu_sy_long  AS cpu_sy_long 
					FROM 
						os_stat_vmstat_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
					WHERE				
						cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
						AND cpu_sy_long > 0 					
				) 
				SELECT COALESCE( corr( v1.curr_lwlock , v2.cpu_sy_long ) , 0 ) AS correlation_value 
				INTO corr_lwlock_sy
				FROM
					lwlock_values v1 JOIN lwlock_sy_values v2 ON ( v1.curr_timestamp = v2.curr_timestamp ) ; 			
			END IF;
			
	ELSE
		corr_lwlock_sy = 0 ;
	END IF;

	result_str[line_count] = 'Корреляция LWLock и sy(system time):| ' ||
	REPLACE ( TO_CHAR( ROUND( corr_lwlock_sy::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' ); 
	line_count=line_count+1;	

	IF corr_lwlock_sy <= 0 
	THEN 
		result_str[line_count] = 'OK: Корреляция LWLock и sy(system time) - отрицательная или отсутствует' ; 
		line_count=line_count+1;				
	ELSIF corr_lwlock_sy > 0.7 
	THEN 
		result_str[line_count] = 'ALARM : Очень высокая корреляция (LWLock-sy)' ; 
		line_count=line_count+1;
		---------------------------------------------------------------------------------------------
		result_str[line_count] = 'Причины и последствия: ' ; 
		line_count=line_count+1;
		result_str[line_count] = 'Высокая нагрузка на CPU из-за сложных запросов (агрегации, JOINs).' ;
		line_count=line_count+1;
		result_str[line_count] = 'Конкуренция за ресурсы CPU (например, из-за параллельных процессов).' ;
		line_count=line_count+1;	
		result_str[line_count] = 'Резкий рост sy может указывать на проблемы с системными вызовами (например, частое переключение контекста).';
		line_count=line_count+1;		
	ELSIF corr_lwlock_sy > 0.5 AND corr_lwlock_sy <= 0.7
	THEN 	
		result_str[line_count] = 'WARNING : Высокая корреляция (LWLock-sy)' ; 
		line_count=line_count+1;
		---------------------------------------------------------------------------------------------
		result_str[line_count] = 'Причины и последствия: ' ; 
		line_count=line_count+1;
		result_str[line_count] = 'Высокая нагрузка на CPU из-за сложных запросов (агрегации, JOINs).' ;
		line_count=line_count+1;
		result_str[line_count] = 'Конкуренция за ресурсы CPU (например, из-за параллельных процессов).' ;
		line_count=line_count+1;	
		result_str[line_count] = 'Резкий рост sy может указывать на проблемы с системными вызовами (например, частое переключение контекста).';
		line_count=line_count+1;				
	ELSE
		result_str[line_count] = 'INFO : Слабая или средняя корреляция (LWLock-sy)' ; 
		line_count=line_count+1;
		---------------------------------------------------------------------------------------------
		result_str[line_count] = 'Причины и последствия: ' ; 
		line_count=line_count+1;
		result_str[line_count] = 'Высокая нагрузка на CPU из-за сложных запросов (агрегации, JOINs).' ;
		line_count=line_count+1;
		result_str[line_count] = 'Конкуренция за ресурсы CPU (например, из-за параллельных процессов).' ;
		line_count=line_count+1;
		result_str[line_count] = 'Резкий рост sy может указывать на проблемы с системными вызовами (например, частое переключение контекста).';
		line_count=line_count+1;				
	END IF ; 
	line_count=line_count+1;	
	-- sy — system time
	--------------------------------------------------------------------	
	--Низкое значение id (idle time) при высоком us (user time) или sy (system time)
	---------------------------------------------------------------------------
	
	
	---------------------------------------------------------------------------
	-- Высокие si, so (активный своппинг)	
	--IO - si 
		IF min_max_rec.min_vmstat_curr_io != min_max_rec.max_curr_io AND 
		   min_max_rec.min_swap_si_long != min_max_rec.max_swap_si_long
		THEN 
			WITH 
			io_values AS
			(
				SELECT 
					cl.curr_timestamp , curr_io  AS curr_io 
				FROM 
					os_stat_vmstat_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
				WHERE				
					cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
					AND curr_io > 0 
			) ,
			swap_si_values AS
			(
				SELECT 
					cl.curr_timestamp , swap_si_long  AS swap_si_long 
				FROM 
					os_stat_vmstat_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
				WHERE				
					cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
					AND swap_si_long > 0 					
			) 
			SELECT COALESCE( corr( v1.curr_io , v2.swap_si_long ) , 0 ) AS correlation_value 
			INTO corr_io_si
			FROM
				io_values v1 JOIN swap_si_values v2 ON ( v1.curr_timestamp = v2.curr_timestamp ) ;
		ELSE
			corr_io_si = 0 ;
		END IF;

		
		result_str[line_count] = 'Корреляция ожиданий IO и si (swap in): Объем данных, загружаемых с свопа в оперативную память: IO-si | ' ||
		REPLACE ( TO_CHAR( ROUND( corr_io_si::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' ); 
		line_count=line_count+1;	

		IF corr_io_si <= 0 
		THEN 
			result_str[line_count] = 'OK: Корреляция (IO-si)  - отрицательная или отсутствует' ; 
			line_count=line_count+1;				
		ELSIF corr_io_si > 0.7 
		THEN 
			result_str[line_count] = 'ALARM : Очень высокая корреляция (IO-si)' ; 
			line_count=line_count+1;
			---------------------------------------------------------------------------------------------
			result_str[line_count] = 'Причины и последствия: ' ; 
			line_count=line_count+1;	
			result_str[line_count] = 'Системе не хватает оперативной памяти. Данные, которые должны быть в кеше, вытесняются на медленный диск (своп),' ; 
			line_count=line_count+1;	
			result_str[line_count] = 'что создает дополнительную дисковую нагрузку и увеличивает время чтения.' ; 
			line_count=line_count+1;				
			result_str[line_count] = 'Чтение страниц БД с диска, которые должны были быть в кеше, теперь требует еще и своппинга,' ; 
			line_count=line_count+1;	
			result_str[line_count] = 'что создает двойную нагрузку на I/O и катастрофически замедляет работу.' ; 
			line_count=line_count+1;	
			---------------------------------------------------------------------------------------------	
								
		ELSIF corr_io_si > 0.5 AND corr_io_si <= 0.7
		THEN 
			result_str[line_count] = 'WARNING : Высокая корреляция (IO-si)' ; 
			line_count=line_count+1;
			---------------------------------------------------------------------------------------------
			result_str[line_count] = 'Причины и последствия: ' ; 
			line_count=line_count+1;	
			result_str[line_count] = 'Системе не хватает оперативной памяти. Данные, которые должны быть в кеше, вытесняются на медленный диск (своп),' ; 
			line_count=line_count+1;	
			result_str[line_count] = 'что создает дополнительную дисковую нагрузку и увеличивает время чтения.' ; 
			line_count=line_count+1;				
			result_str[line_count] = 'Чтение страниц БД с диска, которые должны были быть в кеше, теперь требует еще и своппинга,' ; 
			line_count=line_count+1;	
			result_str[line_count] = 'что создает двойную нагрузку на I/O и катастрофически замедляет работу.' ; 
			line_count=line_count+1;	
			---------------------------------------------------------------------------------------------						
		ELSE
			result_str[line_count] = 'INFO : Слабая или средняя корреляция (IO-si)' ; 
			line_count=line_count+1;	

			---------------------------------------------------------------------------------------------
			result_str[line_count] = 'Причины и последствия: ' ; 
			line_count=line_count+1;	
			result_str[line_count] = 'Системе не хватает оперативной памяти. Данные, которые должны быть в кеше, вытесняются на медленный диск (своп),' ; 
			line_count=line_count+1;	
			result_str[line_count] = 'что создает дополнительную дисковую нагрузку и увеличивает время чтения.' ; 
			line_count=line_count+1;				
			result_str[line_count] = 'Чтение страниц БД с диска, которые должны были быть в кеше, теперь требует еще и своппинга,' ; 
			line_count=line_count+1;	
			result_str[line_count] = 'что создает двойную нагрузку на I/O и катастрофически замедляет работу.' ; 
			line_count=line_count+1;	
			---------------------------------------------------------------------------------------------							
		END IF;
		line_count=line_count+1;
		--------------------------------------------------------------------------------------------------------------------------
	--IO - si 
	------------------------------------------------------------------------------------------------------------------------------
	--IO - so 
		IF min_max_rec.min_vmstat_curr_io != min_max_rec.max_curr_io AND 
		   min_max_rec.min_swap_so_long != min_max_rec.max_swap_so_long
		THEN 
			WITH 
			io_values AS
			(
				SELECT 
					cl.curr_timestamp , curr_io  AS curr_io 
				FROM 
					os_stat_vmstat_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
				WHERE				
					cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
					AND curr_io > 0 
			) ,
			swap_so_values AS
			(
				SELECT 
					cl.curr_timestamp , swap_so_long  AS swap_so_long 
				FROM 
					os_stat_vmstat_median cl JOIN cluster_stat_median cl_w ON ( cl.curr_timestamp = cl_w.curr_timestamp)
				WHERE				
					cl.curr_timestamp BETWEEN min_timestamp AND max_timestamp 
					AND swap_so_long > 0 					
			) 
			SELECT COALESCE( corr( v1.curr_io , v2.swap_so_long ) , 0 ) AS correlation_value 
			INTO corr_io_so
			FROM
				io_values v1 JOIN swap_so_values v2 ON ( v1.curr_timestamp = v2.curr_timestamp ) ;
		ELSE
			corr_io_so = 0 ;
		END IF;
		
		result_str[line_count] = 'Корреляция ожиданий IO и si (swap in): Объем данных, загружаемых из оперативную память в своп: IO-so | ' ||
		REPLACE ( TO_CHAR( ROUND( corr_io_so::numeric , 4 ) , '000000000000D0000' ) , '.' , ',' ); 
		line_count=line_count+1;	

		IF corr_io_so <= 0 
		THEN 
			result_str[line_count] = 'OK: Корреляция (IO-si)  - отрицательная или отсутствует' ; 
			line_count=line_count+1;				
		ELSIF corr_io_so > 0.7 
		THEN 
			result_str[line_count] = 'ALARM : Очень высокая корреляция (IO-si)' ; 
			line_count=line_count+1;
			---------------------------------------------------------------------------------------------
			result_str[line_count] = 'Причины и последствия: ' ; 
			line_count=line_count+1;	
			result_str[line_count] = 'Системе не хватает оперативной памяти. Данные, которые должны быть в кеше, вытесняются на медленный диск (своп),' ; 
			line_count=line_count+1;	
			result_str[line_count] = 'что создает дополнительную дисковую нагрузку и увеличивает время чтения.' ; 
			line_count=line_count+1;				
			result_str[line_count] = 'Чтение страниц БД с диска, которые должны были быть в кеше, теперь требует еще и своппинга,' ; 
			line_count=line_count+1;	
			result_str[line_count] = 'что создает двойную нагрузку на I/O и катастрофически замедляет работу.' ; 
			line_count=line_count+1;	
			---------------------------------------------------------------------------------------------	
								
		ELSIF corr_io_so > 0.5 AND corr_io_so <= 0.7
		THEN 
			result_str[line_count] = 'WARNING : Высокая корреляция (IO-si)' ; 
			line_count=line_count+1;
			---------------------------------------------------------------------------------------------
			result_str[line_count] = 'Причины и последствия: ' ; 
			line_count=line_count+1;	
			result_str[line_count] = 'Системе не хватает оперативной памяти. Данные, которые должны быть в кеше, вытесняются на медленный диск (своп),' ; 
			line_count=line_count+1;	
			result_str[line_count] = 'что создает дополнительную дисковую нагрузку и увеличивает время чтения.' ; 
			line_count=line_count+1;				
			result_str[line_count] = 'Чтение страниц БД с диска, которые должны были быть в кеше, теперь требует еще и своппинга,' ; 
			line_count=line_count+1;	
			result_str[line_count] = 'что создает двойную нагрузку на I/O и катастрофически замедляет работу.' ; 
			line_count=line_count+1;	
			---------------------------------------------------------------------------------------------						
		ELSE
			result_str[line_count] = 'INFO : Слабая или средняя корреляция (IO-si)' ; 
			line_count=line_count+1;	

			---------------------------------------------------------------------------------------------
			result_str[line_count] = 'Причины и последствия: ' ; 
			line_count=line_count+1;	
			result_str[line_count] = 'Системе не хватает оперативной памяти. Данные, которые должны быть в кеше, вытесняются на медленный диск (своп),' ; 
			line_count=line_count+1;	
			result_str[line_count] = 'что создает дополнительную дисковую нагрузку и увеличивает время чтения.' ; 
			line_count=line_count+1;				
			result_str[line_count] = 'Чтение страниц БД с диска, которые должны были быть в кеше, теперь требует еще и своппинга,' ; 
			line_count=line_count+1;	
			result_str[line_count] = 'что создает двойную нагрузку на I/O и катастрофически замедляет работу.' ; 
			line_count=line_count+1;	
			---------------------------------------------------------------------------------------------							
		END IF;
		line_count=line_count+1;
		--------------------------------------------------------------------------------------------------------------------------
	--IO - so 
	-- 2. Высокие si, so (активный своппинг)	
	---------------------------------------------------------------------------

	
  return result_str ; 
END
$$ LANGUAGE plpgsql  ;
COMMENT ON FUNCTION reports_waitings_os_corr IS 'Корреляция ожиданий СУБД и метрик vmstat';
-- Корреляция ожиданий СУБД и метрик vmstat
-------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- wait_event_kb_functions.sql
-- version 3.0
--------------------------------------------------------------------------------
--
-- advice_for_wait_event  Получить ответ нейросети по данному событию ожидания
-- get_min_id_4_tmp_wait_events ПОЛУЧИТЬ МИНИМАЛЬНЫЙ id ИЗ tmp_wait_events
-- get_max_id_4_tmp_wait_events ПОЛУЧИТЬ МАКСИМАЛЬНЫЙ id ИЗ tmp_wait_events
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Получить ответ нейросети по данному событию ожидания
CREATE OR REPLACE FUNCTION advice_for_wait_event_by_id( curr_id integer) RETURNS text AS $$
DECLARE 
  request_text text ;
  tmp_wait_events_rec record ;
BEGIN
	SELECT 
		* 
	INTO
		tmp_wait_events_rec 
	FROM 
		tmp_wait_events
	WHERE id = curr_id ; 
	
	IF tmp_wait_events_rec.id IS NULL 
	THEN 
		return 'NEW';
	END IF ;

	SELECT 
		advice 
	INTO 
		request_text
	FROM 
		wait_event_knowledge_base
	WHERE 
		wait_event = tmp_wait_events_rec.wait_event ;	

	IF request_text IS NULL 
	THEN 
		return 'NEW';
	END IF ;

  return request_text ;
END
$$ LANGUAGE plpgsql ;
COMMENT ON FUNCTION advice_for_wait_event_by_id IS 'Получить ответ нейросети по данному событию ожидания';
-- Получить ответ нейросети по данному событию ожидания
-------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- ПОЛУЧИТЬ МИНИМАЛЬНЫЙ id ИЗ tmp_wait_events
CREATE OR REPLACE FUNCTION get_min_id_4_tmp_wait_events( curr_wait_event_type text ) RETURNS integer AS $$
DECLARE
min_id integer ;
BEGIN
	SELECT MIN(id)
	INTO min_id
	FROM tmp_wait_events 
	WHERE wait_event_type = curr_wait_event_type; 
	
	IF min_id IS NULL 
	THEN 
	 return 0;
	END IF ;
	
	return min_id ; 
END
$$ LANGUAGE plpgsql  ;
COMMENT ON FUNCTION get_min_id_4_tmp_wait_events IS 'ПОЛУЧИТЬ МИНИМАЛЬНЫЙ id ИЗ tmp_wait_events';
-- ПОЛУЧИТЬ МИНИМАЛЬНЫЙ id ИЗ tmp_wait_events
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- ПОЛУЧИТЬ МАКСИМАЛЬНЫЙ id ИЗ tmp_wait_events
CREATE OR REPLACE FUNCTION get_max_id_4_tmp_wait_events( curr_wait_event_type text ) RETURNS integer AS $$
DECLARE
max_id integer ;
BEGIN
	SELECT MAX(id)
	INTO max_id
	FROM tmp_wait_events 
	WHERE wait_event_type = curr_wait_event_type; 
	
	IF max_id IS NULL 
	THEN 
	 return 0;
	END IF ;
	
	return max_id ; 
END
$$ LANGUAGE plpgsql  ;
COMMENT ON FUNCTION get_max_id_4_tmp_wait_events IS 'ПОЛУЧИТЬ МАКСИМАЛЬНЫЙ id ИЗ tmp_wait_events';
-- ПОЛУЧИТЬ МАКСИМАЛЬНЫЙ id ИЗ tmp_wait_events
--------------------------------------------------------------------------------


-------------------------------------------------------------------------------------
-- wait_event_kb_tables.sql
-- version 3.0
-------------------------------------------------------------------------------------
-- База знаний по ответам нейросети на промпт 
-- Как снизить количество событий ожидания wait_event=XXX в СУБД PostgreSQL?
-------------------------------------------------------------------------------------
-- База знаний по ответам нейросети
DROP TABLE IF EXISTS wait_event_knowledge_base ; 
CREATE UNLOGGED TABLE wait_event_knowledge_base
(  
  wait_event text , --Событие ожидания
  advice text       --Совет нейросети по снижению ожиданий
);

ALTER TABLE wait_event_knowledge_base ADD CONSTRAINT wait_event_knowledge_base_pk PRIMARY KEY (wait_event);

COMMENT ON TABLE wait_event_knowledge_base IS 'База знаний по ответам нейросети';
COMMENT ON COLUMN wait_event_knowledge_base.wait_event IS 'Событие ожидания ';
COMMENT ON COLUMN wait_event_knowledge_base.advice IS 'Совет нейросети по снижению ожиданий ';
-- База знаний по ответам нейросети
-------------------------------------------------------------------------------------

-------------------------------------------------------------------------------------
-- События ожидания в ходе выполнения отчета report_wait_event_for_pareto
DROP TABLE IF EXISTS tmp_wait_events;
CREATE UNLOGGED TABLE tmp_wait_events
(
	id integer PRIMARY KEY ,
	wait_event_type text  , --тип ожидания
	wait_event text         --событие ожидания
);
COMMENT ON TABLE tmp_wait_events IS 'События ожидания в ходе выполнения отчета report_wait_event_for_pareto';
COMMENT ON COLUMN tmp_wait_events.wait_event_type IS 'тип ожидания';
COMMENT ON COLUMN tmp_wait_events.wait_event IS 'событие ожидания';
-- События ожидания в ходе выполнения отчета report_wait_event_for_pareto
-------------------------------------------------------------------------------------

----------------------------------------------------------------------------------
-- report_queryid_for_pareto_tables.sql
-- version 3.0
----------------------------------------------------------------------------------
-- Таблицы для обеспечения отчета и семантического анализа SQL запросов нейросетью

--------------------------------------------------
-- SQL запросы по wait_event_type
DROP TABLE IF EXISTS tmp_queryid_for_pareto ; 
CREATE UNLOGGED TABLE tmp_queryid_for_pareto
(
	id integer PRIMARY KEY ,
	wait_event_type text , 
	queryid bigint  
);

COMMENT ON TABLE tmp_queryid_for_pareto IS 'База знаний по ответам нейросети';
COMMENT ON COLUMN tmp_queryid_for_pareto.wait_event_type IS 'Тип события ожидания ';
COMMENT ON COLUMN tmp_queryid_for_pareto.queryid IS 'id SQL запроса';	
-- SQL запросы по wait_event_type
--------------------------------------------------

--------------------------------------------------------------------------------
-- report_queryid_for_pareto_functions.sql
-- version 3.0
--------------------------------------------------------------------------------------------
-- Сервисные функции для обеспечения отчета и семантического анализа SQL запросов нейросетью
--
-- get_min_id_tmp_queryid  Минимальный id для заданного wait_event_type
-- get_max_id_tmp_queryid  Максимальный id для заданного wait_event_type
-- get_queryid_by_id       queryid по заданному индексу 
-- get_sql_by_queryid      текст SQL запроса по queryid
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Минимальный id для заданного wait_event_type
CREATE OR REPLACE FUNCTION get_min_id_tmp_queryid( curr_wait_event_type text ) RETURNS integer AS $$
DECLARE
min_id integer ;
BEGIN
	SELECT MIN(id)
	INTO min_id
	FROM tmp_queryid_for_pareto 
	WHERE wait_event_type = curr_wait_event_type; 
	
	IF min_id IS NULL 
	THEN 
	 return 0;
	END IF ;
	
	return min_id ; 
END
$$ LANGUAGE plpgsql  ;
COMMENT ON FUNCTION get_min_id_tmp_queryid IS 'Минимальный id для заданного wait_event_type';
-- Минимальный id для заданного wait_event_type
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Максимальный id для заданного wait_event_type
CREATE OR REPLACE FUNCTION get_max_id_tmp_queryid( curr_wait_event_type text ) RETURNS integer AS $$
DECLARE
max_id integer ;
BEGIN
	SELECT MAX(id)
	INTO max_id
	FROM tmp_queryid_for_pareto 
	WHERE wait_event_type = curr_wait_event_type; 
	
	IF max_id IS NULL 
	THEN 
	 return 0;
	END IF ;
	
	return max_id ; 
END
$$ LANGUAGE plpgsql  ;
COMMENT ON FUNCTION get_max_id_tmp_queryid IS 'Максимальный id для заданного wait_event_type';
-- Максимальный id для заданного wait_event_type
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- queryid по заданному индексу 
CREATE OR REPLACE FUNCTION get_queryid_by_id( curr_id integer  ) RETURNS bigint AS $$
DECLARE
curr_queryid bigint ;
BEGIN
	SELECT queryid
	INTO curr_queryid
	FROM tmp_queryid_for_pareto 
	WHERE id = curr_id; 

	return curr_queryid ; 
END
$$ LANGUAGE plpgsql  ;
COMMENT ON FUNCTION get_queryid_by_id IS 'queryid по заданному индексу ';
-- queryid по заданному индексу 
-------------------------------------------------------------------------------


-------------------------------------------------------------------------------
-- текст SQL запроса по queryid
CREATE OR REPLACE FUNCTION get_sql_by_queryid( curr_queryid bigint  ) RETURNS text AS $$
DECLARE
 sql_text text ;
BEGIN
	SELECT 
		query 
	INTO 
		sql_text
	FROM 
		statement_stat_sql
	WHERE 
		queryid = curr_queryid;
		
 return sql_text ;
END
$$ LANGUAGE plpgsql  ;
COMMENT ON FUNCTION get_sql_by_queryid IS 'текст SQL запроса по queryid';
-- текст SQL запроса по queryid
-------------------------------------------------------------------------------


