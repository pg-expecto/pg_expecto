-- scenario3.sql
-- 5.3
-- OLTP
-- UPDATE

CREATE OR REPLACE FUNCTION scenario3() RETURNS integer AS $$
DECLARE
 min_i bigint ; 
 max_i bigint ;
 current_aid bigint ; 
 current_delta bigint ;
 counter integer; 
BEGIN
    
    -- Атомарный выбор и блокировка одной строки с пропуском заблокированных
    -- Используем LIMIT 1 и FOR UPDATE SKIP LOCKED для выбора одной доступной строки
	min_i = 1 ;
	SELECT MAX(aid) INTO max_i FROM pgbench_accounts ; 
	
	FOR counter IN 1..10 
	LOOP 
		-- Генерация случайного сдвига
		current_delta := (ROUND(RANDOM())::BIGINT) * 10 + 1;

		current_aid = floor(random() * (max_i - min_i + 1)) + min_i ;
		
		SELECT aid INTO current_aid
		FROM pgbench_accounts
		WHERE aid = current_aid
		FOR UPDATE SKIP LOCKED;

		-- Если строка найдена — обновляем её
		IF current_aid IS NOT NULL THEN
			UPDATE pgbench_accounts
			SET abalance = abalance + current_delta
			WHERE aid = current_aid;
		END IF;
	END LOOP;

 return 0 ; 
END
$$ LANGUAGE plpgsql;


