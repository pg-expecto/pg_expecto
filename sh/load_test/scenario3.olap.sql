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
-- 
-- scenario3.sql
-- 5.3
-- OLAP
-- UPDATE

CREATE OR REPLACE FUNCTION scenario3() RETURNS integer AS $$
DECLARE
 min_i bigint ; 
 max_i bigint ;
 current_aid bigint ; 
 current_delta bigint ; 
BEGIN
-- Генерация случайного сдвига
    current_delta := (ROUND(RANDOM())::BIGINT) * 10 + 1;

    -- Атомарный выбор и блокировка одной строки с пропуском заблокированных
    -- Используем LIMIT 1 и FOR UPDATE SKIP LOCKED для выбора одной доступной строки
	min_i = 1 ;
	SELECT MAX(aid) INTO max_i FROM pgbench_accounts ; 
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

 return 0 ; 
END
$$ LANGUAGE plpgsql;


