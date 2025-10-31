-- scenario3.sql
-- version 1.0

CREATE OR REPLACE FUNCTION scenario3() RETURNS integer AS $$
DECLARE
 test_rec record ; 
 min_i bigint ; 
 max_i bigint ;
 current_aid bigint ; 
 current_tid bigint ; 
 current_bid bigint ; 
 current_delta bigint ; 
 counter bigint;
BEGIN

---------------------------------------------------
--СЦЕНАРИЙ 3 - INSERT ONLY

SELECT MIN(aid) INTO min_i FROM pgbench_accounts ; 
SELECT MAX(aid) INTO max_i FROM pgbench_accounts ; 
current_aid = floor(random() * (max_i - min_i + 1)) + min_i ;

SELECT MIN(tid) INTO min_i FROM pgbench_tellers ; 
SELECT MAX(tid) INTO max_i FROM pgbench_tellers ; 
current_tid = floor(random() * (max_i - min_i + 1)) + min_i ;


SELECT MIN(bid) INTO min_i FROM pgbench_branches ; 
SELECT MAX(bid) INTO max_i FROM pgbench_branches ; 
current_bid = floor(random() * (max_i - min_i + 1)) + min_i ;


SELECT random() * 1000.0 
INTO current_delta;

FOR counter IN 1..1000
LOOP
	INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) 
	VALUES (  current_tid , current_bid , current_aid , current_delta , CURRENT_TIMESTAMP );
END LOOP;

--ССЦЕНАРИЙ 3 - INSERT ONLY
---------------------------------------------------

 return 0 ; 
END
$$ LANGUAGE plpgsql;

