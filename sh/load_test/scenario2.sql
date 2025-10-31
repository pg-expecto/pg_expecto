-- scenario2.sql
-- version 1.0

CREATE OR REPLACE FUNCTION scenario2() RETURNS integer AS $$
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
--СЦЕНАРИЙ 2 - SELECT + UPDATE 
--1)UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid;
--2)SELECT abalance FROM pgbench_accounts WHERE aid = :aid;
--3)UPDATE pgbench_tellers SET tbalance = tbalance + :delta WHERE tid = :tid;
--4) UPDATE pgbench_branches SET bbalance = bbalance + :delta WHERE bid = :bid;

current_delta = (ROUND( random ())::bigint)*10 + 1 ;

SELECT MIN(aid) INTO min_i FROM pgbench_accounts ; 
SELECT MAX(aid) INTO max_i FROM pgbench_accounts ; 
current_aid = floor(random() * (max_i - min_i + 1)) + min_i ;

--1)
UPDATE pgbench_accounts SET abalance = abalance + current_delta WHERE  aid = current_aid ; 
--1)

--2)
SELECT abalance INTO test_rec FROM pgbench_accounts WHERE aid = current_aid ;
--2)

SELECT MIN(tid) INTO min_i FROM pgbench_tellers ; 
SELECT MAX(tid) INTO max_i FROM pgbench_tellers ; 
current_tid = floor(random() * (max_i - min_i + 1)) + min_i ;

--3)
UPDATE pgbench_tellers SET tbalance = tbalance + current_delta WHERE tid = current_tid ;
--3)

SELECT MIN(bid) INTO min_i FROM pgbench_branches ; 
SELECT MAX(bid) INTO max_i FROM pgbench_branches ; 
current_bid = floor(random() * (max_i - min_i + 1)) + min_i ;

--4)
UPDATE pgbench_branches SET bbalance = bbalance + current_delta WHERE bid = current_bid ; 
--4)
-- СЦЕНАРИЙ 2 - OLTP

 return 0 ; 
END
$$ LANGUAGE plpgsql;


