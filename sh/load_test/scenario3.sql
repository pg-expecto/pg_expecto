-- scenario3.sql
-- UPDATE

CREATE OR REPLACE FUNCTION scenario3() RETURNS integer AS $$
DECLARE
 min_i bigint ; 
 max_i bigint ;
 current_aid bigint ; 
 current_delta bigint ; 
BEGIN
---------------------------------------------------
--1)UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid;

current_delta = (ROUND( random ())::bigint)*10 + 1 ;

SELECT MIN(aid) INTO min_i FROM pgbench_accounts ; 
SELECT MAX(aid) INTO max_i FROM pgbench_accounts ; 
current_aid = floor(random() * (max_i - min_i + 1)) + min_i ;

--1)
UPDATE pgbench_accounts SET abalance = abalance + current_delta WHERE  aid = current_aid ; 
--1)


 return 0 ; 
END
$$ LANGUAGE plpgsql;


