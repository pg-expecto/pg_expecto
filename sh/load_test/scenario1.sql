-- scenario1.sql
-- version 1.0

CREATE OR REPLACE FUNCTION scenario1() RETURNS integer AS $$
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
--СЦЕНАРИЙ 1 - SELECT ONLY
SELECT MAX(aid) INTO max_i FROM pgbench_accounts ;
SELECT MIN(aid) INTO min_i FROM pgbench_accounts ;
current_aid = floor(random() * (max_i - min_i + 1)) + min_i ;

select br.bbalance
into test_rec
from pgbench_branches br
join pgbench_accounts acc on (br.bid = acc.bid )
where acc.aid =  current_aid ;
--СЦЕНАРИЙ 1 - SELECT ONLY
---------------------------------------------------
return 0 ;
END
$$ LANGUAGE plpgsql;
