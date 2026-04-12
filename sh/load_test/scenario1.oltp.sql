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
-- scenario1.sql
-- 5.3
-- OLTP
-- SELECT

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
--СЦЕНАРИЙ 1 - SELECT
min_i = 1 ;
SELECT MAX(aid) INTO max_i FROM pgbench_accounts ;
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
