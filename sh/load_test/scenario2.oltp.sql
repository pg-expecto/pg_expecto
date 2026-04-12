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
-- scenario2.sql
-- 5.3
-- OLTP
-- INSERT

CREATE OR REPLACE FUNCTION scenario2() RETURNS integer AS $$
DECLARE
 min_i bigint ; 
 max_i_aid bigint ;
 max_i_tid bigint ;
 max_i_bid bigint ;
 current_aid bigint ; 
 current_tid bigint ; 
 current_bid bigint ;  
 counter integer ;
BEGIN

	min_i = 1 ;
	SELECT MAX(aid) INTO max_i_aid FROM pgbench_accounts ; 
	SELECT MAX(tid) INTO max_i_tid FROM pgbench_tellers ; 
	SELECT MAX(bid) INTO max_i_bid FROM pgbench_branches ; 
	
	
	FOR counter IN 1..10
	LOOP 
		current_aid = floor(random() * (max_i_aid - min_i + 1)) + min_i ;
		current_tid = floor(random() * (max_i_tid - min_i + 1)) + min_i ;
		current_bid = floor(random() * (max_i_bid - min_i + 1)) + min_i ;
	
		INSERT INTO pgbench_history (
		tid, 
		bid, 
		aid, 
		delta, 
		mtime , 
		filler ,
		random_fill ) 
		VALUES (  
		current_tid , 
		current_bid , 
		current_aid , 
		random() * 1000.0 , 
		CURRENT_TIMESTAMP ,
		'1234567890123456789000', 
		random() * 1000.0 );
	END LOOP ;

 return 0 ; 
END
$$ LANGUAGE plpgsql;

