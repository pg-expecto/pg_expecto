-- scenario2.sql
-- 5.3
-- OLAP
-- INSERT

CREATE OR REPLACE FUNCTION scenario2() RETURNS integer AS $$
DECLARE
 min_i bigint ; 
 max_i bigint ;
 current_aid bigint ; 
 current_tid bigint ; 
 current_bid bigint ;  
 counter integer ;
BEGIN

	min_i = 1 ;
	SELECT MAX(aid) INTO max_i FROM pgbench_accounts ; 
	current_aid = floor(random() * (max_i - min_i + 1)) + min_i ;

	SELECT MAX(tid) INTO max_i FROM pgbench_tellers ; 
	current_tid = floor(random() * (max_i - min_i + 1)) + min_i ;

	SELECT MAX(bid) INTO max_i FROM pgbench_branches ; 
	current_bid = floor(random() * (max_i - min_i + 1)) + min_i ;
	
	INSERT INTO pgbench_history (
	tid, 
	bid, 
	aid, 
	delta, 
	mtime , 
	filler ) 
	VALUES (  
	current_tid , 
	current_bid , 
	current_aid , 
	random() * 1000.0 , 
	CURRENT_TIMESTAMP ,
	'1234567890123456789000');

 return 0 ; 
END
$$ LANGUAGE plpgsql;

