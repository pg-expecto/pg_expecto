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

