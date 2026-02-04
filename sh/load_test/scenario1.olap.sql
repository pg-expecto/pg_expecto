-- scenario1.sql
-- 5.3
-- OLAP
-- SELECT


CREATE OR REPLACE FUNCTION scenario1() RETURNS integer AS $$
DECLARE
 test_rec record ;
BEGIN
	WITH 
	branch_summary AS (
		SELECT 
			b.bid,
			COUNT(a.aid) as account_count,
			SUM(a.abalance) as total_balance,
			AVG(a.abalance) as avg_balance
		FROM pgbench_branches b
		LEFT JOIN pgbench_accounts a ON b.bid = a.bid
		GROUP BY b.bid
	),
	transaction_summary AS (
		SELECT 
			h.bid,
			COUNT(*) as transaction_count,
			SUM(h.delta) as net_flow,
			COUNT(DISTINCT h.aid) as active_accounts,
			MIN(h.mtime) as first_transaction,
			MAX(h.mtime) as last_transaction
		FROM pgbench_history h
		WHERE h.mtime > CURRENT_TIMESTAMP - INTERVAL '7 days'
		GROUP BY h.bid
	)
	SELECT 
		bs.bid,
		bs.account_count,
		bs.total_balance,
		bs.avg_balance,
		COALESCE(ts.transaction_count, 0) as transaction_count,
		COALESCE(ts.net_flow, 0) as net_flow,
		COALESCE(ts.active_accounts, 0) as active_accounts,
		EXTRACT(DAY FROM ts.last_transaction - ts.first_transaction) as activity_days,
		CASE 
			WHEN ts.transaction_count > 0 
			THEN bs.total_balance / ts.transaction_count 
			ELSE 0 
		END as balance_per_transaction
	INTO test_rec
	FROM branch_summary bs
	LEFT JOIN transaction_summary ts ON bs.bid = ts.bid
	WHERE bs.total_balance > 0
	ORDER BY bs.total_balance DESC
	LIMIT 100;

return 0 ;
END
$$ LANGUAGE plpgsql;
/*
                                                                               QUERY PLAN
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Limit  (cost=2695040.08..2695040.33 rows=100 width=116) (actual time=39163.719..39163.741 rows=100 loops=1)
   Buffers: shared hit=41289 read=1099776
   I/O Timings: shared read=9946.384
   ->  Sort  (cost=2695040.08..2695040.65 rows=228 width=116) (actual time=39163.716..39163.730 rows=100 loops=1)
         Sort Key: (sum(a.abalance)) DESC
         Sort Method: top-N heapsort  Memory: 48kB
         Buffers: shared hit=41289 read=1099776
         I/O Timings: shared read=9946.384
         ->  Hash Right Join  (cost=2694655.92..2695031.37 rows=228 width=116) (actual time=39159.877..39163.542 rows=685 loops=1)
               Hash Cond: (h.bid = b.bid)
               Buffers: shared hit=41286 read=1099776
               I/O Timings: shared read=9946.384
               ->  GroupAggregate  (cost=2847.70..3219.66 rows=96 width=44) (actual time=10.491..14.002 rows=97 loops=1)
                     Group Key: h.bid
                     Buffers: shared hit=195
                     ->  Sort  (cost=2847.70..2900.70 rows=21200 width=20) (actual time=10.478..11.430 rows=21203 loops=1)
                           Sort Key: h.bid, h.aid
                           Sort Method: quicksort  Memory: 1597kB
                           Buffers: shared hit=195
                           ->  Seq Scan on pgbench_history h  (cost=0.00..566.00 rows=21200 width=20) (actual time=0.049..6.225 rows=21203 loops=1)
                                 Filter: (mtime > (CURRENT_TIMESTAMP - '7 days'::interval))
                                 Buffers: shared hit=195
               ->  Hash  (cost=2691805.37..2691805.37 rows=228 width=52) (actual time=39149.357..39149.361 rows=685 loops=1)
                     Buckets: 1024  Batches: 1  Memory Usage: 52kB
                     Buffers: shared hit=41091 read=1099776
                     I/O Timings: shared read=9946.384
                     ->  HashAggregate  (cost=2691795.09..2691805.37 rows=228 width=52) (actual time=39148.864..39149.195 rows=685 loops=1)
                           Group Key: b.bid
                           Filter: (sum(a.abalance) > 0)
                           Batches: 1  Memory Usage: 169kB
                           Buffers: shared hit=41091 read=1099776
                           I/O Timings: shared read=9946.384
                           ->  Hash Right Join  (cost=19.41..2006800.53 rows=68499456 width=12) (actual time=0.945..29345.250 rows=68500000 loops=1)
                                 Hash Cond: (a.bid = b.bid)
                                 Buffers: shared hit=41091 read=1099776
                                 I/O Timings: shared read=9946.384
                                 ->  Seq Scan on pgbench_accounts a  (cost=0.00..1825857.56 rows=68499456 width=12) (actual time=0.769..17137.036 rows=68500000 loops=1)
                                       Buffers: shared hit=41087 read=1099776
                                       I/O Timings: shared read=9946.384
                                 ->  Hash  (cost=10.85..10.85 rows=685 width=4) (actual time=0.161..0.164 rows=685 loops=1)
                                       Buckets: 1024  Batches: 1  Memory Usage: 33kB
                                       Buffers: shared hit=4
                                       ->  Seq Scan on pgbench_branches b  (cost=0.00..10.85 rows=685 width=4) (actual time=0.010..0.084 rows=685 loops=1)
                                             Buffers: shared hit=4
 Planning:
   Buffers: shared hit=74 dirtied=5
 Planning Time: 1.520 ms
 Execution Time: 39173.153 ms
*/
