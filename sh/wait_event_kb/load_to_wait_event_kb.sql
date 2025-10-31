--load_to_wait_event_kb.sql
-- psql -d expecto_db -f /postgres/pg_expecto/wait_event_kb/load_to_wait_event_kb.sql
-- rm -rf wait_event_kb
COPY wait_event_knowledge_base (wait_event , advice )
FROM '/postgres/pg_expecto/wait_event_kb/kb.txt'
WITH (
    FORMAT text,    
    DELIMITER '|',
    ENCODING 'UTF8'
);