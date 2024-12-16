SELECT pid, usename, datname, client_addr, application_name, state, query
FROM pg_stat_activity
WHERE datname = 'airflow';

SELECT 
    column_name, 
    data_type, 
    character_maximum_length, 
    is_nullable 
FROM 
    information_schema.columns 
WHERE 
    table_name = 'exchange_rates';

CREATE TABLE IF NOT EXISTS exchange_rates (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ,
    base_currency VARCHAR(3),
    currency VARCHAR(3),
    rate DECIMAL(20,10),
    created_at TIMESTAMPTZ DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jerusalem')
);
   
 drop table exchange_rates;
   
   
   
   
select count(*) from exchange_rates;
select * from exchange_rates;
