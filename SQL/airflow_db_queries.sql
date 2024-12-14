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


select count(*) from exchange_rates;
