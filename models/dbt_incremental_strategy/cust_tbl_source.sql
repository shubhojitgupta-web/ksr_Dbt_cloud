{{
    config(
        database= 'DBT_DB',
        schema= 'RAW_SCHEMA',
        materialized= 'table' 
    )
}}

select * from {{ source('cust_master', 'cust_tbl') }}