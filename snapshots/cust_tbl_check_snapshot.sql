{% snapshot cust_tbl_check_snapshot %}

{{
    config(
        target_schema='RAW_SCHEMA',
        target_database='DBT_DB',
        unique_key='id',
        strategy='check',
        check_cols=['spend', 'email'],
        invalidate_hard_deletes=True,
        dbt_valid_to_current= "to_date('9999-12-31')",
        updated_at='updated_at',
        snapshot_meta_column_names={
            "dbt_valid_from": "start_date",
            "dbt_valid_to": "end_date",
            "dbt_scd_id": "scd_id",
            "dbt_updated_at": "modified_date",
            "dbt_is_deleted": "is_deleted"
        }
    )
}}
select id, full_name, email, phone, spend, updated_at from {{ source('cust_master', 'cust_tbl') }}
{% endsnapshot %}