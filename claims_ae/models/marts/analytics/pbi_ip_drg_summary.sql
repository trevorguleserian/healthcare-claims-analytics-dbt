{{ config(materialized='view') }}
select * from {{ ref('mart_ip_drg_summary') }}
