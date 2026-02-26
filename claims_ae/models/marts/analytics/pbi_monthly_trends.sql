{{ config(materialized='view') }}
select * from {{ ref('mart_monthly_trends') }}
