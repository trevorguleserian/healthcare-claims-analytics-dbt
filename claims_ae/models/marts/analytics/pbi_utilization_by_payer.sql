{{ config(materialized='view') }}
select * from {{ ref('mart_utilization_by_payer') }}
