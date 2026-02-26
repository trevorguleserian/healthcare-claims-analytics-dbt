{{ config(materialized='view') }}
select * from {{ ref('mart_utilization_provider_group_asof') }}
