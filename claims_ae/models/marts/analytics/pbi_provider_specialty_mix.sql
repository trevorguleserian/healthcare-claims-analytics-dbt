{{ config(materialized='view') }}
select * from {{ ref('mart_provider_specialty_mix') }}
