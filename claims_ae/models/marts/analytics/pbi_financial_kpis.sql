{{ config(materialized='view') }}
select * from {{ ref('mart_financial_kpis') }}
