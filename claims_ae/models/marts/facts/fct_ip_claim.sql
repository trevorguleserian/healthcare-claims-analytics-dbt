{{ config(
    materialized='incremental',
    unique_key='ip_claim_id',
    on_schema_change='sync_all_columns'
) }}

with src as (
    select
      ip_claim_id,
      member_id,
      provider_id,
      admit_date,
      discharge_date,
      length_of_stay,
      payer_type,
      insurance_name,
      plan_name,
      plan_id,
      drg_code,
      drg_description,
      charge_amount,
      allowed_amount,
      paid_amount
    from {{ ref('stg_ip_claims') }}

    {% if is_incremental() %}
      -- late-arriving stays: reprocess last 7 days of admits
      where admit_date >= (
        select dateadd(day, -7, max(admit_date)) from {{ this }}
      )
    {% endif %}
)

select
  ip_claim_id,
  member_id,
  provider_id,
  admit_date,
  discharge_date,
  length_of_stay,
  payer_type,
  insurance_name,
  plan_name,
  plan_id,
  drg_code,
  drg_description,
  charge_amount,
  allowed_amount,
  paid_amount
from src
