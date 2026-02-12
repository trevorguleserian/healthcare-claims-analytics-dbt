{{ config(
    materialized='incremental',
    unique_key=['claim_id','line_num'],
    on_schema_change='sync_all_columns'
) }}

with src as (
    select
      l.claim_id,
      l.line_num,
      h.member_id,
      h.provider_id,
      h.claim_start_date,
      h.claim_end_date,
      h.place_of_service,
      h.payer_type,
      h.insurance_name,
      h.plan_name,
      h.plan_id,
      l.cpt,
      l.modifier_1,
      l.modifier_2,
      l.icd10,
      l.units,
      l.charge_amount,
      l.allowed_amount,
      l.paid_amount
    from {{ ref('stg_claims_line') }} l
    join {{ ref('stg_claims_header') }} h
      on l.claim_id = h.claim_id

    {% if is_incremental() %}
      -- late-arriving / corrected claims: reprocess last 7 days of claim_start_date
      where h.claim_start_date >= (
        select dateadd(day, -7, max(claim_start_date)) from {{ this }}
      )
    {% endif %}
)

select
  claim_id,
  line_num,
  member_id,
  provider_id,
  claim_start_date,
  claim_end_date,
  place_of_service,
  payer_type,
  insurance_name,
  plan_name,
  plan_id,
  cpt,
  modifier_1,
  modifier_2,
  icd10,
  units,
  charge_amount,
  allowed_amount,
  paid_amount,
  (charge_amount - allowed_amount) as contractual_adjustment,
  (allowed_amount - paid_amount) as member_or_other_responsibility
from src
