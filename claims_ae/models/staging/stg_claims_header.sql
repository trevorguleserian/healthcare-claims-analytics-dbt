select
  claim_id,
  member_id,
  provider_id,
  try_to_date(claim_start_date) as claim_start_date,
  try_to_date(claim_end_date) as claim_end_date,
  place_of_service,
  bill_type,
  payer_type,
  insurance_name,
  plan_name,
  plan_id
from {{ source('raw', 'raw_claims_header') }}
