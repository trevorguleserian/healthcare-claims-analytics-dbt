select
  ip_claim_id,
  member_id,
  provider_id,
  try_to_date(admit_date) as admit_date,
  try_to_date(discharge_date) as discharge_date,
  try_to_number(length_of_stay) as length_of_stay,
  payer_type,
  insurance_name,
  plan_name,
  plan_id,
  drg_code,
  drg_description,
  try_to_double(charge_amount) as charge_amount,
  try_to_double(allowed_amount) as allowed_amount,
  try_to_double(paid_amount) as paid_amount
from {{ source('raw', 'raw_ip_claims') }}
