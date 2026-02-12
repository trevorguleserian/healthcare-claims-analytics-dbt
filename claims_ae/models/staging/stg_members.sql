select
  member_id,
  member_first_name,
  member_last_name,
  try_to_date(dob) as dob,
  upper(gender) as gender,
  zip,
  payer_type,
  insurance_name,
  plan_name,
  plan_id
from {{ source('raw', 'raw_members') }}
