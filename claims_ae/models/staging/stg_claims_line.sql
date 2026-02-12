select
  claim_id,
  try_to_number(line_num) as line_num,
  cpt,
  modifier_1,
  modifier_2,
  icd10,
  try_to_number(units) as units,
  try_to_double(charge_amount) as charge_amount,
  try_to_double(allowed_amount) as allowed_amount,
  try_to_double(paid_amount) as paid_amount
from {{ source('raw', 'raw_claims_line') }}
