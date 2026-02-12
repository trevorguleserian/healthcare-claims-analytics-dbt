select
  provider_id,
  provider_first_name,
  provider_last_name,
  credential,
  try_to_boolean(is_physician) as is_physician,
  specialty,
  provider_group,
  try_to_boolean(is_active) as is_active,
  npi
from {{ source('raw', 'raw_providers') }}
