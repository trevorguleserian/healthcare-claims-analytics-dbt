select
  provider_id,
  provider_first_name,
  provider_last_name,
  credential,
  is_physician,
  specialty,
  provider_group,
  is_active,
  npi
from {{ ref('stg_providers') }}
