select
  member_id,
  member_first_name,
  member_last_name,
  dob,
  gender,
  zip
from {{ ref('stg_members') }}
