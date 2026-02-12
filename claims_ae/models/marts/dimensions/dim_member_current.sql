select *
from {{ ref('snap_dim_member') }}
where dbt_valid_to is null
