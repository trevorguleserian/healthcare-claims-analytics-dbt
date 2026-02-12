select *
from {{ ref('snap_dim_provider') }}
where dbt_valid_to is null
