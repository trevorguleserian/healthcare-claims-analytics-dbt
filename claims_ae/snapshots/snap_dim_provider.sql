{% snapshot snap_dim_provider %}

{{
    config(
      target_schema='AE_CLAIMS_DEV',
      unique_key='provider_id',
      strategy='check',
      check_cols=['specialty','provider_group']
    )
}}

select *
from {{ ref('dim_provider') }}

{% endsnapshot %}
