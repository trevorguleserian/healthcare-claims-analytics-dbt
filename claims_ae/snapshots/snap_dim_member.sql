{% snapshot snap_dim_member %}

{{
    config(
      target_schema='AE_CLAIMS_DEV',
      unique_key='member_id',
      strategy='check',
      check_cols=['insurance_name','plan_name','plan_id']
    )
}}

select *
from {{ ref('dim_member') }}

{% endsnapshot %}
