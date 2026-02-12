with claim_lines as (
  select
    claim_id,
    line_num,
    provider_id,
    claim_start_date,
    payer_type,
    allowed_amount
  from {{ ref('fct_claim_line') }}
),

provider_hist as (
  select
    provider_id,
    provider_group,
    specialty,
    dbt_valid_from,
    coalesce(dbt_valid_to, '9999-12-31'::date) as dbt_valid_to
  from {{ ref('snap_dim_provider') }}
)

select
  p.provider_group,
  p.specialty,
  c.payer_type,
  count(*) as claim_lines,
  sum(c.allowed_amount) as allowed_sum
from claim_lines c
join provider_hist p
  on c.provider_id = p.provider_id
 and c.claim_start_date >= p.dbt_valid_from
 and c.claim_start_date <  p.dbt_valid_to
group by 1,2,3
order by allowed_sum desc
