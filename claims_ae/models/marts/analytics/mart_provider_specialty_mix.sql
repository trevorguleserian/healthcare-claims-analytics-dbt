with lines as (
  select
    provider_id,
    payer_type,
    allowed_amount,
    claim_id
  from {{ ref('fct_claim_line') }}
),

prov as (
  select
    provider_id,
    specialty,
    provider_group
  from {{ ref('dim_provider_current') }}
)

select
  p.provider_group,
  p.specialty,
  l.payer_type,
  count(distinct l.claim_id) as claims,
  count(*) as claim_lines,
  sum(l.allowed_amount) as allowed_sum
from lines l
join prov p on l.provider_id = p.provider_id
group by 1,2,3
order by allowed_sum desc
