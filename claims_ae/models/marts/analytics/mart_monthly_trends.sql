with base as (
  select
    date_trunc('month', claim_start_date) as month,
    payer_type,
    place_of_service,
    allowed_amount,
    paid_amount,
    claim_id
  from {{ ref('fct_claim_line') }}
)

select
  month,
  payer_type,
  place_of_service,
  count(distinct claim_id) as claims,
  count(*) as claim_lines,
  sum(allowed_amount) as allowed_sum,
  sum(paid_amount) as paid_sum,
  case when sum(allowed_amount) = 0 then null
       else sum(paid_amount) / sum(allowed_amount)
  end as paid_to_allowed_ratio
from base
group by 1,2,3
order by 1,2,3
