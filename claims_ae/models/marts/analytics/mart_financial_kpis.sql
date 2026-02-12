with base as (
  select
    payer_type,
    allowed_amount,
    paid_amount,
    charge_amount,
    member_id,
    claim_id
  from {{ ref('fct_claim_line') }}
)

select
  payer_type,
  count(distinct member_id) as members,
  count(distinct claim_id) as claims,
  sum(charge_amount) as charge_sum,
  sum(allowed_amount) as allowed_sum,
  sum(paid_amount) as paid_sum,
  case when sum(charge_amount) = 0 then null
       else sum(allowed_amount) / sum(charge_amount)
  end as allowed_to_charge_ratio,
  case when sum(allowed_amount) = 0 then null
       else sum(paid_amount) / sum(allowed_amount)
  end as paid_to_allowed_ratio,
  (sum(allowed_amount) / nullif(count(distinct member_id),0)) as allowed_per_member,
  (sum(paid_amount) / nullif(count(distinct member_id),0)) as paid_per_member
from base
group by 1
order by paid_sum desc
