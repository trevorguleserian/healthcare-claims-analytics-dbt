select
  payer_type,
  count(distinct claim_id) as op_claims,
  sum(allowed_amount) as op_allowed,
  sum(paid_amount) as op_paid,
  avg(allowed_amount) as avg_allowed_per_line
from {{ ref('fct_claim_line') }}
group by 1
order by op_allowed desc
