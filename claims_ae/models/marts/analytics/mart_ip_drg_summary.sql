select
  payer_type,
  drg_code,
  drg_description,
  count(*) as ip_stays,
  avg(length_of_stay) as avg_los,
  sum(allowed_amount) as allowed_sum,
  sum(paid_amount) as paid_sum
from {{ ref('fct_ip_claim') }}
group by 1,2,3
order by allowed_sum desc
