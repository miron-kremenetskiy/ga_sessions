select
  case
    when transactions > 0 then 'Purchase'
    else 'Abandoned'
  end as add_to_cart_result
  , count(distinct session_id) as unique_users
from unnest_hits
where event_action = 'Add to Cart'
group by 1
;
