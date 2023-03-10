/*
Each event or hit may be associated with nested product-related fields, found in hits.product. Let's suppose we want to know the top product categories (indicated by product.v2ProductCategory) with respect to the total number of unique users who either performed a “Quickview Click”, “Product Click”, or “Promotion Click” action (as indicated by hits.eventInfo.eventAction). We also want to make sure we're analyzing true user-actions (see hits.type) and not page views.
*/
select
  event_action
  , product_category
  , count(distinct fullvisitorid) as unique_users
from unnest_hits
where true
  and event_action in ('Quickview Click', 'Product Click', 'Promotion Click')
  and hit_type <> 'PAGE'
group by 1,2
qualify row_number() over(partition by event_action order by unique_users desc) = 1
;
