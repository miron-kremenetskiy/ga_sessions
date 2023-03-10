with unnest_hits as (

  select
    ga.fullvisitorid
    , products.v2ProductCategory as product_category
    , hits_unnested.eventInfo.eventAction as event_action
    , hits_unnested.type as hit_type
  from `bigquery-public-data.google_analytics_sample.ga_sessions_20170801` as ga
    , unnest(hits) as hits_unnested
    , unnest(hits_unnested.product) as products

)
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
