with unnest_hits as (

  select
    md5(cast(ga.fullvisitorid as string) || cast(ga.visitid as string)) as session_id
    , hits_unnested.eventInfo.eventAction as event_action
    , ga.totals.transactions as transactions
  from `bigquery-public-data.google_analytics_sample.ga_sessions_20170801` as ga
    , unnest(hits) as hits_unnested

)
select
  case
    when transactions > 0 then 'Purchase'
    else 'Abandoned'
  end as add_to_cart_result
  , count(distinct session_id) as sessions
from unnest_hits  
where event_action = 'Add to Cart'
group by 1
;
