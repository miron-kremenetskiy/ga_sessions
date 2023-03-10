with unnest_hits as (

  select
    md5(cast(ga.fullvisitorid as string) || cast(ga.visitid as string)) as session_id
    , ga.fullvisitorid
    , timestamp_seconds(cast(ga.visitStartTime + (hits_unnested.time / 1000) as int64)) as hit_at
  from `bigquery-public-data.google_analytics_sample.ga_sessions_20170801` as ga
    , unnest(hits) as hits_unnested

)
/*What were the number of events, unique sessions, and unique users (i.e., full visitors) that occurred in each hour? By “hour” let's assume we mean the hour in which the event occurred.*/
select
  timestamp_trunc(hit_at, hour) as hit_hour
  , count(*) as events
  , count(distinct session_id) as unique_sessions
  , count(distinct fullvisitorid) as unique_users
from unnest_hits
group by 1
order by 1
;
