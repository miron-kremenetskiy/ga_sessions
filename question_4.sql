with unnest_hits as (

  select
    md5(cast(ga.fullvisitorid as string) || cast(ga.visitid as string)) as session_id
    , ga.fullvisitorid
    -- Sessions
    , timestamp_seconds(ga.visitStartTime) as visit_start_at
    , ga.totals.timeOnSite as session_length_in_seconds
    -- Hits
    , timestamp_seconds(cast(ga.visitStartTime + (hits_unnested.time / 1000) as int64)) as hit_at
    , hits_unnested.hitnumber as hit_number
    , hits_unnested.eventInfo.eventAction as event_action
    , (select experimentId from unnest(hits.experiment)) as experiment_id
    -- Products
    , (select v2ProductCategory from unnest(hits_unnested.product)) as product_category
    , (select v2ProductName from unnest(hits_unnested.product)) as product_name
    -- Transactions
    , ga.totals.transactions as transactions
    , ga.totals.totalTransactionRevenue as transaction_revenue
    -- UTM
    , trafficSource.medium as utm_medium
    , trafficSource.campaign as utm_campaign
    , trafficSource.adContent as utm_content
    , trafficSource.isTrueDirect as is_direct
    , trafficSource.source as utm_source
    -- Device
    -- maybe a type of device is giving people problems and the site needs to be optimized
    , device.browser as device_browser
    , device.deviceCategory as device_category
    -- Geo
    -- maybe certain locations adandon more, due to demographics like income
    , geoNetwork.metro as geo_metro
  from `bigquery-public-data.google_analytics_sample.ga_sessions*` as ga
    , unnest(hits) as hits_unnested

),

aggregated as (

  select
    session_id
    , fullvisitorid
    , transactions
    , transaction_revenue
    , utm_medium
    , utm_campaign
    , utm_content
    , is_direct
    , utm_source
    , session_length_in_seconds
    , device_browser
    , device_category
    , visit_start_at
    , geo_metro
    , logical_or(event_action = 'Add to Cart') as has_add_to_cart_action
    , timestamp_diff(min(case when event_action = 'Add to Cart' then hit_at end), visit_start_at, second) as time_to_first_add_to_cart_action_in_seconds
    , count(*) as events
    , min(case when event_action = 'Add to Cart' then hit_number end) as events_to_first_add_to_cart_action
     --is an experiment working
    , logical_or(experiment_id is not null) as has_experiment
    , array_concat_agg(product_name order by product_name) as products
    , array_concat_agg(product_category order by product_category) as product_categories
  from unnest_hits
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14

)
/*
Now, knowing how to determine sessions with purchases vs. sessions with abandoned carts, let's wrap this up by building a data set that we think contains useful features for a model that predicts if a session will ultimately end up with an abandoned cart or a successful purchase. In this case, feel free to explore the data and add any data you think might be meaningful. You should expand your final data set to pull from bigquery-public-data.google_analytics_sample.ga_sessions*, giving you more data to work with. Please provide a brief write up of the additional columns/features you've chosen and why you think they matter.
*/
select
  session_id
  , fullvisitorid
  /*  Some analysis can be done with when the session starts.
      Are purchases more likely in the morning/evening, things like that */
  , visit_start_at
  /* Flags */
  -- This flag will let us know if there was a purchase during the session
  , transactions > 0 as has_purchase
  /*  This flag will let us know if there was an add to cart action during the session.
      This can be used with the has_purchase flag to limit the sessions. */
  , has_add_to_cart_action
  /* Do more purchases happen on the weekend? */
  , extract(dayofweek from visit_start_at) in (1,7) as is_weekend
  /*  Does being in an experiment impact purchasing?
      This can also be expanded to check which experiment. */
  , has_experiment
  /*  Transaction Info
      Do the number of transactions or total cost predict a purchase? */
  , transactions
  , transaction_revenue
  /*  UTM Info
      Are users coming from specific sources/campaigns/etc. more likely to purchase? */
  , utm_medium
  , utm_campaign
  , utm_content
  , is_direct
  , utm_source
  /*  Events/Length Info
      Does session length and/or number of events impact purchasing?
      The longer the user is engaging with the site, the more they are doing there might imply they are more likely to purchase.
      Similarly, does the time to first adding something to the cart and/or number of events impact purchasing?
      If you are quick to add something to the cart, are you more/less likely to purchase something?
  */
  , session_length_in_seconds
  , time_to_first_add_to_cart_action_in_seconds
  , events
  , events_to_first_add_to_cart_action
  /*  Device Info
      Does a specific browser or device type (e.g, mobile) result in less purchasing?
      This might show the need to optimize the site for mobile or maybe fix some bugs on a specific device type. */
  , device_browser
  , device_category
  /*  Product Info
      Do the products that are added to the cart impact purchasing?
      Are specific products more likely to get added to the cart but abandoned? */
  , array_concat_agg(product_name order by product_name) as products
  , array_concat_agg(product_category order by product_category) as product_categories
  /*  Geo Info
      Do more purchases or abadoned carts come from anywhere specific?
      This could be a proxy for some demographic information. */
  , geo_metro
from aggregated
;
