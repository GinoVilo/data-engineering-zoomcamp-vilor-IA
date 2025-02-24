{{
    config(
        materialized='table'
    )
}}

with green_tripdata as (
    select 
        tripid, 
        vendorid,
        cast(ratecodeid as STRING) as ratecodeid,
        cast(pulocationid as STRING) as pickup_locationid,
        cast(dolocationid as STRING) as dropoff_locationid,
        lpep_pickup_datetime as pickup_datetime,
        lpep_dropoff_datetime as dropoff_datetime,
        store_and_fwd_flag, 
        cast(passenger_count as STRING) as passenger_count,
        cast(trip_distance as STRING) as trip_distance,
        cast(trip_type as STRING) as trip_type,
        cast(fare_amount as STRING) as fare_amount,
        cast(extra as STRING) as extra,
        cast(mta_tax as STRING) as mta_tax,
        cast(tip_amount as STRING) as tip_amount,
        cast(tolls_amount as STRING) as tolls_amount,
        cast(ehail_fee as STRING) as ehail_fee,
        cast(improvement_surcharge as STRING) as improvement_surcharge,
        cast(total_amount as STRING) as total_amount,
        cast(payment_type as STRING) as payment_type,
        {{ get_payment_type_description('payment_type') }} as payment_type_description,
        'Green' as service_type
    from {{ ref('stg_green_tripdata') }}
), 
yellow_tripdata as (
    select 
        tripid, 
        cast(vendorid as STRING) as vendorid,
        cast(ratecodeid as STRING) as ratecodeid,
        cast(pickup_locationid as STRING) as pickup_locationid,
        cast(dropoff_locationid as STRING) as dropoff_locationid,
        pickup_datetime,
        dropoff_datetime,
        store_and_fwd_flag,
        cast(passenger_count as STRING) as passenger_count,
        cast(trip_distance as STRING) as trip_distance,
        cast(trip_type as STRING) as trip_type,
        cast(fare_amount as STRING) as fare_amount,
        cast(extra as STRING) as extra,
        cast(mta_tax as STRING) as mta_tax,
        cast(tip_amount as STRING) as tip_amount,
        cast(tolls_amount as STRING) as tolls_amount,
        cast(ehail_fee as STRING) as ehail_fee,
        cast(improvement_surcharge as STRING) as improvement_surcharge,
        cast(total_amount as STRING) as total_amount,
        cast(payment_type as STRING) as payment_type,
        {{ get_payment_type_description('payment_type') }} as payment_type_description,
        'Yellow' as service_type
    from {{ ref('stg_yellow_tripdata') }}
), 
trips_unioned as (
    select * from green_tripdata
    union all 
    select * from yellow_tripdata
),
dim_zones as (
    select 
        locationid,
        borough,
        zone,
        service_zone
    from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
    trips_unioned.tripid, 
    trips_unioned.vendorid, 
    trips_unioned.service_type,
    trips_unioned.ratecodeid, 
    trips_unioned.pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    trips_unioned.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    trips_unioned.pickup_datetime, 
    trips_unioned.dropoff_datetime, 
    trips_unioned.store_and_fwd_flag, 
    trips_unioned.passenger_count, 
    trips_unioned.trip_distance, 
    trips_unioned.trip_type, 
    trips_unioned.fare_amount, 
    trips_unioned.extra, 
    trips_unioned.mta_tax, 
    trips_unioned.tip_amount, 
    trips_unioned.tolls_amount, 
    trips_unioned.ehail_fee, 
    trips_unioned.improvement_surcharge, 
    trips_unioned.total_amount, 
    trips_unioned.payment_type, 
    trips_unioned.payment_type_description
from trips_unioned
inner join dim_zones as pickup_zone
on cast(trips_unioned.pickup_locationid as INT64) = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on cast(trips_unioned.dropoff_locationid as INT64) = dropoff_zone.locationid