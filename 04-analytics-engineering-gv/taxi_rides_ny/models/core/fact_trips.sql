{{ config(materialized="table") }}

with
    green_tripdata as (
        select
            tripid,
            vendorid,  -- Ya es STRING en la tabla original
            ratecodeid,  -- Ya es STRING en la tabla original
            pulocationid as pickup_locationid,  -- Ya es STRING en la tabla original
            dolocationid as dropoff_locationid,  -- Ya es STRING en la tabla original
            lpep_pickup_datetime as pickup_datetime,
            lpep_dropoff_datetime as dropoff_datetime,
            store_and_fwd_flag,
            passenger_count,
            trip_distance,
            trip_type,  -- Ya es STRING en la tabla original
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            ehail_fee,
            airport_fee,
            improvement_surcharge,
            total_amount,
            payment_type,  -- Ya es STRING en la tabla original
            {{ get_payment_type_description("payment_type") }}
            as payment_type_description,
            distance_between_service,
            time_between_service,
            data_file_year,
            data_file_month,
            'Green' as service_type
        from {{ ref("stg_green_tripdata") }}
    ),
    yellow_tripdata as (
        select
            tripid,
            cast(vendorid as string) as vendorid,  -- Convertido de INT64 a STRING
            cast(ratecodeid as string) as ratecodeid,  -- Convertido de INT64 a STRING
            cast(pickup_locationid as string) as pickup_locationid,  -- Convertido de INT64 a STRING
            cast(dropoff_locationid as string) as dropoff_locationid,  -- Convertido de INT64 a STRING
            pickup_datetime,
            dropoff_datetime,
            store_and_fwd_flag,
            passenger_count,
            trip_distance,
            cast(trip_type as string) as trip_type,  -- Convertido de INT64 a STRING
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            ehail_fee,
            null as airport_fee,  -- Campo faltante en yellow, añadido como NULL
            improvement_surcharge,
            total_amount,
            cast(payment_type as string) as payment_type,  -- Convertido de INT64 a STRING
            {{ get_payment_type_description("payment_type") }}
            as payment_type_description,
            null as distance_between_service,  -- Campo faltante en yellow, añadido como NULL
            null as time_between_service,  -- Campo faltante en yellow, añadido como NULL
            null as data_file_year,  -- Campo faltante en yellow, añadido como NULL
            null as data_file_month,  -- Campo faltante en yellow, añadido como NULL
            'Yellow' as service_type
        from {{ ref("stg_yellow_tripdata") }}
    ),
    trips_unioned as (
        select *
        from green_tripdata
        union all
        select *
        from yellow_tripdata
    ),
    dim_zones as (select * from {{ ref("dim_zones") }} where borough != 'Unknown')
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
inner join
    dim_zones as pickup_zone
    on cast(trips_unioned.pickup_locationid as int64) = pickup_zone.locationid
inner join
    dim_zones as dropoff_zone
    on cast(trips_unioned.dropoff_locationid as int64) = dropoff_zone.locationid
