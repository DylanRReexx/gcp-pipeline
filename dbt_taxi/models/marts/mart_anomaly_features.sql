with trips as (
    select * from {{ ref('stg_taxi_trips') }}
),

-- Estadísticas base para detectar anomalías
stats as (
    select
        AVG(fare)                   as avg_fare,
        STDDEV(fare)                as std_fare,
        AVG(trip_miles)             as avg_miles,
        STDDEV(trip_miles)          as std_miles,
        AVG(trip_seconds)           as avg_seconds,
        STDDEV(trip_seconds)        as std_seconds,
        AVG(avg_speed_mph)          as avg_speed,
        STDDEV(avg_speed_mph)       as std_speed,
        AVG(fare_per_mile)          as avg_fare_per_mile,
        STDDEV(fare_per_mile)       as std_fare_per_mile
    from trips
    where avg_speed_mph is not null
        and fare_per_mile is not null
),

features as (
    select
        t.unique_key,
        t.taxi_id,
        t.trip_date,
        t.trip_hour,
        t.day_of_week,
        t.trip_month,
        t.fare,
        t.trip_miles,
        t.trip_seconds,
        t.tips,
        t.tip_percentage,
        t.avg_speed_mph,
        t.fare_per_mile,
        t.payment_type,
        t.company,
        t.pickup_community_area,
        t.dropoff_community_area,

        -- Z-scores para detección de anomalías
        ROUND((t.fare - s.avg_fare) / NULLIF(s.std_fare, 0), 4)
            as fare_zscore,
        ROUND((t.trip_miles - s.avg_miles) / NULLIF(s.std_miles, 0), 4)
            as miles_zscore,
        ROUND((t.trip_seconds - s.avg_seconds) / NULLIF(s.std_seconds, 0), 4)
            as seconds_zscore,
        ROUND((t.avg_speed_mph - s.avg_speed) / NULLIF(s.std_speed, 0), 4)
            as speed_zscore,
        ROUND((t.fare_per_mile - s.avg_fare_per_mile) / NULLIF(s.std_fare_per_mile, 0), 4)
            as fare_per_mile_zscore

    from trips t
    cross join stats s
    where t.avg_speed_mph is not null
        and t.fare_per_mile is not null
)

select * from features