with source as (
    select * from {{ source('taxi_pipeline', 'raw_taxi_trips') }}
),

cleaned as (
    select
        unique_key,
        taxi_id,
        trip_start_timestamp,
        trip_end_timestamp,
        trip_seconds,
        trip_miles,
        pickup_community_area,
        dropoff_community_area,
        fare,
        tips,
        tolls,
        extras,
        trip_total,
        payment_type,
        company,

        -- Fecha del viaje para análisis temporal
        DATE(trip_start_timestamp)                          as trip_date,
        EXTRACT(HOUR FROM trip_start_timestamp)             as trip_hour,
        EXTRACT(DAYOFWEEK FROM trip_start_timestamp)        as day_of_week,
        EXTRACT(MONTH FROM trip_start_timestamp)            as trip_month,

        -- Velocidad promedio en mph
        CASE
            WHEN trip_seconds > 0 AND trip_miles > 0
            THEN ROUND(trip_miles / (trip_seconds / 3600.0), 2)
            ELSE NULL
        END                                                 as avg_speed_mph,

        -- Porcentaje de propina
        CASE
            WHEN fare > 0
            THEN ROUND(tips / fare * 100, 2)
            ELSE 0
        END                                                 as tip_percentage,

        -- Costo por milla
        CASE
            WHEN trip_miles > 0
            THEN ROUND(fare / trip_miles, 2)
            ELSE NULL
        END                                                 as fare_per_mile

    from source
    where trip_start_timestamp is not null
        and trip_end_timestamp is not null
        and trip_seconds > 0
        and trip_miles > 0
        and fare > 0
)

select * from cleaned