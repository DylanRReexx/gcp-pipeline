with trips as (
    select * from {{ ref('stg_taxi_trips') }}
),

daily_summary as (
    select
        trip_date,
        trip_hour,
        day_of_week,
        trip_month,
        payment_type,
        company,

        -- Volumen
        COUNT(*)                                    as total_trips,
        COUNT(DISTINCT taxi_id)                     as unique_taxis,

        -- Financiero
        ROUND(SUM(fare), 2)                         as total_fare,
        ROUND(AVG(fare), 2)                         as avg_fare,
        ROUND(SUM(tips), 2)                         as total_tips,
        ROUND(AVG(tip_percentage), 2)               as avg_tip_pct,
        ROUND(SUM(trip_total), 2)                   as total_revenue,

        -- Distancia y tiempo
        ROUND(AVG(trip_miles), 2)                   as avg_miles,
        ROUND(AVG(trip_seconds) / 60.0, 2)          as avg_duration_min,
        ROUND(AVG(avg_speed_mph), 2)                as avg_speed_mph,
        ROUND(AVG(fare_per_mile), 2)                as avg_fare_per_mile,

        -- Rangos
        ROUND(MIN(fare), 2)                         as min_fare,
        ROUND(MAX(fare), 2)                         as max_fare,
        ROUND(MIN(trip_miles), 2)                   as min_miles,
        ROUND(MAX(trip_miles), 2)                   as max_miles

    from trips
    group by
        trip_date, trip_hour, day_of_week,
        trip_month, payment_type, company
)

select * from daily_summary