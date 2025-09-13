-- models/staging/stg_cycle_hire.sql

with source as (
    select * from {{ source('LondonBicycles_Raw', 'cycle_hire_raw') }}
),

stations as (
    select * from {{ ref('stg_cycle_stations') }}
),

staged as (
    select
        src.rental_id,
        TIMESTAMP_MICROS(src.start_date) AS start_date,
        -- src.start_date,
        TIMESTAMP_MICROS(src.end_date) AS end_date,
        -- src.end_date,
        src.duration,
        src.bike_id,
        -- ✅ Start station: keep valid id or replace by name
        coalesce(
            st_start_by_id.station_id,
            st_start_by_name.station_id
        ) as start_station_id,

        src.start_station_name,

        -- ✅ End station: keep valid id or replace by name
        coalesce(
            st_end_by_id.station_id,
            st_end_by_name.station_id
        ) as end_station_id,

        src.end_station_name,


        -- Flags for replaced IDs
        case
            when st_start_by_id.station_id is null
                 and st_start_by_name.station_id is not null
            then true else false
        end as start_station_id_replaced,

        case
            when st_end_by_id.station_id is null
                 and st_end_by_name.station_id is not null
            then true else false
        end as end_station_id_replaced

    from source src

    -- Start station checks
    left join stations st_start_by_id
        on src.start_station_id = st_start_by_id.station_id
    left join stations st_start_by_name
        on lower(trim(src.start_station_name)) = lower(trim(st_start_by_name.station_name))

    -- End station checks
    left join stations st_end_by_id
        on src.end_station_id = st_end_by_id.station_id
    left join stations st_end_by_name
        on lower(trim(src.end_station_name)) = lower(trim(st_end_by_name.station_name))
)

select * from staged

