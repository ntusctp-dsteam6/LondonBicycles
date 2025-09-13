with source as (
    select * from {{ source('LondonBicycles_Raw', 'cycle_stations_raw') }}
)

select
    id as station_id,
    trim(name) as station_name,
    latitude,
    longitude,
    installed,
    locked,
    install_date,
    removal_date,
    temporary,
    docks_count
from source