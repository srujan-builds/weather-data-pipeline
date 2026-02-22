with raw_weather as (
    select * from {{ source('source_data', 'raw_weather') }}
)


select
    _dlt_id as weather_id,
    trim(lower(requested_city)) as requested_city,
    dt as measured_at_unix,
    name as weather_station_name,
    main__temp as temp_kelvin,
    main__humidity as humidity_percent,
    wind__speed as wind_speed_mps
from raw_weather