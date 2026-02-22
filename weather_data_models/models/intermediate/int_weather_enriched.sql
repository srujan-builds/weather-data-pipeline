with staging as (
    select * from {{ ref('stg_raw_weather') }}
)

select
    weather_id,
    md5(requested_city) as location_id,
    weather_station_name,

    -- to_timestamp(measured_at_unix) as measured_at,
    -- to_timestamp(measured_at_unix::DOUBLE) as measured_at,
    -- epoch_ms(measured_at_unix * 1000) as measured_at,
    TIMESTAMP '1970-01-01 00:00:00' + (measured_at_unix * INTERVAL 1 SECOND) as measured_at,
    (temp_kelvin - 273.15) as temp_c,

    humidity_percent,
    wind_speed_mps
from staging