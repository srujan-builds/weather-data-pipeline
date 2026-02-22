with raw_geocode as (
    select * from {{ source('source_data', 'raw_geocode') }}
)


select
    trim(lower(requested_city)) as requested_city,
    name as city_name,
    round(lat, 2) as latitude,
    round(lon, 2) as longitude,
    state,
    country
from raw_geocode