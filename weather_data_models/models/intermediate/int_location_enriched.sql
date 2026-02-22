with staging_geocode as (
    select * from {{ ref('stg_raw_geocode') }}
)

select distinct
    md5(requested_city) as location_id,
    city_name,
    latitude,
    longitude,
    state,
    country
from staging_geocode