{{
    config(
        materialized = 'incremental',
        unique_key='weather_id',
        incremental_strategy='append'
    )
}}


with enriched_weather as (
    select * from {{ ref('int_weather_enriched') }}
)

select
    weather_id,
    location_id,
    measured_at,
    temp_c,
    humidity_percent,
    wind_speed_mps
from enriched_weather

{% if is_incremental() %}
  where measured_at > (select max(measured_at) from {{ this }})
{% endif %}