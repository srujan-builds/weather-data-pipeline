{{
    config(
        materialized = 'incremental',
        unique_key='location_id',
        incremental_strategy='delete+insert'
    )
}}

with enriched_location as (
    select * from {{ ref('int_location_enriched') }}
)

select 
    location_id,
    city_name,
    latitude,
    longitude,
    state,
    country
from enriched_location