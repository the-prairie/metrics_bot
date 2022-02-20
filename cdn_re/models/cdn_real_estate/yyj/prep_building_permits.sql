

with source as (

    select *
    from {{ source('yyj_raw', 'building_permits')}}
)


select *
from source