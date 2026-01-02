with source as (
    select * from {{ source('piro_raw', 'raw_ccount') }}
),

renamed as (
    select
        store as store_id,
        date,
        custcoun as traffic_count
    from source
)

select * from renamed
