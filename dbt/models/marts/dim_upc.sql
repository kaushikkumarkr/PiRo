with unioned as (
    select * from {{ source('piro_raw', 'raw_upc_sdr') }}
    union all
    select * from {{ source('piro_raw', 'raw_upc_cer') }}
    union all
    select * from {{ source('piro_raw', 'raw_upc_lnd') }}
    union all
    select * from {{ source('piro_raw', 'raw_upc_sna') }}
)

select
    upc as upc_id,
    descrip as description,
    nitem as items_per_bundle,
    category_id
from unioned
