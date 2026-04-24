select
    row_number() over (order by system_code) as access_system_id,
    system_code,
    max(system_name) as system_name,
    'access_system_catalog' as source_system,
    max(load_dttm) as load_dttm,
    max(batch_id) as batch_id
from {{ source('access_raw', 'access_system_accounts_raw') }}
group by system_code