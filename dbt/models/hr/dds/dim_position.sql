with positions_union as (

    select
        current_position_code as position_code,
        current_position_name as position_name,
        load_dttm,
        batch_id
    from {{ source('hr_raw', 'hr_employee_master_raw') }}

    union all

    select
        position_code,
        position_name,
        load_dttm,
        batch_id
    from {{ source('hr_raw', 'hr_position_history_raw') }}

),

positions_deduplicated as (

    select
        position_code,
        max(position_name) as position_name,
        max(load_dttm) as load_dttm,
        max(batch_id) as batch_id
    from positions_union
    group by position_code

)

select
    row_number() over (order by position_code) as position_id,
    position_code,
    position_name,
    cast(null as string) as grade_code,
    'hr_position_catalog' as source_system,
    load_dttm,
    batch_id
from positions_deduplicated