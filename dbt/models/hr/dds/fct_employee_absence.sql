with absence_base as (

    select
        src_absence_event_id,
        employee_src_id,
        absence_type_code,
        absence_type_name,
        absence_reason_group,
        start_date,
        end_date,
        duration_days,
        approved_flag,
        load_dttm,
        batch_id
    from {{ source('hr_raw', 'hr_absence_events_raw') }}

),

employee_dim as (

    select *
    from {{ ref('dim_hr_employee') }}
    where is_current_flag = true

)

select
    row_number() over (order by a.src_absence_event_id) as employee_absence_id,
    e.employee_id,
    a.absence_type_code,
    a.absence_type_name,
    a.absence_reason_group,
    a.start_date,
    a.end_date,
    a.duration_days,
    case
        when a.approved_flag = 'Y' then true
        else false
    end as approved_flag,
    a.load_dttm,
    a.batch_id
from absence_base a
left join employee_dim e
    on a.employee_src_id = e.employee_src_id