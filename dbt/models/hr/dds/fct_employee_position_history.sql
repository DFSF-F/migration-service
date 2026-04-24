with position_history_base as (

    select
        src_position_event_id,
        employee_src_id,
        position_code,
        department_src_id,
        event_type_code,
        event_type_name,
        salary_change_flag,
        effective_from,
        effective_to,
        load_dttm,
        batch_id
    from {{ source('hr_raw', 'hr_position_history_raw') }}

),

employee_dim as (

    select *
    from {{ ref('dim_hr_employee') }}
    where is_current_flag = true

),

position_dim as (

    select *
    from {{ ref('dim_position') }}

),

department_dim as (

    select *
    from {{ ref('dim_hr_department') }}
    where is_current_flag = true

)

select
    row_number() over (order by src_position_event_id) as employee_position_history_id,
    e.employee_id,
    p.position_id,
    d.department_id,
    h.event_type_code,
    h.event_type_name,
    case
        when h.salary_change_flag = 'Y' then true
        else false
    end as salary_change_flag,
    h.effective_from,
    h.effective_to,
    h.load_dttm,
    h.batch_id
from position_history_base h
left join employee_dim e
    on h.employee_src_id = e.employee_src_id
left join position_dim p
    on h.position_code = p.position_code
left join department_dim d
    on h.department_src_id = d.department_src_id