with employee_dim as (

    select * from {{ ref('dim_hr_employee') }}
    where is_current_flag = true

),

department_dim as (

    select * from {{ ref('dim_hr_department') }}
    where is_current_flag = true

),

position_dim as (

    select * from {{ ref('dim_position') }}

),

signal_fact as (

    select * from {{ ref('fct_employee_activity_signal') }}

),

final as (

    select
        s.employee_activity_signal_id,
        s.employee_id,
        e.employee_src_id,
        e.employee_number,
        e.full_name,
        e.tab_num,

        p.position_name as current_position_name,
        d.department_name,
        d.block_name,
        d.function_name,
        d.region_name,

        s.signal_source_type,
        s.source_event_id,
        s.signal_code,
        s.signal_name,
        s.signal_group,
        s.signal_value_num,
        s.signal_value_text,
        s.detected_at,
        s.signal_status,
        s.load_dttm,
        s.batch_id
    from signal_fact s
    left join employee_dim e
        on s.employee_id = e.employee_id
    left join department_dim d
        on e.department_id = d.department_id
    left join position_dim p
        on e.current_position_id = p.position_id

)

select * from final