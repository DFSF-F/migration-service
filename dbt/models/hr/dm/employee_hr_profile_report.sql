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

snapshot_fact as (

    select * from {{ ref('fct_employee_hr_snapshot') }}

),

final as (

    select
        s.report_date,

        e.employee_id,
        e.employee_src_id,
        e.employee_number,
        e.full_name,
        e.tab_num,
        e.birth_date,
        e.gender_code,
        e.employment_status,
        e.hire_date,
        e.dismissal_date,

        p.position_id as current_position_id,
        e.current_position_code,
        p.position_name as current_position_name,
        e.grade_code,

        d.department_id,
        d.department_src_id,
        d.department_name,
        d.parent_department_id,
        d.block_name,
        d.function_name,
        d.region_name,
        d.org_level,

        e.manager_employee_src_id,
        e.work_format,
        e.location_name,

        s.absence_event_cnt,
        s.absence_days_total,
        s.overtime_hours_total,
        s.dismissal_signal_cnt,
        s.active_signal_cnt,
        s.instability_score,
        s.instability_level_code,

        case when s.dismissal_signal_cnt > 0 then true else false end as dismissal_signal_flag,
        case when s.overtime_hours_total > 20 then true else false end as overtime_risk_flag,
        case when s.absence_days_total > 15 then true else false end as absence_risk_flag,

        s.calculation_dttm,
        s.load_dttm,
        s.batch_id
    from snapshot_fact s
    left join employee_dim e
        on s.employee_id = e.employee_id
    left join department_dim d
        on s.current_department_id = d.department_id
    left join position_dim p
        on s.current_position_id = p.position_id

)

select * from final