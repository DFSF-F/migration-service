truncate table dm.employee_hr_profile_report;

insert into dm.employee_hr_profile_report (
    report_date,
    employee_id,
    employee_src_id,
    employee_number,
    tab_num,
    full_name,
    birth_date,
    gender_code,
    employment_status,
    hire_date,
    dismissal_date,

    current_position_id,
    current_position_code,
    current_position_name,
    grade_code,

    department_id,
    department_src_id,
    department_name,
    parent_department_id,
    block_name,
    function_name,
    region_name,
    org_level,

    manager_employee_src_id,
    work_format,
    location_name,

    absence_event_cnt,
    absence_days_total,
    overtime_hours_total,
    dismissal_signal_cnt,
    active_signal_cnt,
    instability_score,
    instability_level_code,

    has_absence_flag,
    has_overtime_flag,
    has_dismissal_signal_flag,

    last_signal_dttm,
    calculation_dttm,
    load_dttm,
    batch_id
)
select
    s.report_date,
    e.employee_id,
    e.employee_src_id,
    e.employee_number,
    e.tab_num,
    e.full_name,
    e.birth_date,
    e.gender_code,
    e.employment_status,
    e.hire_date,
    e.dismissal_date,

    e.current_position_id,
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

    case when s.absence_event_cnt > 0 then true else false end as has_absence_flag,
    case when s.overtime_hours_total > 0 then true else false end as has_overtime_flag,
    case when s.dismissal_signal_cnt > 0 then true else false end as has_dismissal_signal_flag,

    sig.last_signal_dttm,
    s.calculation_dttm,
    s.load_dttm,
    s.batch_id
from dds.fct_employee_hr_snapshot s
left join dds.dim_hr_employee e
    on s.employee_id = e.employee_id
   and e.is_current_flag = true
left join dds.dim_position p
    on e.current_position_id = p.position_id
left join dds.dim_hr_department d
    on e.department_id = d.department_id
   and d.is_current_flag = true
left join (
    select
        employee_id,
        max(detected_at) as last_signal_dttm
    from dds.fct_employee_activity_signal
    group by employee_id
) sig
    on s.employee_id = sig.employee_id;