truncate table dds.fct_employee_hr_snapshot;
truncate table dds.fct_employee_activity_signal;
truncate table dds.fct_employee_absence;
truncate table dds.fct_employee_position_history;
truncate table dds.dim_hr_employee;
truncate table dds.dim_position;
truncate table dds.dim_hr_department;

insert into dds.dim_hr_department (
    department_id,
    department_src_id,
    department_name,
    parent_department_id,
    parent_department_src_id,
    block_name,
    function_name,
    region_name,
    org_level,
    manager_employee_src_id,
    effective_from,
    effective_to,
    is_current_flag,
    source_system,
    load_dttm,
    batch_id
)
select
    row_number() over (order by department_src_id, effective_from, coalesce(effective_to, date '2999-12-31')) as department_id,
    department_src_id,
    department_name,
    null::bigint as parent_department_id,
    parent_department_src_id,
    block_name,
    function_name,
    region_name,
    org_level,
    manager_src_id,
    effective_from,
    effective_to,
    case when effective_to is null then true else false end as is_current_flag,
    source_system,
    load_dttm,
    batch_id
from (
    select distinct
        department_src_id,
        department_name,
        parent_department_src_id,
        block_name,
        function_name,
        region_name,
        org_level,
        manager_src_id,
        effective_from,
        effective_to,
        source_system,
        load_dttm,
        batch_id
    from raw.hr_department_history_raw
) s;

update dds.dim_hr_department d
set parent_department_id = p.department_id
from dds.dim_hr_department p
where d.parent_department_src_id = p.department_src_id
  and p.is_current_flag = true;

insert into dds.dim_position (
    position_id,
    position_code,
    position_name,
    grade_code,
    source_system,
    load_dttm,
    batch_id
)
select
    row_number() over (order by position_code) as position_id,
    position_code,
    max(position_name) as position_name,
    null::varchar(32) as grade_code,
    'hr_position_catalog' as source_system,
    max(load_dttm) as load_dttm,
    max(batch_id) as batch_id
from (
    select
        current_position_code as position_code,
        current_position_name as position_name,
        load_dttm,
        batch_id
    from raw.hr_employee_master_raw

    union all

    select
        position_code,
        position_name,
        load_dttm,
        batch_id
    from raw.hr_position_history_raw
) p
group by position_code;

insert into dds.dim_hr_employee (
    employee_id,
    employee_src_id,
    employee_number,
    full_name,
    tab_num,
    birth_date,
    gender_code,
    employment_status,
    hire_date,
    dismissal_date,
    current_position_id,
    current_position_code,
    department_id,
    department_src_id,
    manager_employee_src_id,
    grade_code,
    work_format,
    location_name,
    valid_from,
    valid_to,
    is_current_flag,
    source_system,
    load_dttm,
    batch_id
)
select
    row_number() over (order by e.employee_src_id) as employee_id,
    e.employee_src_id,
    e.employee_number,
    e.full_name,
    e.tab_num,
    e.birth_date,
    e.gender_code,
    e.employment_status,
    e.hire_date,
    e.dismissal_date,
    p.position_id,
    e.current_position_code,
    d.department_id,
    e.department_src_id,
    e.manager_src_id,
    e.grade_code,
    e.work_format,
    e.location_name,
    e.snapshot_date as valid_from,
    null::date as valid_to,
    true as is_current_flag,
    e.source_system,
    e.load_dttm,
    e.batch_id
from (
    select distinct
        employee_src_id,
        employee_number,
        full_name,
        tab_num,
        birth_date,
        gender_code,
        employment_status,
        hire_date,
        dismissal_date,
        current_position_code,
        current_position_name,
        department_src_id,
        manager_src_id,
        grade_code,
        work_format,
        location_name,
        snapshot_date,
        source_system,
        load_dttm,
        batch_id
    from raw.hr_employee_master_raw
) e
left join dds.dim_position p
    on e.current_position_code = p.position_code
left join dds.dim_hr_department d
    on e.department_src_id = d.department_src_id
   and d.is_current_flag = true;

insert into dds.fct_employee_position_history (
    employee_position_history_id,
    employee_id,
    position_id,
    department_id,
    event_type_code,
    event_type_name,
    salary_change_flag,
    effective_from,
    effective_to,
    load_dttm,
    batch_id
)
select
    row_number() over (order by h.src_position_event_id) as employee_position_history_id,
    e.employee_id,
    p.position_id,
    d.department_id,
    h.event_type_code,
    h.event_type_name,
    case when h.salary_change_flag = 'Y' then true else false end,
    h.effective_from,
    h.effective_to,
    h.load_dttm,
    h.batch_id
from raw.hr_position_history_raw h
left join dds.dim_hr_employee e
    on h.employee_src_id = e.employee_src_id
   and e.is_current_flag = true
left join dds.dim_position p
    on h.position_code = p.position_code
left join dds.dim_hr_department d
    on h.department_src_id = d.department_src_id
   and d.is_current_flag = true;

insert into dds.fct_employee_absence (
    employee_absence_id,
    employee_id,
    absence_type_code,
    absence_type_name,
    absence_reason_group,
    start_date,
    end_date,
    duration_days,
    approved_flag,
    load_dttm,
    batch_id
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
    case when a.approved_flag = 'Y' then true else false end,
    a.load_dttm,
    a.batch_id
from raw.hr_absence_events_raw a
left join dds.dim_hr_employee e
    on a.employee_src_id = e.employee_src_id
   and e.is_current_flag = true;

insert into dds.fct_employee_activity_signal (
    employee_activity_signal_id,
    employee_id,
    signal_source_type,
    source_event_id,
    signal_code,
    signal_name,
    signal_group,
    signal_value_num,
    signal_value_text,
    detected_at,
    signal_status,
    load_dttm,
    batch_id
)
select
    row_number() over (order by signal_source_type, source_event_id) as employee_activity_signal_id,
    employee_id,
    signal_source_type,
    source_event_id,
    signal_code,
    signal_name,
    signal_group,
    signal_value_num,
    signal_value_text,
    detected_at,
    signal_status,
    load_dttm,
    batch_id
from (
    select
        e.employee_id,
        'overtime' as signal_source_type,
        o.src_overtime_event_id as source_event_id,
        'OVERTIME_HOURS' as signal_code,
        'Сверхурочная работа' as signal_name,
        'WORKLOAD' as signal_group,
        o.overtime_hours as signal_value_num,
        o.overtime_reason_name as signal_value_text,
        o.overtime_date::timestamp as detected_at,
        case when o.approved_flag = 'Y' then 'APPROVED' else 'PENDING' end as signal_status,
        o.load_dttm,
        o.batch_id
    from raw.hr_overtime_events_raw o
    left join dds.dim_hr_employee e
        on o.employee_src_id = e.employee_src_id
       and e.is_current_flag = true

    union all

    select
        e.employee_id,
        'dismissal_signal',
        s.src_signal_id,
        s.signal_code,
        s.signal_name,
        s.signal_group,
        s.signal_value_num,
        s.signal_value_text,
        s.detected_at,
        s.signal_status,
        s.load_dttm,
        s.batch_id
    from raw.hr_dismissal_signals_raw s
    left join dds.dim_hr_employee e
        on s.employee_src_id = e.employee_src_id
       and e.is_current_flag = true
) x;

insert into dds.fct_employee_hr_snapshot (
    employee_hr_snapshot_id,
    employee_id,
    report_date,
    current_position_id,
    current_department_id,
    absence_event_cnt,
    absence_days_total,
    overtime_hours_total,
    dismissal_signal_cnt,
    active_signal_cnt,
    instability_score,
    instability_level_code,
    calculation_dttm,
    load_dttm,
    batch_id
)
select
    row_number() over (order by e.employee_id) as employee_hr_snapshot_id,
    e.employee_id,
    date '2024-12-31' as report_date,
    e.current_position_id,
    e.department_id as current_department_id,
    coalesce(abs_agg.absence_event_cnt, 0) as absence_event_cnt,
    coalesce(abs_agg.absence_days_total, 0) as absence_days_total,
    coalesce(sig_agg.overtime_hours_total, 0) as overtime_hours_total,
    coalesce(sig_agg.dismissal_signal_cnt, 0) as dismissal_signal_cnt,
    coalesce(sig_agg.active_signal_cnt, 0) as active_signal_cnt,
    (
        coalesce(abs_agg.absence_days_total, 0) * 0.20 +
        coalesce(sig_agg.overtime_hours_total, 0) * 0.10 +
        coalesce(sig_agg.dismissal_signal_cnt, 0) * 2.00 +
        coalesce(sig_agg.active_signal_cnt, 0) * 1.50
    )::numeric(18,4) as instability_score,
    case
        when (
            coalesce(abs_agg.absence_days_total, 0) * 0.20 +
            coalesce(sig_agg.overtime_hours_total, 0) * 0.10 +
            coalesce(sig_agg.dismissal_signal_cnt, 0) * 2.00 +
            coalesce(sig_agg.active_signal_cnt, 0) * 1.50
        ) >= 12 then 'HIGH'
        when (
            coalesce(abs_agg.absence_days_total, 0) * 0.20 +
            coalesce(sig_agg.overtime_hours_total, 0) * 0.10 +
            coalesce(sig_agg.dismissal_signal_cnt, 0) * 2.00 +
            coalesce(sig_agg.active_signal_cnt, 0) * 1.50
        ) >= 5 then 'MEDIUM'
        else 'LOW'
    end as instability_level_code,
    timestamp '2024-12-31 23:00:00' as calculation_dttm,
    timestamp '2024-12-31 23:00:00' as load_dttm,
    'hr_dds_build_001' as batch_id
from dds.dim_hr_employee e
left join (
    select
        employee_id,
        count(*) as absence_event_cnt,
        sum(coalesce(duration_days, 0)) as absence_days_total
    from dds.fct_employee_absence
    group by employee_id
) abs_agg
    on e.employee_id = abs_agg.employee_id
left join (
    select
        employee_id,
        sum(case when signal_source_type = 'overtime' then coalesce(signal_value_num, 0) else 0 end) as overtime_hours_total,
        sum(case when signal_source_type = 'dismissal_signal' then 1 else 0 end) as dismissal_signal_cnt,
        sum(case when coalesce(signal_status, '') in ('ACTIVE', 'PENDING') then 1 else 0 end) as active_signal_cnt
    from dds.fct_employee_activity_signal
    group by employee_id
) sig_agg
    on e.employee_id = sig_agg.employee_id
where e.is_current_flag = true;