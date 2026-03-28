truncate table dm.employee_operational_risk_report;

insert into dm.employee_operational_risk_report (
    report_date,
    employee_id,
    employee_src_id,
    employee_number,
    tab_num,
    full_name,
    position_name,
    employment_status,
    hire_date,
    dismissal_date,

    department_id,
    department_src_id,
    department_name,
    parent_department_id,
    parent_department_src_id,
    block_name,
    function_name,
    region_name,
    org_level,

    risk_score_value,
    risk_level_code,

    open_event_cnt,
    critical_event_cnt,
    ib_event_cnt,
    security_event_cnt,
    compliance_event_cnt,
    nonwork_signal_cnt,

    ib_critical_event_cnt,
    security_critical_event_cnt,
    compliance_critical_event_cnt,

    has_open_risk_flag,
    has_critical_risk_flag,
    has_nonwork_signal_flag,

    last_risk_event_dttm,
    calculation_dttm,
    load_dttm,
    batch_id
)
select
    s.report_date,
    s.employee_id,
    e.employee_src_id,
    e.employee_number,
    e.tab_num,
    e.full_name,
    e.position_name,
    e.employment_status,
    e.hire_date,
    e.dismissal_date,

    d.department_id,
    d.department_src_id,
    d.department_name,
    d.parent_department_id,
    d.parent_department_src_id,
    d.block_name,
    d.function_name,
    d.region_name,
    d.org_level,

    s.risk_score_value,
    s.risk_level_code,

    s.open_event_cnt,
    s.critical_event_cnt,
    s.ib_event_cnt,
    s.security_event_cnt,
    s.compliance_event_cnt,
    s.nonwork_signal_cnt,

    coalesce(crit.ib_critical_event_cnt, 0) as ib_critical_event_cnt,
    coalesce(crit.security_critical_event_cnt, 0) as security_critical_event_cnt,
    coalesce(crit.compliance_critical_event_cnt, 0) as compliance_critical_event_cnt,

    case when s.open_event_cnt > 0 then true else false end as has_open_risk_flag,
    case when s.critical_event_cnt > 0 then true else false end as has_critical_risk_flag,
    case when s.nonwork_signal_cnt > 0 then true else false end as has_nonwork_signal_flag,

    evt.last_risk_event_dttm,
    s.calculation_dttm,
    s.load_dttm,
    s.batch_id
from dds.fct_employee_risk_snapshot s
left join dds.dim_employee e
    on s.employee_id = e.employee_id
   and e.is_current_flag = true
left join dds.dim_department d
    on e.department_id = d.department_id
   and d.is_current_flag = true
left join (
    select
        employee_id,
        max(event_detected_dttm) as last_risk_event_dttm
    from dds.fct_risk_event
    group by employee_id
) evt
    on s.employee_id = evt.employee_id
left join (
    select
        employee_id,
        sum(case when event_source_system = 'ib_incidents' and coalesce(severity_level, '') = 'CRITICAL' then 1 else 0 end) as ib_critical_event_cnt,
        sum(case when event_source_system = 'security_cases' and coalesce(severity_level, '') = 'CRITICAL' then 1 else 0 end) as security_critical_event_cnt,
        sum(case when event_source_system = 'compliance_cases' and coalesce(severity_level, '') = 'CRITICAL' then 1 else 0 end) as compliance_critical_event_cnt
    from dds.fct_risk_event
    group by employee_id
) crit
    on s.employee_id = crit.employee_id;

create or replace view dm.v_employee_risk_trend as
select
    s.report_date,
    s.employee_id,
    e.employee_src_id,
    e.full_name,
    e.position_name,
    d.department_name,
    d.block_name,
    d.function_name,
    d.region_name,
    s.risk_score_value,
    s.risk_level_code,
    s.open_event_cnt,
    s.critical_event_cnt,
    s.ib_event_cnt,
    s.security_event_cnt,
    s.compliance_event_cnt,
    s.nonwork_signal_cnt,
    lag(s.risk_score_value) over (
        partition by s.employee_id
        order by s.report_date
    ) as prev_risk_score_value,
    s.risk_score_value
      - lag(s.risk_score_value) over (
            partition by s.employee_id
            order by s.report_date
        ) as risk_score_delta
from dds.fct_employee_risk_snapshot s
left join dds.dim_employee e
    on s.employee_id = e.employee_id
   and e.is_current_flag = true
left join dds.dim_department d
    on e.department_id = d.department_id
   and d.is_current_flag = true;