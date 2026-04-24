with snapshot_fact as (

    select *
    from {{ ref('fct_employee_risk_snapshot') }}

),

employee_dim as (

    select *
    from {{ ref('dim_employee') }}
    where is_current_flag = true

),

department_dim as (

    select *
    from {{ ref('dim_department') }}
    where is_current_flag = true

),

event_last as (

    select
        employee_id,
        max(event_detected_dttm) as last_risk_event_dttm
    from {{ ref('fct_risk_event') }}
    group by employee_id

),

critical_agg as (

    select
        employee_id,
        sum(case when event_source_system = 'ib_incidents' and coalesce(severity_level, '') = 'CRITICAL' then 1 else 0 end) as ib_critical_event_cnt,
        sum(case when event_source_system = 'security_cases' and coalesce(severity_level, '') = 'CRITICAL' then 1 else 0 end) as security_critical_event_cnt,
        sum(case when event_source_system = 'compliance_cases' and coalesce(severity_level, '') = 'CRITICAL' then 1 else 0 end) as compliance_critical_event_cnt
    from {{ ref('fct_risk_event') }}
    group by employee_id

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

    coalesce(c.ib_critical_event_cnt, 0) as ib_critical_event_cnt,
    coalesce(c.security_critical_event_cnt, 0) as security_critical_event_cnt,
    coalesce(c.compliance_critical_event_cnt, 0) as compliance_critical_event_cnt,

    case when s.open_event_cnt > 0 then true else false end as has_open_risk_flag,
    case when s.critical_event_cnt > 0 then true else false end as has_critical_risk_flag,
    case when s.nonwork_signal_cnt > 0 then true else false end as has_nonwork_signal_flag,

    evt.last_risk_event_dttm,
    s.calculation_dttm,
    s.load_dttm,
    s.batch_id
from snapshot_fact s
left join employee_dim e
    on s.employee_id = e.employee_id
left join department_dim d
    on e.department_id = d.department_id
left join event_last evt
    on s.employee_id = evt.employee_id
left join critical_agg c
    on s.employee_id = c.employee_id