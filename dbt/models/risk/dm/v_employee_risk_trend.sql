{{ config(materialized='view') }}

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

)

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
from snapshot_fact s
left join employee_dim e
    on s.employee_id = e.employee_id
left join department_dim d
    on e.department_id = d.department_id