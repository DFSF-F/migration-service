with snapshot_fact as (

    select * from {{ ref('fct_employee_access_snapshot') }}

),

employee_dim as (

    select *
    from {{ ref('dim_hr_employee') }}
    where is_current_flag = true

),

position_dim as (

    select * from {{ ref('dim_position') }}

),

department_dim as (

    select *
    from {{ ref('dim_hr_department') }}
    where is_current_flag = true

),

sig as (

    select
        employee_id,
        max(signal_dttm) as last_signal_dttm
    from {{ ref('fct_employee_access_signal') }}
    group by employee_id

)

select
    s.report_date,
    e.employee_id,
    e.employee_src_id,
    e.employee_number,
    e.tab_num,
    e.full_name,
    p.position_name,
    e.employment_status,

    d.department_id,
    d.department_src_id,
    d.department_name,
    d.block_name,
    d.function_name,
    d.region_name,

    s.active_account_cnt,
    s.privileged_account_cnt,
    s.active_role_cnt,
    s.privileged_access_cnt,
    s.failed_login_cnt,
    s.unusual_geo_login_cnt,
    s.external_transfer_cnt,
    s.blocked_network_cnt,
    s.access_risk_score,
    s.access_risk_level_code,

    case when s.privileged_account_cnt > 0 then true else false end as has_privileged_account_flag,
    case when s.unusual_geo_login_cnt > 0 then true else false end as has_unusual_geo_flag,
    case when s.external_transfer_cnt > 0 then true else false end as has_external_transfer_flag,
    case when s.blocked_network_cnt > 0 then true else false end as has_blocked_network_flag,

    sig.last_signal_dttm,
    s.calculation_dttm,
    s.load_dttm,
    s.batch_id
from snapshot_fact s
left join employee_dim e
    on s.employee_id = e.employee_id
left join position_dim p
    on e.current_position_id = p.position_id
left join department_dim d
    on e.department_id = d.department_id
left join sig
    on s.employee_id = sig.employee_id