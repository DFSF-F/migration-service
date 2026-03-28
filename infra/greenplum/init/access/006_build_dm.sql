truncate table dm.employee_access_control_report;

insert into dm.employee_access_control_report (
    report_date,
    employee_id,
    employee_src_id,
    employee_number,
    tab_num,
    full_name,
    position_name,
    employment_status,

    department_id,
    department_src_id,
    department_name,
    block_name,
    function_name,
    region_name,

    active_account_cnt,
    privileged_account_cnt,
    active_role_cnt,
    privileged_access_cnt,
    failed_login_cnt,
    unusual_geo_login_cnt,
    external_transfer_cnt,
    blocked_network_cnt,
    access_risk_score,
    access_risk_level_code,

    has_privileged_account_flag,
    has_unusual_geo_flag,
    has_external_transfer_flag,
    has_blocked_network_flag,

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
from dds.fct_employee_access_snapshot s
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
        max(signal_dttm) as last_signal_dttm
    from dds.fct_employee_access_signal
    group by employee_id
) sig
    on s.employee_id = sig.employee_id;