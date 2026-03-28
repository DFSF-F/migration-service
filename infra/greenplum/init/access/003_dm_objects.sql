create table if not exists dm.employee_access_control_report (
    report_date                  date          not null,
    employee_id                  bigint        not null,
    employee_src_id              varchar(64)   not null,
    employee_number              varchar(64),
    tab_num                      varchar(64),
    full_name                    varchar(255)  not null,
    position_name                varchar(255),
    employment_status            varchar(64),

    department_id                bigint,
    department_src_id            varchar(64),
    department_name              varchar(255),
    block_name                   varchar(255),
    function_name                varchar(255),
    region_name                  varchar(255),

    active_account_cnt           integer,
    privileged_account_cnt       integer,
    active_role_cnt              integer,
    privileged_access_cnt        integer,
    failed_login_cnt             integer,
    unusual_geo_login_cnt        integer,
    external_transfer_cnt        integer,
    blocked_network_cnt          integer,
    access_risk_score            numeric(18,4),
    access_risk_level_code       varchar(32),

    has_privileged_account_flag  boolean,
    has_unusual_geo_flag         boolean,
    has_external_transfer_flag   boolean,
    has_blocked_network_flag     boolean,

    last_signal_dttm             timestamp,
    calculation_dttm             timestamp     not null,
    load_dttm                    timestamp     not null,
    batch_id                     varchar(64)   not null
)
distributed by (employee_id);

create or replace view dm.v_privileged_access_exceptions as
select
    s.report_date,
    e.employee_id,
    e.employee_src_id,
    e.full_name,
    p.position_name,
    d.department_name,
    d.block_name,
    d.function_name,
    d.region_name,
    a.signal_source_type,
    a.source_event_id,
    a.signal_code,
    a.signal_name,
    a.signal_group,
    a.signal_value_num,
    a.signal_value_text,
    a.signal_dttm,
    a.signal_status
from dds.fct_employee_access_signal a
left join dds.dim_hr_employee e
    on a.employee_id = e.employee_id
   and e.is_current_flag = true
left join dds.dim_position p
    on e.current_position_id = p.position_id
left join dds.dim_hr_department d
    on e.department_id = d.department_id
   and d.is_current_flag = true
left join dds.fct_employee_access_snapshot s
    on a.employee_id = s.employee_id
where a.signal_source_type in ('privileged_access', 'file_operation', 'network_activity');