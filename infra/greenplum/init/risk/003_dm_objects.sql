create table if not exists dm.employee_operational_risk_report (
    report_date                      date          not null,
    employee_id                      bigint        not null,
    employee_src_id                  varchar(64)   not null,
    employee_number                  varchar(64),
    tab_num                          varchar(64),
    full_name                        varchar(255)  not null,
    position_name                    varchar(255),
    employment_status                varchar(64),
    hire_date                        date,
    dismissal_date                   date,

    department_id                    bigint,
    department_src_id                varchar(64),
    department_name                  varchar(255),
    parent_department_id             bigint,
    parent_department_src_id         varchar(64),
    block_name                       varchar(255),
    function_name                    varchar(255),
    region_name                      varchar(255),
    org_level                        integer,

    risk_score_value                 numeric(18,4),
    risk_level_code                  varchar(32),

    open_event_cnt                   integer,
    critical_event_cnt               integer,
    ib_event_cnt                     integer,
    security_event_cnt               integer,
    compliance_event_cnt             integer,
    nonwork_signal_cnt               integer,

    ib_critical_event_cnt            integer,
    security_critical_event_cnt      integer,
    compliance_critical_event_cnt    integer,

    has_open_risk_flag               boolean,
    has_critical_risk_flag           boolean,
    has_nonwork_signal_flag          boolean,

    last_risk_event_dttm             timestamp,
    calculation_dttm                 timestamp     not null,
    load_dttm                        timestamp     not null,
    batch_id                         varchar(64)   not null
)
distributed by (employee_id);

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