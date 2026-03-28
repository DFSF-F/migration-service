create table if not exists dds.dim_department (
    department_id             bigint        not null,
    department_src_id         varchar(64)   not null,
    department_name           varchar(255)  not null,
    parent_department_id      bigint,
    parent_department_src_id  varchar(64),
    block_name                varchar(255),
    function_name             varchar(255),
    region_name               varchar(255),
    org_level                 integer,
    valid_from                date,
    valid_to                  date,
    is_current_flag           boolean       not null,
    source_system             varchar(64)   not null,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (department_id);

create table if not exists dds.dim_employee (
    employee_id               bigint        not null,
    employee_src_id           varchar(64)   not null,
    employee_number           varchar(64),
    full_name                 varchar(255)  not null,
    tab_num                   varchar(64),
    position_name             varchar(255),
    department_id             bigint,
    department_src_id         varchar(64),
    manager_employee_src_id   varchar(64),
    employment_status         varchar(64),
    hire_date                 date,
    dismissal_date            date,
    valid_from                date,
    valid_to                  date,
    is_current_flag           boolean       not null,
    source_system             varchar(64)   not null,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (employee_id);

create table if not exists dds.dim_risk_event_type (
    risk_event_type_id        bigint        not null,
    source_system_name        varchar(64)   not null,
    source_event_code         varchar(64)   not null,
    source_event_name         varchar(255)  not null,
    risk_domain               varchar(64)   not null,
    event_group_name          varchar(128),
    default_severity_level    varchar(32),
    is_violation_flag         boolean       not null,
    is_behavior_signal_flag   boolean       not null,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (risk_event_type_id);

create table if not exists dds.fct_risk_event (
    risk_event_id             bigint        not null,
    source_event_id           varchar(64)   not null,
    employee_id               bigint        not null,
    employee_src_id           varchar(64)   not null,
    risk_event_type_id        bigint        not null,
    event_source_system       varchar(64)   not null,
    event_status              varchar(64),
    severity_level            varchar(32),
    risk_flag                 boolean,
    event_detected_dttm       timestamp     not null,
    event_closed_dttm         timestamp,
    decision_code             varchar(64),
    decision_text             varchar(1000),
    comment_text              varchar(1000),
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (risk_event_id);

create table if not exists dds.fct_employee_risk_factor (
    employee_risk_factor_id   bigint        not null,
    employee_id               bigint        not null,
    report_date               date          not null,
    risk_factor_code          varchar(64)   not null,
    risk_factor_name          varchar(255)  not null,
    factor_value_num          numeric(18,4),
    factor_value_text         varchar(255),
    risk_domain               varchar(64),
    calculation_dttm          timestamp     not null,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (employee_risk_factor_id);

create table if not exists dds.fct_employee_risk_snapshot (
    employee_risk_snapshot_id bigint        not null,
    employee_id               bigint        not null,
    report_date               date          not null,
    risk_score_value          numeric(18,4),
    risk_level_code           varchar(32),
    open_event_cnt            integer,
    critical_event_cnt        integer,
    ib_event_cnt              integer,
    security_event_cnt        integer,
    compliance_event_cnt      integer,
    nonwork_signal_cnt        integer,
    calculation_dttm          timestamp     not null,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (employee_risk_snapshot_id);