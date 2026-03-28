create table if not exists dds.dim_access_system (
    access_system_id          bigint        not null,
    system_code               varchar(64)   not null,
    system_name               varchar(255)  not null,
    source_system             varchar(64)   not null,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (access_system_id);

create table if not exists dds.dim_access_role (
    access_role_id            bigint        not null,
    system_id                 bigint,
    system_code               varchar(64)   not null,
    role_code                 varchar(64)   not null,
    role_name                 varchar(255)  not null,
    role_group                varchar(128),
    source_system             varchar(64)   not null,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (access_role_id);

create table if not exists dds.fct_employee_account (
    employee_account_id       bigint        not null,
    employee_id               bigint        not null,
    employee_src_id           varchar(64)   not null,
    system_id                 bigint,
    system_code               varchar(64)   not null,
    source_account_id         varchar(64)   not null,
    account_login             varchar(255)  not null,
    account_type_code         varchar(64),
    account_type_name         varchar(255),
    privileged_flag           boolean,
    admin_flag                boolean,
    account_status            varchar(64),
    created_at                timestamp,
    disabled_at               timestamp,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (employee_account_id);

create table if not exists dds.fct_employee_role_assignment (
    employee_role_assignment_id bigint      not null,
    employee_id               bigint        not null,
    employee_account_id       bigint,
    access_role_id            bigint,
    source_role_assignment_id varchar(64)   not null,
    assigned_at               timestamp     not null,
    revoked_at                timestamp,
    assignment_status         varchar(64),
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (employee_role_assignment_id);

create table if not exists dds.fct_employee_login_event (
    employee_login_event_id   bigint        not null,
    employee_id               bigint        not null,
    employee_account_id       bigint,
    system_id                 bigint,
    source_login_event_id     varchar(64)   not null,
    login_dttm                timestamp     not null,
    login_result              varchar(64),
    auth_method               varchar(64),
    ip_address                varchar(64),
    device_id                 varchar(128),
    country_name              varchar(128),
    city_name                 varchar(128),
    unusual_geo_flag          boolean,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (employee_login_event_id);

create table if not exists dds.fct_employee_access_signal (
    employee_access_signal_id bigint        not null,
    employee_id               bigint        not null,
    signal_source_type        varchar(64)   not null,
    source_event_id           varchar(64)   not null,
    system_id                 bigint,
    signal_code               varchar(64)   not null,
    signal_name               varchar(255)  not null,
    signal_group              varchar(128),
    signal_value_num          numeric(18,4),
    signal_value_text         varchar(255),
    signal_dttm               timestamp     not null,
    signal_status             varchar(64),
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (employee_access_signal_id);

create table if not exists dds.fct_employee_access_snapshot (
    employee_access_snapshot_id bigint      not null,
    employee_id               bigint        not null,
    report_date               date          not null,
    active_account_cnt        integer,
    privileged_account_cnt    integer,
    active_role_cnt           integer,
    privileged_access_cnt     integer,
    failed_login_cnt          integer,
    unusual_geo_login_cnt     integer,
    external_transfer_cnt     integer,
    blocked_network_cnt       integer,
    access_risk_score         numeric(18,4),
    access_risk_level_code    varchar(32),
    calculation_dttm          timestamp     not null,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (employee_access_snapshot_id);