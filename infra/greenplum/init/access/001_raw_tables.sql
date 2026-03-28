create table if not exists raw.access_system_accounts_raw (
    src_account_id            varchar(64)   not null,
    employee_src_id           varchar(64)   not null,
    system_code               varchar(64)   not null,
    system_name               varchar(255)  not null,
    account_login             varchar(255)  not null,
    account_type_code         varchar(64),
    account_type_name         varchar(255),
    privileged_flag           varchar(8),
    admin_flag                varchar(8),
    account_status            varchar(64),
    created_at                timestamp,
    disabled_at               timestamp,
    source_system             varchar(64)   not null,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (src_account_id);

create table if not exists raw.access_role_assignments_raw (
    src_role_assignment_id    varchar(64)   not null,
    employee_src_id           varchar(64)   not null,
    src_account_id            varchar(64)   not null,
    system_code               varchar(64)   not null,
    role_code                 varchar(64)   not null,
    role_name                 varchar(255)  not null,
    role_group                varchar(128),
    assigned_at               timestamp     not null,
    revoked_at                timestamp,
    assignment_status         varchar(64),
    source_system             varchar(64)   not null,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (src_role_assignment_id);

create table if not exists raw.access_privileged_access_raw (
    src_priv_event_id         varchar(64)   not null,
    employee_src_id           varchar(64)   not null,
    src_account_id            varchar(64),
    system_code               varchar(64)   not null,
    access_type_code          varchar(64)   not null,
    access_type_name          varchar(255)  not null,
    request_id                varchar(64),
    approved_flag             varchar(8),
    start_dttm                timestamp     not null,
    end_dttm                  timestamp,
    access_status             varchar(64),
    source_system             varchar(64)   not null,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (src_priv_event_id);

create table if not exists raw.access_login_events_raw (
    src_login_event_id        varchar(64)   not null,
    employee_src_id           varchar(64)   not null,
    src_account_id            varchar(64),
    system_code               varchar(64)   not null,
    login_dttm                timestamp     not null,
    login_result              varchar(64),
    auth_method               varchar(64),
    ip_address                varchar(64),
    device_id                 varchar(128),
    country_name              varchar(128),
    city_name                 varchar(128),
    unusual_geo_flag          varchar(8),
    source_system             varchar(64)   not null,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (src_login_event_id);

create table if not exists raw.access_file_operations_raw (
    src_file_event_id         varchar(64)   not null,
    employee_src_id           varchar(64)   not null,
    src_account_id            varchar(64),
    system_code               varchar(64)   not null,
    operation_type_code       varchar(64)   not null,
    operation_type_name       varchar(255)  not null,
    file_classification       varchar(128),
    operation_dttm            timestamp     not null,
    object_path               varchar(1000),
    download_flag             varchar(8),
    upload_flag               varchar(8),
    external_transfer_flag    varchar(8),
    source_system             varchar(64)   not null,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (src_file_event_id);

create table if not exists raw.access_network_activity_raw (
    src_network_event_id      varchar(64)   not null,
    employee_src_id           varchar(64)   not null,
    src_account_id            varchar(64),
    system_code               varchar(64),
    event_dttm                timestamp     not null,
    destination_type          varchar(64),
    destination_name          varchar(255),
    traffic_mb                numeric(18,4),
    connection_count          integer,
    blocked_flag              varchar(8),
    vpn_used_flag             varchar(8),
    source_system             varchar(64)   not null,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (src_network_event_id);