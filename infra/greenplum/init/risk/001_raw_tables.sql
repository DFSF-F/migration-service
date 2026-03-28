create table if not exists raw.risk_ib_incidents_raw (
    src_incident_id           varchar(64)   not null,
    employee_src_id           varchar(64)   not null,
    incident_code             varchar(64)   not null,
    incident_name             varchar(255)  not null,
    severity_level            varchar(32),
    incident_status           varchar(64),
    detected_at               timestamp     not null,
    closed_at                 timestamp,
    channel_name              varchar(128),
    source_system             varchar(64)   not null,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (src_incident_id);

create table if not exists raw.risk_security_incidents_raw (
    src_case_id               varchar(64)   not null,
    employee_src_id           varchar(64)   not null,
    case_type_code            varchar(64)   not null,
    case_type_name            varchar(255)  not null,
    risk_flag                 varchar(8),
    case_status               varchar(64),
    registered_at             timestamp     not null,
    resolved_at               timestamp,
    resolution_code           varchar(64),
    resolution_name           varchar(255),
    source_system             varchar(64)   not null,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (src_case_id);

create table if not exists raw.risk_compliance_incidents_raw (
    src_violation_id          varchar(64)   not null,
    employee_src_id           varchar(64)   not null,
    violation_code            varchar(64)   not null,
    violation_name            varchar(255)  not null,
    control_area              varchar(128),
    materiality_level         varchar(32),
    violation_status          varchar(64),
    detected_at               timestamp     not null,
    decision_at               timestamp,
    decision_text             varchar(1000),
    source_system             varchar(64)   not null,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (src_violation_id);

create table if not exists raw.risk_nonwork_activity_raw (
    src_activity_id           varchar(64)   not null,
    employee_src_id           varchar(64)   not null,
    activity_type_code        varchar(64)   not null,
    activity_type_name        varchar(255)  not null,
    activity_group            varchar(128),
    activity_status           varchar(64),
    detected_at               timestamp     not null,
    valid_from                date,
    valid_to                  date,
    comment_text              varchar(1000),
    source_system             varchar(64)   not null,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (src_activity_id);

create table if not exists raw.risk_employee_registry_raw (
    employee_src_id           varchar(64)   not null,
    employee_number           varchar(64),
    full_name                 varchar(255)  not null,
    tab_num                   varchar(64),
    position_name             varchar(255),
    department_src_id         varchar(64),
    department_name           varchar(255),
    manager_src_id            varchar(64),
    employment_status         varchar(64),
    hire_date                 date,
    dismissal_date            date,
    snapshot_date             date          not null,
    source_system             varchar(64)   not null,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (employee_src_id);

create table if not exists raw.risk_org_structure_raw (
    department_src_id         varchar(64)   not null,
    department_name           varchar(255)  not null,
    parent_department_src_id  varchar(64),
    block_name                varchar(255),
    function_name             varchar(255),
    region_name               varchar(255),
    org_level                 integer,
    valid_from                date,
    valid_to                  date,
    snapshot_date             date          not null,
    source_system             varchar(64)   not null,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (department_src_id);