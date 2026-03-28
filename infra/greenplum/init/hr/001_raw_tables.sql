create table if not exists raw.hr_employee_master_raw (
    employee_src_id           varchar(64)   not null,
    employee_number           varchar(64),
    full_name                 varchar(255)  not null,
    tab_num                   varchar(64),
    birth_date                date,
    gender_code               varchar(16),
    employment_status         varchar(64),
    hire_date                 date,
    dismissal_date            date,
    current_position_code     varchar(64),
    current_position_name     varchar(255),
    department_src_id         varchar(64),
    department_name           varchar(255),
    manager_src_id            varchar(64),
    grade_code                varchar(32),
    work_format               varchar(64),
    location_name             varchar(255),
    snapshot_date             date          not null,
    source_system             varchar(64)   not null,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (employee_src_id);

create table if not exists raw.hr_position_history_raw (
    src_position_event_id     varchar(64)   not null,
    employee_src_id           varchar(64)   not null,
    position_code             varchar(64)   not null,
    position_name             varchar(255)  not null,
    department_src_id         varchar(64),
    department_name           varchar(255),
    event_type_code           varchar(64),
    event_type_name           varchar(255),
    grade_code                varchar(32),
    salary_change_flag        varchar(8),
    effective_from            date          not null,
    effective_to              date,
    source_system             varchar(64)   not null,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (src_position_event_id);

create table if not exists raw.hr_department_history_raw (
    src_department_event_id   varchar(64)   not null,
    department_src_id         varchar(64)   not null,
    department_name           varchar(255)  not null,
    parent_department_src_id  varchar(64),
    block_name                varchar(255),
    function_name             varchar(255),
    region_name               varchar(255),
    org_level                 integer,
    manager_src_id            varchar(64),
    effective_from            date          not null,
    effective_to              date,
    source_system             varchar(64)   not null,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (src_department_event_id);

create table if not exists raw.hr_absence_events_raw (
    src_absence_event_id      varchar(64)   not null,
    employee_src_id           varchar(64)   not null,
    absence_type_code         varchar(64)   not null,
    absence_type_name         varchar(255)  not null,
    absence_reason_group      varchar(128),
    start_date                date          not null,
    end_date                  date,
    duration_days             integer,
    approved_flag             varchar(8),
    source_system             varchar(64)   not null,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (src_absence_event_id);

create table if not exists raw.hr_overtime_events_raw (
    src_overtime_event_id     varchar(64)   not null,
    employee_src_id           varchar(64)   not null,
    overtime_date             date          not null,
    overtime_hours            numeric(10,2) not null,
    overtime_reason_code      varchar(64),
    overtime_reason_name      varchar(255),
    approved_flag             varchar(8),
    source_system             varchar(64)   not null,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (src_overtime_event_id);

create table if not exists raw.hr_dismissal_signals_raw (
    src_signal_id             varchar(64)   not null,
    employee_src_id           varchar(64)   not null,
    signal_code               varchar(64)   not null,
    signal_name               varchar(255)  not null,
    signal_group              varchar(128),
    signal_value_num          numeric(18,4),
    signal_value_text         varchar(255),
    detected_at               timestamp     not null,
    signal_status             varchar(64),
    source_system             varchar(64)   not null,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (src_signal_id);