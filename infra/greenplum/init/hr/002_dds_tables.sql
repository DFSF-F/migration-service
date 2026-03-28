create table if not exists dds.dim_hr_department (
    department_id             bigint        not null,
    department_src_id         varchar(64)   not null,
    department_name           varchar(255)  not null,
    parent_department_id      bigint,
    parent_department_src_id  varchar(64),
    block_name                varchar(255),
    function_name             varchar(255),
    region_name               varchar(255),
    org_level                 integer,
    manager_employee_src_id   varchar(64),
    effective_from            date,
    effective_to              date,
    is_current_flag           boolean       not null,
    source_system             varchar(64)   not null,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (department_id);

create table if not exists dds.dim_position (
    position_id               bigint        not null,
    position_code             varchar(64)   not null,
    position_name             varchar(255)  not null,
    grade_code                varchar(32),
    source_system             varchar(64)   not null,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (position_id);

create table if not exists dds.dim_hr_employee (
    employee_id               bigint        not null,
    employee_src_id           varchar(64)   not null,
    employee_number           varchar(64),
    full_name                 varchar(255)  not null,
    tab_num                   varchar(64),
    birth_date                date,
    gender_code               varchar(16),
    employment_status         varchar(64),
    hire_date                 date,
    dismissal_date            date,
    current_position_id       bigint,
    current_position_code     varchar(64),
    department_id             bigint,
    department_src_id         varchar(64),
    manager_employee_src_id   varchar(64),
    grade_code                varchar(32),
    work_format               varchar(64),
    location_name             varchar(255),
    valid_from                date,
    valid_to                  date,
    is_current_flag           boolean       not null,
    source_system             varchar(64)   not null,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (employee_id);

create table if not exists dds.fct_employee_position_history (
    employee_position_history_id bigint     not null,
    employee_id               bigint        not null,
    position_id               bigint,
    department_id             bigint,
    event_type_code           varchar(64),
    event_type_name           varchar(255),
    salary_change_flag        boolean,
    effective_from            date          not null,
    effective_to              date,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (employee_position_history_id);

create table if not exists dds.fct_employee_absence (
    employee_absence_id       bigint        not null,
    employee_id               bigint        not null,
    absence_type_code         varchar(64)   not null,
    absence_type_name         varchar(255)  not null,
    absence_reason_group      varchar(128),
    start_date                date          not null,
    end_date                  date,
    duration_days             integer,
    approved_flag             boolean,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (employee_absence_id);

create table if not exists dds.fct_employee_activity_signal (
    employee_activity_signal_id bigint      not null,
    employee_id               bigint        not null,
    signal_source_type        varchar(64)   not null,
    source_event_id           varchar(64)   not null,
    signal_code               varchar(64)   not null,
    signal_name               varchar(255)  not null,
    signal_group              varchar(128),
    signal_value_num          numeric(18,4),
    signal_value_text         varchar(255),
    detected_at               timestamp     not null,
    signal_status             varchar(64),
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (employee_activity_signal_id);

create table if not exists dds.fct_employee_hr_snapshot (
    employee_hr_snapshot_id   bigint        not null,
    employee_id               bigint        not null,
    report_date               date          not null,
    current_position_id       bigint,
    current_department_id     bigint,
    absence_event_cnt         integer,
    absence_days_total        integer,
    overtime_hours_total      numeric(18,2),
    dismissal_signal_cnt      integer,
    active_signal_cnt         integer,
    instability_score         numeric(18,4),
    instability_level_code    varchar(32),
    calculation_dttm          timestamp     not null,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (employee_hr_snapshot_id);