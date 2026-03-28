create table if not exists dm.employee_hr_profile_report (
    report_date                  date          not null,
    employee_id                  bigint        not null,
    employee_src_id              varchar(64)   not null,
    employee_number              varchar(64),
    tab_num                      varchar(64),
    full_name                    varchar(255)  not null,
    birth_date                   date,
    gender_code                  varchar(16),
    employment_status            varchar(64),
    hire_date                    date,
    dismissal_date               date,

    current_position_id          bigint,
    current_position_code        varchar(64),
    current_position_name        varchar(255),
    grade_code                   varchar(32),

    department_id                bigint,
    department_src_id            varchar(64),
    department_name              varchar(255),
    parent_department_id         bigint,
    block_name                   varchar(255),
    function_name                varchar(255),
    region_name                  varchar(255),
    org_level                    integer,

    manager_employee_src_id      varchar(64),
    work_format                  varchar(64),
    location_name                varchar(255),

    absence_event_cnt            integer,
    absence_days_total           integer,
    overtime_hours_total         numeric(18,2),
    dismissal_signal_cnt         integer,
    active_signal_cnt            integer,
    instability_score            numeric(18,4),
    instability_level_code       varchar(32),

    has_absence_flag             boolean,
    has_overtime_flag            boolean,
    has_dismissal_signal_flag    boolean,

    last_signal_dttm             timestamp,
    calculation_dttm             timestamp     not null,
    load_dttm                    timestamp     not null,
    batch_id                     varchar(64)   not null
)
distributed by (employee_id);

create or replace view dm.v_employee_instability_signals as
select
    s.report_date,
    e.employee_id,
    e.employee_src_id,
    e.full_name,
    p.position_name as current_position_name,
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
    a.detected_at,
    a.signal_status
from dds.fct_employee_activity_signal a
left join dds.dim_hr_employee e
    on a.employee_id = e.employee_id
   and e.is_current_flag = true
left join dds.dim_position p
    on e.current_position_id = p.position_id
left join dds.dim_hr_department d
    on e.department_id = d.department_id
   and d.is_current_flag = true
left join dds.fct_employee_hr_snapshot s
    on a.employee_id = s.employee_id;