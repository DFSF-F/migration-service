CREATE TABLE IF NOT EXISTS `{{PROJECT_ID}}.hr_dm.employee_hr_profile_report` (
    report_date DATE,
    employee_id INT64,
    employee_src_id STRING,
    employee_number STRING,
    tab_num STRING,
    full_name STRING,
    birth_date DATE,
    gender_code STRING,
    employment_status STRING,
    hire_date DATE,
    dismissal_date DATE,

    current_position_id INT64,
    current_position_code STRING,
    current_position_name STRING,
    grade_code STRING,

    department_id INT64,
    department_src_id STRING,
    department_name STRING,
    parent_department_id INT64,
    block_name STRING,
    function_name STRING,
    region_name STRING,
    org_level STRING,

    manager_employee_src_id STRING,
    work_format STRING,
    location_name STRING,

    absence_event_cnt INT64,
    absence_days_total INT64,
    overtime_hours_total NUMERIC,
    dismissal_signal_cnt INT64,
    active_signal_cnt INT64,
    instability_score NUMERIC,
    instability_level_code STRING,

    has_absence_flag BOOL,
    has_overtime_flag BOOL,
    has_dismissal_signal_flag BOOL,

    last_signal_dttm TIMESTAMP,
    calculation_dttm TIMESTAMP,
    load_dttm TIMESTAMP,
    batch_id STRING
);

TRUNCATE TABLE `{{PROJECT_ID}}.hr_dm.employee_hr_profile_report`;

INSERT INTO `{{PROJECT_ID}}.hr_dm.employee_hr_profile_report` (
    report_date,
    employee_id,
    employee_src_id,
    employee_number,
    tab_num,
    full_name,
    birth_date,
    gender_code,
    employment_status,
    hire_date,
    dismissal_date,

    current_position_id,
    current_position_code,
    current_position_name,
    grade_code,

    department_id,
    department_src_id,
    department_name,
    parent_department_id,
    block_name,
    function_name,
    region_name,
    org_level,

    manager_employee_src_id,
    work_format,
    location_name,

    absence_event_cnt,
    absence_days_total,
    overtime_hours_total,
    dismissal_signal_cnt,
    active_signal_cnt,
    instability_score,
    instability_level_code,

    has_absence_flag,
    has_overtime_flag,
    has_dismissal_signal_flag,

    last_signal_dttm,
    calculation_dttm,
    load_dttm,
    batch_id
)
SELECT
    s.report_date,
    e.employee_id,
    e.employee_src_id,
    e.employee_number,
    e.tab_num,
    e.full_name,
    e.birth_date,
    e.gender_code,
    e.employment_status,
    e.hire_date,
    e.dismissal_date,

    e.current_position_id,
    e.current_position_code,
    p.position_name AS current_position_name,
    e.grade_code,

    d.department_id,
    d.department_src_id,
    d.department_name,
    d.parent_department_id,
    d.block_name,
    d.function_name,
    d.region_name,
    CAST(d.org_level AS STRING) AS org_level,

    e.manager_employee_src_id,
    e.work_format,
    e.location_name,

    s.absence_event_cnt,
    s.absence_days_total,
    s.overtime_hours_total,
    s.dismissal_signal_cnt,
    s.active_signal_cnt,
    s.instability_score,
    s.instability_level_code,

    CASE WHEN s.absence_event_cnt > 0 THEN TRUE ELSE FALSE END AS has_absence_flag,
    CASE WHEN s.overtime_hours_total > 0 THEN TRUE ELSE FALSE END AS has_overtime_flag,
    CASE WHEN s.dismissal_signal_cnt > 0 THEN TRUE ELSE FALSE END AS has_dismissal_signal_flag,

    sig.last_signal_dttm,
    s.calculation_dttm,
    s.load_dttm,
    s.batch_id
FROM `{{PROJECT_ID}}.hr_dds.fct_employee_hr_snapshot` s
LEFT JOIN `{{PROJECT_ID}}.hr_dds.dim_hr_employee` e
    ON s.employee_id = e.employee_id
   AND e.is_current_flag = TRUE
LEFT JOIN `{{PROJECT_ID}}.hr_dds.dim_position` p
    ON e.current_position_id = p.position_id
LEFT JOIN `{{PROJECT_ID}}.hr_dds.dim_hr_department` d
    ON e.department_id = d.department_id
   AND d.is_current_flag = TRUE
LEFT JOIN (
    SELECT
        employee_id,
        MAX(detected_at) AS last_signal_dttm
    FROM `{{PROJECT_ID}}.hr_dds.fct_employee_activity_signal`
    GROUP BY employee_id
) sig
    ON s.employee_id = sig.employee_id;