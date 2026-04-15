CREATE TABLE IF NOT EXISTS `{{PROJECT_ID}}.hr_dds.dim_hr_department` (
    department_id INT64,
    department_src_id STRING,
    department_name STRING,
    parent_department_id INT64,
    parent_department_src_id STRING,
    block_name STRING,
    function_name STRING,
    region_name STRING,
    org_level STRING,
    manager_employee_src_id STRING,
    effective_from DATE,
    effective_to DATE,
    is_current_flag BOOL,
    source_system STRING,
    load_dttm INT64,
    batch_id STRING
);

CREATE TABLE IF NOT EXISTS `{{PROJECT_ID}}.hr_dds.dim_position` (
    position_id INT64,
    position_code STRING,
    position_name STRING,
    grade_code STRING,
    source_system STRING,
    load_dttm INT64,
    batch_id STRING
);

CREATE TABLE IF NOT EXISTS `{{PROJECT_ID}}.hr_dds.dim_hr_employee` (
    employee_id INT64,
    employee_src_id STRING,
    employee_number STRING,
    full_name STRING,
    tab_num STRING,
    birth_date DATE,
    gender_code STRING,
    employment_status STRING,
    hire_date DATE,
    dismissal_date DATE,
    current_position_id INT64,
    current_position_code STRING,
    department_id INT64,
    department_src_id STRING,
    manager_employee_src_id STRING,
    grade_code STRING,
    work_format STRING,
    location_name STRING,
    valid_from DATE,
    valid_to DATE,
    is_current_flag BOOL,
    source_system STRING,
    load_dttm INT64,
    batch_id STRING
);

CREATE TABLE IF NOT EXISTS `{{PROJECT_ID}}.hr_dds.fct_employee_position_history` (
    employee_position_history_id INT64,
    employee_id INT64,
    position_id INT64,
    department_id INT64,
    event_type_code STRING,
    event_type_name STRING,
    salary_change_flag BOOL,
    effective_from DATE,
    effective_to DATE,
    load_dttm INT64,
    batch_id STRING
);

CREATE TABLE IF NOT EXISTS `{{PROJECT_ID}}.hr_dds.fct_employee_absence` (
    employee_absence_id INT64,
    employee_id INT64,
    absence_type_code STRING,
    absence_type_name STRING,
    absence_reason_group STRING,
    start_date DATE,
    end_date DATE,
    duration_days INT64,
    approved_flag BOOL,
    load_dttm INT64,
    batch_id STRING
);

CREATE OR REPLACE TABLE `{{PROJECT_ID}}.hr_dds.fct_employee_activity_signal` (
    employee_activity_signal_id INT64,
    employee_id INT64,
    signal_source_type STRING,
    source_event_id STRING,
    signal_code STRING,
    signal_name STRING,
    signal_group STRING,
    signal_value_num NUMERIC,
    signal_value_text STRING,
    detected_at TIMESTAMP,
    signal_status STRING,
    load_dttm INT64,
    batch_id STRING
);

CREATE OR REPLACE TABLE `{{PROJECT_ID}}.hr_dds.fct_employee_hr_snapshot` (
    employee_hr_snapshot_id INT64,
    employee_id INT64,
    report_date DATE,
    current_position_id INT64,
    current_department_id INT64,
    absence_event_cnt INT64,
    absence_days_total INT64,
    overtime_hours_total NUMERIC,
    dismissal_signal_cnt INT64,
    active_signal_cnt INT64,
    instability_score NUMERIC,
    instability_level_code STRING,
    calculation_dttm TIMESTAMP,
    load_dttm TIMESTAMP,
    batch_id STRING
);

TRUNCATE TABLE `{{PROJECT_ID}}.hr_dds.fct_employee_hr_snapshot`;
TRUNCATE TABLE `{{PROJECT_ID}}.hr_dds.fct_employee_activity_signal`;
TRUNCATE TABLE `{{PROJECT_ID}}.hr_dds.fct_employee_absence`;
TRUNCATE TABLE `{{PROJECT_ID}}.hr_dds.fct_employee_position_history`;
TRUNCATE TABLE `{{PROJECT_ID}}.hr_dds.dim_hr_employee`;
TRUNCATE TABLE `{{PROJECT_ID}}.hr_dds.dim_position`;
TRUNCATE TABLE `{{PROJECT_ID}}.hr_dds.dim_hr_department`;

INSERT INTO `{{PROJECT_ID}}.hr_dds.dim_hr_department` (
    department_id,
    department_src_id,
    department_name,
    parent_department_id,
    parent_department_src_id,
    block_name,
    function_name,
    region_name,
    org_level,
    manager_employee_src_id,
    effective_from,
    effective_to,
    is_current_flag,
    source_system,
    load_dttm,
    batch_id
)
SELECT
    ROW_NUMBER() OVER (
        ORDER BY department_src_id, effective_from, COALESCE(effective_to, DATE '2999-12-31')
    ) AS department_id,
    department_src_id,
    department_name,
    CAST(NULL AS INT64) AS parent_department_id,
    parent_department_src_id,
    block_name,
    function_name,
    region_name,
    org_level,
    manager_src_id,
    effective_from,
    effective_to,
    CASE WHEN effective_to IS NULL THEN TRUE ELSE FALSE END AS is_current_flag,
    source_system,
    load_dttm,
    batch_id
FROM (
    SELECT DISTINCT
        department_src_id,
        department_name,
        parent_department_src_id,
        block_name,
        function_name,
        region_name,
        org_level,
        manager_src_id,
        effective_from,
        effective_to,
        source_system,
        load_dttm,
        batch_id
    FROM `{{PROJECT_ID}}.hr_raw.hr_department_history_raw`
) s;

UPDATE `{{PROJECT_ID}}.hr_dds.dim_hr_department` d
SET parent_department_id = p.department_id
FROM `{{PROJECT_ID}}.hr_dds.dim_hr_department` p
WHERE d.parent_department_src_id = p.department_src_id
  AND p.is_current_flag = TRUE;

INSERT INTO `{{PROJECT_ID}}.hr_dds.dim_position` (
    position_id,
    position_code,
    position_name,
    grade_code,
    source_system,
    load_dttm,
    batch_id
)
SELECT
    ROW_NUMBER() OVER (ORDER BY position_code) AS position_id,
    position_code,
    MAX(position_name) AS position_name,
    CAST(NULL AS STRING) AS grade_code,
    'hr_position_catalog' AS source_system,
    MAX(load_dttm) AS load_dttm,
    MAX(batch_id) AS batch_id
FROM (
    SELECT
        current_position_code AS position_code,
        current_position_name AS position_name,
        load_dttm,
        batch_id
    FROM `{{PROJECT_ID}}.hr_raw.hr_employee_master_raw`

    UNION ALL

    SELECT
        position_code,
        position_name,
        load_dttm,
        batch_id
    FROM `{{PROJECT_ID}}.hr_raw.hr_position_history_raw`
) p
GROUP BY position_code;

INSERT INTO `{{PROJECT_ID}}.hr_dds.dim_hr_employee` (
    employee_id,
    employee_src_id,
    employee_number,
    full_name,
    tab_num,
    birth_date,
    gender_code,
    employment_status,
    hire_date,
    dismissal_date,
    current_position_id,
    current_position_code,
    department_id,
    department_src_id,
    manager_employee_src_id,
    grade_code,
    work_format,
    location_name,
    valid_from,
    valid_to,
    is_current_flag,
    source_system,
    load_dttm,
    batch_id
)
SELECT
    ROW_NUMBER() OVER (ORDER BY e.employee_src_id) AS employee_id,
    e.employee_src_id,
    e.employee_number,
    e.full_name,
    e.tab_num,
    e.birth_date,
    e.gender_code,
    e.employment_status,
    e.hire_date,
    e.dismissal_date,
    p.position_id,
    e.current_position_code,
    d.department_id,
    e.department_src_id,
    e.manager_src_id,
    e.grade_code,
    e.work_format,
    e.location_name,
    e.snapshot_date AS valid_from,
    CAST(NULL AS DATE) AS valid_to,
    TRUE AS is_current_flag,
    e.source_system,
    e.load_dttm,
    e.batch_id
FROM (
    SELECT DISTINCT
        employee_src_id,
        employee_number,
        full_name,
        tab_num,
        birth_date,
        gender_code,
        employment_status,
        hire_date,
        dismissal_date,
        current_position_code,
        current_position_name,
        department_src_id,
        manager_src_id,
        grade_code,
        work_format,
        location_name,
        snapshot_date,
        source_system,
        load_dttm,
        batch_id
    FROM `{{PROJECT_ID}}.hr_raw.hr_employee_master_raw`
) e
LEFT JOIN `{{PROJECT_ID}}.hr_dds.dim_position` p
    ON e.current_position_code = p.position_code
LEFT JOIN `{{PROJECT_ID}}.hr_dds.dim_hr_department` d
    ON e.department_src_id = d.department_src_id
   AND d.is_current_flag = TRUE;

INSERT INTO `{{PROJECT_ID}}.hr_dds.fct_employee_position_history` (
    employee_position_history_id,
    employee_id,
    position_id,
    department_id,
    event_type_code,
    event_type_name,
    salary_change_flag,
    effective_from,
    effective_to,
    load_dttm,
    batch_id
)
SELECT
    ROW_NUMBER() OVER (ORDER BY h.src_position_event_id) AS employee_position_history_id,
    e.employee_id,
    p.position_id,
    d.department_id,
    h.event_type_code,
    h.event_type_name,
    CASE WHEN h.salary_change_flag = 'Y' THEN TRUE ELSE FALSE END,
    h.effective_from,
    h.effective_to,
    h.load_dttm,
    h.batch_id
FROM `{{PROJECT_ID}}.hr_raw.hr_position_history_raw` h
LEFT JOIN `{{PROJECT_ID}}.hr_dds.dim_hr_employee` e
    ON h.employee_src_id = e.employee_src_id
   AND e.is_current_flag = TRUE
LEFT JOIN `{{PROJECT_ID}}.hr_dds.dim_position` p
    ON h.position_code = p.position_code
LEFT JOIN `{{PROJECT_ID}}.hr_dds.dim_hr_department` d
    ON h.department_src_id = d.department_src_id
   AND d.is_current_flag = TRUE;

INSERT INTO `{{PROJECT_ID}}.hr_dds.fct_employee_absence` (
    employee_absence_id,
    employee_id,
    absence_type_code,
    absence_type_name,
    absence_reason_group,
    start_date,
    end_date,
    duration_days,
    approved_flag,
    load_dttm,
    batch_id
)
SELECT
    ROW_NUMBER() OVER (ORDER BY a.src_absence_event_id) AS employee_absence_id,
    e.employee_id,
    a.absence_type_code,
    a.absence_type_name,
    a.absence_reason_group,
    a.start_date,
    a.end_date,
    a.duration_days,
    CASE WHEN a.approved_flag = 'Y' THEN TRUE ELSE FALSE END,
    a.load_dttm,
    a.batch_id
FROM `{{PROJECT_ID}}.hr_raw.hr_absence_events_raw` a
LEFT JOIN `{{PROJECT_ID}}.hr_dds.dim_hr_employee` e
    ON a.employee_src_id = e.employee_src_id
   AND e.is_current_flag = TRUE;

INSERT INTO `{{PROJECT_ID}}.hr_dds.fct_employee_activity_signal` (
    employee_activity_signal_id,
    employee_id,
    signal_source_type,
    source_event_id,
    signal_code,
    signal_name,
    signal_group,
    signal_value_num,
    signal_value_text,
    detected_at,
    signal_status,
    load_dttm,
    batch_id
)
SELECT
    ROW_NUMBER() OVER (ORDER BY signal_source_type, source_event_id) AS employee_activity_signal_id,
    employee_id,
    signal_source_type,
    source_event_id,
    signal_code,
    signal_name,
    signal_group,
    signal_value_num,
    signal_value_text,
    detected_at,
    signal_status,
    load_dttm,
    batch_id
FROM (
    SELECT
        e.employee_id,
        'overtime' AS signal_source_type,
        CAST(o.src_overtime_event_id AS STRING) AS source_event_id,
        'OVERTIME_HOURS' AS signal_code,
        'Сверхурочная работа' AS signal_name,
        'WORKLOAD' AS signal_group,
        CAST(o.overtime_hours AS NUMERIC) AS signal_value_num,
        o.overtime_reason_name AS signal_value_text,
        TIMESTAMP(o.overtime_date) AS detected_at,
        CASE WHEN o.approved_flag = 'Y' THEN 'APPROVED' ELSE 'PENDING' END AS signal_status,
        o.load_dttm,
        o.batch_id
    FROM `{{PROJECT_ID}}.hr_raw.hr_overtime_events_raw` o
    LEFT JOIN `{{PROJECT_ID}}.hr_dds.dim_hr_employee` e
        ON o.employee_src_id = e.employee_src_id
       AND e.is_current_flag = TRUE

    UNION ALL

    SELECT
        e.employee_id,
        'dismissal_signal' AS signal_source_type,
        CAST(s.src_signal_id AS STRING) AS source_event_id,
        s.signal_code,
        s.signal_name,
        s.signal_group,
        CAST(s.signal_value_num AS NUMERIC) AS signal_value_num,
        s.signal_value_text,
        CASE
            WHEN s.detected_at IS NULL THEN NULL
            WHEN s.detected_at BETWEEN 0 AND 32503680000 THEN TIMESTAMP_SECONDS(s.detected_at)
            WHEN s.detected_at BETWEEN 0 AND 32503680000000 THEN TIMESTAMP_MILLIS(s.detected_at)
            ELSE NULL
        END AS detected_at,
        s.signal_status,
        s.load_dttm,
        s.batch_id
    FROM `{{PROJECT_ID}}.hr_raw.hr_dismissal_signals_raw` s
    LEFT JOIN `{{PROJECT_ID}}.hr_dds.dim_hr_employee` e
        ON s.employee_src_id = e.employee_src_id
       AND e.is_current_flag = TRUE
) x;

INSERT INTO `{{PROJECT_ID}}.hr_dds.fct_employee_hr_snapshot` (
    employee_hr_snapshot_id,
    employee_id,
    report_date,
    current_position_id,
    current_department_id,
    absence_event_cnt,
    absence_days_total,
    overtime_hours_total,
    dismissal_signal_cnt,
    active_signal_cnt,
    instability_score,
    instability_level_code,
    calculation_dttm,
    load_dttm,
    batch_id
)
SELECT
    ROW_NUMBER() OVER (ORDER BY e.employee_id) AS employee_hr_snapshot_id,
    e.employee_id,
    DATE '2024-12-31' AS report_date,
    e.current_position_id,
    e.department_id AS current_department_id,
    COALESCE(abs_agg.absence_event_cnt, 0) AS absence_event_cnt,
    COALESCE(abs_agg.absence_days_total, 0) AS absence_days_total,
    COALESCE(sig_agg.overtime_hours_total, 0) AS overtime_hours_total,
    COALESCE(sig_agg.dismissal_signal_cnt, 0) AS dismissal_signal_cnt,
    COALESCE(sig_agg.active_signal_cnt, 0) AS active_signal_cnt,
    CAST(
        COALESCE(abs_agg.absence_days_total, 0) * 0.20 +
        COALESCE(sig_agg.overtime_hours_total, 0) * 0.10 +
        COALESCE(sig_agg.dismissal_signal_cnt, 0) * 2.00 +
        COALESCE(sig_agg.active_signal_cnt, 0) * 1.50
        AS NUMERIC
    ) AS instability_score,
    CASE
        WHEN (
            COALESCE(abs_agg.absence_days_total, 0) * 0.20 +
            COALESCE(sig_agg.overtime_hours_total, 0) * 0.10 +
            COALESCE(sig_agg.dismissal_signal_cnt, 0) * 2.00 +
            COALESCE(sig_agg.active_signal_cnt, 0) * 1.50
        ) >= 12 THEN 'HIGH'
        WHEN (
            COALESCE(abs_agg.absence_days_total, 0) * 0.20 +
            COALESCE(sig_agg.overtime_hours_total, 0) * 0.10 +
            COALESCE(sig_agg.dismissal_signal_cnt, 0) * 2.00 +
            COALESCE(sig_agg.active_signal_cnt, 0) * 1.50
        ) >= 5 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS instability_level_code,
    TIMESTAMP '2024-12-31 23:00:00+00' AS calculation_dttm,
    TIMESTAMP '2024-12-31 23:00:00+00' AS load_dttm,
    'hr_dds_build_001' AS batch_id
FROM `{{PROJECT_ID}}.hr_dds.dim_hr_employee` e
LEFT JOIN (
    SELECT
        employee_id,
        COUNT(*) AS absence_event_cnt,
        SUM(COALESCE(duration_days, 0)) AS absence_days_total
    FROM `{{PROJECT_ID}}.hr_dds.fct_employee_absence`
    GROUP BY employee_id
) abs_agg
    ON e.employee_id = abs_agg.employee_id
LEFT JOIN (
    SELECT
        employee_id,
        SUM(CASE WHEN signal_source_type = 'overtime' THEN COALESCE(signal_value_num, 0) ELSE 0 END) AS overtime_hours_total,
        SUM(CASE WHEN signal_source_type = 'dismissal_signal' THEN 1 ELSE 0 END) AS dismissal_signal_cnt,
        SUM(CASE WHEN COALESCE(signal_status, '') IN ('ACTIVE', 'PENDING') THEN 1 ELSE 0 END) AS active_signal_cnt
    FROM `{{PROJECT_ID}}.hr_dds.fct_employee_activity_signal`
    GROUP BY employee_id
) sig_agg
    ON e.employee_id = sig_agg.employee_id
WHERE e.is_current_flag = TRUE;