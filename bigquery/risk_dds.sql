CREATE TEMP FUNCTION to_bq_timestamp(v ANY TYPE)
RETURNS TIMESTAMP
AS (
  CASE
    WHEN v IS NULL THEN NULL

    WHEN REGEXP_CONTAINS(CAST(v AS STRING), r'^\d{10}$')
      THEN TIMESTAMP_SECONDS(SAFE_CAST(CAST(v AS STRING) AS INT64))

    WHEN REGEXP_CONTAINS(CAST(v AS STRING), r'^\d{13}$')
      THEN TIMESTAMP_MILLIS(SAFE_CAST(CAST(v AS STRING) AS INT64))

    WHEN REGEXP_CONTAINS(CAST(v AS STRING), r'^\d{16}$')
      THEN TIMESTAMP_MICROS(SAFE_CAST(CAST(v AS STRING) AS INT64))

    WHEN REGEXP_CONTAINS(CAST(v AS STRING), r'^\d{19}$')
      THEN TIMESTAMP_MICROS(CAST(SAFE_CAST(CAST(v AS STRING) AS INT64) / 1000 AS INT64))

    ELSE SAFE_CAST(CAST(v AS STRING) AS TIMESTAMP)
  END
);

CREATE TEMP FUNCTION to_bq_date(v ANY TYPE)
RETURNS DATE
AS (
  CASE
    WHEN v IS NULL THEN NULL

    WHEN REGEXP_CONTAINS(CAST(v AS STRING), r'^\d{1,5}$')
      THEN DATE_FROM_UNIX_DATE(SAFE_CAST(CAST(v AS STRING) AS INT64))

    WHEN REGEXP_CONTAINS(CAST(v AS STRING), r'^\d{10}$')
      THEN DATE(TIMESTAMP_SECONDS(SAFE_CAST(CAST(v AS STRING) AS INT64)))

    WHEN REGEXP_CONTAINS(CAST(v AS STRING), r'^\d{13}$')
      THEN DATE(TIMESTAMP_MILLIS(SAFE_CAST(CAST(v AS STRING) AS INT64)))

    WHEN REGEXP_CONTAINS(CAST(v AS STRING), r'^\d{16}$')
      THEN DATE(TIMESTAMP_MICROS(SAFE_CAST(CAST(v AS STRING) AS INT64)))

    WHEN REGEXP_CONTAINS(CAST(v AS STRING), r'^\d{19}$')
      THEN DATE(TIMESTAMP_MICROS(CAST(SAFE_CAST(CAST(v AS STRING) AS INT64) / 1000 AS INT64)))

    ELSE SAFE_CAST(CAST(v AS STRING) AS DATE)
  END
);

CREATE OR REPLACE TABLE `{{PROJECT_ID}}.risk_dds.dim_department` (
    department_id INT64,
    department_src_id STRING,
    department_name STRING,
    parent_department_id INT64,
    parent_department_src_id STRING,
    block_name STRING,
    function_name STRING,
    region_name STRING,
    org_level STRING,
    valid_from DATE,
    valid_to DATE,
    is_current_flag BOOL,
    source_system STRING,
    load_dttm INT64,
    batch_id STRING
);

CREATE OR REPLACE TABLE `{{PROJECT_ID}}.risk_dds.dim_employee` (
    employee_id INT64,
    employee_src_id STRING,
    employee_number STRING,
    full_name STRING,
    tab_num STRING,
    position_name STRING,
    department_id INT64,
    department_src_id STRING,
    manager_employee_src_id STRING,
    employment_status STRING,
    hire_date DATE,
    dismissal_date DATE,
    valid_from DATE,
    valid_to DATE,
    is_current_flag BOOL,
    source_system STRING,
    load_dttm INT64,
    batch_id STRING
);

CREATE OR REPLACE TABLE `{{PROJECT_ID}}.risk_dds.dim_risk_event_type` (
    risk_event_type_id INT64,
    source_system_name STRING,
    source_event_code STRING,
    source_event_name STRING,
    risk_domain STRING,
    event_group_name STRING,
    default_severity_level STRING,
    is_violation_flag BOOL,
    is_behavior_signal_flag BOOL,
    load_dttm INT64,
    batch_id STRING
);

CREATE OR REPLACE TABLE `{{PROJECT_ID}}.risk_dds.fct_risk_event` (
    risk_event_id INT64,
    source_event_id STRING,
    employee_id INT64,
    employee_src_id STRING,
    risk_event_type_id INT64,
    event_source_system STRING,
    event_status STRING,
    severity_level STRING,
    risk_flag BOOL,
    event_detected_dttm TIMESTAMP,
    event_closed_dttm TIMESTAMP,
    decision_code STRING,
    decision_text STRING,
    comment_text STRING,
    load_dttm INT64,
    batch_id STRING
);

CREATE OR REPLACE TABLE `{{PROJECT_ID}}.risk_dds.fct_employee_risk_factor` (
    employee_risk_factor_id INT64,
    employee_id INT64,
    report_date DATE,
    risk_factor_code STRING,
    risk_factor_name STRING,
    factor_value_num NUMERIC,
    factor_value_text STRING,
    risk_domain STRING,
    calculation_dttm TIMESTAMP,
    load_dttm TIMESTAMP,
    batch_id STRING
);

CREATE OR REPLACE TABLE `{{PROJECT_ID}}.risk_dds.fct_employee_risk_snapshot` (
    employee_risk_snapshot_id INT64,
    employee_id INT64,
    report_date DATE,
    risk_score_value NUMERIC,
    risk_level_code STRING,
    open_event_cnt INT64,
    critical_event_cnt INT64,
    ib_event_cnt INT64,
    security_event_cnt INT64,
    compliance_event_cnt INT64,
    nonwork_signal_cnt INT64,
    calculation_dttm TIMESTAMP,
    load_dttm TIMESTAMP,
    batch_id STRING
);

TRUNCATE TABLE `{{PROJECT_ID}}.risk_dds.fct_employee_risk_snapshot`;
TRUNCATE TABLE `{{PROJECT_ID}}.risk_dds.fct_employee_risk_factor`;
TRUNCATE TABLE `{{PROJECT_ID}}.risk_dds.fct_risk_event`;
TRUNCATE TABLE `{{PROJECT_ID}}.risk_dds.dim_risk_event_type`;
TRUNCATE TABLE `{{PROJECT_ID}}.risk_dds.dim_employee`;
TRUNCATE TABLE `{{PROJECT_ID}}.risk_dds.dim_department`;

INSERT INTO `{{PROJECT_ID}}.risk_dds.dim_department` (
    department_id,
    department_src_id,
    department_name,
    parent_department_id,
    parent_department_src_id,
    block_name,
    function_name,
    region_name,
    org_level,
    valid_from,
    valid_to,
    is_current_flag,
    source_system,
    load_dttm,
    batch_id
)
SELECT
    ROW_NUMBER() OVER (ORDER BY department_src_id) AS department_id,
    department_src_id,
    department_name,
    CAST(NULL AS INT64) AS parent_department_id,
    parent_department_src_id,
    block_name,
    function_name,
    region_name,
    CAST(org_level AS STRING) AS org_level,
    to_bq_date(valid_from) AS valid_from,
    to_bq_date(valid_to) AS valid_to,
    CASE WHEN to_bq_date(valid_to) IS NULL THEN TRUE ELSE FALSE END AS is_current_flag,
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
        valid_from,
        valid_to,
        source_system,
        load_dttm,
        batch_id
    FROM `{{PROJECT_ID}}.risk_raw.risk_org_structure_raw`
) s;

UPDATE `{{PROJECT_ID}}.risk_dds.dim_department` d
SET parent_department_id = p.department_id
FROM `{{PROJECT_ID}}.risk_dds.dim_department` p
WHERE d.parent_department_src_id = p.department_src_id;

INSERT INTO `{{PROJECT_ID}}.risk_dds.dim_employee` (
    employee_id,
    employee_src_id,
    employee_number,
    full_name,
    tab_num,
    position_name,
    department_id,
    department_src_id,
    manager_employee_src_id,
    employment_status,
    hire_date,
    dismissal_date,
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
    e.position_name,
    d.department_id,
    e.department_src_id,
    e.manager_src_id,
    e.employment_status,
    to_bq_date(e.hire_date) AS hire_date,
    to_bq_date(e.dismissal_date) AS dismissal_date,
    to_bq_date(e.snapshot_date) AS valid_from,
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
        position_name,
        department_src_id,
        manager_src_id,
        employment_status,
        hire_date,
        dismissal_date,
        snapshot_date,
        source_system,
        load_dttm,
        batch_id
    FROM `{{PROJECT_ID}}.risk_raw.risk_employee_registry_raw`
) e
LEFT JOIN `{{PROJECT_ID}}.risk_dds.dim_department` d
    ON e.department_src_id = d.department_src_id
   AND d.is_current_flag = TRUE;

INSERT INTO `{{PROJECT_ID}}.risk_dds.dim_risk_event_type` (
    risk_event_type_id,
    source_system_name,
    source_event_code,
    source_event_name,
    risk_domain,
    event_group_name,
    default_severity_level,
    is_violation_flag,
    is_behavior_signal_flag,
    load_dttm,
    batch_id
)
SELECT
    ROW_NUMBER() OVER (ORDER BY source_system_name, source_event_code) AS risk_event_type_id,
    source_system_name,
    source_event_code,
    source_event_name,
    risk_domain,
    event_group_name,
    default_severity_level,
    is_violation_flag,
    is_behavior_signal_flag,
    MAX(load_dttm) AS load_dttm,
    MAX(batch_id) AS batch_id
FROM (
    SELECT
        'ib_incidents' AS source_system_name,
        incident_code AS source_event_code,
        incident_name AS source_event_name,
        'ib' AS risk_domain,
        'incident' AS event_group_name,
        severity_level AS default_severity_level,
        TRUE AS is_violation_flag,
        FALSE AS is_behavior_signal_flag,
        load_dttm,
        batch_id
    FROM `{{PROJECT_ID}}.risk_raw.risk_ib_incidents_raw`

    UNION ALL

    SELECT
        'security_cases',
        case_type_code,
        case_type_name,
        'security',
        'incident',
        CAST(NULL AS STRING),
        TRUE,
        FALSE,
        load_dttm,
        batch_id
    FROM `{{PROJECT_ID}}.risk_raw.risk_security_incidents_raw`

    UNION ALL

    SELECT
        'compliance_cases',
        violation_code,
        violation_name,
        'compliance',
        'incident',
        materiality_level,
        TRUE,
        FALSE,
        load_dttm,
        batch_id
    FROM `{{PROJECT_ID}}.risk_raw.risk_compliance_incidents_raw`

    UNION ALL

    SELECT
        'hr_risk_monitor',
        activity_type_code,
        activity_type_name,
        'nonwork',
        activity_group,
        CAST(NULL AS STRING),
        FALSE,
        TRUE,
        load_dttm,
        batch_id
    FROM `{{PROJECT_ID}}.risk_raw.risk_nonwork_activity_raw`
) t
GROUP BY
    source_system_name,
    source_event_code,
    source_event_name,
    risk_domain,
    event_group_name,
    default_severity_level,
    is_violation_flag,
    is_behavior_signal_flag;

INSERT INTO `{{PROJECT_ID}}.risk_dds.fct_risk_event` (
    risk_event_id,
    source_event_id,
    employee_id,
    employee_src_id,
    risk_event_type_id,
    event_source_system,
    event_status,
    severity_level,
    risk_flag,
    event_detected_dttm,
    event_closed_dttm,
    decision_code,
    decision_text,
    comment_text,
    load_dttm,
    batch_id
)
SELECT
    ROW_NUMBER() OVER (ORDER BY event_source_system, source_event_id) AS risk_event_id,
    source_event_id,
    e.employee_id,
    src.employee_src_id,
    t.risk_event_type_id,
    src.event_source_system,
    src.event_status,
    src.severity_level,
    src.risk_flag,
    src.event_detected_dttm,
    src.event_closed_dttm,
    src.decision_code,
    src.decision_text,
    src.comment_text,
    src.load_dttm,
    src.batch_id
FROM (
    SELECT
        CAST(src_incident_id AS STRING) AS source_event_id,
        employee_src_id,
        'ib_incidents' AS event_source_system,
        incident_status AS event_status,
        severity_level,
        TRUE AS risk_flag,
        to_bq_timestamp(detected_at) AS event_detected_dttm,
        to_bq_timestamp(closed_at) AS event_closed_dttm,
        CAST(NULL AS STRING) AS decision_code,
        CAST(NULL AS STRING) AS decision_text,
        CAST(NULL AS STRING) AS comment_text,
        incident_code AS source_event_code,
        load_dttm,
        batch_id
    FROM `{{PROJECT_ID}}.risk_raw.risk_ib_incidents_raw`

    UNION ALL

    SELECT
        CAST(src_case_id AS STRING),
        employee_src_id,
        'security_cases',
        case_status,
        CAST(NULL AS STRING),
        CASE WHEN risk_flag = 'Y' THEN TRUE ELSE FALSE END,
        to_bq_timestamp(registered_at),
        to_bq_timestamp(resolved_at),
        resolution_code,
        resolution_name,
        CAST(NULL AS STRING),
        case_type_code,
        load_dttm,
        batch_id
    FROM `{{PROJECT_ID}}.risk_raw.risk_security_incidents_raw`

    UNION ALL

    SELECT
        CAST(src_violation_id AS STRING),
        employee_src_id,
        'compliance_cases',
        violation_status,
        materiality_level,
        TRUE,
        to_bq_timestamp(detected_at),
        to_bq_timestamp(decision_at),
        CAST(NULL AS STRING),
        decision_text,
        CAST(NULL AS STRING),
        violation_code,
        load_dttm,
        batch_id
    FROM `{{PROJECT_ID}}.risk_raw.risk_compliance_incidents_raw`

    UNION ALL

    SELECT
        CAST(src_activity_id AS STRING),
        employee_src_id,
        'hr_risk_monitor',
        activity_status,
        CAST(NULL AS STRING),
        TRUE,
        to_bq_timestamp(detected_at),
        CAST(NULL AS TIMESTAMP),
        CAST(NULL AS STRING),
        CAST(NULL AS STRING),
        comment_text,
        activity_type_code,
        load_dttm,
        batch_id
    FROM `{{PROJECT_ID}}.risk_raw.risk_nonwork_activity_raw`
) src
LEFT JOIN `{{PROJECT_ID}}.risk_dds.dim_employee` e
    ON src.employee_src_id = e.employee_src_id
   AND e.is_current_flag = TRUE
LEFT JOIN `{{PROJECT_ID}}.risk_dds.dim_risk_event_type` t
    ON src.event_source_system = t.source_system_name
   AND src.source_event_code = t.source_event_code;

INSERT INTO `{{PROJECT_ID}}.risk_dds.fct_employee_risk_factor` (
    employee_risk_factor_id,
    employee_id,
    report_date,
    risk_factor_code,
    risk_factor_name,
    factor_value_num,
    factor_value_text,
    risk_domain,
    calculation_dttm,
    load_dttm,
    batch_id
)
SELECT
    ROW_NUMBER() OVER (ORDER BY employee_id, risk_factor_code) AS employee_risk_factor_id,
    employee_id,
    DATE '2024-12-31' AS report_date,
    risk_factor_code,
    risk_factor_name,
    factor_value_num,
    CAST(NULL AS STRING) AS factor_value_text,
    risk_domain,
    TIMESTAMP '2024-12-31 23:00:00+00' AS calculation_dttm,
    TIMESTAMP '2024-12-31 23:00:00+00' AS load_dttm,
    'risk_dds_build_001' AS batch_id
FROM (
    SELECT
        employee_id,
        'OPEN_EVENT_CNT' AS risk_factor_code,
        'Количество открытых событий' AS risk_factor_name,
        CAST(COUNT(*) AS NUMERIC) AS factor_value_num,
        'all' AS risk_domain
    FROM `{{PROJECT_ID}}.risk_dds.fct_risk_event`
    WHERE event_status = 'OPEN'
    GROUP BY employee_id

    UNION ALL

    SELECT
        employee_id,
        'CRITICAL_EVENT_CNT',
        'Количество критичных событий',
        CAST(COUNT(*) AS NUMERIC),
        'all'
    FROM `{{PROJECT_ID}}.risk_dds.fct_risk_event`
    WHERE COALESCE(severity_level, '') = 'CRITICAL'
    GROUP BY employee_id

    UNION ALL

    SELECT
        employee_id,
        'IB_EVENT_CNT',
        'Количество ИБ событий',
        CAST(COUNT(*) AS NUMERIC),
        'ib'
    FROM `{{PROJECT_ID}}.risk_dds.fct_risk_event`
    WHERE event_source_system = 'ib_incidents'
    GROUP BY employee_id

    UNION ALL

    SELECT
        employee_id,
        'SECURITY_EVENT_CNT',
        'Количество событий СБ',
        CAST(COUNT(*) AS NUMERIC),
        'security'
    FROM `{{PROJECT_ID}}.risk_dds.fct_risk_event`
    WHERE event_source_system = 'security_cases'
    GROUP BY employee_id

    UNION ALL

    SELECT
        employee_id,
        'COMPLIANCE_EVENT_CNT',
        'Количество событий комплаенса',
        CAST(COUNT(*) AS NUMERIC),
        'compliance'
    FROM `{{PROJECT_ID}}.risk_dds.fct_risk_event`
    WHERE event_source_system = 'compliance_cases'
    GROUP BY employee_id

    UNION ALL

    SELECT
        employee_id,
        'NONWORK_SIGNAL_CNT',
        'Количество сигналов нерабочей активности',
        CAST(COUNT(*) AS NUMERIC),
        'nonwork'
    FROM `{{PROJECT_ID}}.risk_dds.fct_risk_event`
    WHERE event_source_system = 'hr_risk_monitor'
    GROUP BY employee_id
) f;

INSERT INTO `{{PROJECT_ID}}.risk_dds.fct_employee_risk_snapshot` (
    employee_risk_snapshot_id,
    employee_id,
    report_date,
    risk_score_value,
    risk_level_code,
    open_event_cnt,
    critical_event_cnt,
    ib_event_cnt,
    security_event_cnt,
    compliance_event_cnt,
    nonwork_signal_cnt,
    calculation_dttm,
    load_dttm,
    batch_id
)
SELECT
    ROW_NUMBER() OVER (ORDER BY e.employee_id) AS employee_risk_snapshot_id,
    e.employee_id,
    DATE '2024-12-31' AS report_date,
    CAST(
        COALESCE(SUM(CASE WHEN f.risk_factor_code = 'OPEN_EVENT_CNT' THEN f.factor_value_num END), 0) * 1.5 +
        COALESCE(SUM(CASE WHEN f.risk_factor_code = 'CRITICAL_EVENT_CNT' THEN f.factor_value_num END), 0) * 3 +
        COALESCE(SUM(CASE WHEN f.risk_factor_code = 'IB_EVENT_CNT' THEN f.factor_value_num END), 0) * 1 +
        COALESCE(SUM(CASE WHEN f.risk_factor_code = 'SECURITY_EVENT_CNT' THEN f.factor_value_num END), 0) * 1.2 +
        COALESCE(SUM(CASE WHEN f.risk_factor_code = 'COMPLIANCE_EVENT_CNT' THEN f.factor_value_num END), 0) * 1.1 +
        COALESCE(SUM(CASE WHEN f.risk_factor_code = 'NONWORK_SIGNAL_CNT' THEN f.factor_value_num END), 0) * 1.4
        AS NUMERIC
    ) AS risk_score_value,
    CASE
        WHEN (
            COALESCE(SUM(CASE WHEN f.risk_factor_code = 'OPEN_EVENT_CNT' THEN f.factor_value_num END), 0) * 1.5 +
            COALESCE(SUM(CASE WHEN f.risk_factor_code = 'CRITICAL_EVENT_CNT' THEN f.factor_value_num END), 0) * 3 +
            COALESCE(SUM(CASE WHEN f.risk_factor_code = 'IB_EVENT_CNT' THEN f.factor_value_num END), 0) * 1 +
            COALESCE(SUM(CASE WHEN f.risk_factor_code = 'SECURITY_EVENT_CNT' THEN f.factor_value_num END), 0) * 1.2 +
            COALESCE(SUM(CASE WHEN f.risk_factor_code = 'COMPLIANCE_EVENT_CNT' THEN f.factor_value_num END), 0) * 1.1 +
            COALESCE(SUM(CASE WHEN f.risk_factor_code = 'NONWORK_SIGNAL_CNT' THEN f.factor_value_num END), 0) * 1.4
        ) >= 15 THEN 'HIGH'
        WHEN (
            COALESCE(SUM(CASE WHEN f.risk_factor_code = 'OPEN_EVENT_CNT' THEN f.factor_value_num END), 0) * 1.5 +
            COALESCE(SUM(CASE WHEN f.risk_factor_code = 'CRITICAL_EVENT_CNT' THEN f.factor_value_num END), 0) * 3 +
            COALESCE(SUM(CASE WHEN f.risk_factor_code = 'IB_EVENT_CNT' THEN f.factor_value_num END), 0) * 1 +
            COALESCE(SUM(CASE WHEN f.risk_factor_code = 'SECURITY_EVENT_CNT' THEN f.factor_value_num END), 0) * 1.2 +
            COALESCE(SUM(CASE WHEN f.risk_factor_code = 'COMPLIANCE_EVENT_CNT' THEN f.factor_value_num END), 0) * 1.1 +
            COALESCE(SUM(CASE WHEN f.risk_factor_code = 'NONWORK_SIGNAL_CNT' THEN f.factor_value_num END), 0) * 1.4
        ) >= 7 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS risk_level_code,
    CAST(COALESCE(SUM(CASE WHEN f.risk_factor_code = 'OPEN_EVENT_CNT' THEN f.factor_value_num END), 0) AS INT64) AS open_event_cnt,
    CAST(COALESCE(SUM(CASE WHEN f.risk_factor_code = 'CRITICAL_EVENT_CNT' THEN f.factor_value_num END), 0) AS INT64) AS critical_event_cnt,
    CAST(COALESCE(SUM(CASE WHEN f.risk_factor_code = 'IB_EVENT_CNT' THEN f.factor_value_num END), 0) AS INT64) AS ib_event_cnt,
    CAST(COALESCE(SUM(CASE WHEN f.risk_factor_code = 'SECURITY_EVENT_CNT' THEN f.factor_value_num END), 0) AS INT64) AS security_event_cnt,
    CAST(COALESCE(SUM(CASE WHEN f.risk_factor_code = 'COMPLIANCE_EVENT_CNT' THEN f.factor_value_num END), 0) AS INT64) AS compliance_event_cnt,
    CAST(COALESCE(SUM(CASE WHEN f.risk_factor_code = 'NONWORK_SIGNAL_CNT' THEN f.factor_value_num END), 0) AS INT64) AS nonwork_signal_cnt,
    TIMESTAMP '2024-12-31 23:30:00+00' AS calculation_dttm,
    TIMESTAMP '2024-12-31 23:30:00+00' AS load_dttm,
    'risk_dds_build_001' AS batch_id
FROM `{{PROJECT_ID}}.risk_dds.dim_employee` e
LEFT JOIN `{{PROJECT_ID}}.risk_dds.fct_employee_risk_factor` f
    ON e.employee_id = f.employee_id
   AND f.report_date = DATE '2024-12-31'
WHERE e.is_current_flag = TRUE
GROUP BY e.employee_id;