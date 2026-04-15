CREATE OR REPLACE TABLE `{{PROJECT_ID}}.risk_dm.employee_operational_risk_report` (
    report_date DATE,
    employee_id INT64,
    employee_src_id STRING,
    employee_number STRING,
    tab_num STRING,
    full_name STRING,
    position_name STRING,
    employment_status STRING,
    hire_date DATE,
    dismissal_date DATE,

    department_id INT64,
    department_src_id STRING,
    department_name STRING,
    parent_department_id INT64,
    parent_department_src_id STRING,
    block_name STRING,
    function_name STRING,
    region_name STRING,
    org_level STRING,

    risk_score_value NUMERIC,
    risk_level_code STRING,

    open_event_cnt INT64,
    critical_event_cnt INT64,
    ib_event_cnt INT64,
    security_event_cnt INT64,
    compliance_event_cnt INT64,
    nonwork_signal_cnt INT64,

    ib_critical_event_cnt INT64,
    security_critical_event_cnt INT64,
    compliance_critical_event_cnt INT64,

    has_open_risk_flag BOOL,
    has_critical_risk_flag BOOL,
    has_nonwork_signal_flag BOOL,

    last_risk_event_dttm TIMESTAMP,
    calculation_dttm TIMESTAMP,
    load_dttm TIMESTAMP,
    batch_id STRING
);

TRUNCATE TABLE `{{PROJECT_ID}}.risk_dm.employee_operational_risk_report`;

INSERT INTO `{{PROJECT_ID}}.risk_dm.employee_operational_risk_report` (
    report_date,
    employee_id,
    employee_src_id,
    employee_number,
    tab_num,
    full_name,
    position_name,
    employment_status,
    hire_date,
    dismissal_date,

    department_id,
    department_src_id,
    department_name,
    parent_department_id,
    parent_department_src_id,
    block_name,
    function_name,
    region_name,
    org_level,

    risk_score_value,
    risk_level_code,

    open_event_cnt,
    critical_event_cnt,
    ib_event_cnt,
    security_event_cnt,
    compliance_event_cnt,
    nonwork_signal_cnt,

    ib_critical_event_cnt,
    security_critical_event_cnt,
    compliance_critical_event_cnt,

    has_open_risk_flag,
    has_critical_risk_flag,
    has_nonwork_signal_flag,

    last_risk_event_dttm,
    calculation_dttm,
    load_dttm,
    batch_id
)
SELECT
    s.report_date,
    s.employee_id,
    e.employee_src_id,
    e.employee_number,
    e.tab_num,
    e.full_name,
    e.position_name,
    e.employment_status,
    e.hire_date,
    e.dismissal_date,

    d.department_id,
    d.department_src_id,
    d.department_name,
    d.parent_department_id,
    d.parent_department_src_id,
    d.block_name,
    d.function_name,
    d.region_name,
    CAST(d.org_level AS STRING) AS org_level,

    s.risk_score_value,
    s.risk_level_code,

    s.open_event_cnt,
    s.critical_event_cnt,
    s.ib_event_cnt,
    s.security_event_cnt,
    s.compliance_event_cnt,
    s.nonwork_signal_cnt,

    COALESCE(crit.ib_critical_event_cnt, 0) AS ib_critical_event_cnt,
    COALESCE(crit.security_critical_event_cnt, 0) AS security_critical_event_cnt,
    COALESCE(crit.compliance_critical_event_cnt, 0) AS compliance_critical_event_cnt,

    CASE WHEN s.open_event_cnt > 0 THEN TRUE ELSE FALSE END AS has_open_risk_flag,
    CASE WHEN s.critical_event_cnt > 0 THEN TRUE ELSE FALSE END AS has_critical_risk_flag,
    CASE WHEN s.nonwork_signal_cnt > 0 THEN TRUE ELSE FALSE END AS has_nonwork_signal_flag,

    evt.last_risk_event_dttm,
    s.calculation_dttm,
    s.load_dttm,
    s.batch_id
FROM `{{PROJECT_ID}}.risk_dds.fct_employee_risk_snapshot` s
LEFT JOIN `{{PROJECT_ID}}.risk_dds.dim_employee` e
    ON s.employee_id = e.employee_id
   AND e.is_current_flag = TRUE
LEFT JOIN `{{PROJECT_ID}}.risk_dds.dim_department` d
    ON e.department_id = d.department_id
   AND d.is_current_flag = TRUE
LEFT JOIN (
    SELECT
        employee_id,
        MAX(event_detected_dttm) AS last_risk_event_dttm
    FROM `{{PROJECT_ID}}.risk_dds.fct_risk_event`
    GROUP BY employee_id
) evt
    ON s.employee_id = evt.employee_id
LEFT JOIN (
    SELECT
        employee_id,
        SUM(CASE WHEN event_source_system = 'ib_incidents' AND COALESCE(severity_level, '') = 'CRITICAL' THEN 1 ELSE 0 END) AS ib_critical_event_cnt,
        SUM(CASE WHEN event_source_system = 'security_cases' AND COALESCE(severity_level, '') = 'CRITICAL' THEN 1 ELSE 0 END) AS security_critical_event_cnt,
        SUM(CASE WHEN event_source_system = 'compliance_cases' AND COALESCE(severity_level, '') = 'CRITICAL' THEN 1 ELSE 0 END) AS compliance_critical_event_cnt
    FROM `{{PROJECT_ID}}.risk_dds.fct_risk_event`
    GROUP BY employee_id
) crit
    ON s.employee_id = crit.employee_id;

CREATE OR REPLACE VIEW `{{PROJECT_ID}}.risk_dm.v_employee_risk_trend` AS
SELECT
    s.report_date,
    s.employee_id,
    e.employee_src_id,
    e.full_name,
    e.position_name,
    d.department_name,
    d.block_name,
    d.function_name,
    d.region_name,
    s.risk_score_value,
    s.risk_level_code,
    s.open_event_cnt,
    s.critical_event_cnt,
    s.ib_event_cnt,
    s.security_event_cnt,
    s.compliance_event_cnt,
    s.nonwork_signal_cnt,
    LAG(s.risk_score_value) OVER (
        PARTITION BY s.employee_id
        ORDER BY s.report_date
    ) AS prev_risk_score_value,
    s.risk_score_value
      - LAG(s.risk_score_value) OVER (
            PARTITION BY s.employee_id
            ORDER BY s.report_date
        ) AS risk_score_delta
FROM `{{PROJECT_ID}}.risk_dds.fct_employee_risk_snapshot` s
LEFT JOIN `{{PROJECT_ID}}.risk_dds.dim_employee` e
    ON s.employee_id = e.employee_id
   AND e.is_current_flag = TRUE
LEFT JOIN `{{PROJECT_ID}}.risk_dds.dim_department` d
    ON e.department_id = d.department_id
   AND d.is_current_flag = TRUE;