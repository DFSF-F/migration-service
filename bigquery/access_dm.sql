CREATE OR REPLACE TABLE `{{PROJECT_ID}}.access_dm.employee_access_control_report` (
    report_date DATE,
    employee_id INT64,
    employee_src_id STRING,
    employee_number STRING,
    tab_num STRING,
    full_name STRING,
    position_name STRING,
    employment_status STRING,

    department_id INT64,
    department_src_id STRING,
    department_name STRING,
    block_name STRING,
    function_name STRING,
    region_name STRING,

    active_account_cnt INT64,
    privileged_account_cnt INT64,
    active_role_cnt INT64,
    privileged_access_cnt INT64,
    failed_login_cnt INT64,
    unusual_geo_login_cnt INT64,
    external_transfer_cnt INT64,
    blocked_network_cnt INT64,
    access_risk_score NUMERIC,
    access_risk_level_code STRING,

    has_privileged_account_flag BOOL,
    has_unusual_geo_flag BOOL,
    has_external_transfer_flag BOOL,
    has_blocked_network_flag BOOL,

    last_signal_dttm TIMESTAMP,
    calculation_dttm TIMESTAMP,
    load_dttm TIMESTAMP,
    batch_id STRING
);

TRUNCATE TABLE `{{PROJECT_ID}}.access_dm.employee_access_control_report`;

INSERT INTO `{{PROJECT_ID}}.access_dm.employee_access_control_report` (
    report_date,
    employee_id,
    employee_src_id,
    employee_number,
    tab_num,
    full_name,
    position_name,
    employment_status,

    department_id,
    department_src_id,
    department_name,
    block_name,
    function_name,
    region_name,

    active_account_cnt,
    privileged_account_cnt,
    active_role_cnt,
    privileged_access_cnt,
    failed_login_cnt,
    unusual_geo_login_cnt,
    external_transfer_cnt,
    blocked_network_cnt,
    access_risk_score,
    access_risk_level_code,

    has_privileged_account_flag,
    has_unusual_geo_flag,
    has_external_transfer_flag,
    has_blocked_network_flag,

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
    p.position_name,
    e.employment_status,

    d.department_id,
    d.department_src_id,
    d.department_name,
    d.block_name,
    d.function_name,
    d.region_name,

    s.active_account_cnt,
    s.privileged_account_cnt,
    s.active_role_cnt,
    s.privileged_access_cnt,
    s.failed_login_cnt,
    s.unusual_geo_login_cnt,
    s.external_transfer_cnt,
    s.blocked_network_cnt,
    s.access_risk_score,
    s.access_risk_level_code,

    CASE WHEN s.privileged_account_cnt > 0 THEN TRUE ELSE FALSE END AS has_privileged_account_flag,
    CASE WHEN s.unusual_geo_login_cnt > 0 THEN TRUE ELSE FALSE END AS has_unusual_geo_flag,
    CASE WHEN s.external_transfer_cnt > 0 THEN TRUE ELSE FALSE END AS has_external_transfer_flag,
    CASE WHEN s.blocked_network_cnt > 0 THEN TRUE ELSE FALSE END AS has_blocked_network_flag,

    sig.last_signal_dttm,
    s.calculation_dttm,
    s.load_dttm,
    s.batch_id
FROM `{{PROJECT_ID}}.access_dds.fct_employee_access_snapshot` s
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
        MAX(signal_dttm) AS last_signal_dttm
    FROM `{{PROJECT_ID}}.access_dds.fct_employee_access_signal`
    GROUP BY employee_id
) sig
    ON s.employee_id = sig.employee_id;