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

CREATE OR REPLACE TABLE `{{PROJECT_ID}}.access_dds.dim_access_system` (
    access_system_id INT64,
    system_code STRING,
    system_name STRING,
    source_system STRING,
    load_dttm INT64,
    batch_id STRING
);

CREATE OR REPLACE TABLE `{{PROJECT_ID}}.access_dds.dim_access_role` (
    access_role_id INT64,
    system_id INT64,
    system_code STRING,
    role_code STRING,
    role_name STRING,
    role_group STRING,
    source_system STRING,
    load_dttm INT64,
    batch_id STRING
);

CREATE OR REPLACE TABLE `{{PROJECT_ID}}.access_dds.fct_employee_account` (
    employee_account_id INT64,
    employee_id INT64,
    employee_src_id STRING,
    system_id INT64,
    system_code STRING,
    source_account_id STRING,
    account_login STRING,
    account_type_code STRING,
    account_type_name STRING,
    privileged_flag BOOL,
    admin_flag BOOL,
    account_status STRING,
    created_at TIMESTAMP,
    disabled_at TIMESTAMP,
    load_dttm INT64,
    batch_id STRING
);

CREATE OR REPLACE TABLE `{{PROJECT_ID}}.access_dds.fct_employee_role_assignment` (
    employee_role_assignment_id INT64,
    employee_id INT64,
    employee_account_id INT64,
    access_role_id INT64,
    source_role_assignment_id STRING,
    assigned_at TIMESTAMP,
    revoked_at TIMESTAMP,
    assignment_status STRING,
    load_dttm INT64,
    batch_id STRING
);

CREATE OR REPLACE TABLE `{{PROJECT_ID}}.access_dds.fct_employee_login_event` (
    employee_login_event_id INT64,
    employee_id INT64,
    employee_account_id INT64,
    system_id INT64,
    source_login_event_id STRING,
    login_dttm TIMESTAMP,
    login_result STRING,
    auth_method STRING,
    ip_address STRING,
    device_id STRING,
    country_name STRING,
    city_name STRING,
    unusual_geo_flag BOOL,
    load_dttm INT64,
    batch_id STRING
);

CREATE OR REPLACE TABLE `{{PROJECT_ID}}.access_dds.fct_employee_access_signal` (
    employee_access_signal_id INT64,
    employee_id INT64,
    signal_source_type STRING,
    source_event_id STRING,
    system_id INT64,
    signal_code STRING,
    signal_name STRING,
    signal_group STRING,
    signal_value_num NUMERIC,
    signal_value_text STRING,
    signal_dttm TIMESTAMP,
    signal_status STRING,
    load_dttm INT64,
    batch_id STRING
);

CREATE OR REPLACE TABLE `{{PROJECT_ID}}.access_dds.fct_employee_access_snapshot` (
    employee_access_snapshot_id INT64,
    employee_id INT64,
    report_date DATE,
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
    calculation_dttm TIMESTAMP,
    load_dttm TIMESTAMP,
    batch_id STRING
);

TRUNCATE TABLE `{{PROJECT_ID}}.access_dds.fct_employee_access_snapshot`;
TRUNCATE TABLE `{{PROJECT_ID}}.access_dds.fct_employee_access_signal`;
TRUNCATE TABLE `{{PROJECT_ID}}.access_dds.fct_employee_login_event`;
TRUNCATE TABLE `{{PROJECT_ID}}.access_dds.fct_employee_role_assignment`;
TRUNCATE TABLE `{{PROJECT_ID}}.access_dds.fct_employee_account`;
TRUNCATE TABLE `{{PROJECT_ID}}.access_dds.dim_access_role`;
TRUNCATE TABLE `{{PROJECT_ID}}.access_dds.dim_access_system`;

INSERT INTO `{{PROJECT_ID}}.access_dds.dim_access_system` (
    access_system_id,
    system_code,
    system_name,
    source_system,
    load_dttm,
    batch_id
)
SELECT
    ROW_NUMBER() OVER (ORDER BY system_code) AS access_system_id,
    system_code,
    MAX(system_name) AS system_name,
    'access_system_catalog' AS source_system,
    MAX(load_dttm) AS load_dttm,
    MAX(batch_id) AS batch_id
FROM `{{PROJECT_ID}}.access_raw.access_system_accounts_raw`
GROUP BY system_code;

INSERT INTO `{{PROJECT_ID}}.access_dds.dim_access_role` (
    access_role_id,
    system_id,
    system_code,
    role_code,
    role_name,
    role_group,
    source_system,
    load_dttm,
    batch_id
)
SELECT
    ROW_NUMBER() OVER (ORDER BY r.system_code, r.role_code) AS access_role_id,
    s.access_system_id,
    r.system_code,
    r.role_code,
    MAX(r.role_name) AS role_name,
    MAX(r.role_group) AS role_group,
    'access_role_catalog' AS source_system,
    MAX(r.load_dttm) AS load_dttm,
    MAX(r.batch_id) AS batch_id
FROM `{{PROJECT_ID}}.access_raw.access_role_assignments_raw` r
LEFT JOIN `{{PROJECT_ID}}.access_dds.dim_access_system` s
    ON r.system_code = s.system_code
GROUP BY
    s.access_system_id,
    r.system_code,
    r.role_code;

INSERT INTO `{{PROJECT_ID}}.access_dds.fct_employee_account` (
    employee_account_id,
    employee_id,
    employee_src_id,
    system_id,
    system_code,
    source_account_id,
    account_login,
    account_type_code,
    account_type_name,
    privileged_flag,
    admin_flag,
    account_status,
    created_at,
    disabled_at,
    load_dttm,
    batch_id
)
SELECT
    ROW_NUMBER() OVER (ORDER BY a.src_account_id) AS employee_account_id,
    e.employee_id,
    a.employee_src_id,
    s.access_system_id,
    a.system_code,
    CAST(a.src_account_id AS STRING),
    a.account_login,
    a.account_type_code,
    a.account_type_name,
    CASE WHEN a.privileged_flag = 'Y' THEN TRUE ELSE FALSE END,
    CASE WHEN a.admin_flag = 'Y' THEN TRUE ELSE FALSE END,
    a.account_status,
    to_bq_timestamp(a.created_at) AS created_at,
    to_bq_timestamp(a.disabled_at) AS disabled_at,
    a.load_dttm,
    a.batch_id
FROM `{{PROJECT_ID}}.access_raw.access_system_accounts_raw` a
LEFT JOIN `{{PROJECT_ID}}.hr_dds.dim_hr_employee` e
    ON a.employee_src_id = e.employee_src_id
   AND e.is_current_flag = TRUE
LEFT JOIN `{{PROJECT_ID}}.access_dds.dim_access_system` s
    ON a.system_code = s.system_code;

INSERT INTO `{{PROJECT_ID}}.access_dds.fct_employee_role_assignment` (
    employee_role_assignment_id,
    employee_id,
    employee_account_id,
    access_role_id,
    source_role_assignment_id,
    assigned_at,
    revoked_at,
    assignment_status,
    load_dttm,
    batch_id
)
SELECT
    ROW_NUMBER() OVER (ORDER BY r.src_role_assignment_id) AS employee_role_assignment_id,
    e.employee_id,
    a.employee_account_id,
    role.access_role_id,
    CAST(r.src_role_assignment_id AS STRING),
    to_bq_timestamp(r.assigned_at) AS assigned_at,
    to_bq_timestamp(r.revoked_at) AS revoked_at,
    r.assignment_status,
    r.load_dttm,
    r.batch_id
FROM `{{PROJECT_ID}}.access_raw.access_role_assignments_raw` r
LEFT JOIN `{{PROJECT_ID}}.hr_dds.dim_hr_employee` e
    ON r.employee_src_id = e.employee_src_id
   AND e.is_current_flag = TRUE
LEFT JOIN `{{PROJECT_ID}}.access_dds.fct_employee_account` a
    ON CAST(r.src_account_id AS STRING) = a.source_account_id
   AND r.employee_src_id = a.employee_src_id
LEFT JOIN `{{PROJECT_ID}}.access_dds.dim_access_role` role
    ON r.system_code = role.system_code
   AND r.role_code = role.role_code;

INSERT INTO `{{PROJECT_ID}}.access_dds.fct_employee_login_event` (
    employee_login_event_id,
    employee_id,
    employee_account_id,
    system_id,
    source_login_event_id,
    login_dttm,
    login_result,
    auth_method,
    ip_address,
    device_id,
    country_name,
    city_name,
    unusual_geo_flag,
    load_dttm,
    batch_id
)
SELECT
    ROW_NUMBER() OVER (ORDER BY l.src_login_event_id) AS employee_login_event_id,
    e.employee_id,
    a.employee_account_id,
    s.access_system_id,
    CAST(l.src_login_event_id AS STRING),
    to_bq_timestamp(l.login_dttm) AS login_dttm,
    l.login_result,
    l.auth_method,
    l.ip_address,
    l.device_id,
    l.country_name,
    l.city_name,
    CASE WHEN l.unusual_geo_flag = 'Y' THEN TRUE ELSE FALSE END,
    l.load_dttm,
    l.batch_id
FROM `{{PROJECT_ID}}.access_raw.access_login_events_raw` l
LEFT JOIN `{{PROJECT_ID}}.hr_dds.dim_hr_employee` e
    ON l.employee_src_id = e.employee_src_id
   AND e.is_current_flag = TRUE
LEFT JOIN `{{PROJECT_ID}}.access_dds.fct_employee_account` a
    ON CAST(l.src_account_id AS STRING) = a.source_account_id
   AND l.employee_src_id = a.employee_src_id
LEFT JOIN `{{PROJECT_ID}}.access_dds.dim_access_system` s
    ON l.system_code = s.system_code;

INSERT INTO `{{PROJECT_ID}}.access_dds.fct_employee_access_signal` (
    employee_access_signal_id,
    employee_id,
    signal_source_type,
    source_event_id,
    system_id,
    signal_code,
    signal_name,
    signal_group,
    signal_value_num,
    signal_value_text,
    signal_dttm,
    signal_status,
    load_dttm,
    batch_id
)
SELECT
    ROW_NUMBER() OVER (ORDER BY signal_source_type, source_event_id) AS employee_access_signal_id,
    employee_id,
    signal_source_type,
    source_event_id,
    system_id,
    signal_code,
    signal_name,
    signal_group,
    signal_value_num,
    signal_value_text,
    signal_dttm,
    signal_status,
    load_dttm,
    batch_id
FROM (
    SELECT
        e.employee_id,
        'privileged_access' AS signal_source_type,
        CAST(p.src_priv_event_id AS STRING) AS source_event_id,
        s.access_system_id AS system_id,
        p.access_type_code AS signal_code,
        p.access_type_name AS signal_name,
        'PRIVILEGED_ACCESS' AS signal_group,
        CAST(
            CASE
                WHEN p.end_dttm IS NOT NULL
                THEN TIMESTAMP_DIFF(to_bq_timestamp(p.end_dttm), to_bq_timestamp(p.start_dttm), SECOND) / 3600.0
                ELSE NULL
            END AS NUMERIC
        ) AS signal_value_num,
        CAST(p.request_id AS STRING) AS signal_value_text,
        to_bq_timestamp(p.start_dttm) AS signal_dttm,
        p.access_status AS signal_status,
        p.load_dttm,
        p.batch_id
    FROM `{{PROJECT_ID}}.access_raw.access_privileged_access_raw` p
    LEFT JOIN `{{PROJECT_ID}}.hr_dds.dim_hr_employee` e
        ON p.employee_src_id = e.employee_src_id
       AND e.is_current_flag = TRUE
    LEFT JOIN `{{PROJECT_ID}}.access_dds.dim_access_system` s
        ON p.system_code = s.system_code

    UNION ALL

    SELECT
        e.employee_id,
        'login_event',
        CAST(l.src_login_event_id AS STRING),
        s.access_system_id,
        CASE
            WHEN l.unusual_geo_flag = 'Y' THEN 'UNUSUAL_GEO_LOGIN'
            ELSE 'FAILED_LOGIN'
        END AS signal_code,
        CASE
            WHEN l.unusual_geo_flag = 'Y' THEN 'Нетипичная география входа'
            ELSE 'Неуспешный вход'
        END AS signal_name,
        'AUTH' AS signal_group,
        CAST(NULL AS NUMERIC) AS signal_value_num,
        l.ip_address AS signal_value_text,
        to_bq_timestamp(l.login_dttm) AS signal_dttm,
        l.login_result AS signal_status,
        l.load_dttm,
        l.batch_id
    FROM `{{PROJECT_ID}}.access_raw.access_login_events_raw` l
    LEFT JOIN `{{PROJECT_ID}}.hr_dds.dim_hr_employee` e
        ON l.employee_src_id = e.employee_src_id
       AND e.is_current_flag = TRUE
    LEFT JOIN `{{PROJECT_ID}}.access_dds.dim_access_system` s
        ON l.system_code = s.system_code
    WHERE l.login_result <> 'SUCCESS'
       OR l.unusual_geo_flag = 'Y'

    UNION ALL

    SELECT
        e.employee_id,
        'file_operation',
        CAST(f.src_file_event_id AS STRING),
        s.access_system_id,
        CASE
            WHEN f.external_transfer_flag = 'Y' THEN 'EXTERNAL_TRANSFER'
            ELSE 'SENSITIVE_FILE_OP'
        END AS signal_code,
        CASE
            WHEN f.external_transfer_flag = 'Y' THEN 'Внешняя передача файлов'
            ELSE 'Операция с чувствительными файлами'
        END AS signal_name,
        'DATA_ACCESS' AS signal_group,
        CAST(NULL AS NUMERIC) AS signal_value_num,
        f.object_path AS signal_value_text,
        to_bq_timestamp(f.operation_dttm) AS signal_dttm,
        'ACTIVE' AS signal_status,
        f.load_dttm,
        f.batch_id
    FROM `{{PROJECT_ID}}.access_raw.access_file_operations_raw` f
    LEFT JOIN `{{PROJECT_ID}}.hr_dds.dim_hr_employee` e
        ON f.employee_src_id = e.employee_src_id
       AND e.is_current_flag = TRUE
    LEFT JOIN `{{PROJECT_ID}}.access_dds.dim_access_system` s
        ON f.system_code = s.system_code
    WHERE f.external_transfer_flag = 'Y'
       OR (
            COALESCE(f.file_classification, '') IN ('CONFIDENTIAL', 'STRICTLY_CONFIDENTIAL')
            AND (f.download_flag = 'Y' OR f.upload_flag = 'Y')
       )

    UNION ALL

    SELECT
        e.employee_id,
        'network_activity',
        CAST(n.src_network_event_id AS STRING),
        s.access_system_id,
        CASE
            WHEN n.blocked_flag = 'Y' THEN 'BLOCKED_CONNECTION'
            ELSE 'DIRECT_EXTERNAL_CONNECTION'
        END AS signal_code,
        CASE
            WHEN n.blocked_flag = 'Y' THEN 'Заблокированное сетевое соединение'
            ELSE 'Прямое внешнее соединение без VPN'
        END AS signal_name,
        'NETWORK' AS signal_group,
        CAST(n.traffic_mb AS NUMERIC) AS signal_value_num,
        n.destination_name AS signal_value_text,
        to_bq_timestamp(n.event_dttm) AS signal_dttm,
        CASE WHEN n.blocked_flag = 'Y' THEN 'BLOCKED' ELSE 'ACTIVE' END AS signal_status,
        n.load_dttm,
        n.batch_id
    FROM `{{PROJECT_ID}}.access_raw.access_network_activity_raw` n
    LEFT JOIN `{{PROJECT_ID}}.hr_dds.dim_hr_employee` e
        ON n.employee_src_id = e.employee_src_id
       AND e.is_current_flag = TRUE
    LEFT JOIN `{{PROJECT_ID}}.access_dds.dim_access_system` s
        ON n.system_code = s.system_code
    WHERE n.blocked_flag = 'Y'
       OR (n.destination_type = 'EXTERNAL' AND COALESCE(n.vpn_used_flag, 'N') = 'N')
) x;

INSERT INTO `{{PROJECT_ID}}.access_dds.fct_employee_access_snapshot` (
    employee_access_snapshot_id,
    employee_id,
    report_date,
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
    calculation_dttm,
    load_dttm,
    batch_id
)
SELECT
    ROW_NUMBER() OVER (ORDER BY e.employee_id) AS employee_access_snapshot_id,
    e.employee_id,
    DATE '2024-12-31' AS report_date,
    COALESCE(acc.active_account_cnt, 0) AS active_account_cnt,
    COALESCE(acc.privileged_account_cnt, 0) AS privileged_account_cnt,
    COALESCE(role_agg.active_role_cnt, 0) AS active_role_cnt,
    COALESCE(sig.privileged_access_cnt, 0) AS privileged_access_cnt,
    COALESCE(login_agg.failed_login_cnt, 0) AS failed_login_cnt,
    COALESCE(login_agg.unusual_geo_login_cnt, 0) AS unusual_geo_login_cnt,
    COALESCE(sig.external_transfer_cnt, 0) AS external_transfer_cnt,
    COALESCE(sig.blocked_network_cnt, 0) AS blocked_network_cnt,
    CAST(
        COALESCE(acc.privileged_account_cnt, 0) * 2.0 +
        COALESCE(sig.privileged_access_cnt, 0) * 2.5 +
        COALESCE(login_agg.failed_login_cnt, 0) * 0.2 +
        COALESCE(login_agg.unusual_geo_login_cnt, 0) * 1.5 +
        COALESCE(sig.external_transfer_cnt, 0) * 2.0 +
        COALESCE(sig.blocked_network_cnt, 0) * 1.2
        AS NUMERIC
    ) AS access_risk_score,
    CASE
        WHEN (
            COALESCE(acc.privileged_account_cnt, 0) * 2.0 +
            COALESCE(sig.privileged_access_cnt, 0) * 2.5 +
            COALESCE(login_agg.failed_login_cnt, 0) * 0.2 +
            COALESCE(login_agg.unusual_geo_login_cnt, 0) * 1.5 +
            COALESCE(sig.external_transfer_cnt, 0) * 2.0 +
            COALESCE(sig.blocked_network_cnt, 0) * 1.2
        ) >= 12 THEN 'HIGH'
        WHEN (
            COALESCE(acc.privileged_account_cnt, 0) * 2.0 +
            COALESCE(sig.privileged_access_cnt, 0) * 2.5 +
            COALESCE(login_agg.failed_login_cnt, 0) * 0.2 +
            COALESCE(login_agg.unusual_geo_login_cnt, 0) * 1.5 +
            COALESCE(sig.external_transfer_cnt, 0) * 2.0 +
            COALESCE(sig.blocked_network_cnt, 0) * 1.2
        ) >= 5 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS access_risk_level_code,
    TIMESTAMP '2024-12-31 23:10:00+00' AS calculation_dttm,
    TIMESTAMP '2024-12-31 23:10:00+00' AS load_dttm,
    'access_dds_build_001' AS batch_id
FROM `{{PROJECT_ID}}.hr_dds.dim_hr_employee` e
LEFT JOIN (
    SELECT
        employee_id,
        COUNTIF(account_status = 'ACTIVE') AS active_account_cnt,
        COUNTIF(account_status = 'ACTIVE' AND (COALESCE(privileged_flag, FALSE) = TRUE OR COALESCE(admin_flag, FALSE) = TRUE)) AS privileged_account_cnt
    FROM `{{PROJECT_ID}}.access_dds.fct_employee_account`
    GROUP BY employee_id
) acc
    ON e.employee_id = acc.employee_id
LEFT JOIN (
    SELECT
        employee_id,
        COUNTIF(revoked_at IS NULL AND assignment_status = 'ACTIVE') AS active_role_cnt
    FROM `{{PROJECT_ID}}.access_dds.fct_employee_role_assignment`
    GROUP BY employee_id
) role_agg
    ON e.employee_id = role_agg.employee_id
LEFT JOIN (
    SELECT
        employee_id,
        COUNTIF(login_result <> 'SUCCESS') AS failed_login_cnt,
        COUNTIF(COALESCE(unusual_geo_flag, FALSE) = TRUE) AS unusual_geo_login_cnt
    FROM `{{PROJECT_ID}}.access_dds.fct_employee_login_event`
    GROUP BY employee_id
) login_agg
    ON e.employee_id = login_agg.employee_id
LEFT JOIN (
    SELECT
        employee_id,
        COUNTIF(signal_source_type = 'privileged_access') AS privileged_access_cnt,
        COUNTIF(signal_code = 'EXTERNAL_TRANSFER') AS external_transfer_cnt,
        COUNTIF(signal_code = 'BLOCKED_CONNECTION') AS blocked_network_cnt
    FROM `{{PROJECT_ID}}.access_dds.fct_employee_access_signal`
    GROUP BY employee_id
) sig
    ON e.employee_id = sig.employee_id
WHERE e.is_current_flag = TRUE;