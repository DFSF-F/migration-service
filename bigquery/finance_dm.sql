CREATE OR REPLACE TABLE `{{PROJECT_ID}}.finance_dm.employee_finance_control_report` (
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

    expense_cnt INT64,
    expense_amount_total_rub NUMERIC,
    suspicious_card_txn_cnt INT64,
    suspicious_card_amount_rub NUMERIC,
    overdue_advance_report_cnt INT64,
    rejected_advance_amount_rub NUMERIC,
    manual_payroll_adj_cnt INT64,
    urgent_vendor_payment_cnt INT64,
    finance_risk_score NUMERIC,
    finance_risk_level_code STRING,

    has_suspicious_card_flag BOOL,
    has_overdue_advance_flag BOOL,
    has_manual_payroll_adj_flag BOOL,
    has_urgent_vendor_payment_flag BOOL,

    last_signal_dttm TIMESTAMP,
    calculation_dttm TIMESTAMP,
    load_dttm TIMESTAMP,
    batch_id STRING
);

TRUNCATE TABLE `{{PROJECT_ID}}.finance_dm.employee_finance_control_report`;

INSERT INTO `{{PROJECT_ID}}.finance_dm.employee_finance_control_report` (
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

    expense_cnt,
    expense_amount_total_rub,
    suspicious_card_txn_cnt,
    suspicious_card_amount_rub,
    overdue_advance_report_cnt,
    rejected_advance_amount_rub,
    manual_payroll_adj_cnt,
    urgent_vendor_payment_cnt,
    finance_risk_score,
    finance_risk_level_code,

    has_suspicious_card_flag,
    has_overdue_advance_flag,
    has_manual_payroll_adj_flag,
    has_urgent_vendor_payment_flag,

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

    s.expense_cnt,
    s.expense_amount_total_rub,
    s.suspicious_card_txn_cnt,
    s.suspicious_card_amount_rub,
    s.overdue_advance_report_cnt,
    s.rejected_advance_amount_rub,
    s.manual_payroll_adj_cnt,
    s.urgent_vendor_payment_cnt,
    s.finance_risk_score,
    s.finance_risk_level_code,

    CASE WHEN s.suspicious_card_txn_cnt > 0 THEN TRUE ELSE FALSE END AS has_suspicious_card_flag,
    CASE WHEN s.overdue_advance_report_cnt > 0 THEN TRUE ELSE FALSE END AS has_overdue_advance_flag,
    CASE WHEN s.manual_payroll_adj_cnt > 0 THEN TRUE ELSE FALSE END AS has_manual_payroll_adj_flag,
    CASE WHEN s.urgent_vendor_payment_cnt > 0 THEN TRUE ELSE FALSE END AS has_urgent_vendor_payment_flag,

    sig.last_signal_dttm,
    s.calculation_dttm,
    s.load_dttm,
    s.batch_id
FROM `{{PROJECT_ID}}.finance_dds.fct_employee_finance_snapshot` s
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
    FROM `{{PROJECT_ID}}.finance_dds.fct_employee_finance_signal`
    GROUP BY employee_id
) sig
    ON s.employee_id = sig.employee_id;