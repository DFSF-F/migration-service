CREATE OR REPLACE TABLE `{{PROJECT_ID}}.finance_dds.dim_finance_cost_center` (
    cost_center_id INT64,
    cost_center_code STRING,
    cost_center_name STRING,
    source_system STRING,
    load_dttm INT64,
    batch_id STRING
);

CREATE OR REPLACE TABLE `{{PROJECT_ID}}.finance_dds.dim_finance_vendor` (
    finance_vendor_id INT64,
    vendor_src_id STRING,
    vendor_name STRING,
    source_system STRING,
    load_dttm INT64,
    batch_id STRING
);

CREATE OR REPLACE TABLE `{{PROJECT_ID}}.finance_dds.fct_employee_expense` (
    employee_expense_id INT64,
    employee_id INT64,
    employee_src_id STRING,
    cost_center_id INT64,
    finance_vendor_id INT64,
    source_expense_id STRING,
    expense_date DATE,
    expense_type_code STRING,
    expense_type_name STRING,
    expense_category STRING,
    amount_rub NUMERIC,
    project_code STRING,
    expense_status STRING,
    reimbursable_flag BOOL,
    load_dttm INT64,
    batch_id STRING
);

CREATE OR REPLACE TABLE `{{PROJECT_ID}}.finance_dds.fct_employee_card_transaction` (
    employee_card_transaction_id INT64,
    employee_id INT64,
    employee_src_id STRING,
    source_card_txn_id STRING,
    transaction_dttm TIMESTAMP,
    merchant_name STRING,
    mcc_code STRING,
    transaction_category STRING,
    amount_rub NUMERIC,
    country_name STRING,
    city_name STRING,
    card_present_flag BOOL,
    reversal_flag BOOL,
    suspicious_flag BOOL,
    load_dttm INT64,
    batch_id STRING
);

CREATE OR REPLACE TABLE `{{PROJECT_ID}}.finance_dds.fct_employee_advance_report` (
    employee_advance_report_id INT64,
    employee_id INT64,
    employee_src_id STRING,
    source_advance_report_id STRING,
    report_period DATE,
    total_amount_rub NUMERIC,
    approved_amount_rub NUMERIC,
    rejected_amount_rub NUMERIC,
    overdue_days INT64,
    report_status STRING,
    approver_employee_src_id STRING,
    load_dttm INT64,
    batch_id STRING
);

CREATE OR REPLACE TABLE `{{PROJECT_ID}}.finance_dds.fct_employee_finance_signal` (
    employee_finance_signal_id INT64,
    employee_id INT64,
    signal_source_type STRING,
    source_event_id STRING,
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

CREATE OR REPLACE TABLE `{{PROJECT_ID}}.finance_dds.fct_employee_finance_snapshot` (
    employee_finance_snapshot_id INT64,
    employee_id INT64,
    report_date DATE,
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
    calculation_dttm TIMESTAMP,
    load_dttm TIMESTAMP,
    batch_id STRING
);

TRUNCATE TABLE `{{PROJECT_ID}}.finance_dds.fct_employee_finance_snapshot`;
TRUNCATE TABLE `{{PROJECT_ID}}.finance_dds.fct_employee_finance_signal`;
TRUNCATE TABLE `{{PROJECT_ID}}.finance_dds.fct_employee_advance_report`;
TRUNCATE TABLE `{{PROJECT_ID}}.finance_dds.fct_employee_card_transaction`;
TRUNCATE TABLE `{{PROJECT_ID}}.finance_dds.fct_employee_expense`;
TRUNCATE TABLE `{{PROJECT_ID}}.finance_dds.dim_finance_vendor`;
TRUNCATE TABLE `{{PROJECT_ID}}.finance_dds.dim_finance_cost_center`;

INSERT INTO `{{PROJECT_ID}}.finance_dds.dim_finance_cost_center` (
    cost_center_id,
    cost_center_code,
    cost_center_name,
    source_system,
    load_dttm,
    batch_id
)
SELECT
    ROW_NUMBER() OVER (ORDER BY cost_center_code) AS cost_center_id,
    cost_center_code,
    MAX(cost_center_name) AS cost_center_name,
    'finance_cost_center_catalog' AS source_system,
    MAX(load_dttm) AS load_dttm,
    MAX(batch_id) AS batch_id
FROM (
    SELECT
        cost_center_code,
        cost_center_name,
        load_dttm,
        batch_id
    FROM `{{PROJECT_ID}}.finance_raw.finance_budget_limit_raw`

    UNION ALL

    SELECT
        cost_center_code,
        CONCAT('ЦФО ', cost_center_code) AS cost_center_name,
        load_dttm,
        batch_id
    FROM `{{PROJECT_ID}}.finance_raw.finance_employee_expense_raw`
    WHERE cost_center_code IS NOT NULL
) x
GROUP BY cost_center_code;

INSERT INTO `{{PROJECT_ID}}.finance_dds.dim_finance_vendor` (
    finance_vendor_id,
    vendor_src_id,
    vendor_name,
    source_system,
    load_dttm,
    batch_id
)
SELECT
    ROW_NUMBER() OVER (ORDER BY vendor_src_id) AS finance_vendor_id,
    vendor_src_id,
    MAX(vendor_name) AS vendor_name,
    'finance_vendor_catalog' AS source_system,
    MAX(load_dttm) AS load_dttm,
    MAX(batch_id) AS batch_id
FROM `{{PROJECT_ID}}.finance_raw.finance_vendor_payment_raw`
GROUP BY vendor_src_id;

INSERT INTO `{{PROJECT_ID}}.finance_dds.fct_employee_expense` (
    employee_expense_id,
    employee_id,
    employee_src_id,
    cost_center_id,
    finance_vendor_id,
    source_expense_id,
    expense_date,
    expense_type_code,
    expense_type_name,
    expense_category,
    amount_rub,
    project_code,
    expense_status,
    reimbursable_flag,
    load_dttm,
    batch_id
)
SELECT
    ROW_NUMBER() OVER (ORDER BY CAST(e.src_expense_id AS STRING)) AS employee_expense_id,
    hr.employee_id,
    e.employee_src_id,
    cc.cost_center_id,
    v.finance_vendor_id,
    CAST(e.src_expense_id AS STRING) AS source_expense_id,
    e.expense_date,
    e.expense_type_code,
    e.expense_type_name,
    e.expense_category,
    CAST(e.amount_rub AS NUMERIC) AS amount_rub,
    e.project_code,
    e.expense_status,
    CASE WHEN e.reimbursable_flag = 'Y' THEN TRUE ELSE FALSE END AS reimbursable_flag,
    e.load_dttm,
    e.batch_id
FROM `{{PROJECT_ID}}.finance_raw.finance_employee_expense_raw` e
LEFT JOIN `{{PROJECT_ID}}.hr_dds.dim_hr_employee` hr
    ON e.employee_src_id = hr.employee_src_id
   AND hr.is_current_flag = TRUE
LEFT JOIN `{{PROJECT_ID}}.finance_dds.dim_finance_cost_center` cc
    ON e.cost_center_code = cc.cost_center_code
LEFT JOIN `{{PROJECT_ID}}.finance_dds.dim_finance_vendor` v
    ON e.vendor_src_id = v.vendor_src_id
WHERE hr.employee_id IS NOT NULL;

INSERT INTO `{{PROJECT_ID}}.finance_dds.fct_employee_card_transaction` (
    employee_card_transaction_id,
    employee_id,
    employee_src_id,
    source_card_txn_id,
    transaction_dttm,
    merchant_name,
    mcc_code,
    transaction_category,
    amount_rub,
    country_name,
    city_name,
    card_present_flag,
    reversal_flag,
    suspicious_flag,
    load_dttm,
    batch_id
)
SELECT
    ROW_NUMBER() OVER (ORDER BY CAST(c.src_card_txn_id AS STRING)) AS employee_card_transaction_id,
    hr.employee_id,
    c.employee_src_id,
    CAST(c.src_card_txn_id AS STRING) AS source_card_txn_id,
    CASE
        WHEN c.transaction_dttm IS NULL THEN NULL
        WHEN c.transaction_dttm BETWEEN 0 AND 32503680000 THEN TIMESTAMP_SECONDS(c.transaction_dttm)
        WHEN c.transaction_dttm BETWEEN 0 AND 32503680000000 THEN TIMESTAMP_MILLIS(c.transaction_dttm)
        ELSE NULL
    END AS transaction_dttm,
    c.merchant_name,
    c.mcc_code,
    c.transaction_category,
    CAST(c.amount_rub AS NUMERIC) AS amount_rub,
    c.country_name,
    c.city_name,
    CASE WHEN c.card_present_flag = 'Y' THEN TRUE ELSE FALSE END AS card_present_flag,
    CASE WHEN c.reversal_flag = 'Y' THEN TRUE ELSE FALSE END AS reversal_flag,
    CASE WHEN c.suspicious_flag = 'Y' THEN TRUE ELSE FALSE END AS suspicious_flag,
    c.load_dttm,
    c.batch_id
FROM `{{PROJECT_ID}}.finance_raw.finance_corporate_card_txn_raw` c
LEFT JOIN `{{PROJECT_ID}}.hr_dds.dim_hr_employee` hr
    ON c.employee_src_id = hr.employee_src_id
   AND hr.is_current_flag = TRUE
WHERE hr.employee_id IS NOT NULL;

INSERT INTO `{{PROJECT_ID}}.finance_dds.fct_employee_advance_report` (
    employee_advance_report_id,
    employee_id,
    employee_src_id,
    source_advance_report_id,
    report_period,
    total_amount_rub,
    approved_amount_rub,
    rejected_amount_rub,
    overdue_days,
    report_status,
    approver_employee_src_id,
    load_dttm,
    batch_id
)
SELECT
    ROW_NUMBER() OVER (ORDER BY CAST(a.src_advance_report_id AS STRING)) AS employee_advance_report_id,
    hr.employee_id,
    a.employee_src_id,
    CAST(a.src_advance_report_id AS STRING) AS source_advance_report_id,
    a.report_period,
    CAST(a.total_amount_rub AS NUMERIC) AS total_amount_rub,
    CAST(a.approved_amount_rub AS NUMERIC) AS approved_amount_rub,
    CAST(a.rejected_amount_rub AS NUMERIC) AS rejected_amount_rub,
    a.overdue_days,
    a.report_status,
    a.approver_employee_src_id,
    a.load_dttm,
    a.batch_id
FROM `{{PROJECT_ID}}.finance_raw.finance_advance_report_raw` a
LEFT JOIN `{{PROJECT_ID}}.hr_dds.dim_hr_employee` hr
    ON a.employee_src_id = hr.employee_src_id
   AND hr.is_current_flag = TRUE
WHERE hr.employee_id IS NOT NULL;

INSERT INTO `{{PROJECT_ID}}.finance_dds.fct_employee_finance_signal` (
    employee_finance_signal_id,
    employee_id,
    signal_source_type,
    source_event_id,
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
    ROW_NUMBER() OVER (ORDER BY signal_source_type, source_event_id) AS employee_finance_signal_id,
    employee_id,
    signal_source_type,
    source_event_id,
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
        hr.employee_id,
        'card_transaction' AS signal_source_type,
        CAST(c.src_card_txn_id AS STRING) AS source_event_id,
        'SUSPICIOUS_CARD_TXN' AS signal_code,
        'Подозрительная карточная операция' AS signal_name,
        'CARD' AS signal_group,
        CAST(c.amount_rub AS NUMERIC) AS signal_value_num,
        c.merchant_name AS signal_value_text,
        CASE
            WHEN c.transaction_dttm IS NULL THEN NULL
            WHEN c.transaction_dttm BETWEEN 0 AND 32503680000 THEN TIMESTAMP_SECONDS(c.transaction_dttm)
            WHEN c.transaction_dttm BETWEEN 0 AND 32503680000000 THEN TIMESTAMP_MILLIS(c.transaction_dttm)
            ELSE NULL
        END AS signal_dttm,
        'ACTIVE' AS signal_status,
        c.load_dttm,
        c.batch_id
    FROM `{{PROJECT_ID}}.finance_raw.finance_corporate_card_txn_raw` c
    LEFT JOIN `{{PROJECT_ID}}.hr_dds.dim_hr_employee` hr
        ON c.employee_src_id = hr.employee_src_id
       AND hr.is_current_flag = TRUE
    WHERE hr.employee_id IS NOT NULL
      AND c.suspicious_flag = 'Y'

    UNION ALL

    SELECT
        hr.employee_id,
        'advance_report' AS signal_source_type,
        CAST(a.src_advance_report_id AS STRING) AS source_event_id,
        CASE
            WHEN COALESCE(a.overdue_days, 0) > 0 THEN 'OVERDUE_ADVANCE_REPORT'
            ELSE 'REJECTED_ADVANCE_AMOUNT'
        END AS signal_code,
        CASE
            WHEN COALESCE(a.overdue_days, 0) > 0 THEN 'Просроченный авансовый отчёт'
            ELSE 'Отклонённая сумма в авансовом отчёте'
        END AS signal_name,
        'ADVANCE' AS signal_group,
        CASE
            WHEN COALESCE(a.overdue_days, 0) > 0 THEN CAST(a.overdue_days AS NUMERIC)
            ELSE CAST(COALESCE(a.rejected_amount_rub, 0) AS NUMERIC)
        END AS signal_value_num,
        a.report_status AS signal_value_text,
        TIMESTAMP(a.report_period) AS signal_dttm,
        a.report_status AS signal_status,
        a.load_dttm,
        a.batch_id
    FROM `{{PROJECT_ID}}.finance_raw.finance_advance_report_raw` a
    LEFT JOIN `{{PROJECT_ID}}.hr_dds.dim_hr_employee` hr
        ON a.employee_src_id = hr.employee_src_id
       AND hr.is_current_flag = TRUE
    WHERE hr.employee_id IS NOT NULL
      AND (COALESCE(a.overdue_days, 0) > 0 OR COALESCE(a.rejected_amount_rub, 0) > 0)

    UNION ALL

    SELECT
        hr.employee_id,
        'payroll_adjustment' AS signal_source_type,
        CAST(p.src_payroll_adj_id AS STRING) AS source_event_id,
        'MANUAL_PAYROLL_ADJ' AS signal_code,
        'Ручная корректировка начисления' AS signal_name,
        'PAYROLL' AS signal_group,
        CAST(p.amount_rub AS NUMERIC) AS signal_value_num,
        p.adjustment_type_name AS signal_value_text,
        TIMESTAMP(p.payroll_month) AS signal_dttm,
        CASE WHEN p.approved_flag = 'Y' THEN 'APPROVED' ELSE 'PENDING' END AS signal_status,
        p.load_dttm,
        p.batch_id
    FROM `{{PROJECT_ID}}.finance_raw.finance_payroll_adjustment_raw` p
    LEFT JOIN `{{PROJECT_ID}}.hr_dds.dim_hr_employee` hr
        ON p.employee_src_id = hr.employee_src_id
       AND hr.is_current_flag = TRUE
    WHERE hr.employee_id IS NOT NULL
      AND p.manual_flag = 'Y'

    UNION ALL

    SELECT
        hr.employee_id,
        'vendor_payment' AS signal_source_type,
        CAST(v.src_vendor_payment_id AS STRING) AS source_event_id,
        'URGENT_VENDOR_PAYMENT' AS signal_code,
        'Срочный платёж поставщику' AS signal_name,
        'AP' AS signal_group,
        CAST(v.payment_amount_rub AS NUMERIC) AS signal_value_num,
        v.vendor_name AS signal_value_text,
        TIMESTAMP(v.payment_date) AS signal_dttm,
        'ACTIVE' AS signal_status,
        v.load_dttm,
        v.batch_id
    FROM `{{PROJECT_ID}}.finance_raw.finance_vendor_payment_raw` v
    LEFT JOIN `{{PROJECT_ID}}.hr_dds.dim_hr_employee` hr
        ON v.employee_src_id = hr.employee_src_id
       AND hr.is_current_flag = TRUE
    WHERE hr.employee_id IS NOT NULL
      AND v.urgent_flag = 'Y'
) s;

INSERT INTO `{{PROJECT_ID}}.finance_dds.fct_employee_finance_snapshot` (
    employee_finance_snapshot_id,
    employee_id,
    report_date,
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
    calculation_dttm,
    load_dttm,
    batch_id
)
SELECT
    ROW_NUMBER() OVER (ORDER BY e.employee_id) AS employee_finance_snapshot_id,
    e.employee_id,
    DATE '2024-12-31' AS report_date,
    COALESCE(expense_agg.expense_cnt, 0) AS expense_cnt,
    COALESCE(expense_agg.expense_amount_total_rub, 0) AS expense_amount_total_rub,
    COALESCE(card_agg.suspicious_card_txn_cnt, 0) AS suspicious_card_txn_cnt,
    COALESCE(card_agg.suspicious_card_amount_rub, 0) AS suspicious_card_amount_rub,
    COALESCE(advance_agg.overdue_advance_report_cnt, 0) AS overdue_advance_report_cnt,
    COALESCE(advance_agg.rejected_advance_amount_rub, 0) AS rejected_advance_amount_rub,
    COALESCE(signal_agg.manual_payroll_adj_cnt, 0) AS manual_payroll_adj_cnt,
    COALESCE(signal_agg.urgent_vendor_payment_cnt, 0) AS urgent_vendor_payment_cnt,
    CAST(
        COALESCE(card_agg.suspicious_card_txn_cnt, 0) * 2.2 +
        COALESCE(advance_agg.overdue_advance_report_cnt, 0) * 1.8 +
        COALESCE(signal_agg.manual_payroll_adj_cnt, 0) * 1.3 +
        COALESCE(signal_agg.urgent_vendor_payment_cnt, 0) * 1.5 +
        COALESCE(advance_agg.rejected_advance_amount_rub, 0) / 50000.0 +
        COALESCE(card_agg.suspicious_card_amount_rub, 0) / 70000.0
        AS NUMERIC
    ) AS finance_risk_score,
    CASE
        WHEN (
            COALESCE(card_agg.suspicious_card_txn_cnt, 0) * 2.2 +
            COALESCE(advance_agg.overdue_advance_report_cnt, 0) * 1.8 +
            COALESCE(signal_agg.manual_payroll_adj_cnt, 0) * 1.3 +
            COALESCE(signal_agg.urgent_vendor_payment_cnt, 0) * 1.5 +
            COALESCE(advance_agg.rejected_advance_amount_rub, 0) / 50000.0 +
            COALESCE(card_agg.suspicious_card_amount_rub, 0) / 70000.0
        ) >= 10 THEN 'HIGH'
        WHEN (
            COALESCE(card_agg.suspicious_card_txn_cnt, 0) * 2.2 +
            COALESCE(advance_agg.overdue_advance_report_cnt, 0) * 1.8 +
            COALESCE(signal_agg.manual_payroll_adj_cnt, 0) * 1.3 +
            COALESCE(signal_agg.urgent_vendor_payment_cnt, 0) * 1.5 +
            COALESCE(advance_agg.rejected_advance_amount_rub, 0) / 50000.0 +
            COALESCE(card_agg.suspicious_card_amount_rub, 0) / 70000.0
        ) >= 4 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS finance_risk_level_code,
    TIMESTAMP '2024-12-31 23:20:00+00' AS calculation_dttm,
    TIMESTAMP '2024-12-31 23:20:00+00' AS load_dttm,
    'finance_dds_build_001' AS batch_id
FROM `{{PROJECT_ID}}.hr_dds.dim_hr_employee` e
LEFT JOIN (
    SELECT
        employee_id,
        COUNT(*) AS expense_cnt,
        SUM(CAST(amount_rub AS NUMERIC)) AS expense_amount_total_rub
    FROM `{{PROJECT_ID}}.finance_dds.fct_employee_expense`
    GROUP BY employee_id
) expense_agg
    ON e.employee_id = expense_agg.employee_id
LEFT JOIN (
    SELECT
        employee_id,
        SUM(CASE WHEN COALESCE(suspicious_flag, FALSE) = TRUE THEN 1 ELSE 0 END) AS suspicious_card_txn_cnt,
        SUM(CASE WHEN COALESCE(suspicious_flag, FALSE) = TRUE THEN CAST(amount_rub AS NUMERIC) ELSE 0 END) AS suspicious_card_amount_rub
    FROM `{{PROJECT_ID}}.finance_dds.fct_employee_card_transaction`
    GROUP BY employee_id
) card_agg
    ON e.employee_id = card_agg.employee_id
LEFT JOIN (
    SELECT
        employee_id,
        SUM(CASE WHEN COALESCE(overdue_days, 0) > 0 THEN 1 ELSE 0 END) AS overdue_advance_report_cnt,
        SUM(CAST(COALESCE(rejected_amount_rub, 0) AS NUMERIC)) AS rejected_advance_amount_rub
    FROM `{{PROJECT_ID}}.finance_dds.fct_employee_advance_report`
    GROUP BY employee_id
) advance_agg
    ON e.employee_id = advance_agg.employee_id
LEFT JOIN (
    SELECT
        employee_id,
        SUM(CASE WHEN signal_code = 'MANUAL_PAYROLL_ADJ' THEN 1 ELSE 0 END) AS manual_payroll_adj_cnt,
        SUM(CASE WHEN signal_code = 'URGENT_VENDOR_PAYMENT' THEN 1 ELSE 0 END) AS urgent_vendor_payment_cnt
    FROM `{{PROJECT_ID}}.finance_dds.fct_employee_finance_signal`
    GROUP BY employee_id
) signal_agg
    ON e.employee_id = signal_agg.employee_id
WHERE e.is_current_flag = TRUE;