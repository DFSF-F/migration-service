create table if not exists raw.finance_employee_expense_raw (
    src_expense_id            varchar(64)   not null,
    employee_src_id           varchar(64)   not null,
    expense_date              date          not null,
    expense_type_code         varchar(64)   not null,
    expense_type_name         varchar(255)  not null,
    expense_category          varchar(128),
    amount_rub               numeric(18,2) not null,
    currency_code             varchar(16),
    project_code              varchar(64),
    cost_center_code          varchar(64),
    vendor_src_id             varchar(64),
    expense_status            varchar(64),
    reimbursable_flag         varchar(8),
    source_system             varchar(64)   not null,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (src_expense_id);

create table if not exists raw.finance_corporate_card_txn_raw (
    src_card_txn_id           varchar(64)   not null,
    employee_src_id           varchar(64)   not null,
    transaction_dttm          timestamp     not null,
    merchant_name             varchar(255)  not null,
    mcc_code                  varchar(16),
    transaction_category      varchar(128),
    amount_rub               numeric(18,2) not null,
    currency_code             varchar(16),
    country_name              varchar(128),
    city_name                 varchar(128),
    card_present_flag         varchar(8),
    reversal_flag             varchar(8),
    suspicious_flag           varchar(8),
    source_system             varchar(64)   not null,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (src_card_txn_id);

create table if not exists raw.finance_advance_report_raw (
    src_advance_report_id     varchar(64)   not null,
    employee_src_id           varchar(64)   not null,
    report_period             date          not null,
    total_amount_rub         numeric(18,2) not null,
    approved_amount_rub      numeric(18,2),
    rejected_amount_rub      numeric(18,2),
    overdue_days              integer,
    report_status             varchar(64),
    approver_employee_src_id  varchar(64),
    source_system             varchar(64)   not null,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (src_advance_report_id);

create table if not exists raw.finance_payroll_adjustment_raw (
    src_payroll_adj_id        varchar(64)   not null,
    employee_src_id           varchar(64)   not null,
    payroll_month             date          not null,
    adjustment_type_code      varchar(64)   not null,
    adjustment_type_name      varchar(255)  not null,
    adjustment_reason_group   varchar(128),
    amount_rub               numeric(18,2) not null,
    manual_flag               varchar(8),
    approved_flag             varchar(8),
    source_system             varchar(64)   not null,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (src_payroll_adj_id);

create table if not exists raw.finance_vendor_payment_raw (
    src_vendor_payment_id     varchar(64)   not null,
    employee_src_id           varchar(64),
    vendor_src_id             varchar(64)   not null,
    vendor_name               varchar(255)  not null,
    payment_date              date          not null,
    payment_amount_rub       numeric(18,2) not null,
    payment_type_code         varchar(64),
    payment_type_name         varchar(255),
    contract_code             varchar(64),
    urgent_flag               varchar(8),
    source_system             varchar(64)   not null,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (src_vendor_payment_id);

create table if not exists raw.finance_budget_limit_raw (
    src_budget_limit_id       varchar(64)   not null,
    cost_center_code          varchar(64)   not null,
    cost_center_name          varchar(255)  not null,
    budget_period             date          not null,
    budget_amount_rub        numeric(18,2) not null,
    consumed_amount_rub      numeric(18,2),
    exceeded_flag             varchar(8),
    source_system             varchar(64)   not null,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (src_budget_limit_id);