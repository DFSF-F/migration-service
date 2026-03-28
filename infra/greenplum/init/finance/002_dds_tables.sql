create table if not exists dds.dim_finance_cost_center (
    cost_center_id            bigint        not null,
    cost_center_code          varchar(64)   not null,
    cost_center_name          varchar(255)  not null,
    source_system             varchar(64)   not null,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (cost_center_id);

create table if not exists dds.dim_finance_vendor (
    finance_vendor_id         bigint        not null,
    vendor_src_id             varchar(64)   not null,
    vendor_name               varchar(255)  not null,
    source_system             varchar(64)   not null,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (finance_vendor_id);

create table if not exists dds.fct_employee_expense (
    employee_expense_id       bigint        not null,
    employee_id               bigint        not null,
    employee_src_id           varchar(64)   not null,
    cost_center_id            bigint,
    finance_vendor_id         bigint,
    source_expense_id         varchar(64)   not null,
    expense_date              date          not null,
    expense_type_code         varchar(64)   not null,
    expense_type_name         varchar(255)  not null,
    expense_category          varchar(128),
    amount_rub               numeric(18,2) not null,
    project_code              varchar(64),
    expense_status            varchar(64),
    reimbursable_flag         boolean,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (employee_expense_id);

create table if not exists dds.fct_employee_card_transaction (
    employee_card_transaction_id bigint     not null,
    employee_id               bigint        not null,
    employee_src_id           varchar(64)   not null,
    source_card_txn_id        varchar(64)   not null,
    transaction_dttm          timestamp     not null,
    merchant_name             varchar(255)  not null,
    mcc_code                  varchar(16),
    transaction_category      varchar(128),
    amount_rub               numeric(18,2) not null,
    country_name              varchar(128),
    city_name                 varchar(128),
    card_present_flag         boolean,
    reversal_flag             boolean,
    suspicious_flag           boolean,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (employee_card_transaction_id);

create table if not exists dds.fct_employee_advance_report (
    employee_advance_report_id bigint       not null,
    employee_id               bigint        not null,
    employee_src_id           varchar(64)   not null,
    source_advance_report_id  varchar(64)   not null,
    report_period             date          not null,
    total_amount_rub         numeric(18,2) not null,
    approved_amount_rub      numeric(18,2),
    rejected_amount_rub      numeric(18,2),
    overdue_days              integer,
    report_status             varchar(64),
    approver_employee_src_id  varchar(64),
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (employee_advance_report_id);

create table if not exists dds.fct_employee_finance_signal (
    employee_finance_signal_id bigint       not null,
    employee_id               bigint        not null,
    signal_source_type        varchar(64)   not null,
    source_event_id           varchar(64)   not null,
    signal_code               varchar(64)   not null,
    signal_name               varchar(255)  not null,
    signal_group              varchar(128),
    signal_value_num          numeric(18,4),
    signal_value_text         varchar(255),
    signal_dttm               timestamp     not null,
    signal_status             varchar(64),
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (employee_finance_signal_id);

create table if not exists dds.fct_employee_finance_snapshot (
    employee_finance_snapshot_id bigint     not null,
    employee_id               bigint        not null,
    report_date               date          not null,
    expense_cnt               integer,
    expense_amount_total_rub numeric(18,2),
    suspicious_card_txn_cnt   integer,
    suspicious_card_amount_rub numeric(18,2),
    overdue_advance_report_cnt integer,
    rejected_advance_amount_rub numeric(18,2),
    manual_payroll_adj_cnt    integer,
    urgent_vendor_payment_cnt integer,
    finance_risk_score        numeric(18,4),
    finance_risk_level_code   varchar(32),
    calculation_dttm          timestamp     not null,
    load_dttm                 timestamp     not null,
    batch_id                  varchar(64)   not null
)
distributed by (employee_finance_snapshot_id);