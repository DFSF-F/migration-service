create table if not exists dm.employee_finance_control_report (
    report_date                    date          not null,
    employee_id                    bigint        not null,
    employee_src_id                varchar(64)   not null,
    employee_number                varchar(64),
    tab_num                        varchar(64),
    full_name                      varchar(255)  not null,
    position_name                  varchar(255),
    employment_status              varchar(64),

    department_id                  bigint,
    department_src_id              varchar(64),
    department_name                varchar(255),
    block_name                     varchar(255),
    function_name                  varchar(255),
    region_name                    varchar(255),

    expense_cnt                    integer,
    expense_amount_total_rub      numeric(18,2),
    suspicious_card_txn_cnt        integer,
    suspicious_card_amount_rub    numeric(18,2),
    overdue_advance_report_cnt     integer,
    rejected_advance_amount_rub   numeric(18,2),
    manual_payroll_adj_cnt         integer,
    urgent_vendor_payment_cnt      integer,
    finance_risk_score             numeric(18,4),
    finance_risk_level_code        varchar(32),

    has_suspicious_card_flag       boolean,
    has_overdue_advance_flag       boolean,
    has_manual_payroll_adj_flag    boolean,
    has_urgent_vendor_payment_flag boolean,

    last_signal_dttm               timestamp,
    calculation_dttm               timestamp     not null,
    load_dttm                      timestamp     not null,
    batch_id                       varchar(64)   not null
)
distributed by (employee_id);

create or replace view dm.v_employee_suspicious_expense_signals as
select
    s.report_date,
    e.employee_id,
    e.employee_src_id,
    e.full_name,
    p.position_name,
    d.department_name,
    d.block_name,
    d.function_name,
    d.region_name,
    f.signal_source_type,
    f.source_event_id,
    f.signal_code,
    f.signal_name,
    f.signal_group,
    f.signal_value_num,
    f.signal_value_text,
    f.signal_dttm,
    f.signal_status
from dds.fct_employee_finance_signal f
left join dds.dim_hr_employee e
    on f.employee_id = e.employee_id
   and e.is_current_flag = true
left join dds.dim_position p
    on e.current_position_id = p.position_id
left join dds.dim_hr_department d
    on e.department_id = d.department_id
   and d.is_current_flag = true
left join dds.fct_employee_finance_snapshot s
    on f.employee_id = s.employee_id
where f.signal_source_type in ('card_transaction', 'advance_report', 'payroll_adjustment', 'vendor_payment');