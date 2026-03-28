truncate table dds.fct_employee_finance_snapshot;
truncate table dds.fct_employee_finance_signal;
truncate table dds.fct_employee_advance_report;
truncate table dds.fct_employee_card_transaction;
truncate table dds.fct_employee_expense;
truncate table dds.dim_finance_vendor;
truncate table dds.dim_finance_cost_center;

insert into dds.dim_finance_cost_center (
    cost_center_id,
    cost_center_code,
    cost_center_name,
    source_system,
    load_dttm,
    batch_id
)
select
    row_number() over (order by cost_center_code) as cost_center_id,
    cost_center_code,
    max(cost_center_name) as cost_center_name,
    'finance_cost_center_catalog' as source_system,
    max(load_dttm) as load_dttm,
    max(batch_id) as batch_id
from (
    select
        cost_center_code,
        cost_center_name,
        load_dttm,
        batch_id
    from raw.finance_budget_limit_raw

    union all

    select
        cost_center_code,
        'ЦФО ' || cost_center_code as cost_center_name,
        load_dttm,
        batch_id
    from raw.finance_employee_expense_raw
    where cost_center_code is not null
) x
group by cost_center_code;

insert into dds.dim_finance_vendor (
    finance_vendor_id,
    vendor_src_id,
    vendor_name,
    source_system,
    load_dttm,
    batch_id
)
select
    row_number() over (order by vendor_src_id) as finance_vendor_id,
    vendor_src_id,
    max(vendor_name) as vendor_name,
    'finance_vendor_catalog' as source_system,
    max(load_dttm) as load_dttm,
    max(batch_id) as batch_id
from raw.finance_vendor_payment_raw
group by vendor_src_id;

insert into dds.fct_employee_expense (
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
select
    row_number() over (order by e.src_expense_id) as employee_expense_id,
    hr.employee_id,
    e.employee_src_id,
    cc.cost_center_id,
    v.finance_vendor_id,
    e.src_expense_id,
    e.expense_date,
    e.expense_type_code,
    e.expense_type_name,
    e.expense_category,
    e.amount_rub,
    e.project_code,
    e.expense_status,
    case when e.reimbursable_flag = 'Y' then true else false end,
    e.load_dttm,
    e.batch_id
from raw.finance_employee_expense_raw e
left join dds.dim_hr_employee hr
    on e.employee_src_id = hr.employee_src_id
   and hr.is_current_flag = true
left join dds.dim_finance_cost_center cc
    on e.cost_center_code = cc.cost_center_code
left join dds.dim_finance_vendor v
    on e.vendor_src_id = v.vendor_src_id
where hr.employee_id is not null;

insert into dds.fct_employee_card_transaction (
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
select
    row_number() over (order by c.src_card_txn_id) as employee_card_transaction_id,
    hr.employee_id,
    c.employee_src_id,
    c.src_card_txn_id,
    c.transaction_dttm,
    c.merchant_name,
    c.mcc_code,
    c.transaction_category,
    c.amount_rub,
    c.country_name,
    c.city_name,
    case when c.card_present_flag = 'Y' then true else false end,
    case when c.reversal_flag = 'Y' then true else false end,
    case when c.suspicious_flag = 'Y' then true else false end,
    c.load_dttm,
    c.batch_id
from raw.finance_corporate_card_txn_raw c
left join dds.dim_hr_employee hr
    on c.employee_src_id = hr.employee_src_id
   and hr.is_current_flag = true
where hr.employee_id is not null;

insert into dds.fct_employee_advance_report (
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
select
    row_number() over (order by a.src_advance_report_id) as employee_advance_report_id,
    hr.employee_id,
    a.employee_src_id,
    a.src_advance_report_id,
    a.report_period,
    a.total_amount_rub,
    a.approved_amount_rub,
    a.rejected_amount_rub,
    a.overdue_days,
    a.report_status,
    a.approver_employee_src_id,
    a.load_dttm,
    a.batch_id
from raw.finance_advance_report_raw a
left join dds.dim_hr_employee hr
    on a.employee_src_id = hr.employee_src_id
   and hr.is_current_flag = true
where hr.employee_id is not null;

insert into dds.fct_employee_finance_signal (
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
select
    row_number() over (order by signal_source_type, source_event_id) as employee_finance_signal_id,
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
from (
    select
        hr.employee_id,
        'card_transaction' as signal_source_type,
        c.src_card_txn_id as source_event_id,
        'SUSPICIOUS_CARD_TXN' as signal_code,
        'Подозрительная карточная операция' as signal_name,
        'CARD' as signal_group,
        c.amount_rub::numeric(18,4) as signal_value_num,
        c.merchant_name as signal_value_text,
        c.transaction_dttm as signal_dttm,
        'ACTIVE' as signal_status,
        c.load_dttm,
        c.batch_id
    from raw.finance_corporate_card_txn_raw c
    left join dds.dim_hr_employee hr
        on c.employee_src_id = hr.employee_src_id
       and hr.is_current_flag = true
    where hr.employee_id is not null
      and c.suspicious_flag = 'Y'

    union all

    select
        hr.employee_id,
        'advance_report',
        a.src_advance_report_id,
        case
            when coalesce(a.overdue_days, 0) > 0 then 'OVERDUE_ADVANCE_REPORT'
            else 'REJECTED_ADVANCE_AMOUNT'
        end as signal_code,
        case
            when coalesce(a.overdue_days, 0) > 0 then 'Просроченный авансовый отчёт'
            else 'Отклонённая сумма в авансовом отчёте'
        end as signal_name,
        'ADVANCE' as signal_group,
        case
            when coalesce(a.overdue_days, 0) > 0 then a.overdue_days::numeric(18,4)
            else coalesce(a.rejected_amount_rub, 0)::numeric(18,4)
        end as signal_value_num,
        a.report_status as signal_value_text,
        a.report_period::timestamp as signal_dttm,
        a.report_status as signal_status,
        a.load_dttm,
        a.batch_id
    from raw.finance_advance_report_raw a
    left join dds.dim_hr_employee hr
        on a.employee_src_id = hr.employee_src_id
       and hr.is_current_flag = true
    where hr.employee_id is not null
      and (coalesce(a.overdue_days, 0) > 0 or coalesce(a.rejected_amount_rub, 0) > 0)

    union all

    select
        hr.employee_id,
        'payroll_adjustment',
        p.src_payroll_adj_id,
        'MANUAL_PAYROLL_ADJ' as signal_code,
        'Ручная корректировка начисления' as signal_name,
        'PAYROLL' as signal_group,
        p.amount_rub::numeric(18,4) as signal_value_num,
        p.adjustment_type_name as signal_value_text,
        p.payroll_month::timestamp as signal_dttm,
        case when p.approved_flag = 'Y' then 'APPROVED' else 'PENDING' end as signal_status,
        p.load_dttm,
        p.batch_id
    from raw.finance_payroll_adjustment_raw p
    left join dds.dim_hr_employee hr
        on p.employee_src_id = hr.employee_src_id
       and hr.is_current_flag = true
    where hr.employee_id is not null
      and p.manual_flag = 'Y'

    union all

    select
        hr.employee_id,
        'vendor_payment',
        v.src_vendor_payment_id,
        'URGENT_VENDOR_PAYMENT' as signal_code,
        'Срочный платёж поставщику' as signal_name,
        'AP' as signal_group,
        v.payment_amount_rub::numeric(18,4) as signal_value_num,
        v.vendor_name as signal_value_text,
        v.payment_date::timestamp as signal_dttm,
        'ACTIVE' as signal_status,
        v.load_dttm,
        v.batch_id
    from raw.finance_vendor_payment_raw v
    left join dds.dim_hr_employee hr
        on v.employee_src_id = hr.employee_src_id
       and hr.is_current_flag = true
    where hr.employee_id is not null
      and v.urgent_flag = 'Y'
) s;

insert into dds.fct_employee_finance_snapshot (
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
select
    row_number() over (order by e.employee_id) as employee_finance_snapshot_id,
    e.employee_id,
    date '2024-12-31' as report_date,
    coalesce(expense_agg.expense_cnt, 0) as expense_cnt,
    coalesce(expense_agg.expense_amount_total_rub, 0) as expense_amount_total_rub,
    coalesce(card_agg.suspicious_card_txn_cnt, 0) as suspicious_card_txn_cnt,
    coalesce(card_agg.suspicious_card_amount_rub, 0) as suspicious_card_amount_rub,
    coalesce(advance_agg.overdue_advance_report_cnt, 0) as overdue_advance_report_cnt,
    coalesce(advance_agg.rejected_advance_amount_rub, 0) as rejected_advance_amount_rub,
    coalesce(signal_agg.manual_payroll_adj_cnt, 0) as manual_payroll_adj_cnt,
    coalesce(signal_agg.urgent_vendor_payment_cnt, 0) as urgent_vendor_payment_cnt,
    (
        coalesce(card_agg.suspicious_card_txn_cnt, 0) * 2.2 +
        coalesce(advance_agg.overdue_advance_report_cnt, 0) * 1.8 +
        coalesce(signal_agg.manual_payroll_adj_cnt, 0) * 1.3 +
        coalesce(signal_agg.urgent_vendor_payment_cnt, 0) * 1.5 +
        coalesce(advance_agg.rejected_advance_amount_rub, 0) / 50000.0 +
        coalesce(card_agg.suspicious_card_amount_rub, 0) / 70000.0
    )::numeric(18,4) as finance_risk_score,
    case
        when (
            coalesce(card_agg.suspicious_card_txn_cnt, 0) * 2.2 +
            coalesce(advance_agg.overdue_advance_report_cnt, 0) * 1.8 +
            coalesce(signal_agg.manual_payroll_adj_cnt, 0) * 1.3 +
            coalesce(signal_agg.urgent_vendor_payment_cnt, 0) * 1.5 +
            coalesce(advance_agg.rejected_advance_amount_rub, 0) / 50000.0 +
            coalesce(card_agg.suspicious_card_amount_rub, 0) / 70000.0
        ) >= 10 then 'HIGH'
        when (
            coalesce(card_agg.suspicious_card_txn_cnt, 0) * 2.2 +
            coalesce(advance_agg.overdue_advance_report_cnt, 0) * 1.8 +
            coalesce(signal_agg.manual_payroll_adj_cnt, 0) * 1.3 +
            coalesce(signal_agg.urgent_vendor_payment_cnt, 0) * 1.5 +
            coalesce(advance_agg.rejected_advance_amount_rub, 0) / 50000.0 +
            coalesce(card_agg.suspicious_card_amount_rub, 0) / 70000.0
        ) >= 4 then 'MEDIUM'
        else 'LOW'
    end as finance_risk_level_code,
    timestamp '2024-12-31 23:20:00' as calculation_dttm,
    timestamp '2024-12-31 23:20:00' as load_dttm,
    'finance_dds_build_001' as batch_id
from dds.dim_hr_employee e
left join (
    select
        employee_id,
        count(*) as expense_cnt,
        sum(amount_rub)::numeric(18,2) as expense_amount_total_rub
    from dds.fct_employee_expense
    group by employee_id
) expense_agg
    on e.employee_id = expense_agg.employee_id
left join (
    select
        employee_id,
        sum(case when coalesce(suspicious_flag, false) = true then 1 else 0 end) as suspicious_card_txn_cnt,
        sum(case when coalesce(suspicious_flag, false) = true then amount_rub else 0 end)::numeric(18,2) as suspicious_card_amount_rub
    from dds.fct_employee_card_transaction
    group by employee_id
) card_agg
    on e.employee_id = card_agg.employee_id
left join (
    select
        employee_id,
        sum(case when coalesce(overdue_days, 0) > 0 then 1 else 0 end) as overdue_advance_report_cnt,
        sum(coalesce(rejected_amount_rub, 0))::numeric(18,2) as rejected_advance_amount_rub
    from dds.fct_employee_advance_report
    group by employee_id
) advance_agg
    on e.employee_id = advance_agg.employee_id
left join (
    select
        employee_id,
        sum(case when signal_code = 'MANUAL_PAYROLL_ADJ' then 1 else 0 end) as manual_payroll_adj_cnt,
        sum(case when signal_code = 'URGENT_VENDOR_PAYMENT' then 1 else 0 end) as urgent_vendor_payment_cnt
    from dds.fct_employee_finance_signal
    group by employee_id
) signal_agg
    on e.employee_id = signal_agg.employee_id
where e.is_current_flag = true;