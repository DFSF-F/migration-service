with employee_dim as (

    select *
    from {{ ref('dim_hr_employee') }}
    where is_current_flag = true

),

expense_agg as (

    select
        employee_id,
        count(*) as expense_cnt,
        sum(cast(amount_rub as numeric)) as expense_amount_total_rub
    from {{ ref('fct_employee_expense') }}
    group by employee_id

),

card_agg as (

    select
        employee_id,
        sum(case when coalesce(suspicious_flag, false) = true then 1 else 0 end) as suspicious_card_txn_cnt,
        sum(case when coalesce(suspicious_flag, false) = true then cast(amount_rub as numeric) else 0 end) as suspicious_card_amount_rub
    from {{ ref('fct_employee_card_transaction') }}
    group by employee_id

),

advance_agg as (

    select
        employee_id,
        sum(case when coalesce(overdue_days, 0) > 0 then 1 else 0 end) as overdue_advance_report_cnt,
        sum(cast(coalesce(rejected_amount_rub, 0) as numeric)) as rejected_advance_amount_rub
    from {{ ref('fct_employee_advance_report') }}
    group by employee_id

),

signal_agg as (

    select
        employee_id,
        sum(case when signal_code = 'MANUAL_PAYROLL_ADJ' then 1 else 0 end) as manual_payroll_adj_cnt,
        sum(case when signal_code = 'URGENT_VENDOR_PAYMENT' then 1 else 0 end) as urgent_vendor_payment_cnt
    from {{ ref('fct_employee_finance_signal') }}
    group by employee_id

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
    cast(
        coalesce(card_agg.suspicious_card_txn_cnt, 0) * 2.2 +
        coalesce(advance_agg.overdue_advance_report_cnt, 0) * 1.8 +
        coalesce(signal_agg.manual_payroll_adj_cnt, 0) * 1.3 +
        coalesce(signal_agg.urgent_vendor_payment_cnt, 0) * 1.5 +
        coalesce(advance_agg.rejected_advance_amount_rub, 0) / 50000.0 +
        coalesce(card_agg.suspicious_card_amount_rub, 0) / 70000.0
        as numeric
    ) as finance_risk_score,
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
    timestamp '2024-12-31 23:20:00+00' as calculation_dttm,
    timestamp '2024-12-31 23:20:00+00' as load_dttm,
    'finance_dds_build_001' as batch_id
from employee_dim e
left join expense_agg
    on e.employee_id = expense_agg.employee_id
left join card_agg
    on e.employee_id = card_agg.employee_id
left join advance_agg
    on e.employee_id = advance_agg.employee_id
left join signal_agg
    on e.employee_id = signal_agg.employee_id