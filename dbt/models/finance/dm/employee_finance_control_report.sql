with snapshot_fact as (

    select * from {{ ref('fct_employee_finance_snapshot') }}

),

employee_dim as (

    select *
    from {{ ref('dim_hr_employee') }}
    where is_current_flag = true

),

position_dim as (

    select * from {{ ref('dim_position') }}

),

department_dim as (

    select *
    from {{ ref('dim_hr_department') }}
    where is_current_flag = true

),

sig as (

    select
        employee_id,
        max(signal_dttm) as last_signal_dttm
    from {{ ref('fct_employee_finance_signal') }}
    group by employee_id

)

select
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

    case when s.suspicious_card_txn_cnt > 0 then true else false end as has_suspicious_card_flag,
    case when s.overdue_advance_report_cnt > 0 then true else false end as has_overdue_advance_flag,
    case when s.manual_payroll_adj_cnt > 0 then true else false end as has_manual_payroll_adj_flag,
    case when s.urgent_vendor_payment_cnt > 0 then true else false end as has_urgent_vendor_payment_flag,

    sig.last_signal_dttm,
    s.calculation_dttm,
    s.load_dttm,
    s.batch_id
from snapshot_fact s
left join employee_dim e
    on s.employee_id = e.employee_id
left join position_dim p
    on e.current_position_id = p.position_id
left join department_dim d
    on e.department_id = d.department_id
left join sig
    on s.employee_id = sig.employee_id