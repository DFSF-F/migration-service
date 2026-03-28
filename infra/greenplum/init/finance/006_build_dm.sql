truncate table dm.employee_finance_control_report;

insert into dm.employee_finance_control_report (
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
from dds.fct_employee_finance_snapshot s
left join dds.dim_hr_employee e
    on s.employee_id = e.employee_id
   and e.is_current_flag = true
left join dds.dim_position p
    on e.current_position_id = p.position_id
left join dds.dim_hr_department d
    on e.department_id = d.department_id
   and d.is_current_flag = true
left join (
    select
        employee_id,
        max(signal_dttm) as last_signal_dttm
    from dds.fct_employee_finance_signal
    group by employee_id
) sig
    on s.employee_id = sig.employee_id;