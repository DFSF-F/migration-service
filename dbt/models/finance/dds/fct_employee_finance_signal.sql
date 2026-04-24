with employee_dim as (

    select *
    from {{ ref('dim_hr_employee') }}
    where is_current_flag = true

),

signals as (

    select
        hr.employee_id,
        'card_transaction' as signal_source_type,
        cast(c.src_card_txn_id as string) as source_event_id,
        'SUSPICIOUS_CARD_TXN' as signal_code,
        'Подозрительная карточная операция' as signal_name,
        'CARD' as signal_group,
        cast(c.amount_rub as numeric) as signal_value_num,
        c.merchant_name as signal_value_text,
        {{ to_bq_timestamp("c.transaction_dttm") }} as signal_dttm,
        'ACTIVE' as signal_status,
        c.load_dttm,
        c.batch_id
    from {{ source('finance_raw', 'finance_corporate_card_txn_raw') }} c
    left join employee_dim hr
        on c.employee_src_id = hr.employee_src_id
    where hr.employee_id is not null
      and c.suspicious_flag = 'Y'

    union all

    select
        hr.employee_id,
        'advance_report' as signal_source_type,
        cast(a.src_advance_report_id as string) as source_event_id,
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
            when coalesce(a.overdue_days, 0) > 0 then cast(a.overdue_days as numeric)
            else cast(coalesce(a.rejected_amount_rub, 0) as numeric)
        end as signal_value_num,
        a.report_status as signal_value_text,
        timestamp(a.report_period) as signal_dttm,
        a.report_status as signal_status,
        a.load_dttm,
        a.batch_id
    from {{ source('finance_raw', 'finance_advance_report_raw') }} a
    left join employee_dim hr
        on a.employee_src_id = hr.employee_src_id
    where hr.employee_id is not null
      and (coalesce(a.overdue_days, 0) > 0 or coalesce(a.rejected_amount_rub, 0) > 0)

    union all

    select
        hr.employee_id,
        'payroll_adjustment' as signal_source_type,
        cast(p.src_payroll_adj_id as string) as source_event_id,
        'MANUAL_PAYROLL_ADJ' as signal_code,
        'Ручная корректировка начисления' as signal_name,
        'PAYROLL' as signal_group,
        cast(p.amount_rub as numeric) as signal_value_num,
        p.adjustment_type_name as signal_value_text,
        timestamp(p.payroll_month) as signal_dttm,
        case when p.approved_flag = 'Y' then 'APPROVED' else 'PENDING' end as signal_status,
        p.load_dttm,
        p.batch_id
    from {{ source('finance_raw', 'finance_payroll_adjustment_raw') }} p
    left join employee_dim hr
        on p.employee_src_id = hr.employee_src_id
    where hr.employee_id is not null
      and p.manual_flag = 'Y'

    union all

    select
        hr.employee_id,
        'vendor_payment' as signal_source_type,
        cast(v.src_vendor_payment_id as string) as source_event_id,
        'URGENT_VENDOR_PAYMENT' as signal_code,
        'Срочный платёж поставщику' as signal_name,
        'AP' as signal_group,
        cast(v.payment_amount_rub as numeric) as signal_value_num,
        v.vendor_name as signal_value_text,
        timestamp(v.payment_date) as signal_dttm,
        'ACTIVE' as signal_status,
        v.load_dttm,
        v.batch_id
    from {{ source('finance_raw', 'finance_vendor_payment_raw') }} v
    left join employee_dim hr
        on v.employee_src_id = hr.employee_src_id
    where hr.employee_id is not null
      and v.urgent_flag = 'Y'

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
from signals