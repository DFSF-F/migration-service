with advance_base as (

    select *
    from {{ source('finance_raw', 'finance_advance_report_raw') }}

),

employee_dim as (

    select *
    from {{ ref('dim_hr_employee') }}
    where is_current_flag = true

)

select
    row_number() over (order by cast(a.src_advance_report_id as string)) as employee_advance_report_id,
    hr.employee_id,
    a.employee_src_id,
    cast(a.src_advance_report_id as string) as source_advance_report_id,
    a.report_period,
    cast(a.total_amount_rub as numeric) as total_amount_rub,
    cast(a.approved_amount_rub as numeric) as approved_amount_rub,
    cast(a.rejected_amount_rub as numeric) as rejected_amount_rub,
    a.overdue_days,
    a.report_status,
    a.approver_employee_src_id,
    a.load_dttm,
    a.batch_id
from advance_base a
left join employee_dim hr
    on a.employee_src_id = hr.employee_src_id
where hr.employee_id is not null