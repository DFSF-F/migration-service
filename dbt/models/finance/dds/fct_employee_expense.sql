with expense_base as (

    select *
    from {{ source('finance_raw', 'finance_employee_expense_raw') }}

),

employee_dim as (

    select *
    from {{ ref('dim_hr_employee') }}
    where is_current_flag = true

),

cost_center_dim as (

    select * from {{ ref('dim_finance_cost_center') }}

),

vendor_dim as (

    select * from {{ ref('dim_finance_vendor') }}

)

select
    row_number() over (order by cast(e.src_expense_id as string)) as employee_expense_id,
    hr.employee_id,
    e.employee_src_id,
    cc.cost_center_id,
    v.finance_vendor_id,
    cast(e.src_expense_id as string) as source_expense_id,
    e.expense_date,
    e.expense_type_code,
    e.expense_type_name,
    e.expense_category,
    cast(e.amount_rub as numeric) as amount_rub,
    e.project_code,
    e.expense_status,
    case when e.reimbursable_flag = 'Y' then true else false end as reimbursable_flag,
    e.load_dttm,
    e.batch_id
from expense_base e
left join employee_dim hr
    on e.employee_src_id = hr.employee_src_id
left join cost_center_dim cc
    on e.cost_center_code = cc.cost_center_code
left join vendor_dim v
    on e.vendor_src_id = v.vendor_src_id
where hr.employee_id is not null