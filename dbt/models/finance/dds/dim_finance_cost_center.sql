with unioned as (

    select
        cost_center_code,
        cost_center_name,
        load_dttm,
        batch_id
    from {{ source('finance_raw', 'finance_budget_limit_raw') }}

    union all

    select
        cost_center_code,
        concat('ЦФО ', cost_center_code) as cost_center_name,
        load_dttm,
        batch_id
    from {{ source('finance_raw', 'finance_employee_expense_raw') }}
    where cost_center_code is not null

)

select
    row_number() over (order by cost_center_code) as cost_center_id,
    cost_center_code,
    max(cost_center_name) as cost_center_name,
    'finance_cost_center_catalog' as source_system,
    max(load_dttm) as load_dttm,
    max(batch_id) as batch_id
from unioned
group by cost_center_code