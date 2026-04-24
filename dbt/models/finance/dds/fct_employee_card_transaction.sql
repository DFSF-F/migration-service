with card_base as (

    select *
    from {{ source('finance_raw', 'finance_corporate_card_txn_raw') }}

),

employee_dim as (

    select *
    from {{ ref('dim_hr_employee') }}
    where is_current_flag = true

)

select
    row_number() over (order by cast(c.src_card_txn_id as string)) as employee_card_transaction_id,
    hr.employee_id,
    c.employee_src_id,
    cast(c.src_card_txn_id as string) as source_card_txn_id,
    {{ to_bq_timestamp("c.transaction_dttm") }} as transaction_dttm,
    c.merchant_name,
    c.mcc_code,
    c.transaction_category,
    cast(c.amount_rub as numeric) as amount_rub,
    c.country_name,
    c.city_name,
    case when c.card_present_flag = 'Y' then true else false end as card_present_flag,
    case when c.reversal_flag = 'Y' then true else false end as reversal_flag,
    case when c.suspicious_flag = 'Y' then true else false end as suspicious_flag,
    c.load_dttm,
    c.batch_id
from card_base c
left join employee_dim hr
    on c.employee_src_id = hr.employee_src_id
where hr.employee_id is not null