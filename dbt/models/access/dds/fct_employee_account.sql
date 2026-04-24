with account_base as (

    select *
    from {{ source('access_raw', 'access_system_accounts_raw') }}

),

employee_dim as (

    select *
    from {{ ref('dim_hr_employee') }}
    where is_current_flag = true

),

system_dim as (

    select * from {{ ref('dim_access_system') }}

)

select
    row_number() over (order by a.src_account_id) as employee_account_id,
    e.employee_id,
    a.employee_src_id,
    s.access_system_id as system_id,
    a.system_code,
    cast(a.src_account_id as string) as source_account_id,
    a.account_login,
    a.account_type_code,
    a.account_type_name,
    case when a.privileged_flag = 'Y' then true else false end as privileged_flag,
    case when a.admin_flag = 'Y' then true else false end as admin_flag,
    a.account_status,
    {{ to_bq_timestamp("a.created_at") }} as created_at,
    {{ to_bq_timestamp("a.disabled_at") }} as disabled_at,
    a.load_dttm,
    a.batch_id
from account_base a
left join employee_dim e
    on a.employee_src_id = e.employee_src_id
left join system_dim s
    on a.system_code = s.system_code