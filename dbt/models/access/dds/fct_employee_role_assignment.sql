with role_assignment_base as (

    select *
    from {{ source('access_raw', 'access_role_assignments_raw') }}

),

employee_dim as (

    select *
    from {{ ref('dim_hr_employee') }}
    where is_current_flag = true

),

account_fact as (

    select * from {{ ref('fct_employee_account') }}

),

role_dim as (

    select * from {{ ref('dim_access_role') }}

)

select
    row_number() over (order by r.src_role_assignment_id) as employee_role_assignment_id,
    e.employee_id,
    a.employee_account_id,
    role.access_role_id,
    cast(r.src_role_assignment_id as string) as source_role_assignment_id,
    {{ to_bq_timestamp("r.assigned_at") }} as assigned_at,
    {{ to_bq_timestamp("r.revoked_at") }} as revoked_at,
    r.assignment_status,
    r.load_dttm,
    r.batch_id
from role_assignment_base r
left join employee_dim e
    on r.employee_src_id = e.employee_src_id
left join account_fact a
    on cast(r.src_account_id as string) = a.source_account_id
   and r.employee_src_id = a.employee_src_id
left join role_dim role
    on r.system_code = role.system_code
   and r.role_code = role.role_code