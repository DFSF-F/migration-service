with login_base as (

    select *
    from {{ source('access_raw', 'access_login_events_raw') }}

),

employee_dim as (

    select *
    from {{ ref('dim_hr_employee') }}
    where is_current_flag = true

),

account_fact as (

    select * from {{ ref('fct_employee_account') }}

),

system_dim as (

    select * from {{ ref('dim_access_system') }}

)

select
    row_number() over (order by l.src_login_event_id) as employee_login_event_id,
    e.employee_id,
    a.employee_account_id,
    s.access_system_id as system_id,
    cast(l.src_login_event_id as string) as source_login_event_id,
    {{ to_bq_timestamp("l.login_dttm") }} as login_dttm,
    l.login_result,
    l.auth_method,
    l.ip_address,
    l.device_id,
    l.country_name,
    l.city_name,
    case when l.unusual_geo_flag = 'Y' then true else false end as unusual_geo_flag,
    l.load_dttm,
    l.batch_id
from login_base l
left join employee_dim e
    on l.employee_src_id = e.employee_src_id
left join account_fact a
    on cast(l.src_account_id as string) = a.source_account_id
   and l.employee_src_id = a.employee_src_id
left join system_dim s
    on l.system_code = s.system_code