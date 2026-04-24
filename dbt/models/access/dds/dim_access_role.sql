with role_base as (

    select *
    from {{ source('access_raw', 'access_role_assignments_raw') }}

),

system_dim as (

    select * from {{ ref('dim_access_system') }}

)

select
    row_number() over (order by r.system_code, r.role_code) as access_role_id,
    s.access_system_id as system_id,
    r.system_code,
    r.role_code,
    max(r.role_name) as role_name,
    max(r.role_group) as role_group,
    'access_role_catalog' as source_system,
    max(r.load_dttm) as load_dttm,
    max(r.batch_id) as batch_id
from role_base r
left join system_dim s
    on r.system_code = s.system_code
group by
    s.access_system_id,
    r.system_code,
    r.role_code