with src as (

    select distinct
        department_src_id,
        department_name,
        parent_department_src_id,
        block_name,
        function_name,
        region_name,
        org_level,
        manager_src_id,
        effective_from,
        effective_to,
        source_system,
        load_dttm,
        batch_id
    from {{ source('hr_raw', 'hr_department_history_raw') }}

),

base as (

    select
        row_number() over (
            order by
                department_src_id,
                effective_from,
                coalesce(effective_to, date '2999-12-31')
        ) as department_id,
        department_src_id,
        department_name,
        parent_department_src_id,
        block_name,
        function_name,
        region_name,
        org_level,
        manager_src_id as manager_employee_src_id,
        effective_from,
        effective_to,
        case
            when effective_to is null then true
            else false
        end as is_current_flag,
        source_system,
        load_dttm,
        batch_id
    from src

)

select
    d.department_id,
    d.department_src_id,
    d.department_name,
    p.department_id as parent_department_id,
    d.parent_department_src_id,
    d.block_name,
    d.function_name,
    d.region_name,
    d.org_level,
    d.manager_employee_src_id,
    d.effective_from,
    d.effective_to,
    d.is_current_flag,
    d.source_system,
    d.load_dttm,
    d.batch_id
from base d
left join base p
    on d.parent_department_src_id = p.department_src_id
   and p.is_current_flag = true