with employee_base as (

    select distinct
        employee_src_id,
        employee_number,
        full_name,
        tab_num,
        position_name,
        department_src_id,
        manager_src_id,
        employment_status,
        hire_date,
        dismissal_date,
        snapshot_date,
        source_system,
        load_dttm,
        batch_id
    from {{ source('risk_raw', 'risk_employee_registry_raw') }}

),

department_dim as (

    select *
    from {{ ref('dim_department') }}
    where is_current_flag = true

),

final as (

    select
        row_number() over (order by e.employee_src_id) as employee_id,
        e.employee_src_id,
        e.employee_number,
        e.full_name,
        e.tab_num,
        e.position_name,
        d.department_id,
        e.department_src_id,
        e.manager_src_id as manager_employee_src_id,
        e.employment_status,
        {{ to_bq_date("e.hire_date") }} as hire_date,
        {{ to_bq_date("e.dismissal_date") }} as dismissal_date,
        {{ to_bq_date("e.snapshot_date") }} as valid_from,
        cast(null as date) as valid_to,
        true as is_current_flag,
        e.source_system,
        e.load_dttm,
        e.batch_id
    from employee_base e
    left join department_dim d
        on e.department_src_id = d.department_src_id

)

select * from final