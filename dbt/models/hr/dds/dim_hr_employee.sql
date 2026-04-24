with employee_base as (

    select distinct
        employee_src_id,
        employee_number,
        full_name,
        tab_num,
        birth_date,
        gender_code,
        employment_status,
        hire_date,
        dismissal_date,
        current_position_code,
        current_position_name,
        department_src_id,
        manager_src_id,
        grade_code,
        work_format,
        location_name,
        snapshot_date,
        source_system,
        load_dttm,
        batch_id
    from {{ source('hr_raw', 'hr_employee_master_raw') }}

),

department_current as (

    select *
    from {{ ref('dim_hr_department') }}
    where is_current_flag = true

),

position_dim as (

    select *
    from {{ ref('dim_position') }}

)

select
    row_number() over (order by e.employee_src_id) as employee_id,
    e.employee_src_id,
    e.employee_number,
    e.full_name,
    e.tab_num,
    e.birth_date,
    e.gender_code,
    e.employment_status,
    e.hire_date,
    e.dismissal_date,
    p.position_id as current_position_id,
    e.current_position_code,
    d.department_id,
    e.department_src_id,
    e.manager_src_id as manager_employee_src_id,
    e.grade_code,
    e.work_format,
    e.location_name,
    e.snapshot_date as valid_from,
    cast(null as date) as valid_to,
    true as is_current_flag,
    e.source_system,
    e.load_dttm,
    e.batch_id
from employee_base e
left join position_dim p
    on e.current_position_code = p.position_code
left join department_current d
    on e.department_src_id = d.department_src_id