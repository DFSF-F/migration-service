with department_base as (

    select distinct
        department_src_id,
        department_name,
        parent_department_src_id,
        block_name,
        function_name,
        region_name,
        org_level,
        valid_from,
        valid_to,
        source_system,
        load_dttm,
        batch_id
    from {{ source('risk_raw', 'risk_org_structure_raw') }}

),

final as (

    select
        row_number() over (order by department_src_id) as department_id,
        department_src_id,
        department_name,
        cast(null as int64) as parent_department_id,
        parent_department_src_id,
        block_name,
        function_name,
        region_name,
        cast(org_level as string) as org_level,
        {{ to_bq_date("valid_from") }} as valid_from,
        {{ to_bq_date("valid_to") }} as valid_to,
        case
            when {{ to_bq_date("valid_to") }} is null then true
            else false
        end as is_current_flag,
        source_system,
        load_dttm,
        batch_id
    from department_base

)

select * from final