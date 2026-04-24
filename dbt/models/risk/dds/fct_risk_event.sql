with src as (

    select
        cast(src_incident_id as string) as source_event_id,
        employee_src_id,
        'ib_incidents' as event_source_system,
        incident_status as event_status,
        severity_level,
        true as risk_flag,
        {{ to_bq_timestamp("detected_at") }} as event_detected_dttm,
        {{ to_bq_timestamp("closed_at") }} as event_closed_dttm,
        cast(null as string) as decision_code,
        cast(null as string) as decision_text,
        cast(null as string) as comment_text,
        incident_code as source_event_code,
        load_dttm,
        batch_id
    from {{ source('risk_raw', 'risk_ib_incidents_raw') }}

    union all

    select
        cast(src_case_id as string),
        employee_src_id,
        'security_cases',
        case_status,
        cast(null as string),
        case when risk_flag = 'Y' then true else false end,
        {{ to_bq_timestamp("registered_at") }},
        {{ to_bq_timestamp("resolved_at") }},
        resolution_code,
        resolution_name,
        cast(null as string),
        case_type_code,
        load_dttm,
        batch_id
    from {{ source('risk_raw', 'risk_security_incidents_raw') }}

    union all

    select
        cast(src_violation_id as string),
        employee_src_id,
        'compliance_cases',
        violation_status,
        materiality_level,
        true,
        {{ to_bq_timestamp("detected_at") }},
        {{ to_bq_timestamp("decision_at") }},
        cast(null as string),
        decision_text,
        cast(null as string),
        violation_code,
        load_dttm,
        batch_id
    from {{ source('risk_raw', 'risk_compliance_incidents_raw') }}

    union all

    select
        cast(src_activity_id as string),
        employee_src_id,
        'hr_risk_monitor',
        activity_status,
        cast(null as string),
        true,
        {{ to_bq_timestamp("detected_at") }},
        cast(null as timestamp),
        cast(null as string),
        cast(null as string),
        comment_text,
        activity_type_code,
        load_dttm,
        batch_id
    from {{ source('risk_raw', 'risk_nonwork_activity_raw') }}

),

employee_dim as (

    select *
    from {{ ref('dim_employee') }}
    where is_current_flag = true

),

event_type_dim as (

    select *
    from {{ ref('dim_risk_event_type') }}

)

select
    row_number() over (order by src.event_source_system, src.source_event_id) as risk_event_id,
    src.source_event_id,
    e.employee_id,
    src.employee_src_id,
    t.risk_event_type_id,
    src.event_source_system,
    src.event_status,
    src.severity_level,
    src.risk_flag,
    src.event_detected_dttm,
    src.event_closed_dttm,
    src.decision_code,
    src.decision_text,
    src.comment_text,
    src.load_dttm,
    src.batch_id
from src
left join employee_dim e
    on src.employee_src_id = e.employee_src_id
left join event_type_dim t
    on src.event_source_system = t.source_system_name
   and src.source_event_code = t.source_event_code