with unioned as (

    select
        'ib_incidents' as source_system_name,
        incident_code as source_event_code,
        incident_name as source_event_name,
        'ib' as risk_domain,
        'incident' as event_group_name,
        severity_level as default_severity_level,
        true as is_violation_flag,
        false as is_behavior_signal_flag,
        load_dttm,
        batch_id
    from {{ source('risk_raw', 'risk_ib_incidents_raw') }}

    union all

    select
        'security_cases',
        case_type_code,
        case_type_name,
        'security',
        'incident',
        cast(null as string),
        true,
        false,
        load_dttm,
        batch_id
    from {{ source('risk_raw', 'risk_security_incidents_raw') }}

    union all

    select
        'compliance_cases',
        violation_code,
        violation_name,
        'compliance',
        'incident',
        materiality_level,
        true,
        false,
        load_dttm,
        batch_id
    from {{ source('risk_raw', 'risk_compliance_incidents_raw') }}

    union all

    select
        'hr_risk_monitor',
        activity_type_code,
        activity_type_name,
        'nonwork',
        activity_group,
        cast(null as string),
        false,
        true,
        load_dttm,
        batch_id
    from {{ source('risk_raw', 'risk_nonwork_activity_raw') }}

),

deduplicated as (

    select
        source_system_name,
        source_event_code,
        source_event_name,
        risk_domain,
        event_group_name,
        default_severity_level,
        is_violation_flag,
        is_behavior_signal_flag,
        max(load_dttm) as load_dttm,
        max(batch_id) as batch_id
    from unioned
    group by
        source_system_name,
        source_event_code,
        source_event_name,
        risk_domain,
        event_group_name,
        default_severity_level,
        is_violation_flag,
        is_behavior_signal_flag

)

select
    row_number() over (order by source_system_name, source_event_code) as risk_event_type_id,
    source_system_name,
    source_event_code,
    source_event_name,
    risk_domain,
    event_group_name,
    default_severity_level,
    is_violation_flag,
    is_behavior_signal_flag,
    load_dttm,
    batch_id
from deduplicated