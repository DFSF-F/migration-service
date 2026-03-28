truncate table dds.fct_employee_risk_snapshot;
truncate table dds.fct_employee_risk_factor;
truncate table dds.fct_risk_event;
truncate table dds.dim_risk_event_type;
truncate table dds.dim_employee;
truncate table dds.dim_department;

insert into dds.dim_department (
    department_id,
    department_src_id,
    department_name,
    parent_department_id,
    parent_department_src_id,
    block_name,
    function_name,
    region_name,
    org_level,
    valid_from,
    valid_to,
    is_current_flag,
    source_system,
    load_dttm,
    batch_id
)
select
    row_number() over (order by department_src_id) as department_id,
    department_src_id,
    department_name,
    null::bigint as parent_department_id,
    parent_department_src_id,
    block_name,
    function_name,
    region_name,
    org_level,
    valid_from,
    valid_to,
    case when valid_to is null then true else false end as is_current_flag,
    source_system,
    load_dttm,
    batch_id
from (
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
    from raw.risk_org_structure_raw
) s;

update dds.dim_department d
set parent_department_id = p.department_id
from dds.dim_department p
where d.parent_department_src_id = p.department_src_id;

insert into dds.dim_employee (
    employee_id,
    employee_src_id,
    employee_number,
    full_name,
    tab_num,
    position_name,
    department_id,
    department_src_id,
    manager_employee_src_id,
    employment_status,
    hire_date,
    dismissal_date,
    valid_from,
    valid_to,
    is_current_flag,
    source_system,
    load_dttm,
    batch_id
)
select
    row_number() over (order by e.employee_src_id) as employee_id,
    e.employee_src_id,
    e.employee_number,
    e.full_name,
    e.tab_num,
    e.position_name,
    d.department_id,
    e.department_src_id,
    e.manager_src_id,
    e.employment_status,
    e.hire_date,
    e.dismissal_date,
    e.snapshot_date as valid_from,
    null::date as valid_to,
    true as is_current_flag,
    e.source_system,
    e.load_dttm,
    e.batch_id
from (
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
    from raw.risk_employee_registry_raw
) e
left join dds.dim_department d
    on e.department_src_id = d.department_src_id
   and d.is_current_flag = true;

insert into dds.dim_risk_event_type (
    risk_event_type_id,
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
    max(load_dttm) as load_dttm,
    max(batch_id) as batch_id
from (
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
    from raw.risk_ib_incidents_raw

    union all

    select
        'security_cases',
        case_type_code,
        case_type_name,
        'security',
        'incident',
        null,
        true,
        false,
        load_dttm,
        batch_id
    from raw.risk_security_incidents_raw

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
    from raw.risk_compliance_incidents_raw

    union all

    select
        'hr_risk_monitor',
        activity_type_code,
        activity_type_name,
        'nonwork',
        activity_group,
        null,
        false,
        true,
        load_dttm,
        batch_id
    from raw.risk_nonwork_activity_raw
) t
group by
    source_system_name,
    source_event_code,
    source_event_name,
    risk_domain,
    event_group_name,
    default_severity_level,
    is_violation_flag,
    is_behavior_signal_flag;

insert into dds.fct_risk_event (
    risk_event_id,
    source_event_id,
    employee_id,
    employee_src_id,
    risk_event_type_id,
    event_source_system,
    event_status,
    severity_level,
    risk_flag,
    event_detected_dttm,
    event_closed_dttm,
    decision_code,
    decision_text,
    comment_text,
    load_dttm,
    batch_id
)
select
    row_number() over (order by event_source_system, source_event_id) as risk_event_id,
    source_event_id,
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
from (
    select
        src_incident_id as source_event_id,
        employee_src_id,
        'ib_incidents' as event_source_system,
        incident_status as event_status,
        severity_level,
        true as risk_flag,
        detected_at as event_detected_dttm,
        closed_at as event_closed_dttm,
        null::varchar(64) as decision_code,
        null::varchar(1000) as decision_text,
        null::varchar(1000) as comment_text,
        incident_code as source_event_code,
        load_dttm,
        batch_id
    from raw.risk_ib_incidents_raw

    union all

    select
        src_case_id,
        employee_src_id,
        'security_cases',
        case_status,
        null,
        case when risk_flag = 'Y' then true else false end,
        registered_at,
        resolved_at,
        resolution_code,
        resolution_name,
        null,
        case_type_code,
        load_dttm,
        batch_id
    from raw.risk_security_incidents_raw

    union all

    select
        src_violation_id,
        employee_src_id,
        'compliance_cases',
        violation_status,
        materiality_level,
        true,
        detected_at,
        decision_at,
        null,
        decision_text,
        null,
        violation_code,
        load_dttm,
        batch_id
    from raw.risk_compliance_incidents_raw

    union all

    select
        src_activity_id,
        employee_src_id,
        'hr_risk_monitor',
        activity_status,
        null,
        true,
        detected_at,
        null::timestamp,
        null,
        null,
        comment_text,
        activity_type_code,
        load_dttm,
        batch_id
    from raw.risk_nonwork_activity_raw
) src
left join dds.dim_employee e
    on src.employee_src_id = e.employee_src_id
   and e.is_current_flag = true
left join dds.dim_risk_event_type t
    on src.event_source_system = t.source_system_name
   and src.source_event_code = t.source_event_code;

insert into dds.fct_employee_risk_factor (
    employee_risk_factor_id,
    employee_id,
    report_date,
    risk_factor_code,
    risk_factor_name,
    factor_value_num,
    factor_value_text,
    risk_domain,
    calculation_dttm,
    load_dttm,
    batch_id
)
select
    row_number() over (order by employee_id, risk_factor_code) as employee_risk_factor_id,
    employee_id,
    date '2024-12-31' as report_date,
    risk_factor_code,
    risk_factor_name,
    factor_value_num,
    null::varchar(255) as factor_value_text,
    risk_domain,
    timestamp '2024-12-31 23:00:00' as calculation_dttm,
    timestamp '2024-12-31 23:00:00' as load_dttm,
    'risk_dds_build_001' as batch_id
from (
    select
        employee_id,
        'OPEN_EVENT_CNT' as risk_factor_code,
        'Количество открытых событий' as risk_factor_name,
        count(*)::numeric(18,4) as factor_value_num,
        'all' as risk_domain
    from dds.fct_risk_event
    where event_status = 'OPEN'
    group by employee_id

    union all

    select
        employee_id,
        'CRITICAL_EVENT_CNT',
        'Количество критичных событий',
        count(*)::numeric(18,4),
        'all'
    from dds.fct_risk_event
    where coalesce(severity_level, '') = 'CRITICAL'
    group by employee_id

    union all

    select
        employee_id,
        'IB_EVENT_CNT',
        'Количество ИБ событий',
        count(*)::numeric(18,4),
        'ib'
    from dds.fct_risk_event
    where event_source_system = 'ib_incidents'
    group by employee_id

    union all

    select
        employee_id,
        'SECURITY_EVENT_CNT',
        'Количество событий СБ',
        count(*)::numeric(18,4),
        'security'
    from dds.fct_risk_event
    where event_source_system = 'security_cases'
    group by employee_id

    union all

    select
        employee_id,
        'COMPLIANCE_EVENT_CNT',
        'Количество событий комплаенса',
        count(*)::numeric(18,4),
        'compliance'
    from dds.fct_risk_event
    where event_source_system = 'compliance_cases'
    group by employee_id

    union all

    select
        employee_id,
        'NONWORK_SIGNAL_CNT',
        'Количество сигналов нерабочей активности',
        count(*)::numeric(18,4),
        'nonwork'
    from dds.fct_risk_event
    where event_source_system = 'hr_risk_monitor'
    group by employee_id
) f;

insert into dds.fct_employee_risk_snapshot (
    employee_risk_snapshot_id,
    employee_id,
    report_date,
    risk_score_value,
    risk_level_code,
    open_event_cnt,
    critical_event_cnt,
    ib_event_cnt,
    security_event_cnt,
    compliance_event_cnt,
    nonwork_signal_cnt,
    calculation_dttm,
    load_dttm,
    batch_id
)
select
    row_number() over (order by e.employee_id) as employee_risk_snapshot_id,
    e.employee_id,
    date '2024-12-31' as report_date,
    (
        coalesce(sum(case when f.risk_factor_code = 'OPEN_EVENT_CNT' then f.factor_value_num end), 0) * 1.5 +
        coalesce(sum(case when f.risk_factor_code = 'CRITICAL_EVENT_CNT' then f.factor_value_num end), 0) * 3 +
        coalesce(sum(case when f.risk_factor_code = 'IB_EVENT_CNT' then f.factor_value_num end), 0) * 1 +
        coalesce(sum(case when f.risk_factor_code = 'SECURITY_EVENT_CNT' then f.factor_value_num end), 0) * 1.2 +
        coalesce(sum(case when f.risk_factor_code = 'COMPLIANCE_EVENT_CNT' then f.factor_value_num end), 0) * 1.1 +
        coalesce(sum(case when f.risk_factor_code = 'NONWORK_SIGNAL_CNT' then f.factor_value_num end), 0) * 1.4
    )::numeric(18,4) as risk_score_value,
    case
        when (
            coalesce(sum(case when f.risk_factor_code = 'OPEN_EVENT_CNT' then f.factor_value_num end), 0) * 1.5 +
            coalesce(sum(case when f.risk_factor_code = 'CRITICAL_EVENT_CNT' then f.factor_value_num end), 0) * 3 +
            coalesce(sum(case when f.risk_factor_code = 'IB_EVENT_CNT' then f.factor_value_num end), 0) * 1 +
            coalesce(sum(case when f.risk_factor_code = 'SECURITY_EVENT_CNT' then f.factor_value_num end), 0) * 1.2 +
            coalesce(sum(case when f.risk_factor_code = 'COMPLIANCE_EVENT_CNT' then f.factor_value_num end), 0) * 1.1 +
            coalesce(sum(case when f.risk_factor_code = 'NONWORK_SIGNAL_CNT' then f.factor_value_num end), 0) * 1.4
        ) >= 15 then 'HIGH'
        when (
            coalesce(sum(case when f.risk_factor_code = 'OPEN_EVENT_CNT' then f.factor_value_num end), 0) * 1.5 +
            coalesce(sum(case when f.risk_factor_code = 'CRITICAL_EVENT_CNT' then f.factor_value_num end), 0) * 3 +
            coalesce(sum(case when f.risk_factor_code = 'IB_EVENT_CNT' then f.factor_value_num end), 0) * 1 +
            coalesce(sum(case when f.risk_factor_code = 'SECURITY_EVENT_CNT' then f.factor_value_num end), 0) * 1.2 +
            coalesce(sum(case when f.risk_factor_code = 'COMPLIANCE_EVENT_CNT' then f.factor_value_num end), 0) * 1.1 +
            coalesce(sum(case when f.risk_factor_code = 'NONWORK_SIGNAL_CNT' then f.factor_value_num end), 0) * 1.4
        ) >= 7 then 'MEDIUM'
        else 'LOW'
    end as risk_level_code,
    coalesce(sum(case when f.risk_factor_code = 'OPEN_EVENT_CNT' then f.factor_value_num end), 0)::integer as open_event_cnt,
    coalesce(sum(case when f.risk_factor_code = 'CRITICAL_EVENT_CNT' then f.factor_value_num end), 0)::integer as critical_event_cnt,
    coalesce(sum(case when f.risk_factor_code = 'IB_EVENT_CNT' then f.factor_value_num end), 0)::integer as ib_event_cnt,
    coalesce(sum(case when f.risk_factor_code = 'SECURITY_EVENT_CNT' then f.factor_value_num end), 0)::integer as security_event_cnt,
    coalesce(sum(case when f.risk_factor_code = 'COMPLIANCE_EVENT_CNT' then f.factor_value_num end), 0)::integer as compliance_event_cnt,
    coalesce(sum(case when f.risk_factor_code = 'NONWORK_SIGNAL_CNT' then f.factor_value_num end), 0)::integer as nonwork_signal_cnt,
    timestamp '2024-12-31 23:30:00' as calculation_dttm,
    timestamp '2024-12-31 23:30:00' as load_dttm,
    'risk_dds_build_001' as batch_id
from dds.dim_employee e
left join dds.fct_employee_risk_factor f
    on e.employee_id = f.employee_id
   and f.report_date = date '2024-12-31'
where e.is_current_flag = true
group by e.employee_id;