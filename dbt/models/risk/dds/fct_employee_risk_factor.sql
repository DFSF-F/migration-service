with factors as (

    select
        employee_id,
        'OPEN_EVENT_CNT' as risk_factor_code,
        'Количество открытых событий' as risk_factor_name,
        cast(count(*) as numeric) as factor_value_num,
        'all' as risk_domain
    from {{ ref('fct_risk_event') }}
    where event_status = 'OPEN'
    group by employee_id

    union all

    select
        employee_id,
        'CRITICAL_EVENT_CNT',
        'Количество критичных событий',
        cast(count(*) as numeric),
        'all'
    from {{ ref('fct_risk_event') }}
    where coalesce(severity_level, '') = 'CRITICAL'
    group by employee_id

    union all

    select
        employee_id,
        'IB_EVENT_CNT',
        'Количество ИБ событий',
        cast(count(*) as numeric),
        'ib'
    from {{ ref('fct_risk_event') }}
    where event_source_system = 'ib_incidents'
    group by employee_id

    union all

    select
        employee_id,
        'SECURITY_EVENT_CNT',
        'Количество событий СБ',
        cast(count(*) as numeric),
        'security'
    from {{ ref('fct_risk_event') }}
    where event_source_system = 'security_cases'
    group by employee_id

    union all

    select
        employee_id,
        'COMPLIANCE_EVENT_CNT',
        'Количество событий комплаенса',
        cast(count(*) as numeric),
        'compliance'
    from {{ ref('fct_risk_event') }}
    where event_source_system = 'compliance_cases'
    group by employee_id

    union all

    select
        employee_id,
        'NONWORK_SIGNAL_CNT',
        'Количество сигналов нерабочей активности',
        cast(count(*) as numeric),
        'nonwork'
    from {{ ref('fct_risk_event') }}
    where event_source_system = 'hr_risk_monitor'
    group by employee_id

)

select
    row_number() over (order by employee_id, risk_factor_code) as employee_risk_factor_id,
    employee_id,
    date '2024-12-31' as report_date,
    risk_factor_code,
    risk_factor_name,
    factor_value_num,
    cast(null as string) as factor_value_text,
    risk_domain,
    timestamp '2024-12-31 23:00:00+00' as calculation_dttm,
    timestamp '2024-12-31 23:00:00+00' as load_dttm,
    'risk_dds_build_001' as batch_id
from factors