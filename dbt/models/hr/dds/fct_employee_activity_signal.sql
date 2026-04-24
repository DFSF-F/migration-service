with overtime_signals as (

    select
        e.employee_id,
        'overtime' as signal_source_type,
        cast(o.src_overtime_event_id as string) as source_event_id,
        'OVERTIME_HOURS' as signal_code,
        'Сверхурочная работа' as signal_name,
        'WORKLOAD' as signal_group,
        cast(o.overtime_hours as numeric) as signal_value_num,
        o.overtime_reason_name as signal_value_text,
        timestamp(o.overtime_date) as detected_at,
        case
            when o.approved_flag = 'Y' then 'APPROVED'
            else 'PENDING'
        end as signal_status,
        o.load_dttm,
        o.batch_id
    from {{ source('hr_raw', 'hr_overtime_events_raw') }} o
    left join {{ ref('dim_hr_employee') }} e
        on o.employee_src_id = e.employee_src_id
       and e.is_current_flag = true

),

dismissal_signals as (

    select
        e.employee_id,
        'dismissal_signal' as signal_source_type,
        cast(s.src_signal_id as string) as source_event_id,
        s.signal_code,
        s.signal_name,
        s.signal_group,
        cast(s.signal_value_num as numeric) as signal_value_num,
        s.signal_value_text,
        case
            when s.detected_at is null then null
            when s.detected_at between 0 and 32503680000 then timestamp_seconds(s.detected_at)
            when s.detected_at between 0 and 32503680000000 then timestamp_millis(s.detected_at)
            else null
        end as detected_at,
        s.signal_status,
        s.load_dttm,
        s.batch_id
    from {{ source('hr_raw', 'hr_dismissal_signals_raw') }} s
    left join {{ ref('dim_hr_employee') }} e
        on s.employee_src_id = e.employee_src_id
       and e.is_current_flag = true

),

unioned as (

    select * from overtime_signals
    union all
    select * from dismissal_signals

)

select
    row_number() over (order by signal_source_type, source_event_id) as employee_activity_signal_id,
    employee_id,
    signal_source_type,
    source_event_id,
    signal_code,
    signal_name,
    signal_group,
    signal_value_num,
    signal_value_text,
    detected_at,
    signal_status,
    load_dttm,
    batch_id
from unioned