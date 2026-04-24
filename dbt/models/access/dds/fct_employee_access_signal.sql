with employee_dim as (

    select *
    from {{ ref('dim_hr_employee') }}
    where is_current_flag = true

),

system_dim as (

    select * from {{ ref('dim_access_system') }}

),

signals as (

    select
        e.employee_id,
        'privileged_access' as signal_source_type,
        cast(p.src_priv_event_id as string) as source_event_id,
        s.access_system_id as system_id,
        p.access_type_code as signal_code,
        p.access_type_name as signal_name,
        'PRIVILEGED_ACCESS' as signal_group,
        cast(
            case
                when p.end_dttm is not null
                then timestamp_diff({{ to_bq_timestamp("p.end_dttm") }}, {{ to_bq_timestamp("p.start_dttm") }}, second) / 3600.0
                else null
            end as numeric
        ) as signal_value_num,
        cast(p.request_id as string) as signal_value_text,
        {{ to_bq_timestamp("p.start_dttm") }} as signal_dttm,
        p.access_status as signal_status,
        p.load_dttm,
        p.batch_id
    from {{ source('access_raw', 'access_privileged_access_raw') }} p
    left join employee_dim e
        on p.employee_src_id = e.employee_src_id
    left join system_dim s
        on p.system_code = s.system_code

    union all

    select
        e.employee_id,
        'login_event',
        cast(l.src_login_event_id as string),
        s.access_system_id,
        case
            when l.unusual_geo_flag = 'Y' then 'UNUSUAL_GEO_LOGIN'
            else 'FAILED_LOGIN'
        end,
        case
            when l.unusual_geo_flag = 'Y' then 'Нетипичная география входа'
            else 'Неуспешный вход'
        end,
        'AUTH',
        cast(null as numeric),
        l.ip_address,
        {{ to_bq_timestamp("l.login_dttm") }},
        l.login_result,
        l.load_dttm,
        l.batch_id
    from {{ source('access_raw', 'access_login_events_raw') }} l
    left join employee_dim e
        on l.employee_src_id = e.employee_src_id
    left join system_dim s
        on l.system_code = s.system_code
    where l.login_result <> 'SUCCESS'
       or l.unusual_geo_flag = 'Y'

    union all

    select
        e.employee_id,
        'file_operation',
        cast(f.src_file_event_id as string),
        s.access_system_id,
        case
            when f.external_transfer_flag = 'Y' then 'EXTERNAL_TRANSFER'
            else 'SENSITIVE_FILE_OP'
        end,
        case
            when f.external_transfer_flag = 'Y' then 'Внешняя передача файлов'
            else 'Операция с чувствительными файлами'
        end,
        'DATA_ACCESS',
        cast(null as numeric),
        f.object_path,
        {{ to_bq_timestamp("f.operation_dttm") }},
        'ACTIVE',
        f.load_dttm,
        f.batch_id
    from {{ source('access_raw', 'access_file_operations_raw') }} f
    left join employee_dim e
        on f.employee_src_id = e.employee_src_id
    left join system_dim s
        on f.system_code = s.system_code
    where f.external_transfer_flag = 'Y'
       or (
            coalesce(f.file_classification, '') in ('CONFIDENTIAL', 'STRICTLY_CONFIDENTIAL')
            and (f.download_flag = 'Y' or f.upload_flag = 'Y')
       )

    union all

    select
        e.employee_id,
        'network_activity',
        cast(n.src_network_event_id as string),
        s.access_system_id,
        case
            when n.blocked_flag = 'Y' then 'BLOCKED_CONNECTION'
            else 'DIRECT_EXTERNAL_CONNECTION'
        end,
        case
            when n.blocked_flag = 'Y' then 'Заблокированное сетевое соединение'
            else 'Прямое внешнее соединение без VPN'
        end,
        'NETWORK',
        cast(n.traffic_mb as numeric),
        n.destination_name,
        {{ to_bq_timestamp("n.event_dttm") }},
        case when n.blocked_flag = 'Y' then 'BLOCKED' else 'ACTIVE' end,
        n.load_dttm,
        n.batch_id
    from {{ source('access_raw', 'access_network_activity_raw') }} n
    left join employee_dim e
        on n.employee_src_id = e.employee_src_id
    left join system_dim s
        on n.system_code = s.system_code
    where n.blocked_flag = 'Y'
       or (n.destination_type = 'EXTERNAL' and coalesce(n.vpn_used_flag, 'N') = 'N')

)

select
    row_number() over (order by signal_source_type, source_event_id) as employee_access_signal_id,
    employee_id,
    signal_source_type,
    source_event_id,
    system_id,
    signal_code,
    signal_name,
    signal_group,
    signal_value_num,
    signal_value_text,
    signal_dttm,
    signal_status,
    load_dttm,
    batch_id
from signals