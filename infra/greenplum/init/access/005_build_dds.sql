truncate table dds.fct_employee_access_snapshot;
truncate table dds.fct_employee_access_signal;
truncate table dds.fct_employee_login_event;
truncate table dds.fct_employee_role_assignment;
truncate table dds.fct_employee_account;
truncate table dds.dim_access_role;
truncate table dds.dim_access_system;

insert into dds.dim_access_system (
    access_system_id,
    system_code,
    system_name,
    source_system,
    load_dttm,
    batch_id
)
select
    row_number() over (order by system_code) as access_system_id,
    system_code,
    max(system_name) as system_name,
    'access_system_catalog' as source_system,
    max(load_dttm) as load_dttm,
    max(batch_id) as batch_id
from raw.access_system_accounts_raw
group by system_code;

insert into dds.dim_access_role (
    access_role_id,
    system_id,
    system_code,
    role_code,
    role_name,
    role_group,
    source_system,
    load_dttm,
    batch_id
)
select
    row_number() over (order by r.system_code, r.role_code) as access_role_id,
    s.access_system_id,
    r.system_code,
    r.role_code,
    max(r.role_name) as role_name,
    max(r.role_group) as role_group,
    'access_role_catalog' as source_system,
    max(r.load_dttm) as load_dttm,
    max(r.batch_id) as batch_id
from raw.access_role_assignments_raw r
left join dds.dim_access_system s
    on r.system_code = s.system_code
group by
    s.access_system_id,
    r.system_code,
    r.role_code;

insert into dds.fct_employee_account (
    employee_account_id,
    employee_id,
    employee_src_id,
    system_id,
    system_code,
    source_account_id,
    account_login,
    account_type_code,
    account_type_name,
    privileged_flag,
    admin_flag,
    account_status,
    created_at,
    disabled_at,
    load_dttm,
    batch_id
)
select
    row_number() over (order by a.src_account_id) as employee_account_id,
    e.employee_id,
    a.employee_src_id,
    s.access_system_id,
    a.system_code,
    a.src_account_id,
    a.account_login,
    a.account_type_code,
    a.account_type_name,
    case when a.privileged_flag = 'Y' then true else false end,
    case when a.admin_flag = 'Y' then true else false end,
    a.account_status,
    a.created_at,
    a.disabled_at,
    a.load_dttm,
    a.batch_id
from raw.access_system_accounts_raw a
left join dds.dim_hr_employee e
    on a.employee_src_id = e.employee_src_id
   and e.is_current_flag = true
left join dds.dim_access_system s
    on a.system_code = s.system_code;

insert into dds.fct_employee_role_assignment (
    employee_role_assignment_id,
    employee_id,
    employee_account_id,
    access_role_id,
    source_role_assignment_id,
    assigned_at,
    revoked_at,
    assignment_status,
    load_dttm,
    batch_id
)
select
    row_number() over (order by r.src_role_assignment_id) as employee_role_assignment_id,
    e.employee_id,
    a.employee_account_id,
    role.access_role_id,
    r.src_role_assignment_id,
    r.assigned_at,
    r.revoked_at,
    r.assignment_status,
    r.load_dttm,
    r.batch_id
from raw.access_role_assignments_raw r
left join dds.dim_hr_employee e
    on r.employee_src_id = e.employee_src_id
   and e.is_current_flag = true
left join dds.fct_employee_account a
    on r.src_account_id = a.source_account_id
   and r.employee_src_id = a.employee_src_id
left join dds.dim_access_role role
    on r.system_code = role.system_code
   and r.role_code = role.role_code;

insert into dds.fct_employee_login_event (
    employee_login_event_id,
    employee_id,
    employee_account_id,
    system_id,
    source_login_event_id,
    login_dttm,
    login_result,
    auth_method,
    ip_address,
    device_id,
    country_name,
    city_name,
    unusual_geo_flag,
    load_dttm,
    batch_id
)
select
    row_number() over (order by l.src_login_event_id) as employee_login_event_id,
    e.employee_id,
    a.employee_account_id,
    s.access_system_id,
    l.src_login_event_id,
    l.login_dttm,
    l.login_result,
    l.auth_method,
    l.ip_address,
    l.device_id,
    l.country_name,
    l.city_name,
    case when l.unusual_geo_flag = 'Y' then true else false end,
    l.load_dttm,
    l.batch_id
from raw.access_login_events_raw l
left join dds.dim_hr_employee e
    on l.employee_src_id = e.employee_src_id
   and e.is_current_flag = true
left join dds.fct_employee_account a
    on l.src_account_id = a.source_account_id
   and l.employee_src_id = a.employee_src_id
left join dds.dim_access_system s
    on l.system_code = s.system_code;

insert into dds.fct_employee_access_signal (
    employee_access_signal_id,
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
from (
    select
        e.employee_id,
        'privileged_access' as signal_source_type,
        p.src_priv_event_id as source_event_id,
        s.access_system_id as system_id,
        p.access_type_code as signal_code,
        p.access_type_name as signal_name,
        'PRIVILEGED_ACCESS' as signal_group,
        case
            when p.end_dttm is not null then extract(epoch from (p.end_dttm - p.start_dttm)) / 3600.0
            else null
        end::numeric(18,4) as signal_value_num,
        p.request_id as signal_value_text,
        p.start_dttm as signal_dttm,
        p.access_status as signal_status,
        p.load_dttm,
        p.batch_id
    from raw.access_privileged_access_raw p
    left join dds.dim_hr_employee e
        on p.employee_src_id = e.employee_src_id
       and e.is_current_flag = true
    left join dds.dim_access_system s
        on p.system_code = s.system_code

    union all

    select
        e.employee_id,
        'login_event',
        l.src_login_event_id,
        s.access_system_id,
        case
            when l.unusual_geo_flag = 'Y' then 'UNUSUAL_GEO_LOGIN'
            else 'FAILED_LOGIN'
        end as signal_code,
        case
            when l.unusual_geo_flag = 'Y' then 'Нетипичная география входа'
            else 'Неуспешный вход'
        end as signal_name,
        'AUTH' as signal_group,
        null::numeric(18,4) as signal_value_num,
        l.ip_address as signal_value_text,
        l.login_dttm as signal_dttm,
        l.login_result as signal_status,
        l.load_dttm,
        l.batch_id
    from raw.access_login_events_raw l
    left join dds.dim_hr_employee e
        on l.employee_src_id = e.employee_src_id
       and e.is_current_flag = true
    left join dds.dim_access_system s
        on l.system_code = s.system_code
    where l.login_result <> 'SUCCESS'
       or l.unusual_geo_flag = 'Y'

    union all

    select
        e.employee_id,
        'file_operation',
        f.src_file_event_id,
        s.access_system_id,
        case
            when f.external_transfer_flag = 'Y' then 'EXTERNAL_TRANSFER'
            else 'SENSITIVE_FILE_OP'
        end as signal_code,
        case
            when f.external_transfer_flag = 'Y' then 'Внешняя передача файлов'
            else 'Операция с чувствительными файлами'
        end as signal_name,
        'DATA_ACCESS' as signal_group,
        null::numeric(18,4) as signal_value_num,
        f.object_path as signal_value_text,
        f.operation_dttm as signal_dttm,
        'ACTIVE' as signal_status,
        f.load_dttm,
        f.batch_id
    from raw.access_file_operations_raw f
    left join dds.dim_hr_employee e
        on f.employee_src_id = e.employee_src_id
       and e.is_current_flag = true
    left join dds.dim_access_system s
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
        n.src_network_event_id,
        s.access_system_id,
        case
            when n.blocked_flag = 'Y' then 'BLOCKED_CONNECTION'
            else 'DIRECT_EXTERNAL_CONNECTION'
        end as signal_code,
        case
            when n.blocked_flag = 'Y' then 'Заблокированное сетевое соединение'
            else 'Прямое внешнее соединение без VPN'
        end as signal_name,
        'NETWORK' as signal_group,
        n.traffic_mb as signal_value_num,
        n.destination_name as signal_value_text,
        n.event_dttm as signal_dttm,
        case when n.blocked_flag = 'Y' then 'BLOCKED' else 'ACTIVE' end as signal_status,
        n.load_dttm,
        n.batch_id
    from raw.access_network_activity_raw n
    left join dds.dim_hr_employee e
        on n.employee_src_id = e.employee_src_id
       and e.is_current_flag = true
    left join dds.dim_access_system s
        on n.system_code = s.system_code
    where n.blocked_flag = 'Y'
       or (n.destination_type = 'EXTERNAL' and coalesce(n.vpn_used_flag, 'N') = 'N')
) x;

insert into dds.fct_employee_access_snapshot (
    employee_access_snapshot_id,
    employee_id,
    report_date,
    active_account_cnt,
    privileged_account_cnt,
    active_role_cnt,
    privileged_access_cnt,
    failed_login_cnt,
    unusual_geo_login_cnt,
    external_transfer_cnt,
    blocked_network_cnt,
    access_risk_score,
    access_risk_level_code,
    calculation_dttm,
    load_dttm,
    batch_id
)
select
    row_number() over (order by e.employee_id) as employee_access_snapshot_id,
    e.employee_id,
    date '2024-12-31' as report_date,
    coalesce(acc.active_account_cnt, 0) as active_account_cnt,
    coalesce(acc.privileged_account_cnt, 0) as privileged_account_cnt,
    coalesce(role_agg.active_role_cnt, 0) as active_role_cnt,
    coalesce(sig.privileged_access_cnt, 0) as privileged_access_cnt,
    coalesce(login_agg.failed_login_cnt, 0) as failed_login_cnt,
    coalesce(login_agg.unusual_geo_login_cnt, 0) as unusual_geo_login_cnt,
    coalesce(sig.external_transfer_cnt, 0) as external_transfer_cnt,
    coalesce(sig.blocked_network_cnt, 0) as blocked_network_cnt,
    (
        coalesce(acc.privileged_account_cnt, 0) * 2.0 +
        coalesce(sig.privileged_access_cnt, 0) * 2.5 +
        coalesce(login_agg.failed_login_cnt, 0) * 0.2 +
        coalesce(login_agg.unusual_geo_login_cnt, 0) * 1.5 +
        coalesce(sig.external_transfer_cnt, 0) * 2.0 +
        coalesce(sig.blocked_network_cnt, 0) * 1.2
    )::numeric(18,4) as access_risk_score,
    case
        when (
            coalesce(acc.privileged_account_cnt, 0) * 2.0 +
            coalesce(sig.privileged_access_cnt, 0) * 2.5 +
            coalesce(login_agg.failed_login_cnt, 0) * 0.2 +
            coalesce(login_agg.unusual_geo_login_cnt, 0) * 1.5 +
            coalesce(sig.external_transfer_cnt, 0) * 2.0 +
            coalesce(sig.blocked_network_cnt, 0) * 1.2
        ) >= 12 then 'HIGH'
        when (
            coalesce(acc.privileged_account_cnt, 0) * 2.0 +
            coalesce(sig.privileged_access_cnt, 0) * 2.5 +
            coalesce(login_agg.failed_login_cnt, 0) * 0.2 +
            coalesce(login_agg.unusual_geo_login_cnt, 0) * 1.5 +
            coalesce(sig.external_transfer_cnt, 0) * 2.0 +
            coalesce(sig.blocked_network_cnt, 0) * 1.2
        ) >= 5 then 'MEDIUM'
        else 'LOW'
    end as access_risk_level_code,
    timestamp '2024-12-31 23:10:00' as calculation_dttm,
    timestamp '2024-12-31 23:10:00' as load_dttm,
    'access_dds_build_001' as batch_id
from dds.dim_hr_employee e
left join (
    select
        employee_id,
        count(*) filter (where account_status = 'ACTIVE') as active_account_cnt,
        count(*) filter (where account_status = 'ACTIVE' and (coalesce(privileged_flag, false) = true or coalesce(admin_flag, false) = true)) as privileged_account_cnt
    from dds.fct_employee_account
    group by employee_id
) acc
    on e.employee_id = acc.employee_id
left join (
    select
        employee_id,
        count(*) filter (where coalesce(revoked_at, null) is null and assignment_status = 'ACTIVE') as active_role_cnt
    from dds.fct_employee_role_assignment
    group by employee_id
) role_agg
    on e.employee_id = role_agg.employee_id
left join (
    select
        employee_id,
        count(*) filter (where login_result <> 'SUCCESS') as failed_login_cnt,
        count(*) filter (where coalesce(unusual_geo_flag, false) = true) as unusual_geo_login_cnt
    from dds.fct_employee_login_event
    group by employee_id
) login_agg
    on e.employee_id = login_agg.employee_id
left join (
    select
        employee_id,
        count(*) filter (where signal_source_type = 'privileged_access') as privileged_access_cnt,
        count(*) filter (where signal_code = 'EXTERNAL_TRANSFER') as external_transfer_cnt,
        count(*) filter (where signal_code = 'BLOCKED_CONNECTION') as blocked_network_cnt
    from dds.fct_employee_access_signal
    group by employee_id
) sig
    on e.employee_id = sig.employee_id
where e.is_current_flag = true;