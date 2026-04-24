with employee_dim as (

    select *
    from {{ ref('dim_hr_employee') }}
    where is_current_flag = true

),

acc as (

    select
        employee_id,
        countif(account_status = 'ACTIVE') as active_account_cnt,
        countif(account_status = 'ACTIVE' and (coalesce(privileged_flag, false) = true or coalesce(admin_flag, false) = true)) as privileged_account_cnt
    from {{ ref('fct_employee_account') }}
    group by employee_id

),

role_agg as (

    select
        employee_id,
        countif(revoked_at is null and assignment_status = 'ACTIVE') as active_role_cnt
    from {{ ref('fct_employee_role_assignment') }}
    group by employee_id

),

login_agg as (

    select
        employee_id,
        countif(login_result <> 'SUCCESS') as failed_login_cnt,
        countif(coalesce(unusual_geo_flag, false) = true) as unusual_geo_login_cnt
    from {{ ref('fct_employee_login_event') }}
    group by employee_id

),

sig as (

    select
        employee_id,
        countif(signal_source_type = 'privileged_access') as privileged_access_cnt,
        countif(signal_code = 'EXTERNAL_TRANSFER') as external_transfer_cnt,
        countif(signal_code = 'BLOCKED_CONNECTION') as blocked_network_cnt
    from {{ ref('fct_employee_access_signal') }}
    group by employee_id

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
    cast(
        coalesce(acc.privileged_account_cnt, 0) * 2.0 +
        coalesce(sig.privileged_access_cnt, 0) * 2.5 +
        coalesce(login_agg.failed_login_cnt, 0) * 0.2 +
        coalesce(login_agg.unusual_geo_login_cnt, 0) * 1.5 +
        coalesce(sig.external_transfer_cnt, 0) * 2.0 +
        coalesce(sig.blocked_network_cnt, 0) * 1.2
        as numeric
    ) as access_risk_score,
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
    timestamp '2024-12-31 23:10:00+00' as calculation_dttm,
    timestamp '2024-12-31 23:10:00+00' as load_dttm,
    'access_dds_build_001' as batch_id
from employee_dim e
left join acc
    on e.employee_id = acc.employee_id
left join role_agg
    on e.employee_id = role_agg.employee_id
left join login_agg
    on e.employee_id = login_agg.employee_id
left join sig
    on e.employee_id = sig.employee_id