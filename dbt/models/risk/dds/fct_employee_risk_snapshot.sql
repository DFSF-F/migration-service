with employee_dim as (

    select *
    from {{ ref('dim_employee') }}
    where is_current_flag = true

),

factor_dim as (

    select *
    from {{ ref('fct_employee_risk_factor') }}
    where report_date = date '2024-12-31'

),

aggregated as (

    select
        e.employee_id,

        coalesce(sum(case when f.risk_factor_code = 'OPEN_EVENT_CNT' then f.factor_value_num end), 0) as open_event_cnt_num,
        coalesce(sum(case when f.risk_factor_code = 'CRITICAL_EVENT_CNT' then f.factor_value_num end), 0) as critical_event_cnt_num,
        coalesce(sum(case when f.risk_factor_code = 'IB_EVENT_CNT' then f.factor_value_num end), 0) as ib_event_cnt_num,
        coalesce(sum(case when f.risk_factor_code = 'SECURITY_EVENT_CNT' then f.factor_value_num end), 0) as security_event_cnt_num,
        coalesce(sum(case when f.risk_factor_code = 'COMPLIANCE_EVENT_CNT' then f.factor_value_num end), 0) as compliance_event_cnt_num,
        coalesce(sum(case when f.risk_factor_code = 'NONWORK_SIGNAL_CNT' then f.factor_value_num end), 0) as nonwork_signal_cnt_num
    from employee_dim e
    left join factor_dim f
        on e.employee_id = f.employee_id
    group by e.employee_id

),

final as (

    select
        row_number() over (order by employee_id) as employee_risk_snapshot_id,
        employee_id,
        date '2024-12-31' as report_date,

        cast(
            open_event_cnt_num * 1.5 +
            critical_event_cnt_num * 3 +
            ib_event_cnt_num * 1 +
            security_event_cnt_num * 1.2 +
            compliance_event_cnt_num * 1.1 +
            nonwork_signal_cnt_num * 1.4
            as numeric
        ) as risk_score_value,

        case
            when (
                open_event_cnt_num * 1.5 +
                critical_event_cnt_num * 3 +
                ib_event_cnt_num * 1 +
                security_event_cnt_num * 1.2 +
                compliance_event_cnt_num * 1.1 +
                nonwork_signal_cnt_num * 1.4
            ) >= 15 then 'HIGH'
            when (
                open_event_cnt_num * 1.5 +
                critical_event_cnt_num * 3 +
                ib_event_cnt_num * 1 +
                security_event_cnt_num * 1.2 +
                compliance_event_cnt_num * 1.1 +
                nonwork_signal_cnt_num * 1.4
            ) >= 7 then 'MEDIUM'
            else 'LOW'
        end as risk_level_code,

        cast(open_event_cnt_num as int64) as open_event_cnt,
        cast(critical_event_cnt_num as int64) as critical_event_cnt,
        cast(ib_event_cnt_num as int64) as ib_event_cnt,
        cast(security_event_cnt_num as int64) as security_event_cnt,
        cast(compliance_event_cnt_num as int64) as compliance_event_cnt,
        cast(nonwork_signal_cnt_num as int64) as nonwork_signal_cnt,

        timestamp '2024-12-31 23:30:00+00' as calculation_dttm,
        timestamp '2024-12-31 23:30:00+00' as load_dttm,
        'risk_dds_build_001' as batch_id
    from aggregated

)

select * from final