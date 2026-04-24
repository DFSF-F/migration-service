with employee_dim as (

    select *
    from {{ ref('dim_hr_employee') }}
    where is_current_flag = true

),

absence_agg as (

    select
        employee_id,
        count(*) as absence_event_cnt,
        sum(coalesce(duration_days, 0)) as absence_days_total
    from {{ ref('fct_employee_absence') }}
    group by employee_id

),

signal_agg as (

    select
        employee_id,
        sum(
            case
                when signal_source_type = 'overtime'
                    then coalesce(signal_value_num, 0)
                else 0
            end
        ) as overtime_hours_total,

        sum(
            case
                when signal_source_type = 'dismissal_signal'
                    then 1
                else 0
            end
        ) as dismissal_signal_cnt,

        sum(
            case
                when coalesce(signal_status, '') in ('ACTIVE', 'PENDING')
                    then 1
                else 0
            end
        ) as active_signal_cnt
    from {{ ref('fct_employee_activity_signal') }}
    group by employee_id

),

final as (

    select
        row_number() over (order by e.employee_id) as employee_hr_snapshot_id,
        e.employee_id,
        date '2024-12-31' as report_date,
        e.current_position_id,
        e.department_id as current_department_id,
        coalesce(a.absence_event_cnt, 0) as absence_event_cnt,
        coalesce(a.absence_days_total, 0) as absence_days_total,
        coalesce(s.overtime_hours_total, 0) as overtime_hours_total,
        coalesce(s.dismissal_signal_cnt, 0) as dismissal_signal_cnt,
        coalesce(s.active_signal_cnt, 0) as active_signal_cnt,
        cast(
            coalesce(a.absence_days_total, 0) * 0.20 +
            coalesce(s.overtime_hours_total, 0) * 0.10 +
            coalesce(s.dismissal_signal_cnt, 0) * 2.00 +
            coalesce(s.active_signal_cnt, 0) * 1.50
            as numeric
        ) as instability_score,
        case
            when (
                coalesce(a.absence_days_total, 0) * 0.20 +
                coalesce(s.overtime_hours_total, 0) * 0.10 +
                coalesce(s.dismissal_signal_cnt, 0) * 2.00 +
                coalesce(s.active_signal_cnt, 0) * 1.50
            ) >= 12 then 'HIGH'
            when (
                coalesce(a.absence_days_total, 0) * 0.20 +
                coalesce(s.overtime_hours_total, 0) * 0.10 +
                coalesce(s.dismissal_signal_cnt, 0) * 2.00 +
                coalesce(s.active_signal_cnt, 0) * 1.50
            ) >= 5 then 'MEDIUM'
            else 'LOW'
        end as instability_level_code,
        timestamp '2024-12-31 23:00:00+00' as calculation_dttm,
        timestamp '2024-12-31 23:00:00+00' as load_dttm,
        'hr_dds_build_001' as batch_id
    from employee_dim e
    left join absence_agg a
        on e.employee_id = a.employee_id
    left join signal_agg s
        on e.employee_id = s.employee_id

)

select * from final