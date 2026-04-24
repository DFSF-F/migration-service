{% macro to_bq_timestamp(v) %}
case
    when {{ v }} is null then null

    when regexp_contains(cast({{ v }} as string), r'^\d{10}$')
        then timestamp_seconds(safe_cast(cast({{ v }} as string) as int64))

    when regexp_contains(cast({{ v }} as string), r'^\d{13}$')
        then timestamp_millis(safe_cast(cast({{ v }} as string) as int64))

    when regexp_contains(cast({{ v }} as string), r'^\d{16}$')
        then timestamp_micros(safe_cast(cast({{ v }} as string) as int64))

    when regexp_contains(cast({{ v }} as string), r'^\d{19}$')
        then timestamp_micros(cast(safe_cast(cast({{ v }} as string) as int64) / 1000 as int64))

    else safe_cast(cast({{ v }} as string) as timestamp)
end
{% endmacro %}


{% macro to_bq_date(v) %}
case
    when {{ v }} is null then null

    when regexp_contains(cast({{ v }} as string), r'^\d{1,5}$')
        then date_from_unix_date(safe_cast(cast({{ v }} as string) as int64))

    when regexp_contains(cast({{ v }} as string), r'^\d{10}$')
        then date(timestamp_seconds(safe_cast(cast({{ v }} as string) as int64)))

    when regexp_contains(cast({{ v }} as string), r'^\d{13}$')
        then date(timestamp_millis(safe_cast(cast({{ v }} as string) as int64)))

    when regexp_contains(cast({{ v }} as string), r'^\d{16}$')
        then date(timestamp_micros(safe_cast(cast({{ v }} as string) as int64)))

    when regexp_contains(cast({{ v }} as string), r'^\d{19}$')
        then date(timestamp_micros(cast(safe_cast(cast({{ v }} as string) as int64) / 1000 as int64)))

    else safe_cast(cast({{ v }} as string) as date)
end
{% endmacro %}