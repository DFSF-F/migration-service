select
    row_number() over (order by vendor_src_id) as finance_vendor_id,
    vendor_src_id,
    max(vendor_name) as vendor_name,
    'finance_vendor_catalog' as source_system,
    max(load_dttm) as load_dttm,
    max(batch_id) as batch_id
from {{ source('finance_raw', 'finance_vendor_payment_raw') }}
group by vendor_src_id