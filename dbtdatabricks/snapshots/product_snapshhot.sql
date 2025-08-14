{% snapshot product_snapshot %}

{{
    config(
        file_format="delta",
        location_root="/mnt/gold/product_snapshot",
        target_schema="snapshots",
        invalidate_hard_deletes=True,
        unique_key="product_id",
        strategy="check",
        check_cols="all"
    )
}}

select *
from {{ ref('dim_product') }}

{% endsnapshot %}
