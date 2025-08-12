{{ config(
    materialized = "table",
    file_format = "delta",
    location_root = "/mnt/gold"
) }}

with inter_labellevie_sales as (
    {{ sales('inter_labellevie') }}
),

inter_biocoop_sales as (
    {{ sales('inter_biocoop') }}
),

inter_carrefour_sales as (
    {{ sales('inter_carrefour') }}
),

inter_auchan_sales as (
    {{ sales('inter_auchan') }}
),

-- Combine all sales data
raw_union as (
    select * from inter_labellevie_sales
    union all
    select * from inter_biocoop_sales
    union all
    select * from inter_carrefour_sales
    union all
    select * from inter_auchan_sales
),

-- Deduplicate rows within raw_union by product_id and date, keeping highest stock
raw_fact as (
    select *
    from (
        select *,
            row_number() over (
                partition by product_id, date
                order by stock desc nulls last
            ) as rn
        from raw_union
    ) sub
    where rn = 1
),

-- Join with dimensions
final_fact as (
    select
        f.stock,
        case
            when sales between 9850 and 10000 then 1
            else sales
        end as sales,
        f.price,
        f.quantity,
        f.price_per_quantity,
        f.unit,
        p.product_id,
        d.date_id
    from raw_fact f
    left join {{ ref('dim_product') }} p
        on f.product_id = p.product_id
    left join {{ ref('dim_date') }} d
        on f.date = d.date
),

-- Final deduplication after join, in case of duplicates introduced
deduplicated as (
    select *
    from (
        select *,
            row_number() over (
                partition by product_id, date_id
                order by stock desc nulls last
            ) as rn
        from final_fact
    ) sub
    where rn = 1
)

select
    row_number() over (order by product_id, date_id) as id,
    stock,
    sales,
    price,
    quantity,
    price_per_quantity,
    unit,
    product_id,
    date_id
from deduplicated
