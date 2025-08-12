{{
    config(
        materialized = "table",
        file_format = "delta",
        location_root = "/mnt/bronze"
    )
}}

with raw_labellevie as (
    select
        row_number() over (order by date) as id,
        date_trunc('day', date)::date as date_cleaned,
        name,
        regexp_replace(quantity_stock, '[^0-9.]', '', 'g')::numeric as stock,
        nullif(replace(promotion, '%', ''), '')::numeric / 100 as promotion,
        CASE
            WHEN nullif(replace(promotion, '%', ''), '')::numeric IS NOT NULL THEN
                replace(replace(replace(price, '€', ''), 'au lieu de', ''), '+', '')::numeric
                * (1 - (nullif(replace(promotion, '%', ''), '')::numeric / 100))
            WHEN promotion = '' THEN
                replace(replace(replace(price, '€', ''), 'au lieu de', ''), '+', '')::numeric
        END as price_cleaned,
        image_url,
        store
    from {{ source('dbo', 'labellevie') }}
),

raw_category as (
    select product_name, category
    from (
        select
            product_name,
            category,
            row_number() over (partition by product_name order by category) as rn
        from {{ source('dbo', 'labellevie_cat') }}
    ) t
    where rn = 1
),

raw_section as (
    select product_name, section
    from (
        select
            product_name,
            section,
            row_number() over (partition by product_name order by section) as rn
        from {{ source('dbo', 'labellevie_section') }}
    ) t
    where rn = 1
),

raw_norm as (
    select name,
        CASE WHEN quantity::numeric = 0 THEN NULL ELSE quantity::numeric END AS quantity,
        unit
    from (
        select
            name,
            quantity,
            unit,
            row_number() over (partition by name order by quantity desc nulls last) as rn
        from {{ source('dbo', 'labellevie_norm') }}
    ) t
    where rn = 1
),

final as (
    select
        *,
        row_number() over (partition by image_url, date order by id) as dedup_rank
    from (
        select
            id,
            l.name,
            price_cleaned as price,
            stock,
            n.quantity,
            n.unit,
            CASE
                WHEN n.quantity::numeric IS NULL THEN NULL
                ELSE price_cleaned::numeric / n.quantity::numeric
            END AS price_per_quantity,
            c.category,
            s.section,
            date_cleaned as date,
            promotion,
            store,
            image_url
        from raw_labellevie as l
        join raw_category as c on c.product_name = l.name
        join raw_section as s on s.product_name = l.name
        join raw_norm as n on n.name = l.name
        where l.image_url is not null
          and l.image_url != ''
          and lower(l.image_url) != 'nan'
    ) as subquery
)

select
    id,
    name,
    price,
    stock,
    quantity,
    unit,
    price_per_quantity,
    category,
    section,
    date,
    store,
    image_url
from final
where dedup_rank = 1
