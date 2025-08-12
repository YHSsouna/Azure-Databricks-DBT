{{
    config(
        materialized = "table",
        file_format = "delta",
        location_root = "/mnt/bronze"
    )
}}

with raw_auchan as (
    select
        row_number() over (order by date) as id,
        date::date as date,
        name,
        quantity_stock::numeric AS stock,
        replace(replace(replace(replace(price,'€',''),' ',''),',','.'),'Àpartirde','0')::numeric as cleaned_price,
        replace(replace(regexp_replace(price_per_quantity, '[^a-zA-Z\s]+', '', 'g'),'€',''),'x','') as unit,
        replace(replace(regexp_replace(quantity, '[^a-zA-Z\s]+', '', 'g'),'€',''),'x','') as ss,
        regexp_replace(quantity, '[^0-9\.]+', '', 'g') AS quantity,
        marque,
        store,
        image_url
    from {{ source('dbo', 'auchan') }}
),

raw_category as (
    select product_name, category
    from (
        select
            product_name,
            category,
            row_number() over (partition by product_name order by category) as rn
        from {{ source('dbo', 'auchan_cat') }}
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
        from {{ source('dbo', 'auchan_section') }}
    ) t
    where rn = 1
),

raw_norm as (
    select name, unit, quantity
    from (
        select
            name,
            unit,
            CASE
                WHEN quantity::numeric = 0 THEN NULL
                ELSE quantity::numeric
            END AS quantity,
            row_number() over (partition by name order by quantity desc nulls last) as rn
        from {{ source('dbo', 'auchan_norm') }}
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
            cleaned_price as price,
            stock,
            n.quantity,
            n.unit,
            CASE
                WHEN n.quantity::numeric IS NULL THEN NULL
                ELSE cleaned_price::numeric / n.quantity::numeric
            END AS price_per_quantity,
            c.category,
            s.section,
            date,
            marque,
            store,
            image_url
        from raw_auchan as l
        left join raw_category as c
            on c.product_name = l.name
        left join raw_section as s
            on s.product_name = l.name
        left join raw_norm as n
            on n.name = l.name
        where l.image_url is not null
          and l.image_url != ''
          and lower(l.image_url) != 'nan'
    ) subquery
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
    marque,
    store,
    image_url
from final
where dedup_rank = 1
