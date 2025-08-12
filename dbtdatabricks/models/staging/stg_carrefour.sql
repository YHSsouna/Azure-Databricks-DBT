{{
    config(
        materialized = "table",
        file_format = "delta",
        location_root = "/mnt/bronze"
    )
}}

with raw_carrefour as(
    select
        row_number() over (order by date) as id,
        date::date as date,
        name,
        quantity_stock::numeric as stock,
        replace(replace(replace(price,'€',''),' ',''),',','.')::numeric as cleaned_price,
        CASE
            WHEN nullif(replace(replace(replace(promotion, '€', ''), ' ', ''), ',', '.'), '')::numeric IS NOT NULL THEN
                1 - (
                    replace(replace(replace(promotion, '€', ''), ' ', ''), ',', '.')::numeric /
                    replace(replace(replace(price, '€', ''), ' ', ''), ',', '.')::numeric
                )
            ELSE
                replace(replace(replace(promotion,'€',''),' ',''),',','.')::numeric
        END AS cleaned_promotion,
        replace(replace(regexp_replace(price_per_quantity, '.*/\s*([a-zA-Z]+)', '\1'),'Kilogramme','Kg'),'le flacon de 250ml','ml') as unit,
        nullif(
            replace(
                replace(
                    regexp_replace(price_per_quantity, '^([0-9]+(?:\.[0-9]+)?)\s.*', '\1'),
                    '1,067.50 € / KG', ''
                ),
                'le flacon de 250ml', ''
            ),
            ''
        )::numeric as price_per_unit,
        CASE
            WHEN NULLIF(
                regexp_replace(
                    replace(
                        replace(price_per_quantity, '1,067.50 € / KG', ''),
                        'le flacon de 250ml', ''
                    ),
                    '^([0-9]+(?:[.,][0-9]+)?)\s.*',
                    '\1'
                ),
                ''
            )::numeric IS NULL
                OR NULLIF(
                    regexp_replace(
                        replace(
                            replace(price_per_quantity, '1,067.50 € / KG', ''),
                            'le flacon de 250ml', ''
                        ),
                        '^([0-9]+(?:[.,][0-9]+)?)\s.*',
                        '\1'
                    ),
                    ''
                )::numeric = 0
            THEN NULL
            ELSE replace(replace(replace(price,'€',''),' ',''),',','.')::numeric / (
                NULLIF(
                    regexp_replace(
                        replace(
                            replace(price_per_quantity, '1,067.50 € / KG', ''),
                            'le flacon de 250ml', ''
                        ),
                        '^([0-9]+(?:[.,][0-9]+)?)\s.*',
                        '\1'
                    ),
                    ''
                )::numeric
            )
        END AS quantity,
        store,
        image_url
    from {{ source('dbo', 'carrefour') }}
),

raw_category as (
    select product_name, category
    from (
        select
            product_name,
            category,
            row_number() over (partition by product_name order by category) as rn
        from {{ source('dbo', 'carrefour_cat') }}
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
        from {{ source('dbo', 'carrefour_section') }}
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
            CASE
                WHEN l.unit = 'kg' THEN l.quantity * 1000
                WHEN l.unit = 'Kg' THEN l.quantity * 1000
                WHEN l.unit = 'KG' THEN l.quantity * 1000
                WHEN l.unit = 'ml' THEN l.quantity / 1000
                ELSE l.quantity
            END AS quantity,
            replace(replace(replace(replace(replace(l.unit,'kg','g'),'KG','g'),'Kg','g'),'ml','L'),'U','UNITE') as unit,
            CASE
                WHEN l.quantity::numeric IS NULL THEN NULL
                WHEN l.unit IN ('kg', 'Kg', 'KG') THEN price_per_unit * 1000
                WHEN l.unit = 'ml' THEN price_per_unit / 1000
                ELSE price_per_unit
            END AS price_per_quantity,
            c.category,
            s.section,
            date,
            store,
            cleaned_promotion as promotion,
            image_url
        from raw_carrefour as l
        join raw_category as c
          on c.product_name = l.name
        join raw_section as s
          on s.product_name = l.name
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
    store,
    image_url
from final
where dedup_rank = 1
