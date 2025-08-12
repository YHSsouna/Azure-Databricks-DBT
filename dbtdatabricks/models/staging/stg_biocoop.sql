{{
    config(
        materialized = "table",
        file_format = "delta",
        location_root = "/mnt/bronze"
    )
}}

WITH raw_biocoop AS (
    SELECT
        row_number() OVER (ORDER BY date) AS id,
        cast(date as date) AS date,
        name,
        CASE
            WHEN stock rlike '^[0-9]+(\\.[0-9]+)?$' THEN cast(stock as double)
            WHEN lower(stock) IN ('in stock', 'en stock', 'disponible') THEN -1
            WHEN stock IS NULL OR trim(stock) = '' THEN 0
            ELSE 0
        END AS stock,
        cast(replace(replace(price,'2150','2.15'),'1975','1.975') as double) AS cleaned_price,
        replace(replace(regexp_replace(price_per_quantity, '[^a-zA-Z\\s]+', '', 'g'),'€',''),' ','') AS unit,
        cast(regexp_replace(replace(price_per_quantity,',','.'), '[^0-9\\.]+', '', 'g') as double) AS price_per_quantity,
        CASE
            WHEN NULLIF(
                regexp_replace(replace(price_per_quantity, ',', '.'), '[^0-9\\.]+', '', 'g'),
                ''
            ) IS NULL
            THEN NULL
            ELSE round(
                cast(replace(replace(price,'2150','2.15'),'1975','1.975') as double)
                / cast(regexp_replace(replace(price_per_quantity, ',', '.'), '[^0-9\\.]+', '', 'g') as double),
                3
            )
        END AS quantity,
        store,
        img AS image_url
    FROM {{ source('dbo', 'biocoop') }}
),

raw_category AS (
    SELECT product_name, category
    FROM (
        SELECT
            product_name,
            category,
            row_number() OVER (PARTITION BY product_name ORDER BY category) AS rn
        FROM {{ source('dbo', 'biocoop_cat') }}
    ) t
    WHERE rn = 1
),

raw_section AS (
    SELECT product_name, section
    FROM (
        SELECT
            product_name,
            section,
            row_number() OVER (PARTITION BY product_name ORDER BY section) AS rn
        FROM {{ source('dbo', 'biocoop_section') }}
    ) t
    WHERE rn = 1
),

final_data AS (
    SELECT
        id,
        l.name,
        cleaned_price AS price,
        stock,
        CASE
            WHEN l.unit = 'kg' THEN l.quantity * 1000
            ELSE l.quantity
        END AS quantity,
        replace(l.unit,'kg','g') AS unit,
        CASE
            WHEN l.quantity IS NULL THEN NULL
            WHEN l.unit = 'kg' THEN (cleaned_price / (l.quantity * 1000))
            ELSE cleaned_price / l.quantity
        END AS price_per_quantity,
        c.category,
        s.section,
        date,
        store,
        image_url,
        ROW_NUMBER() OVER (PARTITION BY image_url, date ORDER BY id) AS dedup_rank
    FROM raw_biocoop l
    JOIN raw_category c ON c.product_name = l.name
    JOIN raw_section s ON s.product_name = l.name
)

SELECT
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
FROM final_data
WHERE dedup_rank = 1
