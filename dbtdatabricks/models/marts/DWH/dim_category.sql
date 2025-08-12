{{ config(
    materialized = "table",
    file_format = "delta",
    location_root = "/mnt/gold"
) }}

with raw_cat as (
    select category, section from {{ ref('inter_labellevie') }}
    union
    select category, section from {{ ref('inter_biocoop') }}
    union
    select category, section from {{ ref('inter_carrefour') }}
    union
    select category, section from {{ ref('inter_auchan') }}
),

dim_category as (
    select
        row_number() over(order by section, category) as id_category,
        section,
        category
    from raw_cat
    where category is not null
      and trim(category) <> ''
      and section is not null
      and trim(section) <> ''
)

select * from dim_category
