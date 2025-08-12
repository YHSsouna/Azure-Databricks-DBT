{{ config(
    materialized = "table",
    file_format = "delta",
    location_root = "/mnt/gold"
) }}

with raw_store as (
    select distinct store from {{ ref('inter_labellevie') }}
    union
    select distinct store from {{ ref('inter_biocoop') }}
    union
    select distinct store from {{ ref('inter_carrefour') }}
    union
    select distinct store from {{ ref('inter_auchan') }}
),

raw_category as (
    select store, image_str,
        row_number() over (partition by store order by store) as rn
    from {{ source('dbo', 'store_img') }}
),

filtered_category as (
    select store, image_str
    from raw_category
    where rn = 1
),

dim_store as (
    select
        row_number() over (order by rs.store) as id_store,
        rs.store,
        fc.image_str
    from raw_store rs
    left join filtered_category fc
      on rs.store = fc.store
    where rs.store is not null
      and trim(rs.store) <> ''
)

select * from dim_store
