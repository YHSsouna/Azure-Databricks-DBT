{{ config(
    materialized = "table",
    file_format = "delta",
    location_root = "/mnt/gold"
) }}

with raw_picture as (

    select distinct image_url as picture
    from {{ ref('inter_labellevie') }}
    where image_url is not null and trim(image_url) <> ''

    union

    select distinct image_url as picture
    from {{ ref('inter_biocoop') }}
    where image_url is not null and trim(image_url) <> ''

    union

    select distinct image_url as picture
    from {{ ref('inter_carrefour') }}
    where image_url is not null and trim(image_url) <> ''

    union

    select distinct image_url as picture
    from {{ ref('inter_auchan') }}
    where image_url is not null and trim(image_url) <> ''

),

dim_picture as (

    select
        row_number() over (order by picture) as id_picture,
        picture
    from raw_picture

)

select * from dim_picture
