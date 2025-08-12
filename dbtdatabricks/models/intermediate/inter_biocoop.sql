{{ config(
    materialized = "table",
    file_format = "delta",
    location_root = "/mnt/silver"
) }}

with image_url_dates as (
    select
        image_url,
        name,
        min(date) as min_date,
        max(date) as max_date,
        datediff(max(date), min(date)) as date_diff
    from {{ ref('stg_biocoop') }}
    where image_url is not null and name is not null
    group by image_url, name
),

numbers as (
    select explode(array(
        0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,
        21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,
        41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,
        61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,
        81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,
        101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,
        121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,
        141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,
        161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,180,
        181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197,198,199,200,
        201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,217,218,219,220,
        221,222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,
        241,242,243,244,245,246,247,248,249,250,251,252,253,254,255,256,257,258,259,260,
        261,262,263,264,265,266,267,268,269,270,271,272,273,274,275,276,277,278,279,280,
        281,282,283,284,285,286,287,288,289,290,291,292,293,294,295,296,297,298,299,300,
        301,302,303,304,305,306,307,308,309,310,311,312,313,314,315,316,317,318,319,320,
        321,322,323,324,325,326,327,328,329,330,331,332,333,334,335,336,337,338,339,340,
        341,342,343,344,345,346,347,348,349,350,351,352,353,354,355,356,357,358,359,360,
        361,362,363,364,365,366,367,368,369,370
    )) as num
),

image_date_matrix as (
    select
        i.image_url,
        i.name,
        date_add(i.min_date, n.num) as date_day
    from image_url_dates i
    cross join numbers n
    where n.num <= i.date_diff
),

joined as (
    select
        m.image_url,
        m.name,
        m.date_day,
        a.price,
        a.stock,
        a.quantity,
        a.unit,
        a.price_per_quantity,
        a.category,
        a.section,
        a.store
    from image_date_matrix m
    left join {{ ref('stg_biocoop') }} a
        on a.image_url = m.image_url
        and a.name = m.name
        and a.date = m.date_day
),

ordered as (
    select *
    from joined
    order by image_url, name, date_day
),

grouped_data as (
    select
        image_url,
        name,
        date_day,
        price,
        stock,
        quantity,
        unit,
        price_per_quantity,
        category,
        section,
        store,

        sum(case when price is not null then 1 else 0 end) over (
            partition by image_url, name
            order by date_day
            rows between unbounded preceding and current row
        ) as price_group,

        sum(case when stock is not null then 1 else 0 end) over (
            partition by image_url, name
            order by date_day
            rows between unbounded preceding and current row
        ) as stock_group,

        sum(case when quantity is not null then 1 else 0 end) over (
            partition by image_url, name
            order by date_day
            rows between unbounded preceding and current row
        ) as quantity_group,

        sum(case when unit is not null then 1 else 0 end) over (
            partition by image_url, name
            order by date_day
            rows between unbounded preceding and current row
        ) as unit_group,

        sum(case when price_per_quantity is not null then 1 else 0 end) over (
            partition by image_url, name
            order by date_day
            rows between unbounded preceding and current row
        ) as price_per_quantity_group,

        sum(case when category is not null then 1 else 0 end) over (
            partition by image_url, name
            order by date_day
            rows between unbounded preceding and current row
        ) as category_group,

        sum(case when section is not null then 1 else 0 end) over (
            partition by image_url, name
            order by date_day
            rows between unbounded preceding and current row
        ) as section_group,

        sum(case when store is not null then 1 else 0 end) over (
            partition by image_url, name
            order by date_day
            rows between unbounded preceding and current row
        ) as store_group

    from ordered
)

select
    row_number() over (order by image_url, name, date_day) as id,
    cast(conv(substr(md5(concat(image_url, name)), 1, 15), 16, 10) as bigint) as product_id,
    image_url,
    date_day as date,
    name,
    first_value(price) over (partition by image_url, name, price_group order by date_day rows between unbounded preceding and current row) as price,
    first_value(stock) over (partition by image_url, name, stock_group order by date_day rows between unbounded preceding and current row) as stock,
    first_value(quantity) over (partition by image_url, name, quantity_group order by date_day rows between unbounded preceding and current row) as quantity,
    first_value(unit) over (partition by image_url, name, unit_group order by date_day rows between unbounded preceding and current row) as unit,
    first_value(price_per_quantity) over (partition by image_url, name, price_per_quantity_group order by date_day rows between unbounded preceding and current row) as price_per_quantity,
    first_value(category) over (partition by image_url, name, category_group order by date_day rows between unbounded preceding and current row) as category,
    first_value(section) over (partition by image_url, name, section_group order by date_day rows between unbounded preceding and current row) as section,
    first_value(store) over (partition by image_url, name, store_group order by date_day rows between unbounded preceding and current row) as store
from grouped_data
order by image_url, name, date_day
