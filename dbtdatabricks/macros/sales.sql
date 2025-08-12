-- macros/sales_macros.sql

{% macro stock_diff_hors_restock(stock_diff_col, image_col, mean_neg_diff_col) %}
    case
        when {{ stock_diff_col }} > 0 then {{ mean_neg_diff_col }}
        when {{ stock_diff_col }} is null then 0
        else abs({{ stock_diff_col }})
    end
{% endmacro %}

{% macro sales(source_table, product_col='product_id', date_col='date', stock_col='stock', image_col='image_url', quantity_col='quantity', price_per_quantity_col='price_per_quantity', unit_col='unit', price_col='price') %}

with base as (
    select
        {{ product_col }} as product_id,
        {{ image_col }} as image_url,
        {{ date_col }} as date,
        {{ price_col }} as price,
        {{ stock_col }} as stock,
        {{ quantity_col }} as quantity,
        {{ price_per_quantity_col }} as price_per_quantity,
        {{ unit_col }} as unit,
        lag({{ stock_col }}) over (partition by {{ image_col }} order by {{ date_col }}) as prev_stock
    from {{ ref(source_table) }}
),

diffs as (
    select
        *,
        stock - prev_stock as raw_stock_diff
    from base
),

mean_neg as (
    select
        image_url,
        avg(abs(raw_stock_diff)) as mean_negative_stock_diff
    from diffs
    where raw_stock_diff <= 0
    group by image_url
),

final as (
    select
        product_id,
        date,
        price,
        stock,
        quantity,
        price_per_quantity,
        unit,
        {{ stock_diff_hors_restock('raw_stock_diff', 'image_url', 'mean_negative_stock_diff') }} as sales
    from diffs d
    left join mean_neg m on d.image_url = m.image_url
)

select * from final

{% endmacro %}