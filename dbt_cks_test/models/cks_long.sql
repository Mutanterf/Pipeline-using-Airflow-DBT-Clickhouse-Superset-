{{
  config(
    materialized = 'table'
  )
}}

with base as (

    select *
    FROM {{ source('public', 'cks') }}

),

unpivoted as (

    {% set filtr_columns = [
        'filtr1', 'filtr2', 'filtr3', 'filtr4', 'filtr5', 'filtr6', 'filtr7', 'filtr8', 'filtr9', 'filtr10',
        'filtr11', 'filtr12', 'filtr13', 'filtr14', 'filtr15', 'filtr16', 'filtr17', 'filtr18', 'filtr19', 'filtr20',
        'filtr21', 'filtr22', 'filtr23', 'filtr24', 'filtr25', 'filtr26', 'filtr27', 'filtr28', 'filtr29', 'filtr30',
        'filtr31', 'filtr32', 'filtr33', 'filtr34', 'filtr65'
    ] %}

    {% for col in filtr_columns %}
    select
        id_sk_family_quality22,
        "Rating",
        FULL_KATO_NAME,
        KATO_2_NAME,
        KATO_2_y,
        KATO_4_NAME,
        KATO_4,
        FAMILY_CAT_NEW,
        {{ col }} as filter_value,
        '{{ col }}' as filter_code
    from base
    {% if not loop.last %}union all{% endif %}
    {% endfor %}

)

select * from unpivoted


