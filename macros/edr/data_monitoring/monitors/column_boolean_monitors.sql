{% macro count_true(column_name) -%}
    {{ adapter.dispatch('count_true','elementary')(column_name) }}
{%- endmacro %}

{% macro default__count_true(column_name) -%}
    coalesce(sum(case when cast({{ column_name }} as {{ elementary.edr_type_bool() }}) = true then 1 else 0 end), 0)
{%- endmacro %}

{% macro sqlserver__count_true(column_name) %}
    coalesce(sum(case when cast({{ column_name }} as {{ elementary.edr_type_bool() }}) = 1 then 1 else 0 end), 0)
{% endmacro %}

{% macro count_false(column_name) -%}
    {{ adapter.dispatch('count_false','elementary')(column_name) }}
{%- endmacro %}

{% macro default__count_false(column_name) -%}
    coalesce(sum(case when cast({{ column_name }} as {{ elementary.edr_type_bool() }}) = true then 0 else 1 end), 0)
{%- endmacro %}

{% macro sqlserver__count_false(column_name) %}
    coalesce(sum(case when cast({{ column_name }} as {{ elementary.edr_type_bool() }}) = 0 then 1 else 0 end), 0)
{% endmacro %}
