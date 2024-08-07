{% macro get_failed_row_count(flattened_test) %}
  {% set test_result = elementary.get_test_result() %}
  {% if config.get("fail_calc").strip() == elementary.get_failed_row_count_calc(flattened_test) %}
    {% do elementary.debug_log("Using test failures as failed_rows value.") %}
    {% do return(test_result.failures|int) %}
  {% endif %}
  {% if elementary.did_test_pass(test_result) %}
    {% do return(none) %}
  {% endif %}
  {% set failed_row_count_query = elementary.get_failed_row_count_query(flattened_test) %}
  {% if failed_row_count_query %}
    {% set result_count = elementary.result_value(failed_row_count_query) %}
    {% do return(result_count) %}
  {% endif %}
  {% do return(none) %}
{% endmacro %}

{% macro get_failed_row_count_query(flattened_test) %}
  {% set failed_row_count_calc = elementary.get_failed_row_count_calc(flattened_test) %}
  {% if failed_row_count_calc %}
    {% set failed_row_count_query = elementary.get_failed_row_count_calc_query(failed_row_count_calc) %}
    {% do return(failed_row_count_query) %}
  {% endif %}
  {% do return(none) %}
{% endmacro %}

{% macro get_failed_row_count_calc(flattened_test) %}
  {% if "failed_row_count_calc" in flattened_test["meta"] %}
    {% do return(flattened_test["meta"]["failed_row_count_calc"]) %}
  {% endif %}
  {% set common_test_config = elementary.get_common_test_config(flattened_test) %}
  {% if common_test_config %}
    {% do return(common_test_config.get("failed_row_count_calc")) %}
  {% endif %}
  {% do return('count(*)') %}
{% endmacro %}

{% macro get_failed_row_count_calc_query(failed_row_count_calc) %}
  {% set sql_lower = sql.lower() %}
  {% if "with" in sql_lower %}
    {% set sql_parts = sql_lower.split("select") %}
    {% if sql_parts | length > 1 %}
      {# Separate the last SELECT statement #}
      {% set last_select_parts = sql_parts[-1].split("from", 1) %}
      
      {# Replace columns in the last SELECT with failed_row_count_calc #}
      {% set modified_last_select = "select " + failed_row_count_calc + " as count from " + last_select_parts[1] %}
      
      {# Recombine all parts except the last one, then add the modified SELECT #}
      {% set modified_sql = "select".join(sql_parts[:-1]) + modified_last_select %}
    {% else %}
      {% set select_parts = sql_lower.split("from", 1) %}
      {% set modified_sql = "select " + failed_row_count_calc + " as count from " + select_parts[1] %}
    {% endif %}

    {{ modified_sql }}
  {% else %}
    with results as (
        {{ sql }}
    )
    select {{ failed_row_count_calc }} as count from results
  {% endif %}
{% endmacro %}