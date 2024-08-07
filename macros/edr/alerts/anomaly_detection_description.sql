{% macro anomaly_detection_description() %}
    case
        when dimension is not null and column_name is null then {{ elementary.dimension_metric_description() }}
        when dimension is not null and column_name is not null then {{ elementary.column_dimension_metric_description() }}
        when metric_name = 'freshness' then {{ elementary.freshness_description() }}
        when column_name is null then {{ elementary.table_metric_description() }}
        when column_name is not null then {{ elementary.column_metric_description() }}
        else null
    end as anomaly_description
{% endmacro %}

{% macro freshness_description() %}
   {{ adapter.dispatch('freshness_description','elementary')() }}
{% endmacro %}

{% macro default__freshness_description() %}
    'Last update was at ' || anomalous_value || ', ' || {{ elementary.edr_cast_as_string('abs(round(' ~ elementary.edr_cast_as_numeric('metric_value/3600') ~ ', 2))') }} || ' hours ago. Usually the table is updated within ' || {{ elementary.edr_cast_as_string('abs(round(' ~ elementary.edr_cast_as_numeric('training_avg/3600') ~ ', 2))') }} || ' hours.'
{% endmacro %}

{% macro sqlserver__freshness_description() %}
    'Last update was at ' + CAST(anomalous_value AS VARCHAR) + ', ' + CAST(ABS(ROUND(CAST(metric_value AS FLOAT)/3600, 2)) AS VARCHAR) + ' hours ago. Usually, the table is updated within ' + CAST(ABS(ROUND(CAST(training_avg AS FLOAT)/3600, 2)) AS VARCHAR) + ' hours.'
{% endmacro %}

{% macro table_metric_description() %}
   {{ adapter.dispatch('table_metric_description','elementary')() }}
{% endmacro %}

{% macro default__table_metric_description() %}
    'The last ' || metric_name || ' value is ' || {{ elementary.edr_cast_as_string('round(' ~ elementary.edr_cast_as_numeric('metric_value') ~ ', 3)') }} ||
    '. The average for this metric is ' || {{ elementary.edr_cast_as_string('round(' ~ elementary.edr_cast_as_numeric('training_avg') ~ ', 3)') }} || '.'
{% endmacro %}

{% macro sqlserver__table_metric_description() %}
    'The last ' + metric_name + ' value is ' + CAST(ROUND(CAST(metric_value AS FLOAT), 3) AS VARCHAR) +
    '. The average for this metric is ' + CAST(ROUND(CAST(training_avg AS FLOAT), 3) AS VARCHAR) + '.'
{% endmacro %}

{% macro column_metric_description() %}
   {{ adapter.dispatch('column_metric_description','elementary')() }}
{% endmacro %}

{% macro default__column_metric_description() %}
    'In column ' || column_name || ', the last ' || metric_name || ' value is ' || {{ elementary.edr_cast_as_string('round(' ~ elementary.edr_cast_as_numeric('metric_value') ~ ', 3)') }} ||
    '. The average for this metric is ' || {{ elementary.edr_cast_as_string('round(' ~ elementary.edr_cast_as_numeric('training_avg') ~ ', 3)') }} || '.'
{% endmacro %}

{% macro sqlserver__column_metric_description() %}
    'In column ' + column_name + ', the last ' + metric_name + ' value is ' + CAST(ROUND(CAST(metric_value AS FLOAT), 3) AS VARCHAR) +
    '. The average for this metric is ' + CAST(ROUND(CAST(training_avg AS FLOAT), 3) AS VARCHAR) + '.'
{% endmacro %}

{% macro column_dimension_metric_description() %}
    {{ adapter.dispatch('column_dimension_metric_description','elementary')() }}
{% endmacro %}

{% macro default__column_dimension_metric_description() %}
    'In column ' || column_name || ', the last ' || metric_name || ' value for dimension ' || dimension || ' is ' || {{ elementary.edr_cast_as_string('round(' ~ elementary.edr_cast_as_numeric('metric_value') ~ ', 3)') }} ||
    '. The average for this metric is ' || {{ elementary.edr_cast_as_string('round(' ~ elementary.edr_cast_as_numeric('training_avg') ~ ', 3)') }} || '.'
{% endmacro %}

{% macro sqlserver__column_dimension_metric_description() %}
    'In column ' + column_name + ', the last ' + metric_name + ' value for dimension ' + dimension + ' is ' + {{ elementary.edr_cast_as_string('round(' ~ elementary.edr_cast_as_numeric('metric_value') ~ ', 3)') }} +
    '. The average for this metric is ' + {{ elementary.edr_cast_as_string('round(' ~ elementary.edr_cast_as_numeric('training_avg') ~ ', 3)') }} + '.'
{% endmacro %}

{% macro dimension_metric_description() %}
   {{ adapter.dispatch('dimension_metric_description','elementary')() }}
{% endmacro %}

{% macro default__dimension_metric_description() %}
    'The last ' || metric_name || ' value for dimension ' || dimension || ' - ' ||
    case when dimension_value is null then 'NULL' else dimension_value end || ' is ' || {{ elementary.edr_cast_as_string('round(' ~ elementary.edr_cast_as_numeric('metric_value') ~ ', 3)') }} ||
    '. The average for this metric is ' || {{ elementary.edr_cast_as_string('round(' ~ elementary.edr_cast_as_numeric('training_avg') ~ ', 3)') }} || '.'
{% endmacro %}

{% macro sqlserver__dimension_metric_description() %}
    'The last ' + metric_name + ' value for dimension ' + CAST(dimension AS VARCHAR) + ' - ' +
    CASE WHEN dimension_value IS NULL THEN 'NULL' ELSE CAST(dimension_value AS VARCHAR) END + ' is ' + CAST(ROUND(CAST(metric_value AS FLOAT), 3) AS VARCHAR) +
    '. The average for this metric is ' + CAST(ROUND(CAST(training_avg AS FLOAT), 3) AS VARCHAR) + '.'
{% endmacro %}