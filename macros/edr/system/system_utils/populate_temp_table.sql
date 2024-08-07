{% macro populate_temp_table(table_prefix, sql) %}
    {% set results = run_query(sql) %}
    {% set current_timestamp = run_started_at.strftime('%Y%m%d%H%M%S%f') %}
    {% set random_number = range(1, 100000) | random %}
    {% set temp_table_name = '#' ~ table_prefix ~ current_timestamp ~ '_' ~ random_number %}
    {% set column_names = [] %}
    {% for column in results.columns %}
        {% set _ = column_names.append(column.name) %}
    {% endfor %}
    {% set columns = column_names | join(', ') %}
    {{ elementary.debug_log("Columns: " ~ columns) }}

    {% set create_temp_table_sql %}
        IF OBJECT_ID('tempdb..{{ temp_table_name }}') IS NOT NULL DROP TABLE {{ temp_table_name }};
        
        SELECT *
        INTO {{ temp_table_name }}
        FROM (VALUES
        -- Assuming results is a list of dictionaries and iterating over each row to get the values for each column
        {% for row in results %}
            ({% for value in row.values() %}'{{ value }}'{% if not loop.last %}, {% endif %}{% endfor %}){% if not loop.last %}, {% endif %}
        {% endfor %}
        ) AS T ({{ columns }})
    {% endset %}
    {% do run_query(create_temp_table_sql) %}
    {{ return(temp_table_name) }}
{% endmacro %}


       