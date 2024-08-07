{% macro build_table_from_query(sql) %}
    {% set results = run_query(sql) %}
    {% set column_names = [] %}
    {% for column in results.columns %}
        {% set _ = column_names.append(column.name) %}
    {% endfor %}
    {% set columns = column_names | join(', ') %}
    {{ elementary.debug_log("Columns: " ~ columns) }}
    
    {% set create_table_sql %}
        SELECT *
        FROM (VALUES
        -- Assuming results is a list of dictionaries and iterating over each row to get the values for each column
        {% for row in results %}
            ({% for value in row.values() %}'{{ value }}'{% if not loop.last %}, {% endif %}{% endfor %}){% if not loop.last %}, {% endif %}
        {% endfor %}
        ) AS T ({{ columns }})
    {% endset %}

    {{ return(create_table_sql) }}
{% endmacro %}
