{% macro agate_to_dicts(agate_table, row_limit=None) %}
    {% set column_types = agate_table.column_types %}
    {% set serializable_rows = [] %}
    
    {% for agate_row in agate_table.rows[:row_limit] %}
        {% set serializable_row = {} %}
        {% for col_name, col_value in agate_row.items() %}
            {% set column_type = column_types[loop.index0] %}
            {# Check if the col_value is a whole number #}
            {% if col_value is number and col_value == col_value | int %}
                {% set serializable_col_value = col_value | int %}
            {% else %}
                {% set serializable_col_value = column_type.jsonify(col_value) %}
            {% endif %}
            
            {% set serializable_col_name = col_name %}
            {% do serializable_row.update({serializable_col_name: serializable_col_value}) %}
        {% endfor %}
        {% do serializable_rows.append(serializable_row) %}
    {% endfor %}
    
    {{ return(serializable_rows) }}
{% endmacro %}
