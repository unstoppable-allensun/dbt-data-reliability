{% macro insert_as_select(table_relation, select_query) %}
    {# when calling this macro, you need to add depends on ref comment #}
    {# ref_model and select_query need to have the same columns #}

    {# Find the index of the last occurrence of 'SELECT' (case-insensitive) before the main query #}
    {% set index_of_last_select = select_query.lower().rfind('select') %}
    
    {# Check if 'SELECT' was found #}
    {% if index_of_last_select != -1 %}
        {# Split the query into CTEs and the main SELECT statement #}
        {% set ctes = select_query[:index_of_last_select] %}
        {% set main_select = select_query[index_of_last_select:] %}
        
        {# Construct the final query with the INSERT INTO statement #}
        {% set final_query %}
            {{ ctes }}
            insert into {{ table_relation }}
            {{ main_select }}
        {% endset %}
        
        {{ return(final_query) }}
    {% else %}
        {# Handle the case where no 'SELECT' is found in the query. #}
        {{ log("No SELECT found in the query.", "warn") }}
        {{ return("") }}
    {% endif %}

{% endmacro %}