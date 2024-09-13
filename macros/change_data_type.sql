{% macro change_data_type(column_name, data_type) %}
    try_cast({{ column_name }} as {{ data_type }}) as {{ column_name }}
{% endmacro %}
