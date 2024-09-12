{% macro enhance_table_with_meta(source_table, hash_columns=None, hash_algorithm='md5', additional_meta=None) %}
    -- Get all columns from the source table
    {% set columns = adapter.get_columns_in_relation(ref(source_table)) %}
    
    -- Generate a comma-separated list of columns
    {% set selected_columns = columns | map(attribute='name') | join(', ') %}
    
    -- Create hash column logic
    {% if hash_columns is none %}
        -- Default to all columns if no specific columns are provided for the hash
        {% set hash_columns = selected_columns %}
    {% else %}
        -- Join the user-specified hash columns
        {% set hash_columns = hash_columns | join(', ') %}
    {% endif %}
    
    -- Create hash logic (uses configurable hash algorithm)
    {% set hash_column = hash_algorithm ~ '(' ~ hash_columns ~ ')' %}
    
    -- Define meta columns (this could be configured)
    {% set meta_columns = [
        'now() as loaded_at',  -- Timestamp when the data was loaded
        'row_number() over() as row_num'  -- Example row number column
    ] %}
    
    -- Add any user-specified additional meta columns
    {% if additional_meta is not none %}
        {% set meta_columns = meta_columns + additional_meta %}
    {% endif %}
    
    -- Final SQL statement
    select
        {{ selected_columns }},
        {{ hash_column }} as hash_column,
        {{ meta_columns | join(', ') }}
    from {{ ref(source_table) }}

{% endmacro %}
