{% macro safe_divide(numerator, denominator, default_value=0) %}
    case
        when {{ denominator }} > 0 then {{ numerator }} / {{ denominator }}
        else {{ default_value }}
    end
{% endmacro %}

{% macro is_status_in(column_name, var_name) %}
    lower({{ column_name }}) in (
        {%- for value in var(var_name) -%}
            '{{ value }}'{% if not loop.last %}, {% endif %}
        {%- endfor -%}
    )
{% endmacro %}
