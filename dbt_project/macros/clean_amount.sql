{% macro clean_amount(amount_col) %}
    try_to_decimal(replace({{ amount_col }}, '$', ''), 11, 2)
{% endmacro %}
