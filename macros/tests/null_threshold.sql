{% test null_threshold(
        model,
        column_name,
        threshold_percent
    ) %}
    -- threshold_percent: decimal representing percent of values that should NOT be null
    WITH t AS (
        SELECT
            COUNT(*) * {{ threshold_percent }} AS threshold_num
        FROM
            {{ model }}
    ),
    C AS (
        SELECT
            COUNT(*) AS cnt
        FROM
            {{ model }}
        WHERE
            {{ column_name }} IS NOT NULL
    )
SELECT
    *
FROM
    C
WHERE
    C.cnt <= (
        SELECT
            MAX(threshold_num)
        FROM
            t
    )
{% endtest %}
