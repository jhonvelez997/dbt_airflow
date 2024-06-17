{{ config(
        materialized='incremental',
        unique_key='gender'
) }}

{% if is_incremental() %}
    with existing_data as (
        SELECT
            GENDER,
            TOTAL
        FROM
            {{ this }}
    )
{% else %}
    with existing_data as (
        SELECT
            NULL AS GENDER,
            0 AS TOTAL
        WHERE 1=0
    )
{% endif %}

, new_data as (
    SELECT
        GENDER,
        COUNT(*) AS TOTAL
    FROM
        RAW_USER_DATA
    GROUP BY
        GENDER
),
final_data AS (
    SELECT
        GENDER,
        SUM(TOTAL) AS TOTAL
    FROM (
        SELECT * FROM new_data
        UNION ALL
        SELECT * FROM existing_data
    ) grouped
    GROUP BY
        GENDER
)
SELECT * FROM final_data

