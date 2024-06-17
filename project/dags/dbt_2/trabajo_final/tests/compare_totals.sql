with mod_1_total as (
	SELECT SUM(TOTAL) AS TOTAL
	FROM {{ ref('country_count') }}
)
,
mod_2_total as (
	SELECT sum(total) as total
	FROM {{ ref('gender_count') }}
)


SELECT
    CASE
        WHEN mod_1_total.total = mod_2_total.total THEN 0
        ELSE 1
    END AS test_result
FROM
    mod_1_total,
    mod_2_total
HAVING test_result = 1


