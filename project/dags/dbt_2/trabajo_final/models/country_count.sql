{{ config(
	materialized='incremental',
	unique_key='country')
}}

{% if is_incremental() %}
	WITH already_existing as (
		SELECT country,total
		FROM {{ this }}
	)
{% else %}
	WITH already_existing as (
		SELECT NULL AS country, 0 as TOTAL
		WHERE 1=0
	)

{% endif %}
, new as (
	SELECT location_country as country, count(*) as total
	FROM RAW_USER_DATA
	group by country
),
final as (
	SELECT country,SUM(total) as TOTAL
	FROM (
		SELECT * FROM already_existing
		UNION ALL
		SELECT * FROM new
	)
	GROUP BY country
)

SELECT * FROM final
