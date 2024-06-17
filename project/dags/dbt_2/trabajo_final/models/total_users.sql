{{ config(materialized='table') }}

WITH TOTAL_USERS AS (
	SELECT SUM(TOTAL) TOTAL_PEOPLE
	FROM {{ ref('gender_count') }}
)

SELECT * FROM 
TOTAL_USERS
