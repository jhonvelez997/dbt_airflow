select 
    "YEAR", "COUNTRY", "SOLAR_CAPACITY", "TOTAL_CAPACITY", "RENEWABLES_CAPACITY"
from energy
where "COUNTRY" = '{{ var("country_code") }}'
