{% set biz_start = 9 %}
{% set biz_end = 18 %}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="hour",
        start_date="to_date('11/01/2019', 'mm/dd/yyyy')",
        end_date="dateadd(month, 1, current_date)"
       )
    }}
)

, biz_calendar AS (
    SELECT *
    FROM date_spine
    WHERE DAYOFWEEK(date_hour) NOT IN (6, 7)
        AND DATE_PART(hour, date_hour) >= {{ biz_start }}
        AND DATE_PART(hour, date_hour) <= {{ biz_end }}
)

SELECT * FROM biz_calendar