{% macro calc_biz_diff(biz_period, p1, p2) -%}

-- could move these to project vars
{% set biz_start = 9 %}
{% set biz_end = 18 %}

WITH start_ref AS (
    SELECT
        CASE 
            WHEN DAYOFWEEK({{ p1 }}) = 6
            THEN DATEADD(hour, {{ biz_start}}, DATEADD(day, 2, {{ p1 }}))
            WHEN DAYOFWEEK({{ p1 }}) = 7
            THEN DATEADD(hour, {{ biz_start}}, DATEADD(day, 1, {{ p1 }}))
            WHEN DATE_TRUNC('hour', {{ p1 }}) >= biz_end
            AND DAYOFWEEK({{ p1 }}) = 5
            THEN DATEADD(hour, {{ biz_start}}, DATEADD(day, 3, {{ p1 }}))
            WHEN DATE_TRUNC('hour', {{ p1 }}) >= biz_end
            THEN DATEADD(hour, {{ biz_start}}, DATEADD(day, 1, {{ p1 }}))
        END AS ts
)

, date_spine AS (
    {{ dbt_utils.date_spine(
        datepart={{biz_period}},
        start_date=SELECT ts::timestamp_utc FROM start_ref,
        end_date={{p2}}
       )
    }}
)

, filtered AS (
    SELECT *
    FROM date_spine
    WHERE (
        CASE 
            WHEN {{biz_period}} = 'hour'
            THEN date_{{biz_period}} >= biz_start
            AND date_{{biz_period}} <= biz_end
            WHEN {{biz_period}} = 'day'
            THEN date_{{biz_period}} NOT IN (6, 7)
        END)
)

SELECT 
    ARRAY_SIZE(
        ARRAYAGG(date_{{biz_period}}) WITHIN GROUP (ORDER BY date_{{biz_period}})
    )
FROM filtered
{%- endmacro %}