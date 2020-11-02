{% macro calc_business_diff(biz_period, p1, p2) %}

-- could move these to project vars
{% set biz_start = 9 %}
{% set biz_end = 18 %}

{% set start_date_query %}
WITH start_ref AS (
    SELECT
        CASE 
            WHEN DAYOFWEEK({{ p1 }}) = 6
            THEN DATEADD(hour, {{ biz_start }}, DATEADD(day, 2, {{ p1 }})::date)
            WHEN DAYOFWEEK({{ p1 }}) = 7
            THEN DATEADD(hour, {{ biz_start }}, DATEADD(day, 1, {{ p1 }})::date)
            WHEN DATE_PART(hour, {{ p1 }}) >= {{ biz_end }}
            AND DAYOFWEEK({{ p1 }}) = 5
            THEN DATEADD(hour, {{ biz_start }}, DATEADD(day, 3, {{ p1 }})::date)
            WHEN DATE_PART(hour, {{ p1 }}) >= {{ biz_end }}
            THEN DATEADD(hour, {{ biz_start }}, DATEADD(day, 1, {{ p1 }})::date)
        END AS ts
)

SELECT ts::timestamp FROM start_ref
{% endset %}

{% set results = run_query(start_date_query) %}

{% if execute %}
{% set input_date = results.columns[0][0] %}
{% endif %}

, date_spine AS (
    {{ dbt_utils.date_spine(
        datepart=biz_period,
        start_date=input_date,
        end_date=p2
       )
    }}
)

, filtered AS (
    SELECT *
    FROM date_spine
    WHERE (
        CASE 
            WHEN {{biz_period}} = 'hour'
            THEN 
                CASE 
                    WHEN date_{{biz_period}} >= biz_start
                    AND date_{{biz_period}} <= biz_end
                    AND DAYOFWEEK(date_{{biz_period}}) NOT IN (6, 7)
                    THEN 1
                END
            WHEN {{biz_period}} = 'day'
            THEN 
                CASE 
                    WHEN DAYOFWEEK(date_{{biz_period}}) NOT IN (6, 7)
                    THEN 1
                END
        END) = 1
)

SELECT
    ARRAY_SIZE(
        ARRAYAGG(date_{{biz_period}}) WITHIN GROUP (ORDER BY date_{{biz_period}})
    )
FROM filtered
{% endmacro %}