/* macro still WIP
i've left this in to show that i would like to create a more elegant solution but ran out of time

macro call be called as:

{{ calc_business_diff(day, application_created_at, application_complete_at) }} AS biz_days_to_onboard
*/

{% macro calc_business_diff(biz_period, p1, p2) %}

{% set biz_start = 9 %}
{% set biz_end = 18 %}

{% call statement('input_date', fetch_result=True) %}
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
                WHEN DATE_PART(hour, {{ p1 }}) < {{ biz_start }}
                THEN DATEADD(hour, {{ biz_start }}, {{ p1 }}::date)                
                ELSE DATEADD(hour, DATE_PART(hour, {{ p1 }}), {{ p1 }}::date)
            END AS ts
    )

    SELECT ts::timestamp FROM start_ref
{% endcall %}

{% set input_startdate = load_result('input_date') %}

, date_spine AS (
    {{ dbt_utils.date_spine(
        datepart=biz_period,
        start_date=input_startdate[0][0],
        end_date=DATEADD(hour, DATE_PART(hour, {{ p2 }}), {{ p2 }}::date)
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
                    WHEN DATE_PART(hour, date_{{biz_period}}) >= biz_start
                    AND DATE_PART(hour, date_{{biz_period}}) <= biz_end
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