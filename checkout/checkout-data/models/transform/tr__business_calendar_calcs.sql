WITH applications AS (
    
    SELECT * 
        , CASE 
            WHEN DAYOFWEEK(created_at) = 6
            THEN DATEADD(hour, '{{ var("biz_start_hour") }}', DATEADD(day, 2, created_at)::date)
            WHEN DAYOFWEEK(created_at) = 7
            THEN DATEADD(hour, '{{ var("biz_start_hour") }}', DATEADD(day, 1, created_at)::date)
            WHEN DATE_PART(hour, created_at) >= '{{ var("biz_end_hour") }}'
                AND DAYOFWEEK(created_at) = 5
            THEN DATEADD(hour, '{{ var("biz_start_hour") }}', DATEADD(day, 3, created_at)::date)
            WHEN DATE_PART(hour, created_at) >= '{{ var("biz_end_hour") }}'
            THEN DATEADD(hour, '{{ var("biz_start_hour") }}', DATEADD(day, 1, created_at)::date)
            WHEN DATE_PART(hour, created_at) < '{{ var("biz_start_hour") }}'
            THEN DATEADD(hour, '{{ var("biz_start_hour") }}', created_at::date)
            ELSE DATEADD(hour, DATE_PART(hour, created_at), created_at::date)
        END AS application_biz_start_date
        , DATEADD(hour, DATE_PART(hour, modified_at), modified_at::date) AS application_modified_at_tidy
    FROM {{ ref('stg__onboarding_applications')}}

)

, opportunities AS (

    SELECT * 
        , DATEADD(hour, DATE_PART(hour, created_at), created_at::date) AS opportunity_created_at_tidy
    FROM {{ ref('stg__dim_opportunity')}}

)

, merchants AS (

    SELECT * 
        , DATEADD(hour, DATE_PART(hour, merchant_created_at), merchant_created_at::date) AS application_complete_at_tidy
    FROM {{ ref('stg__dim_merchant')}}

)

, business_calendar AS (

    SELECT * FROM {{ ref('stg__business_calendar')}}
)

, biz_calendar_calculations AS (
    SELECT
        applications.application_id
        , COUNT(biz_cal_opportunity.date_hour) AS application_created_to_opportunity_biz_hours
        , COUNT(biz_cal_modify.date_hour) AS application_created_to_modified_biz_hours
        , COUNT(DISTINCT biz_cal_merchant.date_hour::date) AS application_created_to_complete_biz_days
        /* array too large, but would have preferred something akin to this
        , ARRAY_SIZE(ARRAYAGG(biz_cal_opportunity.date_hour)) AS application_created_to_opportunity_biz_hours
        , ARRAY_SIZE(ARRAYAGG(biz_cal_modify.date_hour)) AS application_created_to_modified_biz_hours
        , ARRAY_SIZE(ARRAYAGG(DISTINCT biz_cal_merchant.date_hour::date)) AS application_created_to_complete_biz_days
        */
    FROM applications
    LEFT JOIN opportunities USING(opportunity_id)
    LEFT JOIN merchants USING(merchant_account_id)
    LEFT JOIN business_calendar AS biz_cal_opportunity
        ON application_biz_start_date <= biz_cal_opportunity.date_hour
        AND biz_cal_opportunity.date_hour <= opportunity_created_at_tidy
    LEFT JOIN business_calendar AS biz_cal_modify
        ON application_biz_start_date <= biz_cal_opportunity.date_hour
        AND biz_cal_opportunity.date_hour <= application_modified_at_tidy
    LEFT JOIN business_calendar AS biz_cal_merchant
        ON application_biz_start_date <= biz_cal_merchant.date_hour
        AND biz_cal_merchant.date_hour <= application_complete_at_tidy        
    GROUP BY 1
)

SELECT * FROM biz_calendar_calculations
