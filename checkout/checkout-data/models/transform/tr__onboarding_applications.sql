WITH applications AS (
    
    SELECT * FROM {{ ref('stg__onboarding_applications')}}

)

, opportunities AS (

    SELECT * FROM {{ ref('stg__dim_opportunity')}}

)

, merchants AS (

    SELECT * FROM {{ ref('stg__dim_merchant')}}

)

, business_calendar_calcs AS (

    SELECT * FROM {{ ref('tr__business_calendar_calcs')}}

)

, base AS (

    SELECT 
        applications.application_id
        , applications.pk
        , applications.status
        , applications.opportunity_id
        , applications.merchant_account_id
        , applications.line_of_business
        , applications.country
        , applications.final_reviewer_id
        , applications.created_at AS application_created_at
        , applications.modified_at
        , opportunities.created_at AS opportunity_created_at
        , merchants.merchant_created_at AS application_complete_at
        , DATEDIFF(day, applications.created_at, merchants.merchant_created_at) AS days_to_onboard
        , business_calendar_calcs.application_created_to_complete_biz_days AS buinsess_days_to_onboard
        , ROW_NUMBER() OVER(
            PARTITION BY application_id 
            ORDER BY COALESCE(modified_at, created_at)
            ) AS progress
        , DATEDIFF(hour, applications.created_at, opportunities.created_at) AS application_created_to_opportunity_hours
        , business_calendar_calcs.application_created_to_opportunity_biz_hours
        , DATEDIFF(hour, application_created_at, applications.modified_at) AS application_created_to_modified_hours
        , business_calendar_calcs.application_created_to_modified_biz_hours
    FROM applications
    LEFT JOIN opportunities USING(opportunity_id)
    LEFT JOIN merchants USING(merchant_account_id)
    LEFT JOIN business_calendar_calcs USING(application_id)
)

SELECT * FROM base
