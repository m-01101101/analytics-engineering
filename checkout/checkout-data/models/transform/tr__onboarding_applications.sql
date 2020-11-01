WITH applications AS (
    
    SELECT * FROM {{ ref('stg__onboarding_applications')}}

)

, opportunities AS (

    SELECT * FROM {{ ref('stg__dim_opportunity')}}

)

, merchants AS (

    SELECT * FROM {{ ref('stg__dim_merchant')}}

)

, base AS (

    SELECT 
        applications.*
        , opportunities.created_at AS opportunity_created_at
        , merchants.merchant_created_at AS application_complete_at
        , DATEDIFF(day, applications.created_at, merchants.merchant_created_at) AS days_to_onboard
        -- , calc_business_diff(day, applications.created_at, merchants.created_at) AS biz_days_to_onboard
        , ROW_NUMBER() OVER(
            PARTITION BY application_id 
            ORDER BY COALESCE(modified_at, created_at)
            ) AS progress
        , DATEDIFF(hour, applications.created_at, opportunities.created_at) AS application_to_opportunity_hours
        -- , calc_business_diff(hour, applications.created_at, opportunities.created_at) AS application_to_opportunity_biz_hours
        , DATEDIFF(hour, applications.created_at, applications.modified_at) AS application_created_to_modified_hours
        , calc_business_diff(hour, applications.created_at, opportunities.modified_at) AS application_created_to_modified_biz_hours
    FROM applications
    LEFT JOIN opportunities USING(opportunity_id)
    LEFT JOIN merchants USING(merchant_account_id)
)

SELECT * FROM base
