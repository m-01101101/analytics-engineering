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
        , datediff(days, applications.created_at, merchants.merchant_created_at) AS days_to_onboard
        , ROW_NUMBER() OVER(
            PARTITION BY application_id 
            ORDER BY COALESCE(modified_at, created_at)
            ) AS progress
        , datediff(hours, applications.created_at, opportunities.created_at) application_to_opportunity_hours
        , datediff(hours, applications.created_at, applications.modified_at) application_created_to_modified_hours
    FROM applications
    LEFT JOIN opportunities USING(opportunity_id)
    LEFT JOIN merchants USING(merchant_account_id)
)

SELECT * FROM base
