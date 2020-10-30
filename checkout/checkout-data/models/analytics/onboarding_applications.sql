WITH applications AS (
    
    SELECT * FROM {{ ref('tr__onboarding_applications')}}

)

, opportunities AS (

    SELECT * FROM {{ ref('stg__dim_opportunity')}}

)

, merchants AS (

    SELECT * FROM {{ ref('stg__dim_merchant')}}

)

, country AS (

    SELECT * FROM {{ ref('stg__dim_country')}}

)

, reviewer AS (

    SELECT * FROM {{ ref('stg__dim_user')}}

)

, final AS (
    SELECT DISTINCT
        applications.application_id
        , DATE_TRUNC('month', applications.created_at) application_created_month
        , LAST_VALUE(applications.status) OVER(
            PARTITION BY application_id
            ORDER BY progress
            ) AS current_status
        , country.risk_level
        , applications.merchant_account_id
        , merchants.merchant_name
        , country.country_iso_name AS country_full_name
        , country.region
        , opportunities.sales_rep AS sales_rep_name
        , opportunities.lead_source_bucket
        , reviewer.user_name AS final_reviewer_name
        , COUNT(CASE 
                    WHEN application_complete_at IS NOT NULL
                    THEN application_id
                END) OVER(PARTITION BY application_created_month) AS total_merchants_onboarded_in_month
        , applications.application_to_opportunity_hours
        , CASE
            WHEN progress = 2 
            THEN applications.application_created_to_modified_hours
        END application_to_initial_review_hours
        , applications.days_to_onboard
    FROM applications
    LEFT JOIN merchants USING(merchant_account_id)
    LEFT JOIN opportunities USING(opportunity_id)
    LEFT JOIN country 
        ON applications.country = country_key
    LEFT JOIN reviewer 
        ON applications.final_reviewer_id = user_id    
)

SELECT * FROM final