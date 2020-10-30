WITH base AS (
    
    SELECT *
    FROM {{ source('raw', 'ONBOARDING_APPLICATIONS')}}

)

SELECT * FROM base