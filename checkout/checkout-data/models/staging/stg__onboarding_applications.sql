WITH base AS (
    
    SELECT *
        , {{ dbt_utils.surrogate_key(['application_id', 'modified_at']) }} AS pk
    FROM {{ source('raw', 'ONBOARDING_APPLICATIONS')}}

)

SELECT * FROM base