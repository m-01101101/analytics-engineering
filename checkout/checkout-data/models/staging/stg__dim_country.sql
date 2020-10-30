WITH base AS (
    
    SELECT * FROM {{ source('raw', 'DIM_COUNTRY')}}

)

SELECT * FROM base