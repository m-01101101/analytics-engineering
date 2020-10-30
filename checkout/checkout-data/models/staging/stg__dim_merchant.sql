WITH base AS (
    
    SELECT * FROM {{ source('raw', 'DIM_MERCHANT')}}

)

SELECT * FROM base