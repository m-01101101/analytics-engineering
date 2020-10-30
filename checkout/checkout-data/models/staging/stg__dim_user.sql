WITH base AS (
    
    SELECT * FROM {{ source('raw', 'DIM_USER')}}

)

SELECT * FROM base