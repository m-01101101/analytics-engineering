WITH base AS (
    
    SELECT * FROM {{ source('raw', 'DIM_OPPORTUNITY')}}

)

SELECT * FROM base