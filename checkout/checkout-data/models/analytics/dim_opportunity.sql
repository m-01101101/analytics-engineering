{{ config(
        materialized="table",
    )
}}

WITH opportunities AS (

    SELECT * FROM {{ ref('stg__dim_opportunity')}}

)

SELECT * FROM opportunities
