{{ config(
        materialized="table",
    )
}}

WITH country AS (

    SELECT * FROM {{ ref('stg__dim_country')}}

)

SELECT * FROM country
