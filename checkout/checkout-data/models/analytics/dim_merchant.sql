{{ config(
        materialized="table",
    )
}}

WITH merchant AS (

    SELECT * FROM {{ ref('stg__dim_merchant')}}

)

SELECT * FROM merchant
