{{ config(
        materialized="table",
    )
}}

WITH user AS (

    SELECT * FROM {{ ref('stg__dim_user')}}

)

SELECT * FROM user
