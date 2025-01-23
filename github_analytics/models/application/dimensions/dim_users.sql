{{
    config(
        materialized='table',
        schema='application'
    )
}}

SELECT DISTINCT
    user as user_id,
    user as username
FROM {{ source('raw', 'stars') }}
UNION
SELECT DISTINCT
    user as user_id,
    user as username
FROM {{ source('raw', 'forks') }} 