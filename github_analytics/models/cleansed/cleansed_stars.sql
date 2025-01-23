
{{
    config(
        materialized='table',
        schema='cleansed'
    )
}}

SELECT 
    user as user_id,
    CAST(starred_at AS TIMESTAMP) as starred_at
FROM {{ source('raw', 'stars') }}