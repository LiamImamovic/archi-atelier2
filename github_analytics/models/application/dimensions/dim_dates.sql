{{
    config(
        materialized='table',
        schema='application'
    )
}}

WITH all_dates AS (
    SELECT CAST(created_at AS TIMESTAMP) as date FROM {{ source('raw', 'issues') }}
    UNION
    SELECT CAST(closed_at AS TIMESTAMP) as date FROM {{ source('raw', 'issues') }} WHERE closed_at IS NOT NULL
    UNION
    SELECT CAST(created_at AS TIMESTAMP) as date FROM {{ source('raw', 'pull_requests') }}
    UNION
    SELECT CAST(merged_at AS TIMESTAMP) as date FROM {{ source('raw', 'pull_requests') }} WHERE merged_at IS NOT NULL
    UNION
    SELECT CAST(starred_at AS TIMESTAMP) as date FROM {{ source('raw', 'stars') }}
    UNION
    SELECT CAST(created_at AS TIMESTAMP) as date FROM {{ source('raw', 'forks') }}
)

SELECT DISTINCT
    date,
    date_part('year', date) as year,
    date_part('month', date) as month,
    date_part('day', date) as day
FROM all_dates
WHERE date IS NOT NULL
ORDER BY date 