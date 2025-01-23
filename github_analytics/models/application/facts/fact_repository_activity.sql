{{
    config(
        materialized='table',
        schema='application'
    )
}}

WITH daily_stats AS (
    SELECT
        d.date,
        COUNT(DISTINCT s.user) as new_stars,
        COUNT(DISTINCT f.user) as new_forks,
        COUNT(DISTINCT CASE WHEN i.state = 'open' THEN i.id END) as new_issues,
        COUNT(DISTINCT CASE WHEN i.state = 'closed' THEN i.id END) as closed_issues,
        COUNT(DISTINCT CASE WHEN pr.state = 'open' THEN pr.id END) as new_prs,
        COUNT(DISTINCT CASE WHEN pr.merged_at IS NOT NULL THEN pr.id END) as merged_prs
    FROM {{ ref('dim_dates') }} d
    LEFT JOIN {{ source('raw', 'stars') }} s 
        ON CAST(s.starred_at AS TIMESTAMP)::DATE = d.date::DATE
    LEFT JOIN {{ source('raw', 'forks') }} f 
        ON CAST(f.created_at AS TIMESTAMP)::DATE = d.date::DATE
    LEFT JOIN {{ source('raw', 'issues') }} i 
        ON CAST(i.created_at AS TIMESTAMP)::DATE = d.date::DATE 
        OR CAST(i.closed_at AS TIMESTAMP)::DATE = d.date::DATE
    LEFT JOIN {{ source('raw', 'pull_requests') }} pr 
        ON CAST(pr.created_at AS TIMESTAMP)::DATE = d.date::DATE 
        OR CAST(pr.merged_at AS TIMESTAMP)::DATE = d.date::DATE
    GROUP BY d.date
)

SELECT
    date,
    new_stars,
    new_forks,
    new_issues,
    closed_issues,
    new_prs,
    merged_prs,
    new_stars + new_forks + new_issues + new_prs as total_activity
FROM daily_stats
ORDER BY date 