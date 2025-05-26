SELECT
    record_id,
    date,
    hour,
    season_id,
    year,
    month,
    day_of_week,
    is_holiday,
    is_workingday,
    weather_id,
    casual_users,
    registered_users,
    total_rentals,
    CASE season_id
        WHEN 1 THEN 'Spring'
        WHEN 2 THEN 'Summer'
        WHEN 3 THEN 'Fall'
        WHEN 4 THEN 'Winter'
    END as season_name,
    CASE day_of_week
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END as day_name,
    CASE 
        WHEN hour BETWEEN 6 AND 9 THEN 'Morning Rush'
        WHEN hour BETWEEN 10 AND 15 THEN 'Mid-Day'
        WHEN hour BETWEEN 16 AND 19 THEN 'Evening Rush'
        WHEN hour BETWEEN 20 AND 23 THEN 'Night'
        ELSE 'Early Morning'
    END as time_of_day
FROM {{ ref('stg_bikeshare') }} 