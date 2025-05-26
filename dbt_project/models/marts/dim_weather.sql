SELECT DISTINCT
    weather_id,
    CASE weather_id
        WHEN 1 THEN 'Clear/Partly Cloudy'
        WHEN 2 THEN 'Mist/Cloudy'
        WHEN 3 THEN 'Light Rain/Snow'
        WHEN 4 THEN 'Heavy Rain/Snow/Storm'
    END as weather_desc,
    temp_celsius,
    feels_like_celsius,
    humidity_percent,
    windspeed_kmh
FROM {{ ref('stg_bikeshare') }} 