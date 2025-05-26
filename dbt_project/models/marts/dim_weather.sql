SELECT 
    weather_id,
    {{ get_weather_description('weather_id') }} as weather_desc,
    AVG(temp_celsius) as avg_temp_celsius,
    AVG(feels_like_celsius) as avg_feels_like_celsius,
    AVG(humidity_percent) as avg_humidity_percent,
    AVG(windspeed_kmh) as avg_windspeed_kmh
FROM {{ ref('stg_bikeshare') }}
GROUP BY weather_id 