{% macro get_weather_description(weather_id_column) %}
    CASE {{ weather_id_column }}
        WHEN 1 THEN 'Clear/Partly Cloudy'
        WHEN 2 THEN 'Mist/Cloudy'
        WHEN 3 THEN 'Light Rain/Snow'
        WHEN 4 THEN 'Heavy Rain/Snow/Storm'
        ELSE 'Unknown'
    END
{% endmacro %} 