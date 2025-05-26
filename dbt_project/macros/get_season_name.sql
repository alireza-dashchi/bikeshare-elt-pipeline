{% macro get_season_name(season_id_column) %}
    CASE {{ season_id_column }}
        WHEN 1 THEN 'Spring'
        WHEN 2 THEN 'Summer'
        WHEN 3 THEN 'Fall'
        WHEN 4 THEN 'Winter'
        ELSE 'Unknown'
    END
{% endmacro %} 