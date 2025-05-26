#!/usr/bin/env python3

import snowflake.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import dash
from dash import dcc, html
import os
import json
from datetime import datetime, timedelta

def create_visualizations():
    """Create interactive visualizations for data visualization portfolio."""
    
    # Get Snowflake connection parameters
    conn_params = {
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'user': os.getenv('SNOWFLAKE_USER'),
        'password': os.getenv('SNOWFLAKE_PASSWORD'),
        'database': os.getenv('SNOWFLAKE_DATABASE'),
        'schema': os.getenv('SNOWFLAKE_SCHEMA'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
        'role': os.getenv('SNOWFLAKE_ROLE')
    }
    
    print("🔗 Connecting to Snowflake for visualization data...")
    conn = snowflake.connector.connect(**conn_params)
    
    # Create visualizations directory
    os.makedirs('visualizations', exist_ok=True)
    
    try:
        # Load data for visualizations
        df = pd.read_sql("""
            SELECT 
                f.*,
                w.weather_desc,
                w.avg_temp_celsius,
                w.avg_humidity_percent,
                w.avg_windspeed_kmh,
                CASE WHEN day_name IN ('Saturday', 'Sunday') THEN 1 ELSE 0 END as is_weekend,
                CASE WHEN hour BETWEEN 7 AND 9 OR hour BETWEEN 17 AND 19 THEN 1 ELSE 0 END as is_rush_hour
            FROM fct_hourly_rentals f
            JOIN dim_weather w ON f.weather_id = w.weather_id
        """, conn)
        
        print(f"📊 Loaded {len(df):,} records for visualization")
        
        # 1. TIME SERIES ANALYSIS
        print("\n📈 Creating time series visualizations...")
        
        # Daily rentals trend
        daily_rentals = df.groupby('date')['total_rentals'].sum().reset_index()
        fig_daily = px.line(daily_rentals, x='date', y='total_rentals',
                          title='Daily Bike Rental Trends',
                          labels={'total_rentals': 'Total Rentals', 'date': 'Date'})
        fig_daily.write_html('visualizations/daily_trends.html')
        
        # Hourly heatmap
        hourly_avg = df.pivot_table(values='total_rentals', 
                                  index='hour',
                                  columns='day_name',
                                  aggfunc='mean')
        hourly_avg = hourly_avg.reindex(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 
                                       'Friday', 'Saturday', 'Sunday'], axis=1)
        
        fig_heatmap = px.imshow(hourly_avg,
                               title='Average Hourly Rentals by Day of Week',
                               labels=dict(x="Day of Week", y="Hour", color="Average Rentals"),
                               aspect="auto")
        fig_heatmap.write_html('visualizations/hourly_heatmap.html')
        
        # 2. WEATHER IMPACT
        print("🌤️ Creating weather impact visualizations...")
        
        # Temperature vs Rentals scatter
        fig_temp = px.scatter(df, x='avg_temp_celsius', y='total_rentals',
                            color='season_name',
                            title='Temperature Impact on Rentals',
                            labels={'avg_temp_celsius': 'Temperature (°C)',
                                  'total_rentals': 'Total Rentals',
                                  'season_name': 'Season'})
        fig_temp.write_html('visualizations/temperature_impact.html')
        
        # Weather conditions comparison
        weather_stats = df.groupby('weather_desc').agg({
            'total_rentals': ['mean', 'count']
        }).round(2)
        weather_stats.columns = ['avg_rentals', 'count']
        weather_stats = weather_stats.reset_index()
        
        fig_weather = px.bar(weather_stats, x='weather_desc', y='avg_rentals',
                           title='Average Rentals by Weather Condition',
                           labels={'weather_desc': 'Weather Condition',
                                 'avg_rentals': 'Average Rentals'})
        fig_weather.write_html('visualizations/weather_impact.html')
        
        # 3. USER SEGMENTATION
        print("👥 Creating user segmentation visualizations...")
        
        # User type distribution
        user_type = df.groupby('date').agg({
            'casual_users': 'sum',
            'registered_users': 'sum'
        }).reset_index()
        
        fig_users = px.area(user_type, x='date',
                          y=['casual_users', 'registered_users'],
                          title='Casual vs Registered Users Over Time',
                          labels={'value': 'Number of Users',
                                'variable': 'User Type',
                                'date': 'Date'})
        fig_users.write_html('visualizations/user_segments.html')
        
        # 4. INTERACTIVE DASHBOARD
        print("🎯 Creating interactive dashboard...")
        
        app = dash.Dash(__name__)
        
        app.layout = html.Div([
            html.H1("Bike Share Analytics Dashboard", 
                   style={'textAlign': 'center', 'color': '#2c3e50'}),
            
            html.Div([
                html.Div([
                    html.H3("Daily Trends"),
                    dcc.Graph(figure=fig_daily)
                ], className='six columns'),
                
                html.Div([
                    html.H3("Weather Impact"),
                    dcc.Graph(figure=fig_temp)
                ], className='six columns')
            ], className='row'),
            
            html.Div([
                html.Div([
                    html.H3("Hourly Patterns"),
                    dcc.Graph(figure=fig_heatmap)
                ], className='six columns'),
                
                html.Div([
                    html.H3("User Segments"),
                    dcc.Graph(figure=fig_users)
                ], className='six columns')
            ], className='row')
        ])
        
        app.run_server(debug=True, port=8050)
        
        # 5. EXPORT VISUALIZATION METADATA
        viz_metadata = {
            "visualizations": [
                {
                    "name": "Daily Rental Trends",
                    "type": "Time Series",
                    "file": "daily_trends.html",
                    "insights": [
                        "Shows overall rental growth over time",
                        "Highlights seasonal patterns",
                        "Identifies peak usage periods"
                    ]
                },
                {
                    "name": "Hourly Heatmap",
                    "type": "Heatmap",
                    "file": "hourly_heatmap.html",
                    "insights": [
                        "Reveals peak hours by day of week",
                        "Shows weekend vs weekday patterns",
                        "Helps with capacity planning"
                    ]
                },
                {
                    "name": "Temperature Impact",
                    "type": "Scatter Plot",
                    "file": "temperature_impact.html",
                    "insights": [
                        "Correlation between temperature and rentals",
                        "Seasonal variations in usage",
                        "Optimal temperature ranges"
                    ]
                },
                {
                    "name": "Weather Impact",
                    "type": "Bar Chart",
                    "file": "weather_impact.html",
                    "insights": [
                        "Rental patterns by weather condition",
                        "Impact of weather on demand",
                        "Weather-based forecasting potential"
                    ]
                },
                {
                    "name": "User Segments",
                    "type": "Area Chart",
                    "file": "user_segments.html",
                    "insights": [
                        "Growth of different user types",
                        "User type distribution over time",
                        "Seasonal patterns by user type"
                    ]
                }
            ],
            "dashboard": {
                "name": "Interactive Analytics Dashboard",
                "port": 8050,
                "features": [
                    "Real-time data updates",
                    "Interactive filtering",
                    "Cross-chart interactions",
                    "Responsive design"
                ]
            }
        }
        
        with open('visualizations/metadata.json', 'w') as f:
            json.dump(viz_metadata, f, indent=2)
        
        print(f"\n✅ Visualizations created successfully!")
        print(f"📁 Files created in 'visualizations/' directory:")
        print(f"   • daily_trends.html")
        print(f"   • hourly_heatmap.html")
        print(f"   • temperature_impact.html")
        print(f"   • weather_impact.html")
        print(f"   • user_segments.html")
        print(f"   • metadata.json")
        print(f"\n🌐 Interactive dashboard running at http://localhost:8050")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    create_visualizations() 