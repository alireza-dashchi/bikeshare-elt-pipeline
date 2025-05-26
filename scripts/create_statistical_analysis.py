#!/usr/bin/env python3

import snowflake.connector
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import scipy.stats as stats
from scipy.stats import pearsonr, spearmanr
import os
import warnings
warnings.filterwarnings('ignore')

def statistical_analysis():
    """Comprehensive statistical analysis for data analyst portfolio."""
    
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
    
    print("🔗 Connecting to Snowflake for statistical analysis...")
    conn = snowflake.connector.connect(**conn_params)
    
    # Create analysis directory
    os.makedirs('statistical_analysis', exist_ok=True)
    
    try:
        # Load data for analysis
        df = pd.read_sql("""
            SELECT 
                f.*,
                w.avg_temp_celsius,
                w.avg_humidity_percent,
                w.avg_windspeed_kmh,
                CASE WHEN day_name IN ('Saturday', 'Sunday') THEN 1 ELSE 0 END as is_weekend,
                CASE WHEN hour BETWEEN 7 AND 9 OR hour BETWEEN 17 AND 19 THEN 1 ELSE 0 END as is_rush_hour
            FROM fct_hourly_rentals f
            JOIN dim_weather w ON f.weather_id = w.weather_id
        """, conn)
        
        print(f"📊 Loaded {len(df):,} records for statistical analysis")
        
        # 1. DESCRIPTIVE STATISTICS
        print("\n📈 DESCRIPTIVE STATISTICS ANALYSIS")
        print("=" * 50)
        
        desc_stats = df[['total_rentals', 'casual_users', 'registered_users', 
                        'avg_temp_celsius', 'avg_humidity_percent']].describe()
        desc_stats.to_csv('statistical_analysis/descriptive_statistics.csv')
        
        print("Key Statistics:")
        print(f"• Average hourly rentals: {df['total_rentals'].mean():.1f}")
        print(f"• Median hourly rentals: {df['total_rentals'].median():.1f}")
        print(f"• Standard deviation: {df['total_rentals'].std():.1f}")
        print(f"• 95th percentile: {df['total_rentals'].quantile(0.95):.1f}")
        
        # 2. CORRELATION ANALYSIS
        print("\n🔗 CORRELATION ANALYSIS")
        print("=" * 30)
        
        correlation_vars = ['total_rentals', 'avg_temp_celsius', 'avg_humidity_percent', 
                           'avg_windspeed_kmh', 'hour', 'is_weekend', 'is_rush_hour']
        
        corr_matrix = df[correlation_vars].corr()
        corr_matrix.to_csv('statistical_analysis/correlation_matrix.csv')
        
        # Strong correlations (>0.3 or <-0.3)
        strong_corrs = []
        for i in range(len(corr_matrix.columns)):
            for j in range(i+1, len(corr_matrix.columns)):
                corr_val = corr_matrix.iloc[i,j]
                if abs(corr_val) > 0.3:
                    strong_corrs.append({
                        'var1': corr_matrix.columns[i],
                        'var2': corr_matrix.columns[j], 
                        'correlation': corr_val
                    })
        
        print("Strong correlations found:")
        for corr in sorted(strong_corrs, key=lambda x: abs(x['correlation']), reverse=True):
            print(f"• {corr['var1']} ↔ {corr['var2']}: {corr['correlation']:.3f}")
        
        # 3. HYPOTHESIS TESTING
        print("\n🧪 HYPOTHESIS TESTING")
        print("=" * 25)
        
        # T-test: Weekend vs Weekday rentals
        weekend_rentals = df[df['is_weekend'] == 1]['total_rentals']
        weekday_rentals = df[df['is_weekend'] == 0]['total_rentals']
        
        t_stat, p_value = stats.ttest_ind(weekend_rentals, weekday_rentals)
        print(f"Weekend vs Weekday T-test:")
        print(f"• Weekend avg: {weekend_rentals.mean():.1f}")
        print(f"• Weekday avg: {weekday_rentals.mean():.1f}")
        print(f"• T-statistic: {t_stat:.3f}")
        print(f"• P-value: {p_value:.6f}")
        print(f"• Significant difference: {'Yes' if p_value < 0.05 else 'No'}")
        
        # ANOVA: Seasonal differences
        spring = df[df['season_name'] == 'Spring']['total_rentals']
        summer = df[df['season_name'] == 'Summer']['total_rentals'] 
        fall = df[df['season_name'] == 'Fall']['total_rentals']
        winter = df[df['season_name'] == 'Winter']['total_rentals']
        
        f_stat, p_value_anova = stats.f_oneway(spring, summer, fall, winter)
        print(f"\nSeasonal ANOVA Test:")
        print(f"• F-statistic: {f_stat:.3f}")
        print(f"• P-value: {p_value_anova:.6f}")
        print(f"• Significant seasonal effect: {'Yes' if p_value_anova < 0.05 else 'No'}")
        
        # 4. WEATHER IMPACT ANALYSIS
        print("\n🌤️ WEATHER IMPACT QUANTIFICATION")
        print("=" * 35)
        
        # Linear regression coefficients for weather variables
        from sklearn.linear_model import LinearRegression
        
        weather_vars = ['avg_temp_celsius', 'avg_humidity_percent', 'avg_windspeed_kmh']
        X = df[weather_vars]
        y = df['total_rentals']
        
        model = LinearRegression()
        model.fit(X, y)
        
        print("Weather impact coefficients (rentals per unit change):")
        for i, var in enumerate(weather_vars):
            print(f"• {var}: {model.coef_[i]:.2f}")
        
        r2_score = model.score(X, y)
        print(f"• Model R²: {r2_score:.3f}")
        
        # 5. TIME SERIES PATTERNS
        print("\n⏰ TIME SERIES PATTERN ANALYSIS")
        print("=" * 35)
        
        # Group by hour for daily patterns
        hourly_avg = df.groupby('hour')['total_rentals'].agg(['mean', 'std']).round(2)
        hourly_avg.to_csv('statistical_analysis/hourly_patterns.csv')
        
        peak_hours = hourly_avg.nlargest(3, 'mean')
        print("Top 3 peak hours:")
        for hour, data in peak_hours.iterrows():
            print(f"• Hour {hour}: {data['mean']:.1f} ± {data['std']:.1f} rentals")
        
        # Day of week patterns
        dow_avg = df.groupby('day_name')['total_rentals'].agg(['mean', 'std']).round(2)
        dow_avg = dow_avg.reindex(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 
                                  'Friday', 'Saturday', 'Sunday'])
        dow_avg.to_csv('statistical_analysis/daily_patterns.csv')
        
        # 6. OUTLIER ANALYSIS
        print("\n🔍 OUTLIER DETECTION")
        print("=" * 20)
        
        Q1 = df['total_rentals'].quantile(0.25)
        Q3 = df['total_rentals'].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        
        outliers = df[(df['total_rentals'] < lower_bound) | 
                     (df['total_rentals'] > upper_bound)]
        
        print(f"• Total outliers detected: {len(outliers)} ({len(outliers)/len(df)*100:.1f}%)")
        print(f"• Upper outliers (>{upper_bound:.0f}): {len(outliers[outliers['total_rentals'] > upper_bound])}")
        print(f"• Lower outliers (<{lower_bound:.0f}): {len(outliers[outliers['total_rentals'] < lower_bound])}")
        
        outliers.to_csv('statistical_analysis/outliers.csv', index=False)
        
        # 7. BUSINESS INSIGHTS SUMMARY
        insights = {
            "key_findings": [
                f"Peak demand occurs at hour {hourly_avg.idxmax()['mean']} with {hourly_avg.max()['mean']:.0f} avg rentals",
                f"Temperature has {model.coef_[0]:.1f} rental impact per degree celsius",
                f"{'Significant' if p_value < 0.05 else 'No significant'} difference between weekend/weekday usage",
                f"Weather explains {r2_score*100:.1f}% of rental variance",
                f"{len(outliers)/len(df)*100:.1f}% of hours show unusual demand patterns"
            ],
            "recommendations": [
                "Increase fleet size during evening rush hours (5-7 PM)",
                "Implement weather-based demand forecasting",
                "Different pricing strategies for weekend vs weekday",
                "Monitor and investigate outlier patterns for operational issues",
                "Seasonal capacity planning based on statistical patterns"
            ],
            "statistical_significance": {
                "weekend_effect": p_value < 0.05,
                "seasonal_effect": p_value_anova < 0.05,
                "weather_correlation": r2_score > 0.3
            }
        }
        
        import json
        with open('statistical_analysis/business_insights.json', 'w') as f:
            json.dump(insights, f, indent=2)
        
        print(f"\n✅ Statistical analysis completed!")
        print(f"📁 Files created in 'statistical_analysis/' directory:")
        print(f"   • descriptive_statistics.csv")
        print(f"   • correlation_matrix.csv") 
        print(f"   • hourly_patterns.csv")
        print(f"   • daily_patterns.csv")
        print(f"   • outliers.csv ({len(outliers)} records)")
        print(f"   • business_insights.json")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    statistical_analysis() 