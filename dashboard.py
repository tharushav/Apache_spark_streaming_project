import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pymongo
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
MONGODB_URI = os.getenv("MONGODB_URI")

# Connect to MongoDB
client = pymongo.MongoClient(MONGODB_URI)
db = client["census"]

# Initialize Dash app
app = dash.Dash(__name__, 
                external_stylesheets=[
                    'https://stackpath.bootstrapcdn.com/bootstrap/4.5.0/css/bootstrap.min.css'
                ])

# Define app layout
app.layout = html.Div([
    html.Div([
        html.H1("Real-Time Census Data Analytics", className="text-center my-4"),
        html.P("Interactive dashboard showing real-time analytics from streaming census data", 
               className="text-center text-muted mb-4")
    ], className="container"),
    
    # Control Panel
    html.Div([
        html.Div([
            html.H4("Dashboard Controls", className="mb-3"),
            html.Label("Update Frequency:"),
            dcc.Dropdown(
                id='update-frequency',
                options=[
                    {'label': '5 seconds', 'value': 5000},
                    {'label': '10 seconds', 'value': 10000},
                    {'label': '30 seconds', 'value': 30000}
                ],
                value=10000,
                clearable=False,
                className="mb-3"
            ),
            html.Label("Time Range:"),
            dcc.Dropdown(
                id='time-range',
                options=[
                    {'label': 'Last 5 minutes', 'value': 5},
                    {'label': 'Last 15 minutes', 'value': 15},
                    {'label': 'Last 30 minutes', 'value': 30},
                    {'label': 'All data', 'value': 'all'}
                ],
                value='all',
                clearable=False,
                className="mb-3"
            ),
            html.Button('Reset View', id='reset-button', className="btn btn-secondary"),
            dcc.Interval(id="interval-component", interval=10000, n_intervals=0)
        ], className="bg-light p-3 border rounded")
    ], className="container mb-4"),
    
    # Summary Statistics
    html.Div([
        html.H3("Real-Time Summary Statistics", className="mb-3"),
        html.Div([
            html.Div([
                html.Div([
                    html.H5("Age Statistics", className="card-title"),
                    html.Div(id="age-stats", className="stats-content")
                ], className="card-body")
            ], className="card"),
            html.Div([
                html.Div([
                    html.H5("Income Distribution", className="card-title"),
                    html.Div(id="income-stats", className="stats-content")
                ], className="card-body")
            ], className="card"),
            html.Div([
                html.Div([
                    html.H5("Work Hours", className="card-title"),
                    html.Div(id="hours-stats", className="stats-content")
                ], className="card-body")
            ], className="card"),
        ], className="card-deck mb-4")
    ], className="container mb-4"),
    
    # First row of charts
    html.Div([
        html.Div([
            dcc.Graph(id="income-trend-chart")
        ], className="col-md-6"),
        html.Div([
            dcc.Graph(id="age-distribution-chart")
        ], className="col-md-6")
    ], className="row mb-4"),
    
    # Second row of charts
    html.Div([
        html.Div([
            dcc.Graph(id="education-income-chart")
        ], className="col-md-6"),
        html.Div([
            dcc.Graph(id="gender-income-chart")
        ], className="col-md-6")
    ], className="row mb-4"),
    
    # Third row of charts
    html.Div([
        html.Div([
            dcc.Graph(id="occupation-chart")
        ], className="col-md-6"),
        html.Div([
            dcc.Graph(id="anomalies-chart")
        ], className="col-md-6")
    ], className="row mb-4")
    
], className="container-fluid")

# Set up callbacks
@app.callback(
    Output("interval-component", "interval"),
    [Input("update-frequency", "value")]
)
def update_interval(value):
    return value

@app.callback(
    [
        Output("age-stats", "children"),
        Output("income-stats", "children"),
        Output("hours-stats", "children")
    ],
    [Input("interval-component", "n_intervals"),
     Input("time-range", "value")]
)
def update_summary_stats(n, time_range):
    # Get time filter
    time_filter = get_time_filter(time_range)
    
    # Query the latest summary statistics
    if time_filter is None:
        stats = list(db["summary_statistics"].find().sort("timestamp", -1).limit(1))
    else:
        stats = list(db["summary_statistics"].find({"timestamp": {"$gte": time_filter}}).sort("timestamp", -1).limit(1))
    
    if not stats:
        return "No data available", "No data available", "No data available"
    
    latest_stats = stats[0]
    
    # Age stats (without stddev)
    age_stats = html.Div([
        html.P(f"Average: {latest_stats.get('avg_age', 'N/A'):.1f} years"),
        html.P(f"Min: {latest_stats.get('min_age', 'N/A')} years"),
        html.P(f"Max: {latest_stats.get('max_age', 'N/A')} years")
    ])
    
    # Income stats
    high_income = latest_stats.get('count_High_Income_(>50K)', 0)
    low_income = latest_stats.get('count_Low_Income_(<=50K)', 0)
    total_income = high_income + low_income
    high_pct = (high_income / total_income * 100) if total_income > 0 else 0
    
    income_stats = html.Div([
        html.P(f"High Income: {high_income} ({high_pct:.1f}%)"),
        html.P(f"Low Income: {low_income} ({100-high_pct:.1f}%)"),
        html.P(f"Total Records: {total_income}")
    ])
    
    # Hours stats (without stddev)
    hours_stats = html.Div([
        html.P(f"Average: {latest_stats.get('avg_hours', 'N/A'):.1f} hours/week")
    ])
    
    return age_stats, income_stats, hours_stats

@app.callback(
    Output("income-trend-chart", "figure"),
    [Input("interval-component", "n_intervals"),
     Input("time-range", "value")]
)
def update_income_trend(n, time_range):
    time_filter = get_time_filter(time_range)
    
    # Get summary statistics over time
    query = {} if time_filter is None else {"timestamp": {"$gte": time_filter}}
    stats_df = pd.DataFrame(list(db["summary_statistics"].find(query).sort("timestamp", 1)))
    
    if stats_df.empty:
        return create_empty_figure("No income trend data available")
    
    # Convert timestamp to datetime for better x-axis display
    stats_df['datetime'] = pd.to_datetime(stats_df['timestamp'], unit='s')
    
    # Create figure with secondary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    # Add high income count
    if 'count_High_Income_(>50K)' in stats_df.columns:
        fig.add_trace(
            go.Scatter(
                x=stats_df['datetime'],
                y=stats_df['count_High_Income_(>50K)'],
                name="High Income (>50K)",
                mode='lines+markers'
            )
        )
    
    # Add low income count
    if 'count_Low_Income_(<=50K)' in stats_df.columns:
        fig.add_trace(
            go.Scatter(
                x=stats_df['datetime'],
                y=stats_df['count_Low_Income_(<=50K)'],
                name="Low Income (<=50K)",
                mode='lines+markers'
            )
        )
    
    # Add average age on secondary axis if available
    if 'avg_age' in stats_df.columns:
        fig.add_trace(
            go.Scatter(
                x=stats_df['datetime'],
                y=stats_df['avg_age'],
                name="Avg Age",
                mode='lines',
                line=dict(dash='dash', color='rgba(0,150,136,0.7)'),
                yaxis="y2"
            ),
            secondary_y=True
        )
    
    fig.update_layout(
        title="Income Distribution Trend Over Time",
        xaxis_title="Time",
        yaxis_title="Count",
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
        height=400
    )
    
    # Set y-axes titles
    fig.update_yaxes(title_text="Count", secondary_y=False)
    fig.update_yaxes(title_text="Average Age", secondary_y=True)
    
    return fig

@app.callback(
    Output("age-distribution-chart", "figure"),
    [Input("interval-component", "n_intervals"),
     Input("time-range", "value")]
)
def update_age_distribution(n, time_range):
    time_filter = get_time_filter(time_range)
    
    # Get latest age group distribution
    query = {} if time_filter is None else {"timestamp": {"$gte": time_filter}}
    age_data = list(db["age_group_distribution"].find(query).sort("timestamp", -1))
    
    if not age_data:
        return create_empty_figure("No age distribution data available")
    
    # Group by age_group and sum counts for the selected time period
    age_df = pd.DataFrame(age_data)
    age_summary = age_df.groupby('age_group', as_index=False)['count'].sum()
    
    # Order age groups correctly
    age_order = ["Under 18", "18-29", "30-44", "45-64", "65+"]
    age_summary['age_group'] = pd.Categorical(age_summary['age_group'], categories=age_order, ordered=True)
    age_summary = age_summary.sort_values('age_group')
    
    fig = px.bar(
        age_summary, 
        x='age_group', 
        y='count',
        title="Age Group Distribution",
        labels={'age_group': 'Age Group', 'count': 'Count'},
        color='age_group',
        color_discrete_sequence=px.colors.qualitative.Set1
    )
    
    fig.update_layout(height=400)
    return fig

@app.callback(
    Output("education-income-chart", "figure"),
    [Input("interval-component", "n_intervals"),
     Input("time-range", "value")]
)
def update_education_income(n, time_range):
    time_filter = get_time_filter(time_range)
    
    # Get education vs income data
    query = {} if time_filter is None else {"timestamp": {"$gte": time_filter}}
    edu_income_data = list(db["education_income"].find(query))
    
    if not edu_income_data:
        return create_empty_figure("No education vs income data available")
    
    edu_df = pd.DataFrame(edu_income_data)
    edu_summary = edu_df.groupby(['education', 'income_category'], as_index=False)['count'].sum()
    
    # Filter to top education categories by count for better visualization
    top_edu = edu_df.groupby('education')['count'].sum().nlargest(8).index.tolist()
    edu_summary = edu_summary[edu_summary['education'].isin(top_edu)]
    
    fig = px.bar(
        edu_summary, 
        x='education', 
        y='count', 
        color='income_category',
        title='Income Distribution by Education Level (Top 8)',
        labels={'education': 'Education Level', 'count': 'Count', 'income_category': 'Income'},
        barmode='group',
        height=400
    )
    
    fig.update_layout(xaxis={'categoryorder': 'total descending'})
    return fig

@app.callback(
    Output("gender-income-chart", "figure"),
    [Input("interval-component", "n_intervals"),
     Input("time-range", "value")]
)
def update_gender_income(n, time_range):
    time_filter = get_time_filter(time_range)
    
    # Get gender vs income data
    query = {} if time_filter is None else {"timestamp": {"$gte": time_filter}}
    gender_income_data = list(db["gender_income"].find(query))
    
    if not gender_income_data:
        return create_empty_figure("No gender vs income data available")
    
    gender_df = pd.DataFrame(gender_income_data)
    gender_summary = gender_df.groupby(['gender', 'income_category'], as_index=False)['count'].sum()
    
    # Calculate percentages
    total_by_gender = gender_summary.groupby('gender')['count'].transform('sum')
    gender_summary['percentage'] = gender_summary['count'] / total_by_gender * 100
    
    # Create a pie chart for each gender
    fig = make_subplots(rows=1, cols=2, specs=[[{'type': 'pie'}, {'type': 'pie'}]], 
                       subplot_titles=['Male Income Distribution', 'Female Income Distribution'])
    
    # Male pie chart
    male_data = gender_summary[gender_summary['gender'] == 'Male']
    if not male_data.empty:
        fig.add_trace(
            go.Pie(
                labels=male_data['income_category'],
                values=male_data['count'],
                name='Male',
                hole=0.4
            ),
            row=1, col=1
        )
    
    # Female pie chart
    female_data = gender_summary[gender_summary['gender'] == 'Female']
    if not female_data.empty:
        fig.add_trace(
            go.Pie(
                labels=female_data['income_category'],
                values=female_data['count'],
                name='Female',
                hole=0.4
            ),
            row=1, col=2
        )
    
    fig.update_layout(
        title="Income Distribution by Gender",
        height=400
    )
    
    return fig

@app.callback(
    Output("occupation-chart", "figure"),
    [Input("interval-component", "n_intervals"),
     Input("time-range", "value")]
)
def update_occupation_chart(n, time_range):
    time_filter = get_time_filter(time_range)
    
    # Get occupation stats
    query = {} if time_filter is None else {"timestamp": {"$gte": time_filter}}
    occupation_data = list(db["occupation_stats"].find(query))
    
    if not occupation_data:
        return create_empty_figure("No occupation data available")
    
    # Get the most recent stats for each occupation
    occ_df = pd.DataFrame(occupation_data)
    
    # For each occupation, get the record with the most recent timestamp
    latest_timestamps = occ_df.groupby('occupation')['timestamp'].max().reset_index()
    latest_records = pd.merge(occ_df, latest_timestamps, on=['occupation', 'timestamp'])
    
    # Focus on top occupations by count for better visualization
    top_occ = latest_records.nlargest(10, 'count')
    
    # Create scatter plot: x=avg_age, y=avg_hours, size=count
    fig = px.scatter(
        top_occ,
        x='avg_age',
        y='avg_hours',
        size='count',
        color='occupation',
        hover_name='occupation',
        title='Top 10 Occupations: Age vs Work Hours',
        labels={'avg_age': 'Average Age', 'avg_hours': 'Average Work Hours/Week', 'count': 'Count'}
    )
    
    fig.update_layout(height=400)
    return fig

@app.callback(
    Output("anomalies-chart", "figure"),
    [Input("interval-component", "n_intervals"),
     Input("time-range", "value")]
)
def update_anomalies(n, time_range):
    time_filter = get_time_filter(time_range)
    
    # Get anomaly data
    query = {} if time_filter is None else {"detected_at": {"$gte": time_filter}}
    anomalies = list(db["anomalies"].find(query))
    
    if not anomalies:
        # Create empty figure with message
        return create_empty_figure("No anomalies detected in the selected time range")
    
    # Create DataFrame
    anomalies_df = pd.DataFrame(anomalies)
    anomalies_df['detected_time'] = pd.to_datetime(anomalies_df['detected_at'], unit='s')
    
    # Check if there are hours outliers
    if 'hours_z_score' in anomalies_df.columns and 'hours_per_week' in anomalies_df.columns:
        fig = px.scatter(
            anomalies_df,
            x='detected_time',
            y='hours_per_week',
            size='z_score',
            color='z_score',
            hover_data=['age', 'occupation', 'education'],
            title='Detected Anomalies: Work Hours Outliers',
            labels={
                'detected_time': 'Detection Time',
                'hours_per_week': 'Hours per Week',
                'z_score': 'Anomaly Score (Z-Score)'
            },
            color_continuous_scale='Viridis'
        )
        
        fig.update_layout(height=400)
        return fig
    else:
        return create_empty_figure("Anomaly data structure not as expected")

# Helper functions
def get_time_filter(time_range):
    """Convert time range selection to a timestamp for filtering MongoDB queries"""
    if time_range == 'all':
        return None
    else:
        # Convert minutes to seconds and calculate timestamp
        minutes = int(time_range)
        return datetime.now().timestamp() - (minutes * 60)

def create_empty_figure(message):
    """Create an empty figure with a message"""
    fig = go.Figure()
    fig.update_layout(
        title=message,
        xaxis={"visible": False},
        yaxis={"visible": False},
        annotations=[
            {
                "text": message,
                "xref": "paper",
                "yref": "paper",
                "showarrow": False,
                "font": {"size": 20}
            }
        ],
        height=400
    )
    return fig

# Run the app
if __name__ == "__main__":
    app.run(debug=True)
