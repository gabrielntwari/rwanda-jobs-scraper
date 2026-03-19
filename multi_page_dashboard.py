"""
Rwanda Jobs Multi-Page Dashboard - ENHANCED VERSION
====================================================
Beautiful, professional multi-page dashboard with 3 sections.

Run: python dashboard_enhanced.py
Then open: http://localhost:8050
"""

import os
from datetime import datetime, timedelta
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import dash
from dash import dcc, html, Input, Output, State, dash_table, callback
import dash_bootstrap_components as dbc
from sqlalchemy import create_engine

# Database connection
DATABASE_URL = os.getenv('DATABASE_URL')

def get_jobs_data():
    """Fetch jobs from database"""
    engine = create_engine(DATABASE_URL)
    
    query = """
    SELECT 
        id,
        title,
        company,
        source,
        sector,
        district,
        employment_type,
        job_level,
        experience_years,
        education_level,
        description,
        posted_date,
        deadline,
        scraped_at,
        is_active,
        source_url
    FROM jobs
    WHERE is_active = true
    ORDER BY scraped_at DESC
    """
    
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    
    # Process dates
    df['posted_date'] = pd.to_datetime(df['posted_date'], errors='coerce')
    df['deadline'] = pd.to_datetime(df['deadline'], errors='coerce')
    df['scraped_at'] = pd.to_datetime(df['scraped_at'])
    
    # For deadlines without time component, set to 11:59 PM (end of day)
    for idx in df.index:
        if pd.notna(df.loc[idx, 'deadline']):
            deadline = df.loc[idx, 'deadline']
            # Check if time is midnight (00:00:00) - means no time was specified
            if deadline.hour == 0 and deadline.minute == 0 and deadline.second == 0:
                # Set to 11:59 PM of that day
                df.loc[idx, 'deadline'] = deadline.replace(hour=23, minute=59, second=59)
    
    # Calculate time until deadline (full timedelta, not just days)
    df['time_to_deadline'] = df['deadline'] - pd.Timestamp.now()
    df['days_to_deadline'] = df['time_to_deadline'].dt.days
    
    return df

# Load data
df = get_jobs_data()

# Initialize Dash app with Bootstrap theme
app = dash.Dash(__name__, external_stylesheets=[
    dbc.themes.BOOTSTRAP,
    "https://use.fontawesome.com/releases/v6.1.1/css/all.css"  # Font Awesome icons
], suppress_callback_exceptions=True)
app.title = "Rwanda Jobs Portal"
server = app.server  # Required for Gunicorn: gunicorn multi_page_dashboard:server

# Custom CSS
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <style>
            body {
                background-color: #f5f7fa;
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            }
            
            .job-card {
                transition: all 0.3s ease;
                border-left: 4px solid transparent;
            }
            
            .job-card:hover {
                transform: translateY(-5px);
                box-shadow: 0 12px 24px rgba(0,0,0,0.15) !important;
                border-left-color: #667eea;
            }
            
            .stat-card {
                border-left: 4px solid #667eea;
                transition: all 0.2s ease;
            }
            
            .stat-card:hover {
                transform: scale(1.02);
            }
            
            .navbar-dark {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%) !important;
            }
            
            .btn-apply {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                border: none;
                transition: all 0.3s ease;
            }
            
            .btn-apply:hover {
                transform: translateY(-2px);
                box-shadow: 0 5px 15px rgba(102, 126, 234, 0.4);
            }
            
            .deadline-urgent {
                animation: pulse 1.5s infinite;
            }
            
            @keyframes pulse {
                0%, 100% { opacity: 1; }
                50% { opacity: 0.6; }
            }
            
            .search-box {
                border-radius: 50px;
                border: 2px solid #e0e0e0;
                padding: 12px 24px;
                transition: all 0.3s ease;
            }
            
            .search-box:focus {
                border-color: #667eea;
                box-shadow: 0 0 0 0.2rem rgba(102, 126, 234, 0.25);
            }
            
            .hover-lift {
                transition: all 0.3s cubic-bezier(0.25, 0.8, 0.25, 1);
            }
            
            .hover-lift:hover {
                transform: translateY(-5px);
                box-shadow: 0 10px 25px rgba(0,0,0,0.15) !important;
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

# Navbar
navbar = dbc.NavbarSimple(
    children=[
        dbc.NavItem(dbc.NavLink([
            html.I(className="fas fa-search me-2"),
            "Job Search"
        ], href="/", active="exact")),
        dbc.NavItem(dbc.NavLink([
            html.I(className="fas fa-chart-line me-2"),
            "Market Insights"
        ], href="/insights", active="exact")),
        dbc.NavItem(dbc.NavLink([
            html.I(className="fas fa-history me-2"),
            "Historical Jobs"
        ], href="/historical", active="exact")),
    ],
    brand=[
        html.I(className="fas fa-briefcase me-2"),
        "🇷🇼 Rwanda Jobs Portal"
    ],
    brand_href="/",
    color="primary",
    dark=True,
    className="mb-4 shadow",
)

# App layout
app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    navbar,
    html.Div(id='page-content')
])


# ============================================================================
# PAGE 1: JOB SEEKER PORTAL (ENHANCED)
# ============================================================================

def create_job_seeker_page():
    # Calculate statistics
    total_jobs = len(df)
    jobs_by_source = df['source'].value_counts()
    
    # New jobs (posted within 3 days)
    three_days_ago = pd.Timestamp.now() - pd.Timedelta(days=3)
    new_jobs_count = len(df[df['scraped_at'] >= three_days_ago])
    
    # About to expire (deadline within 2 days)
    expiring_soon_count = len(df[df['days_to_deadline'].notna() & 
                                   (df['days_to_deadline'] <= 2) & 
                                   (df['days_to_deadline'] >= 0)])
    
    # Jobs with no deadline
    no_deadline_count = len(df[df['deadline'].isna()])
    
    return dbc.Container([
        # Hero Section
        dbc.Row([
            dbc.Col([
                html.Div([
                    html.H1([
                        html.I(className="fas fa-rocket me-3 text-primary"),
                        "Find Your Dream Job in Rwanda"
                    ], className="text-center mb-3", style={'fontWeight': '700'}),
                    html.P(f"Browse {total_jobs:,} active opportunities across Rwanda", 
                           className="text-center text-muted lead mb-4"),
                ], style={'padding': '2rem 0'})
            ])
        ]),
        
        # Statistics Cards Row (3 main metrics + 1 sources card)
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.Div([
                            html.I(className="fas fa-briefcase fa-3x text-primary mb-3"),
                            html.H2(f"{total_jobs:,}", className="mb-1", style={'fontWeight': '700'}),
                            html.P("Total Active Jobs", className="text-muted mb-0 small"),
                        ], className="text-center")
                    ])
                ], className="shadow-sm border-0 stat-card hover-lift")
            ], lg=3, md=4, sm=6, className="mb-3"),
            
            # NEW JOBS
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.Div([
                            html.I(className="fas fa-fire fa-3x text-danger mb-3"),
                            html.H2(f"{new_jobs_count}", className="mb-1", style={'fontWeight': '700'}),
                            html.P("New (Last 3 Days)", className="text-muted mb-0 small"),
                        ], className="text-center")
                    ])
                ], className="shadow-sm border-0 stat-card hover-lift")
            ], lg=3, md=4, sm=6, className="mb-3"),
            
            # EXPIRING SOON
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.Div([
                            html.I(className="fas fa-hourglass-end fa-3x text-warning mb-3"),
                            html.H2(f"{expiring_soon_count}", className="mb-1", style={'fontWeight': '700'}),
                            html.P("Expiring Soon (2 Days)", className="text-muted mb-0 small"),
                        ], className="text-center")
                    ])
                ], className="shadow-sm border-0 stat-card hover-lift")
            ], lg=3, md=4, sm=6, className="mb-3"),
            
            # Top Sources Card
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H6([
                            html.I(className="fas fa-chart-pie me-2 text-primary"),
                            "📊 Top Job Sources"
                        ], className="mb-3", style={'fontWeight': '600'}),
                        html.Div([
                            html.Div([
                                dbc.Badge(source, color="primary", className="me-2", pill=True),
                                html.Span(f"{count}", className="fw-bold text-dark")
                            ], className="mb-2 d-flex justify-content-between align-items-center")
                            for source, count in jobs_by_source.head(3).items()
                        ])
                    ])
                ], className="shadow-sm border-0 stat-card hover-lift", 
                style={'background': 'linear-gradient(135deg, #667eea15 0%, #764ba215 100%)'})
            ], lg=3, md=12, className="mb-3"),
        ], className="mb-4"),
        
        # Search Bar (Prominent)
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        dbc.InputGroup([
                            dbc.InputGroupText([
                                html.I(className="fas fa-search fa-lg")
                            ], style={'background': 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)', 
                                     'border': 'none', 'color': 'white'}),
                            dbc.Input(
                                id="search-input",
                                type="text",
                                placeholder="🔍 Search by job title, company, or keywords...",
                                className="search-box",
                                style={'fontSize': '1.1rem', 'border': 'none', 'padding': '0.8rem 1.5rem'}
                            ),
                        ], className="shadow", size="lg"),
                    ])
                ], className="border-0 shadow-lg mb-4")
            ])
        ]),
        
        # Filters Section
        dbc.Card([
            dbc.CardBody([
                html.H5([
                    html.I(className="fas fa-filter me-2"),
                    "Filters"
                ], className="mb-3"),
                
                dbc.Row([
                    dbc.Col([
                        dbc.Label([html.I(className="fas fa-industry me-2"), "Sector"]),
                        dcc.Dropdown(
                            id='sector-dropdown',
                            options=[{'label': '📂 All Sectors', 'value': 'all'}] + 
                                    [{'label': s, 'value': s} for s in sorted(df['sector'].dropna().unique())],
                            value='all',
                            clearable=False,
                            className="mb-2"
                        ),
                    ], lg=3, md=6, className="mb-3"),
                    
                    dbc.Col([
                        dbc.Label([html.I(className="fas fa-map-marker-alt me-2"), "Location"]),
                        dcc.Dropdown(
                            id='district-dropdown',
                            options=[{'label': '📍 All Locations', 'value': 'all'}] + 
                                    [{'label': d, 'value': d} for d in sorted(df['district'].dropna().unique())],
                            value='all',
                            clearable=False,
                            className="mb-2"
                        ),
                    ], lg=3, md=6, className="mb-3"),
                    
                    dbc.Col([
                        dbc.Label([html.I(className="fas fa-globe me-2"), "Source"]),
                        dcc.Dropdown(
                            id='source-dropdown',
                            options=[{'label': '🌐 All Sources', 'value': 'all'}] + 
                                    [{'label': s, 'value': s} for s in sorted(df['source'].unique())],
                            value='all',
                            clearable=False,
                            className="mb-2"
                        ),
                    ], lg=3, md=6, className="mb-3"),
                    
                    dbc.Col([
                        dbc.Label([html.I(className="fas fa-calendar me-2"), "Deadline"]),
                        dcc.Dropdown(
                            id='deadline-dropdown',
                            options=[
                                {'label': '⏰ All Deadlines', 'value': 'all'},
                                {'label': '🚨 Expiring Soon (2 days)', 'value': '2'},
                                {'label': '📅 This Week (7 days)', 'value': '7'},
                                {'label': '📆 This Month (30 days)', 'value': '30'},
                                {'label': '📊 Next 3 Months', 'value': '90'},
                            ],
                            value='all',
                            clearable=False,
                            className="mb-2"
                        ),
                    ], lg=3, md=6, className="mb-3"),
                ]),
                
                dbc.Row([
                    dbc.Col([
                        dbc.Button([
                            html.I(className="fas fa-sync-alt me-2"),
                            "Reset All Filters"
                        ], id="reset-btn", color="secondary", outline=True, size="sm"),
                    ]),
                ]),
            ])
        ], className="mb-4 shadow-sm border-0"),
        
        # Results Count
        dbc.Row([
            dbc.Col([
                html.Div(id="results-count", className="mb-3")
            ])
        ]),
        
        # MAIN LAYOUT: Left Sidebar + Job Cards
        dbc.Row([
            # LEFT SIDEBAR
            dbc.Col([
                # Quick Filters Card
                dbc.Card([
                    dbc.CardBody([
                        html.H5([
                            html.I(className="fas fa-sliders-h me-2"),
                            "Quick Filters"
                        ], className="mb-4", style={'fontWeight': '600'}),
                        
                        # Location Quick Filter
                        html.Div([
                            html.H6("📍 Location", className="mb-2", style={'fontSize': '0.9rem', 'fontWeight': '600'}),
                            dcc.Checklist(
                                id='quick-location-filter',
                                options=[
                                    {'label': ' Kigali', 'value': 'Kigali'},
                                    {'label': ' Huye', 'value': 'Huye'},
                                    {'label': ' Musanze', 'value': 'Musanze'},
                                    {'label': ' Rubavu', 'value': 'Rubavu'},
                                ],
                                value=[],
                                className="mb-3",
                                inputStyle={'marginRight': '8px'}
                            ),
                        ], className="mb-3"),
                        
                        # Sector Quick Filter
                        html.Div([
                            html.H6("💼 Sector", className="mb-2", style={'fontSize': '0.9rem', 'fontWeight': '600'}),
                            dcc.Checklist(
                                id='quick-sector-filter',
                                options=[
                                    {'label': ' IT & Tech', 'value': 'IT'},
                                    {'label': ' Healthcare', 'value': 'Healthcare'},
                                    {'label': ' Education', 'value': 'Education'},
                                    {'label': ' Finance', 'value': 'Finance'},
                                ],
                                value=[],
                                className="mb-3",
                                inputStyle={'marginRight': '8px'}
                            ),
                        ], className="mb-3"),
                        
                        # Trending Keywords
                        html.Div([
                            html.H6("🔥 Trending", className="mb-2", style={'fontSize': '0.9rem', 'fontWeight': '600'}),
                            html.Div([
                                dbc.Badge("Manager", color="light", text_color="dark", className="me-1 mb-2", pill=True, style={'cursor': 'pointer'}),
                                dbc.Badge("Developer", color="light", text_color="dark", className="me-1 mb-2", pill=True, style={'cursor': 'pointer'}),
                                dbc.Badge("Officer", color="light", text_color="dark", className="me-1 mb-2", pill=True, style={'cursor': 'pointer'}),
                                dbc.Badge("Nurse", color="light", text_color="dark", className="me-1 mb-2", pill=True, style={'cursor': 'pointer'}),
                                dbc.Badge("Driver", color="light", text_color="dark", className="me-1 mb-2", pill=True, style={'cursor': 'pointer'}),
                            ])
                        ], className="mb-3"),
                    ])
                ], className="shadow-sm border-0 mb-3", style={'position': 'sticky', 'top': '20px'}),
                
                # Contact Card (Bottom of Sidebar)
                dbc.Card([
                    dbc.CardBody([
                        html.H6([
                            html.I(className="fas fa-address-book me-2 text-primary"),
                            "Contact"
                        ], className="mb-3 text-center", style={'fontWeight': '600'}),
                        
                        html.Div([
                            html.A([
                                html.I(className="fab fa-whatsapp fa-2x text-success")
                            ], href="https://wa.me/250782765421", target="_blank", 
                               className="me-3", 
                               title="WhatsApp",
                               style={'textDecoration': 'none'}),
                            
                            html.A([
                                html.I(className="fas fa-envelope fa-2x text-danger")
                            ], href="mailto:ntwaridigabia@gmail.com", 
                               className="me-3",
                               title="Email",
                               style={'textDecoration': 'none'}),
                            
                            html.A([
                                html.I(className="fab fa-linkedin fa-2x text-primary")
                            ], href="https://www.linkedin.com/in/gabriel-ntwari/", target="_blank", 
                               className="me-3",
                               title="LinkedIn",
                               style={'textDecoration': 'none'}),
                            
                            html.A([
                                html.I(className="fab fa-x-twitter fa-2x", style={'color': '#000'})
                            ], href="https://x.com/ntwari_gabriel", target="_blank",
                               title="Twitter/X",
                               style={'textDecoration': 'none'}),
                        ], className="d-flex justify-content-center")
                    ])
                ], className="shadow-sm border-0", 
                   style={'position': 'sticky', 'top': 'calc(100vh - 200px)', 'background': 'linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%)'}),
                
            ], lg=2, md=3, className="mb-4"),
            
            # MAIN CONTENT (Job Cards)
            dbc.Col([
                html.Div(id="job-cards-container"),
            ], lg=10, md=9),
        ]),
        
    ], fluid=True, style={'maxWidth': '1600px'})


@callback(
    [Output("job-cards-container", "children"),
     Output("results-count", "children")],
    [Input("search-input", "value"),
     Input("sector-dropdown", "value"),
     Input("district-dropdown", "value"),
     Input("source-dropdown", "value"),
     Input("deadline-dropdown", "value"),
     Input("reset-btn", "n_clicks"),
     Input("quick-location-filter", "value"),
     Input("quick-sector-filter", "value")]
)
def update_job_cards(search, sector, district, source, deadline, reset_clicks, quick_locations, quick_sectors):
    # Reset filters if button clicked
    ctx = dash.callback_context
    if ctx.triggered and ctx.triggered[0]['prop_id'] == 'reset-btn.n_clicks':
        filtered_df = df.copy()
    else:
        filtered_df = df.copy()
        
        # FILTER OUT EXPIRED JOBS BY DEFAULT
        filtered_df = filtered_df[
            (filtered_df['days_to_deadline'].isna()) |  # No deadline = keep
            (filtered_df['days_to_deadline'] >= 0)       # Future deadline = keep
        ]
        
        # Apply other filters
        if search:
            mask = (filtered_df['title'].str.contains(search, case=False, na=False) |
                   filtered_df['company'].str.contains(search, case=False, na=False))
            filtered_df = filtered_df[mask]
        
        if sector != 'all':
            filtered_df = filtered_df[filtered_df['sector'] == sector]
        
        if district != 'all':
            filtered_df = filtered_df[filtered_df['district'] == district]
        
        if source != 'all':
            filtered_df = filtered_df[filtered_df['source'] == source]
        
        if deadline != 'all':
            days = int(deadline)
            filtered_df = filtered_df[filtered_df['days_to_deadline'] <= days]
        
        # QUICK FILTERS FROM SIDEBAR
        if quick_locations:
            filtered_df = filtered_df[filtered_df['district'].isin(quick_locations)]
        
        if quick_sectors:
            # Handle partial matching for sectors (e.g., "IT" matches "IT & Technology")
            sector_mask = filtered_df['sector'].str.contains('|'.join(quick_sectors), case=False, na=False)
            filtered_df = filtered_df[sector_mask]
    
    # Create job cards in PARALLEL layout (2 columns)
    cards_list = []
    
    for idx, job in filtered_df.head(100).iterrows():
        # Deadline badge with hours/minutes for urgent jobs
        deadline_badge = None
        deadline_text = ""
        
        if pd.notna(job['time_to_deadline']):
            total_seconds = job['time_to_deadline'].total_seconds()
            
            if total_seconds < 0:
                # Expired - skip this job (already filtered out, but just in case)
                continue
            elif total_seconds < 3600:  # Less than 1 hour
                minutes = int(total_seconds / 60)
                deadline_badge = dbc.Badge(
                    f"🚨 {minutes} minutes left!", 
                    color="danger", 
                    className="fw-bold pulse"
                )
                deadline_text = f"Deadline: {job['deadline'].strftime('%b %d, %Y %I:%M %p')}"
            elif total_seconds < 86400:  # Less than 24 hours (1 day)
                hours = int(total_seconds / 3600)
                minutes = int((total_seconds % 3600) / 60)
                deadline_badge = dbc.Badge(
                    f"🚨 {hours}h {minutes}m left!", 
                    color="danger", 
                    className="fw-bold"
                )
                deadline_text = f"Deadline: {job['deadline'].strftime('%b %d, %Y %I:%M %p')}"
            elif job['days_to_deadline'] <= 2:
                hours = int((total_seconds % 86400) / 3600)
                deadline_badge = dbc.Badge(
                    f"🚨 {int(job['days_to_deadline'])} days {hours}h left!", 
                    color="danger", 
                    className="fw-bold"
                )
                deadline_text = f"Deadline: {job['deadline'].strftime('%b %d, %Y')}"
            elif job['days_to_deadline'] <= 7:
                deadline_badge = dbc.Badge(
                    f"⏰ {int(job['days_to_deadline'])} days left", 
                    color="warning"
                )
                deadline_text = f"Deadline: {job['deadline'].strftime('%b %d, %Y')}"
            else:
                deadline_badge = dbc.Badge(
                    f"✅ {int(job['days_to_deadline'])} days left", 
                    color="success"
                )
                deadline_text = f"Deadline: {job['deadline'].strftime('%b %d, %Y')}"
        else:
            deadline_badge = dbc.Badge("No deadline", color="info", className="text-white")
            deadline_text = "No deadline specified"
        
        # Get education and experience values (always show with "Not Specified" if missing)
        education_value = job['education_level'] if (pd.notna(job['education_level']) and 
                                                      str(job['education_level']).upper() != 'EMPTY' and 
                                                      str(job['education_level']).strip() != '') else 'Not Specified'
        
        experience_value = (f"{job['experience_years']} years" if (pd.notna(job['experience_years']) and 
                                                                     str(job['experience_years']).upper() != 'EMPTY' and 
                                                                     str(job['experience_years']).strip() != '') 
                           else 'Not Specified')
        
        # Build job card
        card = dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    # Title
                    html.H5(job['title'], className="mb-2", 
                           style={'fontWeight': '600', 'color': '#1a202c', 'lineHeight': '1.3'}),
                    
                    # Company
                    html.Div([
                        html.I(className="fas fa-building me-2", style={'color': '#667eea'}),
                        html.Span(job['company'], style={'fontSize': '0.95rem', 'color': '#4a5568', 'fontWeight': '500'})
                    ], className="mb-3"),
                    
                    # Badges Row
                    html.Div([
                        dbc.Badge([
                            html.I(className="fas fa-briefcase me-1"),
                            job['sector'] if pd.notna(job['sector']) else 'General'
                        ], color="primary", className="me-2 mb-2", pill=True, 
                           style={'fontSize': '0.75rem', 'padding': '0.4rem 0.8rem'}),
                        
                        dbc.Badge([
                            html.I(className="fas fa-map-marker-alt me-1"),
                            job['district'] if pd.notna(job['district']) else 'Rwanda'
                        ], color="secondary", className="me-2 mb-2", pill=True,
                           style={'fontSize': '0.75rem', 'padding': '0.4rem 0.8rem'}),
                        
                        dbc.Badge([
                            html.I(className="fas fa-globe me-1"),
                            job['source']
                        ], color="light", text_color="dark", className="me-2 mb-2", pill=True,
                           style={'fontSize': '0.75rem', 'padding': '0.4rem 0.8rem'}),
                    ], className="mb-3"),
                    
                    # Education & Experience (ALWAYS show, with "Not Specified" if empty)
                    html.Div([
                        # Education
                        html.Div([
                            html.Span("Education: ", 
                                     style={'fontSize': '0.85rem', 'color': '#718096', 'fontWeight': '600'}),
                            html.Span(education_value,
                                     style={'fontSize': '0.85rem', 'color': '#2d3748' if education_value != 'Not Specified' else '#a0aec0'})
                        ], className="mb-2"),
                        
                        # Experience
                        html.Div([
                            html.Span("Experience: ", 
                                     style={'fontSize': '0.85rem', 'color': '#718096', 'fontWeight': '600'}),
                            html.Span(experience_value,
                                     style={'fontSize': '0.85rem', 'color': '#2d3748' if experience_value != 'Not Specified' else '#a0aec0'})
                        ], className="mb-2"),
                    ], className="mb-3"),
                    
                    # Deadline Badge
                    html.Div(deadline_badge, className="mb-3"),
                    
                    # Bottom section with Apply button and DEADLINE (not scraped date)
                    html.Div([
                        dbc.Button([
                            html.I(className="fas fa-external-link-alt me-2"),
                            "Job Details and Application"
                        ], 
                        href=job['source_url'], 
                        target="_blank",
                        color="primary", 
                        size="sm",
                        className="w-100",
                        style={
                            'borderRadius': '8px', 
                            'fontWeight': '600',
                            'padding': '0.6rem 1rem',
                            'background': 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
                            'border': 'none',
                            'transition': 'transform 0.2s'
                        }),
                        
                        # Show DEADLINE instead of scraped date
                        html.Div([
                            html.I(className="fas fa-calendar-alt me-1 text-muted", style={'fontSize': '0.7rem'}),
                            html.Span(deadline_text, 
                                     className="text-muted", 
                                     style={'fontSize': '0.75rem'})
                        ], className="text-center mt-2"),
                    ]),
                ])
            ], className="shadow-sm job-card border-0 h-100", 
               style={
                   'borderRadius': '16px', 
                   'border': '1px solid #e2e8f0',
                   'transition': 'all 0.3s ease'
               })
        ], lg=6, md=12, className="mb-4")
        
        cards_list.append(card)
    
    # Wrap cards in a Row for parallel layout
    cards_row = dbc.Row(cards_list) if cards_list else []
    
    # Results text
    results_text = html.Div([
        html.I(className="fas fa-check-circle me-2 text-success"),
        html.Span(f"Showing {len(cards_list)} active jobs", style={'fontSize': '1.1rem', 'fontWeight': '500'})
    ], className="mb-4")
    
    if not cards_list:
        return [
            dbc.Alert([
                html.I(className="fas fa-info-circle fa-2x mb-3"),
                html.H4("No active jobs found", className="alert-heading"),
                html.P("All jobs matching your filters have expired. Try removing some filters or check back later!"),
            ], color="info", className="text-center")
        ], html.Div([
            html.I(className="fas fa-times-circle me-2 text-warning"),
            html.Span("0 active jobs found", style={'fontSize': '1.1rem'})
        ])
    
    return cards_row, results_text


# ============================================================================
# PAGE 2: MARKET INSIGHTS (Keep the existing one - it's already good)
# ============================================================================

def create_market_insights_page():
    # Calculate stats
    jobs_by_sector = df['sector'].value_counts().reset_index()
    jobs_by_sector.columns = ['sector', 'count']
    
    jobs_by_source = df['source'].value_counts().reset_index()
    jobs_by_source.columns = ['source', 'count']
    
    jobs_by_district = df['district'].value_counts().reset_index()
    jobs_by_district.columns = ['district', 'count']
    
    # Timeline data
    df_timeline = df.groupby(df['scraped_at'].dt.date).size().reset_index()
    df_timeline.columns = ['Date', 'Jobs']
    
    # Top companies
    top_companies = df['company'].value_counts().head(10).reset_index()
    top_companies.columns = ['company', 'count']
    
    return dbc.Container([
        # Hero Section with Gradient Background
        dbc.Row([
            dbc.Col([
                html.Div([
                    html.H1([
                        html.I(className="fas fa-chart-line me-3"),
                        "📊 Rwanda Job Market Insights"
                    ], className="text-center mb-3", style={'fontWeight': '700', 'color': '#2c3e50'}),
                    html.P("Data-driven insights into the Rwandan job market", 
                           className="text-center text-muted lead mb-0"),
                ], style={
                    'padding': '2.5rem 1rem',
                    'background': 'linear-gradient(135deg, #667eea10 0%, #764ba210 100%)',
                    'borderRadius': '15px',
                    'marginBottom': '2rem',
                    'boxShadow': '0 4px 6px rgba(0,0,0,0.05)'
                })
            ])
        ]),
        
        # Key Metrics - Enhanced Cards with Icons
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.Div([
                            html.I(className="fas fa-briefcase fa-3x mb-3", 
                                  style={'color': '#667eea'}),
                            html.H2(f"{len(df):,}", className="mb-1", 
                                   style={'fontWeight': '700', 'color': '#2c3e50'}),
                            html.P("Total Active Jobs", className="text-muted mb-0 small"),
                        ], className="text-center")
                    ])
                ], className="shadow-sm border-0 hover-lift", 
                style={'borderLeft': '4px solid #667eea'})
            ], md=3, className="mb-3"),
            
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.Div([
                            html.I(className="fas fa-industry fa-3x mb-3", 
                                  style={'color': '#2ecc71'}),
                            html.H2(f"{len(jobs_by_sector)}", className="mb-1", 
                                   style={'fontWeight': '700', 'color': '#2c3e50'}),
                            html.P("Job Sectors", className="text-muted mb-0 small"),
                        ], className="text-center")
                    ])
                ], className="shadow-sm border-0 hover-lift", 
                style={'borderLeft': '4px solid #2ecc71'})
            ], md=3, className="mb-3"),
            
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.Div([
                            html.I(className="fas fa-globe fa-3x mb-3", 
                                  style={'color': '#3498db'}),
                            html.H2(f"{len(jobs_by_source)}", className="mb-1", 
                                   style={'fontWeight': '700', 'color': '#2c3e50'}),
                            html.P("Job Sources", className="text-muted mb-0 small"),
                        ], className="text-center")
                    ])
                ], className="shadow-sm border-0 hover-lift", 
                style={'borderLeft': '4px solid #3498db'})
            ], md=3, className="mb-3"),
            
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.Div([
                            html.I(className="fas fa-building fa-3x mb-3", 
                                  style={'color': '#f39c12'}),
                            html.H2(f"{len(top_companies)}", className="mb-1", 
                                   style={'fontWeight': '700', 'color': '#2c3e50'}),
                            html.P("Top Employers", className="text-muted mb-0 small"),
                        ], className="text-center")
                    ])
                ], className="shadow-sm border-0 hover-lift", 
                style={'borderLeft': '4px solid #f39c12'})
            ], md=3, className="mb-3"),
        ], className="mb-4"),
        
        # Charts Row 1 - Jobs by Sector & Source (Enhanced)
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader([
                        html.H5([
                            html.I(className="fas fa-chart-bar me-2 text-primary"),
                            "Jobs by Sector"
                        ], className="mb-0", style={'fontWeight': '600'})
                    ], style={'background': 'linear-gradient(135deg, #667eea15 0%, #764ba215 100%)', 
                             'border': 'none', 'borderRadius': '10px 10px 0 0'}),
                    dbc.CardBody([
                        dcc.Graph(
                            figure=px.bar(
                                jobs_by_sector,
                                x='count',
                                y='sector',
                                orientation='h',
                                labels={'sector': '', 'count': 'Number of Jobs'},
                                color='count',
                                color_continuous_scale=[[0, '#667eea'], [1, '#764ba2']]
                            ).update_layout(
                                showlegend=False, 
                                height=500,
                                plot_bgcolor='rgba(0,0,0,0)',
                                paper_bgcolor='rgba(0,0,0,0)',
                                font=dict(family="Segoe UI, system-ui, -apple-system", size=12, color='#2c3e50'),
                                margin=dict(l=20, r=20, t=20, b=20),
                                xaxis=dict(
                                    gridcolor='rgba(200,200,200,0.2)',
                                    showgrid=True,
                                    zeroline=False
                                ),
                                yaxis=dict(
                                    gridcolor='rgba(200,200,200,0.2)',
                                    showgrid=False
                                )
                            ).update_traces(
                                marker=dict(
                                    line=dict(width=0)
                                ),
                                hovertemplate='<b>%{y}</b><br>Jobs: %{x}<extra></extra>'
                            )
                        )
                    ])
                ], className="shadow-sm border-0", style={'borderRadius': '10px'})
            ], md=6, className="mb-4"),
            
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader([
                        html.H5([
                            html.I(className="fas fa-pie-chart me-2 text-success"),
                            "Jobs by Source"
                        ], className="mb-0", style={'fontWeight': '600'})
                    ], style={'background': 'linear-gradient(135deg, #2ecc7115 0%, #27ae6015 100%)', 
                             'border': 'none', 'borderRadius': '10px 10px 0 0'}),
                    dbc.CardBody([
                        dcc.Graph(
                            figure=px.pie(
                                jobs_by_source,
                                values='count',
                                names='source',
                                hole=0.45,
                                color_discrete_sequence=['#667eea', '#764ba2', '#f093fb', '#4facfe', 
                                                        '#43e97b', '#fa709a', '#fee140', '#30cfd0']
                            ).update_layout(
                                height=500,
                                plot_bgcolor='rgba(0,0,0,0)',
                                paper_bgcolor='rgba(0,0,0,0)',
                                font=dict(family="Segoe UI, system-ui, -apple-system", size=12, color='#2c3e50'),
                                margin=dict(l=20, r=20, t=20, b=20),
                                legend=dict(
                                    orientation="v",
                                    yanchor="middle",
                                    y=0.5,
                                    xanchor="left",
                                    x=1.02
                                )
                            ).update_traces(
                                textposition='inside',
                                textinfo='percent',
                                hovertemplate='<b>%{label}</b><br>Jobs: %{value}<br>Percentage: %{percent}<extra></extra>',
                                marker=dict(line=dict(color='white', width=2))
                            )
                        )
                    ])
                ], className="shadow-sm border-0", style={'borderRadius': '10px'})
            ], md=6, className="mb-4"),
        ]),
        
        # Charts Row 2 - Timeline (Enhanced)
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader([
                        html.H5([
                            html.I(className="fas fa-chart-line me-2 text-info"),
                            "Jobs Added Over Time"
                        ], className="mb-0", style={'fontWeight': '600'})
                    ], style={'background': 'linear-gradient(135deg, #3498db15 0%, #2980b915 100%)', 
                             'border': 'none', 'borderRadius': '10px 10px 0 0'}),
                    dbc.CardBody([
                        dcc.Graph(
                            figure=px.line(
                                df_timeline,
                                x='Date',
                                y='Jobs',
                                markers=True
                            ).update_layout(
                                height=400,
                                plot_bgcolor='rgba(0,0,0,0)',
                                paper_bgcolor='rgba(0,0,0,0)',
                                font=dict(family="Segoe UI, system-ui, -apple-system", size=12, color='#2c3e50'),
                                margin=dict(l=20, r=20, t=20, b=20),
                                xaxis=dict(
                                    gridcolor='rgba(200,200,200,0.2)',
                                    showgrid=True
                                ),
                                yaxis=dict(
                                    gridcolor='rgba(200,200,200,0.2)',
                                    showgrid=True,
                                    title='Number of Jobs'
                                )
                            ).update_traces(
                                line=dict(color='#3498db', width=3),
                                marker=dict(size=8, color='#2980b9', line=dict(width=2, color='white')),
                                hovertemplate='<b>%{x}</b><br>Jobs Added: %{y}<extra></extra>',
                                fill='tozeroy',
                                fillcolor='rgba(52, 152, 219, 0.1)'
                            )
                        )
                    ])
                ], className="shadow-sm border-0", style={'borderRadius': '10px'})
            ], md=12, className="mb-4"),
        ]),
        
        # Charts Row 3 - Top Companies & Locations (Enhanced)
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader([
                        html.H5([
                            html.I(className="fas fa-building me-2 text-success"),
                            "Top 10 Hiring Companies"
                        ], className="mb-0", style={'fontWeight': '600'})
                    ], style={'background': 'linear-gradient(135deg, #2ecc7115 0%, #27ae6015 100%)', 
                             'border': 'none', 'borderRadius': '10px 10px 0 0'}),
                    dbc.CardBody([
                        dcc.Graph(
                            figure=px.bar(
                                top_companies,
                                x='count',
                                y='company',
                                orientation='h',
                                labels={'company': '', 'count': 'Open Positions'},
                                color='count',
                                color_continuous_scale=[[0, '#2ecc71'], [1, '#27ae60']]
                            ).update_layout(
                                showlegend=False, 
                                height=500,
                                plot_bgcolor='rgba(0,0,0,0)',
                                paper_bgcolor='rgba(0,0,0,0)',
                                font=dict(family="Segoe UI, system-ui, -apple-system", size=12, color='#2c3e50'),
                                margin=dict(l=20, r=20, t=20, b=20),
                                xaxis=dict(gridcolor='rgba(200,200,200,0.2)'),
                                yaxis=dict(gridcolor='rgba(200,200,200,0.2)')
                            ).update_traces(
                                marker=dict(line=dict(width=0)),
                                hovertemplate='<b>%{y}</b><br>Positions: %{x}<extra></extra>'
                            )
                        )
                    ])
                ], className="shadow-sm border-0", style={'borderRadius': '10px'})
            ], md=6, className="mb-4"),
            
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader([
                        html.H5([
                            html.I(className="fas fa-map-marker-alt me-2 text-warning"),
                            "Top 10 Locations"
                        ], className="mb-0", style={'fontWeight': '600'})
                    ], style={'background': 'linear-gradient(135deg, #f39c1215 0%, #e67e2215 100%)', 
                             'border': 'none', 'borderRadius': '10px 10px 0 0'}),
                    dbc.CardBody([
                        dcc.Graph(
                            figure=px.bar(
                                jobs_by_district.head(10),
                                x='count',
                                y='district',
                                orientation='h',
                                labels={'district': '', 'count': 'Number of Jobs'},
                                color='count',
                                color_continuous_scale=[[0, '#f39c12'], [1, '#e67e22']]
                            ).update_layout(
                                showlegend=False, 
                                height=500,
                                plot_bgcolor='rgba(0,0,0,0)',
                                paper_bgcolor='rgba(0,0,0,0)',
                                font=dict(family="Segoe UI, system-ui, -apple-system", size=12, color='#2c3e50'),
                                margin=dict(l=20, r=20, t=20, b=20),
                                xaxis=dict(gridcolor='rgba(200,200,200,0.2)'),
                                yaxis=dict(gridcolor='rgba(200,200,200,0.2)')
                            ).update_traces(
                                marker=dict(line=dict(width=0)),
                                hovertemplate='<b>%{y}</b><br>Jobs: %{x}<extra></extra>'
                            )
                        )
                    ])
                ], className="shadow-sm border-0", style={'borderRadius': '10px'})
            ], md=6, className="mb-4"),
        ]),
        
        # Charts Row 4 - Top Sectors, Education & Job Level
        dbc.Row([
            # Top Sectors Chart (NEW)
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader([
                        html.H5([
                            html.I(className="fas fa-industry me-2 text-primary"),
                            "Top Hiring Sectors"
                        ], className="mb-0", style={'fontWeight': '600'})
                    ], style={'background': 'linear-gradient(135deg, #667eea15 0%, #764ba215 100%)', 
                             'border': 'none', 'borderRadius': '10px 10px 0 0'}),
                    dbc.CardBody([
                        dcc.Graph(
                            figure=px.bar(
                                jobs_by_sector.head(10),
                                x='count',
                                y='sector',
                                orientation='h',
                                labels={'sector': '', 'count': 'Number of Jobs'},
                                color='count',
                                color_continuous_scale=[[0, '#667eea'], [1, '#764ba2']]
                            ).update_layout(
                                showlegend=False,
                                height=400,
                                plot_bgcolor='rgba(0,0,0,0)',
                                paper_bgcolor='rgba(0,0,0,0)',
                                font=dict(family="Segoe UI, system-ui, -apple-system", size=12, color='#2c3e50'),
                                margin=dict(l=20, r=20, t=20, b=20),
                                xaxis=dict(gridcolor='rgba(200,200,200,0.2)'),
                                yaxis=dict(gridcolor='rgba(200,200,200,0.2)', title='')
                            ).update_traces(
                                marker=dict(line=dict(width=0)),
                                hovertemplate='<b>%{y}</b><br>Jobs: %{x}<extra></extra>'
                            )
                        )
                    ])
                ], className="shadow-sm border-0", style={'borderRadius': '10px'})
            ], md=4, className="mb-4"),
            
            # Education Level Chart
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader([
                        html.H5([
                            html.I(className="fas fa-graduation-cap me-2 text-success"),
                            "Education Requirements"
                        ], className="mb-0", style={'fontWeight': '600'})
                    ], style={'background': 'linear-gradient(135deg, #2ecc7115 0%, #27ae6015 100%)', 
                             'border': 'none', 'borderRadius': '10px 10px 0 0'}),
                    dbc.CardBody([
                        dcc.Graph(
                            figure=px.pie(
                                # Filter out EMPTY values before counting
                                df[df['education_level'].notna() & (df['education_level'] != '') & (df['education_level'].str.upper() != 'EMPTY')]['education_level'].value_counts().reset_index(),
                                values='count',
                                names='education_level',
                                hole=0.4,
                                color_discrete_sequence=['#2ecc71', '#27ae60', '#26de81', '#20bf6b']
                            ).update_layout(
                                height=400,
                                plot_bgcolor='rgba(0,0,0,0)',
                                paper_bgcolor='rgba(0,0,0,0)',
                                font=dict(family="Segoe UI, system-ui, -apple-system", size=12, color='#2c3e50'),
                                margin=dict(l=20, r=20, t=20, b=20)
                            ).update_traces(
                                textposition='inside',
                                textinfo='percent+label',
                                hovertemplate='<b>%{label}</b><br>Jobs: %{value}<br>%{percent}<extra></extra>'
                            )
                        )
                    ])
                ], className="shadow-sm border-0", style={'borderRadius': '10px'})
            ], md=4, className="mb-4"),
            
            # Job Level Chart (REPLACED experience_years)
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader([
                        html.H5([
                            html.I(className="fas fa-layer-group me-2 text-info"),
                            "Job Levels"
                        ], className="mb-0", style={'fontWeight': '600'})
                    ], style={'background': 'linear-gradient(135deg, #3498db15 0%, #2980b915 100%)', 
                             'border': 'none', 'borderRadius': '10px 10px 0 0'}),
                    dbc.CardBody([
                        dcc.Graph(
                            figure=px.bar(
                                # Filter out EMPTY values and get top levels
                                df[df['job_level'].notna() & (df['job_level'] != '') & (df['job_level'].str.upper() != 'EMPTY')]['job_level'].value_counts().head(10).reset_index(),
                                x='count',
                                y='job_level',
                                orientation='h',
                                labels={'job_level': '', 'count': 'Number of Jobs'},
                                color='count',
                                color_continuous_scale=[[0, '#3498db'], [1, '#2980b9']]
                            ).update_layout(
                                showlegend=False,
                                height=400,
                                plot_bgcolor='rgba(0,0,0,0)',
                                paper_bgcolor='rgba(0,0,0,0)',
                                font=dict(family="Segoe UI, system-ui, -apple-system", size=12, color='#2c3e50'),
                                margin=dict(l=20, r=20, t=20, b=20),
                                xaxis=dict(gridcolor='rgba(200,200,200,0.2)'),
                                yaxis=dict(gridcolor='rgba(200,200,200,0.2)', title='')
                            ).update_traces(
                                marker=dict(line=dict(width=0)),
                                hovertemplate='<b>%{y}</b><br>Jobs: %{x}<extra></extra>'
                            )
                        )
                    ])
                ], className="shadow-sm border-0", style={'borderRadius': '10px'})
            ], md=4, className="mb-4"),
        ]),
        
        # Charts Row 5 - Employment Type (Cleaned up categories)
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader([
                        html.H5([
                            html.I(className="fas fa-briefcase me-2 text-warning"),
                            "Employment Types"
                        ], className="mb-0", style={'fontWeight': '600'})
                    ], style={'background': 'linear-gradient(135deg, #f39c1215 0%, #e67e2215 100%)', 
                             'border': 'none', 'borderRadius': '10px 10px 0 0'}),
                    dbc.CardBody([
                        dcc.Graph(
                            figure=px.pie(
                                # Create a mapping for employment types to clean categories
                                df[df['employment_type'].notna() & (df['employment_type'] != '') & (df['employment_type'].str.upper() != 'EMPTY')]
                                .assign(
                                    clean_type=lambda x: x['employment_type'].str.lower().map(
                                        lambda val: 
                                            'Full-time/Permanent' if any(term in str(val).lower() for term in ['full', 'permanent', 'full-time']) else
                                            'Contract' if 'contract' in str(val).lower() else
                                            'Consultancy' if 'consult' in str(val).lower() else
                                            'Internship' if 'intern' in str(val).lower() else
                                            'Volunteer' if 'volunteer' in str(val).lower() else
                                            'Tender' if 'tender' in str(val).lower() else
                                            'Others'
                                    )
                                )['clean_type'].value_counts().reset_index(),
                                values='count',
                                names='clean_type',
                                hole=0.4,
                                color_discrete_sequence=['#3498db', '#2ecc71', '#f39c12', '#e74c3c', '#9b59b6', '#1abc9c', '#95a5a6'],
                                category_orders={'clean_type': ['Full-time/Permanent', 'Contract', 'Consultancy', 'Internship', 'Tender', 'Volunteer', 'Others']}
                            ).update_layout(
                                height=400,
                                plot_bgcolor='rgba(0,0,0,0)',
                                paper_bgcolor='rgba(0,0,0,0)',
                                font=dict(family="Segoe UI, system-ui, -apple-system", size=12, color='#2c3e50'),
                                margin=dict(l=20, r=20, t=20, b=20),
                                legend=dict(orientation="v", yanchor="middle", y=0.5, xanchor="left", x=1.02)
                            ).update_traces(
                                textposition='inside',
                                textinfo='percent+label',
                                hovertemplate='<b>%{label}</b><br>Jobs: %{value}<br>%{percent}<extra></extra>'
                            )
                        )
                    ])
                ], className="shadow-sm border-0", style={'borderRadius': '10px'})
            ], md=6, className="mb-4"),
        ]),
        
        # Market Summary
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader([
                        html.H5([
                            html.I(className="fas fa-chart-line me-2"),
                            "📈 Market Summary"
                        ], className="mb-0", style={'fontWeight': '600'})
                    ], style={'background': 'linear-gradient(135deg, #f39c1215 0%, #e67e2215 100%)', 'border': 'none'}),
                    dbc.CardBody([
                        html.P([
                            html.Strong("Most Active Sector: "),
                            f"{jobs_by_sector.iloc[0]['sector']} ({jobs_by_sector.iloc[0]['count']} jobs)"
                        ], className="mb-2"),
                        html.P([
                            html.Strong("Leading Job Source: "),
                            f"{jobs_by_source.iloc[0]['source']} ({jobs_by_source.iloc[0]['count']} jobs)"
                        ], className="mb-2"),
                        html.P([
                            html.Strong("Top Hiring Location: "),
                            f"{jobs_by_district.iloc[0]['district']} ({jobs_by_district.iloc[0]['count']} jobs)"
                        ], className="mb-2"),
                        html.P([
                            html.Strong("Total Active Jobs: "),
                            f"{len(df):,} opportunities"
                        ], className="mb-0"),
                    ])
                ], className="shadow-sm border-0", style={'borderRadius': '10px'})
            ], md=12, className="mb-4"),
        ]),
        
    ], fluid=True)


# ============================================================================
# PAGE 3: HISTORICAL JOBS (Placeholder)
# ============================================================================

def create_historical_page():
    return dbc.Container([
        dbc.Row([
            dbc.Col([
                html.H1("📜 Historical Jobs", className="text-center mb-4"),
                html.P("Explore historical job postings using Wayback Machine", 
                       className="text-center text-muted mb-4"),
            ])
        ]),
        
        dbc.Alert([
            html.H4("🚧 Coming Soon!", className="alert-heading"),
            html.P("This page will feature:"),
            html.Ul([
                html.Li("Historical job data from archived websites"),
                html.Li("Integration with Wayback Machine API"),
                html.Li("Trend analysis over multiple years"),
                html.Li("Company hiring history"),
                html.Li("Salary evolution tracking"),
            ]),
            html.Hr(),
            html.P("Check back soon for this feature!", className="mb-0"),
        ], color="info"),
        
    ], fluid=True)


# ============================================================================
# URL ROUTING
# ============================================================================

@callback(Output('page-content', 'children'),
          Input('url', 'pathname'))
def display_page(pathname):
    if pathname == '/insights':
        return create_market_insights_page()
    elif pathname == '/historical':
        return create_historical_page()
    else:
        return create_job_seeker_page()


# ============================================================================
# RUN APP
# ============================================================================

if __name__ == '__main__':
    print("\n" + "="*70)
    print("Opening dashboard at: http://localhost:8050")
    print("="*70 + "\n")
    debug_mode = os.getenv("DASH_DEBUG", "false").lower() == "true"
    app.run(debug=debug_mode, host="0.0.0.0", port=int(os.getenv("PORT", 8050)))
