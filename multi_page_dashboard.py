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
from functools import lru_cache
import threading
import time

# Database connection
DATABASE_URL = os.getenv('DATABASE_URL')

# Cache the engine - one connection pool, never recreated
@lru_cache(maxsize=1)
def _get_engine():
    return create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        pool_size=1,        # free tier: 1 connection is enough
        max_overflow=0,     # no extra connections
        connect_args={"connect_timeout": 10},
    )

# In-memory cache: avoids DB query on every page load/interaction
_data_cache = {"df": None, "ts": 0}
_cache_lock = threading.Lock()
CACHE_TTL = 60   # refresh every 1 minute

def get_jobs_data():
    """Fetch jobs from DB, cached for 5 minutes"""
    with _cache_lock:
        if _data_cache["df"] is not None and (time.time() - _data_cache["ts"]) < CACHE_TTL:
            return _data_cache["df"]
    engine = _get_engine()
    
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
    
    # Vectorised: set midnight deadlines to 11:59 PM (no slow row-by-row loop)
    midnight_mask = (
        df['deadline'].notna() &
        (df['deadline'].dt.hour == 0) &
        (df['deadline'].dt.minute == 0) &
        (df['deadline'].dt.second == 0)
    )
    df.loc[midnight_mask, 'deadline'] = (
        df.loc[midnight_mask, 'deadline'] + pd.Timedelta(hours=23, minutes=59, seconds=59)
    )
    
    # Calculate time until deadline (full timedelta, not just days)
    df['time_to_deadline'] = df['deadline'] - pd.Timestamp.now()
    df['days_to_deadline'] = df['time_to_deadline'].dt.days

    with _cache_lock:
        _data_cache["df"] = df
        _data_cache["ts"] = time.time()

    return df

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
            @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&display=swap');
            *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
            html { font-size: 16px; }

            body {
                background: #f7f8fc;
                font-family: 'Inter', sans-serif;
                color: #111827;
                min-height: 100vh;
            }

            /* ══ NAVBAR ══ */
            .top-navbar {
                background: #111827;
                padding: 0 3rem;
                height: 64px;
                display: flex;
                align-items: center;
                justify-content: space-between;
                position: sticky;
                top: 0;
                z-index: 1000;
                box-shadow: 0 1px 0 rgba(255,255,255,0.06);
            }
            .nav-brand {
                font-size: 1.25rem;
                font-weight: 800;
                color: white;
                text-decoration: none;
                display: flex;
                align-items: center;
                gap: 10px;
                letter-spacing: -0.3px;
            }
            .nav-brand span { color: #818cf8; }
            .nav-links { display: flex; gap: 4px; }
            .nav-pill {
                color: rgba(255,255,255,0.65);
                text-decoration: none;
                font-size: 0.9rem;
                font-weight: 500;
                padding: 0.45rem 1rem;
                border-radius: 8px;
                transition: all 0.15s;
                display: flex;
                align-items: center;
                gap: 6px;
            }
            .nav-pill:hover { background: rgba(255,255,255,0.08); color: white; }
            .nav-pill.active { background: #4f46e5; color: white; }

            /* ══ PAGE SHELL ══ */
            #page-content {
                padding: 2.5rem 3rem !important;
                max-width: 1600px;
                margin: 0 auto;
            }

            /* ══ HERO ══ */
            .hero-title {
                font-size: clamp(2rem, 3.5vw, 3.2rem) !important;
                font-weight: 900 !important;
                color: #111827 !important;
                letter-spacing: -1px;
                line-height: 1.15 !important;
                margin-bottom: 0.6rem !important;
            }
            .hero-sub {
                font-size: clamp(1rem, 1.5vw, 1.2rem) !important;
                color: #6b7280 !important;
                font-weight: 400 !important;
            }

            /* ══ STAT CARDS ══ */
            .stat-card {
                background: white !important;
                border: 1px solid #e5e7eb !important;
                border-radius: 16px !important;
                transition: all 0.2s ease;
                height: 100%;
            }
            .stat-card:hover {
                border-color: #818cf8 !important;
                box-shadow: 0 8px 30px rgba(79,70,229,0.12) !important;
                transform: translateY(-2px);
            }
            .stat-card .card-body { padding: 1.75rem !important; }
            .stat-number {
                font-size: clamp(2rem, 3vw, 2.8rem) !important;
                font-weight: 900 !important;
                color: #111827 !important;
                line-height: 1 !important;
                letter-spacing: -1px;
                display: block;
            }
            .stat-label {
                font-size: 0.9rem !important;
                color: #6b7280 !important;
                font-weight: 500 !important;
                margin-top: 0.4rem !important;
            }

            /* ══ SEARCH ══ */
            .search-hero {
                background: white;
                border: 2px solid #e5e7eb;
                border-radius: 16px;
                padding: 0.4rem 0.4rem 0.4rem 1.5rem;
                display: flex;
                align-items: center;
                gap: 1rem;
                transition: border-color 0.2s;
            }
            .search-hero:focus-within {
                border-color: #4f46e5;
                box-shadow: 0 0 0 4px rgba(79,70,229,0.08);
            }
            .search-hero input {
                border: none !important;
                outline: none !important;
                box-shadow: none !important;
                font-size: 1.05rem !important;
                color: #111827 !important;
                background: transparent !important;
                flex: 1;
                padding: 0.6rem 0 !important;
            }
            .search-hero input::placeholder { color: #9ca3af; }
            .search-btn {
                background: #4f46e5 !important;
                border: none !important;
                border-radius: 12px !important;
                color: white !important;
                font-weight: 600 !important;
                font-size: 0.95rem !important;
                padding: 0.7rem 1.75rem !important;
                white-space: nowrap;
                transition: background 0.2s !important;
            }
            .search-btn:hover { background: #4338ca !important; }

            /* ══ FILTER ROW ══ */
            .filter-row {
                background: white;
                border: 1px solid #e5e7eb;
                border-radius: 16px;
                padding: 1.25rem 1.5rem;
                display: flex;
                align-items: center;
                gap: 1rem;
                flex-wrap: wrap;
            }
            .filter-label {
                font-size: 0.85rem;
                font-weight: 600;
                color: #374151;
                white-space: nowrap;
            }
            #reset-btn {
                background: transparent !important;
                border: 1.5px solid #d1d5db !important;
                color: #374151 !important;
                border-radius: 8px !important;
                font-size: 0.85rem !important;
                font-weight: 600 !important;
                padding: 0.4rem 1rem !important;
                white-space: nowrap;
                transition: all 0.15s !important;
            }
            #reset-btn:hover { border-color: #4f46e5 !important; color: #4f46e5 !important; }

            /* ══ RESULTS BAR ══ */
            .results-bar {
                display: flex;
                align-items: center;
                justify-content: space-between;
                margin-bottom: 1.5rem;
            }
            .results-count {
                font-size: 1rem;
                font-weight: 600;
                color: #374151;
            }
            .results-count span { color: #4f46e5; }

            /* ══ SIDEBAR ══ */
            .sidebar-panel {
                background: white;
                border: 1px solid #e5e7eb;
                border-radius: 16px;
                padding: 1.5rem;
                position: sticky;
                top: 84px;
            }
            .sidebar-section-title {
                font-size: 0.75rem;
                font-weight: 700;
                color: #9ca3af;
                text-transform: uppercase;
                letter-spacing: 0.08em;
                margin-bottom: 0.75rem;
                margin-top: 1.25rem;
            }
            .sidebar-section-title:first-child { margin-top: 0; }
            .form-check-label {
                font-size: 0.92rem !important;
                color: #374151 !important;
                font-weight: 500 !important;
            }
            .form-check-input:checked { background-color: #4f46e5 !important; border-color: #4f46e5 !important; }
            .form-check { margin-bottom: 0.5rem !important; }

            /* ══ JOB CARDS ══ */
            .job-card {
                background: white !important;
                border: 1.5px solid #e5e7eb !important;
                border-radius: 16px !important;
                transition: all 0.2s ease;
                height: 100%;
                display: flex;
                flex-direction: column;
            }
            .job-card:hover {
                border-color: #818cf8 !important;
                box-shadow: 0 10px 30px rgba(79,70,229,0.12) !important;
                transform: translateY(-3px);
            }
            .job-card .card-body {
                padding: 1.5rem !important;
                display: flex;
                flex-direction: column;
                flex: 1;
            }
            .job-title {
                font-size: 1.05rem !important;
                font-weight: 700 !important;
                color: #111827 !important;
                line-height: 1.4 !important;
                margin-bottom: 0.35rem !important;
            }
            .job-company {
                font-size: 0.9rem !important;
                color: #6b7280 !important;
                font-weight: 500 !important;
                margin-bottom: 1rem !important;
                display: flex;
                align-items: center;
                gap: 5px;
            }
            .job-badge {
                font-size: 0.78rem !important;
                font-weight: 600 !important;
                padding: 0.3em 0.8em !important;
                border-radius: 6px !important;
                border: 1.5px solid;
            }
            .badge-sector { background: #eef2ff !important; color: #4f46e5 !important; border-color: #c7d2fe !important; }
            .badge-location { background: #f0fdf4 !important; color: #16a34a !important; border-color: #bbf7d0 !important; }
            .badge-source { background: #f9fafb !important; color: #6b7280 !important; border-color: #e5e7eb !important; }
            .badge-urgent { background: #fef2f2 !important; color: #dc2626 !important; border-color: #fecaca !important; }
            .badge-warning { background: #fffbeb !important; color: #d97706 !important; border-color: #fde68a !important; }
            .badge-ok { background: #f0fdf4 !important; color: #16a34a !important; border-color: #bbf7d0 !important; }
            .job-meta {
                font-size: 0.85rem !important;
                color: #6b7280 !important;
                margin-bottom: 0.3rem !important;
            }
            .job-meta strong { color: #374151; font-weight: 600; }
            .apply-btn {
                background: #111827 !important;
                border: none !important;
                border-radius: 10px !important;
                color: white !important;
                font-weight: 600 !important;
                font-size: 0.9rem !important;
                padding: 0.65rem 1rem !important;
                width: 100%;
                transition: background 0.2s !important;
                margin-top: auto;
            }
            .apply-btn:hover { background: #4f46e5 !important; }
            .deadline-line {
                font-size: 0.78rem !important;
                color: #9ca3af !important;
                text-align: center;
                margin-top: 0.5rem !important;
            }

            /* ══ LOAD MORE ══ */
            .load-more-btn {
                background: white !important;
                border: 1.5px solid #e5e7eb !important;
                border-radius: 12px !important;
                color: #374151 !important;
                font-weight: 600 !important;
                font-size: 0.95rem !important;
                padding: 0.85rem !important;
                width: 100%;
                transition: all 0.2s !important;
                margin-top: 1rem;
            }
            .load-more-btn:hover {
                border-color: #4f46e5 !important;
                color: #4f46e5 !important;
                box-shadow: 0 4px 12px rgba(79,70,229,0.1) !important;
            }

            /* ══ CONTACT CARD ══ */
            .contact-card {
                background: white;
                border: 1px solid #e5e7eb;
                border-radius: 16px;
                padding: 1.25rem;
                margin-top: 1rem;
            }
            .contact-title {
                font-size: 0.75rem;
                font-weight: 700;
                color: #9ca3af;
                text-transform: uppercase;
                letter-spacing: 0.08em;
                margin-bottom: 1rem;
            }
            .contact-icons { display: flex; gap: 8px; }
            .contact-icon {
                width: 40px; height: 40px;
                border-radius: 10px;
                display: flex; align-items: center; justify-content: center;
                font-size: 1.1rem;
                color: white;
                text-decoration: none;
                transition: transform 0.15s, opacity 0.15s;
            }
            .contact-icon:hover { transform: scale(1.1); opacity: 0.9; }

            /* ══ FOOTER ══ */
            .page-footer {
                border-top: 1px solid #e5e7eb;
                margin-top: 3rem;
                padding: 1.5rem 0 0.5rem;
                display: flex;
                justify-content: center;
                align-items: center;
                gap: 1.5rem;
                flex-wrap: wrap;
            }
            .footer-text { font-size: 0.85rem; color: #9ca3af; }
            .footer-link { font-size: 0.85rem; color: #6b7280; text-decoration: none; font-weight: 500; }
            .footer-link:hover { color: #4f46e5; }

            /* ══ TRENDING BADGES ══ */
            .trend-badge {
                display: inline-block;
                background: #f3f4f6;
                color: #374151;
                font-size: 0.8rem;
                font-weight: 600;
                padding: 0.3em 0.75em;
                border-radius: 6px;
                margin: 0 4px 6px 0;
                cursor: pointer;
                transition: all 0.15s;
                text-decoration: none;
            }
            .trend-badge:hover { background: #eef2ff; color: #4f46e5; }

            /* ══ SCROLLBAR ══ */
            ::-webkit-scrollbar { width: 6px; }
            ::-webkit-scrollbar-track { background: #f7f8fc; }
            ::-webkit-scrollbar-thumb { background: #c7d2fe; border-radius: 3px; }

            /* ══ DASH DROPDOWN OVERRIDES ══ */
            .Select-control { border: 1px solid #e5e7eb !important; border-radius: 8px !important; font-size: 0.9rem !important; }
            .Select-control:hover { border-color: #4f46e5 !important; }
            .Select-value-label, .Select-placeholder { font-size: 0.9rem !important; color: #374151 !important; }

            /* ══ ANIMATIONS ══ */
            .deadline-urgent { animation: pulse 1.5s infinite; }
            @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.5} }

            @keyframes fadeUp {
                from { opacity: 0; transform: translateY(12px); }
                to   { opacity: 1; transform: translateY(0); }
            }
            .job-card { animation: fadeUp 0.3s ease both; }
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

# Navbar — dark modern navbar
navbar = html.Div([
    html.A([
        html.I(className="fas fa-briefcase", style={'color':'#818cf8'}),
        html.Span([" Rwanda", html.Span("Jobs", style={'color':'#818cf8'}), " Portal"])
    ], href="/", className="nav-brand"),
    html.Div([
        dcc.Link([html.I(className="fas fa-search me-1"), " Find Jobs"],
                 href="/", className="nav-pill", id="nav-jobs"),
        dcc.Link([html.I(className="fas fa-chart-line me-1"), " Market Insights"],
                 href="/insights", className="nav-pill", id="nav-insights"),
        dcc.Link([html.I(className="fas fa-history me-1"), " Historical"],
                 href="/historical", className="nav-pill", id="nav-historical"),
    ], className="nav-links"),
], className="top-navbar")
# App layout
app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    dcc.Store(id='cards-shown', data=9),
    navbar,
    html.Div(
        id='page-content',
        style={"width": "100%", "padding": "1.5rem 2rem"}
    )
], style={"width": "100%", "minHeight": "100vh"})


# ============================================================================
# PAGE 1: JOB SEEKER PORTAL (ENHANCED)
# ============================================================================

def create_job_seeker_page():
    df = get_jobs_data()  # uses 5-min cache
    # Only count jobs with a future deadline (matches job cards filter)
    active_df = df[df['days_to_deadline'].notna() & (df['days_to_deadline'] >= 0)]
    total_jobs = len(active_df)
    jobs_by_source = active_df['source'].value_counts()

    # New jobs (posted within 3 days, with valid deadline)
    three_days_ago = pd.Timestamp.now() - pd.Timedelta(days=3)
    new_jobs_count = len(active_df[active_df['scraped_at'] >= three_days_ago])

    # About to expire (deadline within 2 days)
    expiring_soon_count = len(active_df[
                                   (active_df['days_to_deadline'] <= 2) &
                                   (active_df['days_to_deadline'] >= 0)])

    
    return html.Div([

        # ── HERO ──
        html.Div([
            html.H1([
                "Find Your Dream Job ",
                html.Span("in Rwanda", style={'color':'#4f46e5'})
            ], className="hero-title"),
            html.P(
                f"Browse {total_jobs:,} active opportunities · Updated daily from top Rwanda job boards",
                className="hero-sub"
            ),
        ], style={'paddingBottom':'2rem','borderBottom':'1px solid #e5e7eb','marginBottom':'2rem'}),

        # ── STAT CARDS ──
        dbc.Row([
            dbc.Col([
                dbc.Card([dbc.CardBody([
                    html.Div([
                        html.Div(html.I(className="fas fa-briefcase", style={'fontSize':'1.3rem','color':'#4f46e5'}),
                            style={'width':'48px','height':'48px','borderRadius':'12px','background':'#eef2ff',
                                   'display':'flex','alignItems':'center','justifyContent':'center','marginBottom':'1rem'}),
                        html.Div(f"{total_jobs:,}", className="stat-number"),
                        html.P("Total Active Jobs", className="stat-label"),
                    ])
                ])], className="border-0 stat-card h-100")
            ], lg=3, md=6, sm=6, className="mb-3"),

            dbc.Col([
                dbc.Card([dbc.CardBody([
                    html.Div([
                        html.Div(html.I(className="fas fa-bolt", style={'fontSize':'1.3rem','color':'#db2777'}),
                            style={'width':'48px','height':'48px','borderRadius':'12px','background':'#fdf2f8',
                                   'display':'flex','alignItems':'center','justifyContent':'center','marginBottom':'1rem'}),
                        html.Div(f"{new_jobs_count}", className="stat-number"),
                        html.P("New (Last 3 Days)", className="stat-label"),
                    ])
                ])], className="border-0 stat-card h-100")
            ], lg=3, md=6, sm=6, className="mb-3"),

            dbc.Col([
                dbc.Card([dbc.CardBody([
                    html.Div([
                        html.Div(html.I(className="fas fa-clock", style={'fontSize':'1.3rem','color':'#d97706'}),
                            style={'width':'48px','height':'48px','borderRadius':'12px','background':'#fffbeb',
                                   'display':'flex','alignItems':'center','justifyContent':'center','marginBottom':'1rem'}),
                        html.Div(f"{expiring_soon_count}", className="stat-number"),
                        html.P("Expiring in 2 Days", className="stat-label"),
                    ])
                ])], className="border-0 stat-card h-100")
            ], lg=3, md=6, sm=6, className="mb-3"),

            dbc.Col([
                dbc.Card([dbc.CardBody([
                    html.Div([
                        html.Div(html.I(className="fas fa-globe", style={'fontSize':'1.3rem','color':'#0891b2'}),
                            style={'width':'48px','height':'48px','borderRadius':'12px','background':'#ecfeff',
                                   'display':'flex','alignItems':'center','justifyContent':'center','marginBottom':'1rem'}),
                        html.P("Top Sources", className="stat-label", style={'marginTop':'0','marginBottom':'0.6rem','fontSize':'0.78rem','textTransform':'uppercase','letterSpacing':'0.05em','fontWeight':'700'}),
                        html.Div([
                            html.Div([
                                html.Span(source, style={'fontSize':'0.85rem','fontWeight':'600','color':'#374151'}),
                                html.Span(f"{count}", style={'fontSize':'0.85rem','fontWeight':'700','color':'#4f46e5'}),
                            ], style={'display':'flex','justifyContent':'space-between','marginBottom':'0.35rem'})
                            for source, count in jobs_by_source.head(3).items()
                        ])
                    ])
                ])], className="border-0 stat-card h-100")
            ], lg=3, md=6, sm=6, className="mb-3"),
        ], className="mb-3 align-items-stretch g-3"),

        # ── SEARCH BAR ──
        html.Div([
            html.Div([
                html.I(className="fas fa-search", style={'color':'#9ca3af','fontSize':'1.1rem','flexShrink':'0'}),
                dbc.Input(
                    id="search-input",
                    type="text",
                    placeholder="Search job title, company, or keyword...",
                    style={'border':'none','outline':'none','boxShadow':'none',
                           'fontSize':'1.05rem','background':'transparent','flex':'1','padding':'0.6rem 0'}
                ),
                dbc.Button([html.I(className="fas fa-search me-2"), "Search"],
                           id="search-btn-vis", className="search-btn"),
            ], className="search-hero"),
        ], style={'marginBottom':'1rem'}),

        # ── FILTER ROW ──
        html.Div([
            html.Span("Filters:", className="filter-label"),
            html.Div([
                dcc.Dropdown(id='sector-dropdown',
                    options=[{'label':'All Sectors','value':'all'}] +
                            [{'label':s,'value':s} for s in ['IT & Technology','Healthcare','Education',
                             'Finance','Agriculture','Construction','Logistics','HR','Legal','NGO / Development']],
                    value='all', clearable=False,
                    style={'minWidth':'160px','fontSize':'0.9rem'}),
            ]),
            html.Div([
                dcc.Dropdown(id='district-dropdown',
                    options=[{'label':'All Locations','value':'all'}] +
                            [{'label':d,'value':d} for d in ['Kigali','Huye','Musanze','Rubavu','Nyagatare','Muhanga']],
                    value='all', clearable=False,
                    style={'minWidth':'155px','fontSize':'0.9rem'}),
            ]),
            html.Div([
                dcc.Dropdown(id='source-dropdown',
                    options=[{'label':'All Sources','value':'all'}] +
                            [{'label':s,'value':s} for s in ['jobinrwanda','impactpool','musuratool','greatrwandajobs']],
                    value='all', clearable=False,
                    style={'minWidth':'160px','fontSize':'0.9rem'}),
            ]),
            html.Div([
                dcc.Dropdown(id='deadline-dropdown',
                    options=[
                        {'label':'All Deadlines','value':'all'},
                        {'label':'Expiring Soon (2 days)','value':'2'},
                        {'label':'This Week (7 days)','value':'7'},
                        {'label':'This Month (30 days)','value':'30'},
                    ],
                    value='all', clearable=False,
                    style={'minWidth':'185px','fontSize':'0.9rem'}),
            ]),
            dbc.Button([html.I(className="fas fa-rotate-left me-1"), " Reset"],
                       id="reset-btn", n_clicks=0),
        ], className="filter-row", style={'marginBottom':'2rem'}),

        # ── MAIN LAYOUT ──
        dbc.Row([
            # SIDEBAR
            dbc.Col([
                html.Div([
                    # Quick Location
                    html.P("Location", className="sidebar-section-title"),
                    dcc.Checklist(
                        id='quick-location-filter',
                        options=[
                            {'label':' Kigali','value':'Kigali'},
                            {'label':' Huye','value':'Huye'},
                            {'label':' Musanze','value':'Musanze'},
                            {'label':' Rubavu','value':'Rubavu'},
                        ],
                        value=[], className="mb-1",
                        inputStyle={'marginRight':'8px','accentColor':'#4f46e5'}
                    ),
                    # Quick Sector
                    html.P("Sector", className="sidebar-section-title"),
                    dcc.Checklist(
                        id='quick-sector-filter',
                        options=[
                            {'label':' IT & Tech','value':'IT'},
                            {'label':' Healthcare','value':'Healthcare'},
                            {'label':' Education','value':'Education'},
                            {'label':' Finance','value':'Finance'},
                        ],
                        value=[], className="mb-1",
                        inputStyle={'marginRight':'8px','accentColor':'#4f46e5'}
                    ),
                    # Trending
                    html.P("Trending", className="sidebar-section-title"),
                    html.Div([
                        html.Span("Manager", className="trend-badge"),
                        html.Span("Developer", className="trend-badge"),
                        html.Span("Officer", className="trend-badge"),
                        html.Span("Nurse", className="trend-badge"),
                        html.Span("Driver", className="trend-badge"),
                    ]),
                ], className="sidebar-panel"),

                # Contact
                html.Div([
                    html.P("Get in touch", className="contact-title"),
                    html.Div([
                        html.A(html.I(className="fab fa-whatsapp"),
                               href="https://wa.me/250782765421", target="_blank",
                               className="contact-icon", style={'background':'#25D366'},
                               title="WhatsApp"),
                        html.A(html.I(className="fas fa-envelope"),
                               href="mailto:ntwaridigabia@gmail.com",
                               className="contact-icon", style={'background':'#EA4335'},
                               title="Email"),
                        html.A(html.I(className="fab fa-linkedin-in"),
                               href="https://www.linkedin.com/in/gabriel-ntwari/", target="_blank",
                               className="contact-icon", style={'background':'#0077B5'},
                               title="LinkedIn"),
                        html.A(html.I(className="fab fa-x-twitter"),
                               href="https://x.com/ntwari_gabriel", target="_blank",
                               className="contact-icon", style={'background':'#111827'},
                               title="Twitter/X"),
                    ], className="contact-icons"),
                ], className="contact-card"),

            ], width="auto", style={"width":"240px","flexShrink":"0"}),

            # JOB CARDS
            dbc.Col([
                html.Div(id="results-count", className="results-bar", style={'marginBottom':'1.25rem'}),
                html.Div(id="job-cards-container"),
                html.Div([
                    dbc.Button([
                        html.I(className="fas fa-chevron-down me-2"),
                        html.Span(id="load-more-label", children="Load More Jobs")
                    ], id="load-more-btn", n_clicks=0, className="load-more-btn")
                ], id="load-more-container"),
            ], style={"flex":"1","minWidth":"0"}),
        ], style={'display':'flex','gap':'1.5rem','alignItems':'flex-start'}),

        # ── FOOTER ──
        html.Div([
            html.Span("© 2025 Rwanda Jobs Portal", className="footer-text"),
            html.A("Built by Gabriel Ntwari", href="https://www.linkedin.com/in/gabriel-ntwari/",
                   target="_blank", className="footer-link"),
            html.A([html.I(className="fab fa-whatsapp me-1"), "WhatsApp"],
                   href="https://wa.me/250782765421", target="_blank", className="footer-link"),
        ], className="page-footer"),

    ], style={'width':'100%'})


@callback(
    Output("cards-shown", "data"),
    Input("load-more-btn", "n_clicks"),
    State("cards-shown", "data"),
    prevent_initial_call=True
)
def load_more(n_clicks, current_shown):
    if n_clicks:
        return current_shown + 9
    return current_shown


@callback(
    Output("cards-shown", "data", allow_duplicate=True),
    [Input("search-input", "value"),
     Input("sector-dropdown", "value"),
     Input("district-dropdown", "value"),
     Input("source-dropdown", "value"),
     Input("deadline-dropdown", "value"),
     Input("reset-btn", "n_clicks"),
     Input("quick-location-filter", "value"),
     Input("quick-sector-filter", "value")],
    prevent_initial_call=True
)
def reset_cards_on_filter(*args):
    return 9


@callback(
    [Output("job-cards-container", "children"),
     Output("results-count", "children"),
     Output("load-more-container", "style"),
     Output("load-more-label", "children")],
    [Input("search-input", "value"),
     Input("sector-dropdown", "value"),
     Input("district-dropdown", "value"),
     Input("source-dropdown", "value"),
     Input("deadline-dropdown", "value"),
     Input("reset-btn", "n_clicks"),
     Input("quick-location-filter", "value"),
     Input("quick-sector-filter", "value"),
     Input("cards-shown", "data")]
)
def update_job_cards(search, sector, district, source, deadline, reset_clicks, quick_locations, quick_sectors, cards_shown):
    if cards_shown is None:
        cards_shown = 9
    df = get_jobs_data()  # uses 5-min cache
    # Reset filters if button clicked
    ctx = dash.callback_context
    if ctx.triggered and ctx.triggered[0]['prop_id'] == 'reset-btn.n_clicks':
        filtered_df = df.copy()
    else:
        filtered_df = df.copy()
        
        # FILTER OUT EXPIRED JOBS AND JOBS WITH NO DEADLINE
        filtered_df = filtered_df[
            (filtered_df['days_to_deadline'].notna()) &  # Must have a deadline
            (filtered_df['days_to_deadline'] >= 0)       # Deadline must be in future
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
    
    total_filtered = len(filtered_df)
    for idx, job in filtered_df.head(cards_shown).iterrows():
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
        
        # Deadline badge class
        if pd.notna(job.get('days_to_deadline')):
            d = job['days_to_deadline']
            if d <= 2:
                dbadge_class = "job-badge badge-urgent"
            elif d <= 7:
                dbadge_class = "job-badge badge-warning"
            else:
                dbadge_class = "job-badge badge-ok"
        else:
            dbadge_class = "job-badge badge-source"

        card = dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    # Title
                    html.H5(job['title'], className="job-title"),

                    # Company
                    html.Div([
                        html.I(className="fas fa-building", style={'fontSize':'0.8rem'}),
                        html.Span(job['company'])
                    ], className="job-company"),

                    # Badges
                    html.Div([
                        html.Span([
                            html.I(className="fas fa-tag me-1"),
                            job['sector'] if pd.notna(job['sector']) else 'General'
                        ], className="job-badge badge-sector me-1 mb-1"),
                        html.Span([
                            html.I(className="fas fa-map-marker-alt me-1"),
                            job['district'] if pd.notna(job['district']) else 'Rwanda'
                        ], className="job-badge badge-location me-1 mb-1"),
                        html.Span(job['source'], className="job-badge badge-source me-1 mb-1"),
                    ], style={'marginBottom':'1rem','display':'flex','flexWrap':'wrap','gap':'2px'}),

                    # Meta
                    html.Div([
                        html.P([html.Strong("Education: "), education_value], className="job-meta"),
                        html.P([html.Strong("Experience: "), experience_value], className="job-meta"),
                    ], style={'marginBottom':'0.75rem'}),

                    # Deadline badge
                    html.Span(
                        deadline_badge.children if hasattr(deadline_badge, 'children') else str(deadline_badge),
                        className=dbadge_class,
                        style={'display':'inline-block','marginBottom':'1rem'}
                    ),

                    # Apply button (pushed to bottom)
                    html.Div(style={'flex':'1'}),
                    html.A([
                        html.I(className="fas fa-arrow-up-right-from-square me-2"),
                        "View & Apply"
                    ], href=job['source_url'], target="_blank", className="apply-btn",
                       style={'display':'block','textAlign':'center','textDecoration':'none'}),
                    html.P([
                        html.I(className="fas fa-calendar me-1"),
                        deadline_text
                    ], className="deadline-line"),
                ])
            ], className="job-card border-0 h-100")
        ], xl=4, lg=6, md=12, className="mb-3")
        
        cards_list.append(card)
    
    # Wrap cards in a Row for parallel layout
    cards_row = dbc.Row(cards_list) if cards_list else []
    
    # Results text
    results_text = html.Div([
        html.Span([html.Span(f"{len(cards_list)}", style={'color':'#4f46e5','fontWeight':'800'}),
                   f" of {total_filtered} jobs shown"]),
    ], className="results-count")
    
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
        ]), {'display': 'none'}, "Load More Jobs"
    
    shown = min(cards_shown, total_filtered)
    remaining = total_filtered - shown
    btn_label = f"Load More ({remaining} remaining)" if remaining > 0 else "All jobs loaded"
    btn_style = {
        'background': 'linear-gradient(135deg,#667eea,#764ba2)',
        'border': 'none', 'borderRadius': '12px', 'fontWeight': '600',
        'fontSize': '1rem', 'padding': '0.85rem', 'color': 'white',
        'cursor': 'pointer', 'marginTop': '0.5rem', 'transition': 'opacity 0.2s',
        'width': '100%', 'display': 'block' if remaining > 0 else 'none'
    }
    return cards_row, results_text, btn_style, btn_label


# ============================================================================
# PAGE 2: MARKET INSIGHTS (Keep the existing one - it's already good)
# ============================================================================

def create_market_insights_page():
    df = get_jobs_data()  # uses 5-min cache
    # Only use jobs with a future deadline (consistent with job cards)
    active_df = df[df['days_to_deadline'].notna() & (df['days_to_deadline'] >= 0)]
    # Calculate stats
    jobs_by_sector = active_df['sector'].value_counts().reset_index()
    jobs_by_sector.columns = ['sector', 'count']

    jobs_by_source = active_df['source'].value_counts().reset_index()
    jobs_by_source.columns = ['source', 'count']
    
    jobs_by_district = df['district'].value_counts().reset_index()
    jobs_by_district.columns = ['district', 'count']
    
    # Timeline data
    df_timeline = df.groupby(df['scraped_at'].dt.date).size().reset_index()
    df_timeline.columns = ['Date', 'Jobs']
    
    # Top companies
    top_companies = df['company'].value_counts().head(10).reset_index()
    top_companies.columns = ['company', 'count']
    
    return html.Div([
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
                            html.P("Total Posted Jobs", className="text-muted mb-0 small"),
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
                            html.Strong("Total Posted Jobs: "),
                            f"{len(df):,} opportunities"
                        ], className="mb-0"),
                    ])
                ], className="shadow-sm border-0", style={'borderRadius': '10px'})
            ], md=12, className="mb-4"),
        ]),
        
    ], style={'width': '100%'})


# ============================================================================
# PAGE 3: HISTORICAL JOBS (Placeholder)
# ============================================================================

def create_historical_page():
    return html.Div([
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
        
    ], style={'width': '100%'})


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
