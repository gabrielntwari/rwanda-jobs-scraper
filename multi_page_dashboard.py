"""
Rwanda Jobs Multi-Page Dashboard — REDESIGNED UI
=================================================
Same data layer, callbacks and component IDs as before; brand-new visual
design system: Rwanda green + sun-yellow palette, imigongo-inspired chevron
motif, Bricolage Grotesque / DM Sans type pairing, mobile-first layout.

Run:  python multi_page_dashboard.py
Open: http://localhost:8050
Prod: gunicorn multi_page_dashboard:server
"""

import os
from datetime import datetime, timedelta
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import dash
from dash import dcc, html, Input, Output, State, dash_table, callback, ALL
from dash.exceptions import PreventUpdate
import json
import dash_bootstrap_components as dbc
from sqlalchemy import create_engine
from functools import lru_cache
import threading
import time

# ============================================================================
# DATA LAYER (unchanged)
# ============================================================================

DATABASE_URL = os.getenv('DATABASE_URL')

@lru_cache(maxsize=1)
def _get_engine():
    return create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        pool_size=1,        # free tier: 1 connection is enough
        max_overflow=0,     # no extra connections
        connect_args={"connect_timeout": 10},
    )

_data_cache = {"df": None, "ts": 0}
_cache_lock = threading.Lock()
CACHE_TTL = 60   # refresh every 1 minute

def get_jobs_data():
    """Fetch jobs from DB, cached"""
    with _cache_lock:
        if _data_cache["df"] is not None and (time.time() - _data_cache["ts"]) < CACHE_TTL:
            return _data_cache["df"]
    engine = _get_engine()

    query = """
    SELECT
        id, title, company, source, sector, district,
        employment_type, job_level, experience_years, education_level,
        posted_date, deadline, scraped_at, is_active, source_url
    FROM jobs
    WHERE is_active = true
    ORDER BY scraped_at DESC
    """

    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    df['posted_date'] = pd.to_datetime(df['posted_date'], errors='coerce')
    df['deadline'] = pd.to_datetime(df['deadline'], errors='coerce')
    df['scraped_at'] = pd.to_datetime(df['scraped_at'])

    # Midnight deadlines are really "end of that day"
    midnight_mask = (
        df['deadline'].notna() &
        (df['deadline'].dt.hour == 0) &
        (df['deadline'].dt.minute == 0) &
        (df['deadline'].dt.second == 0)
    )
    df.loc[midnight_mask, 'deadline'] = (
        df.loc[midnight_mask, 'deadline'] + pd.Timedelta(hours=23, minutes=59, seconds=59)
    )

    df['time_to_deadline'] = df['deadline'] - pd.Timestamp.now()
    df['days_to_deadline'] = df['time_to_deadline'].dt.days

    with _cache_lock:
        _data_cache["df"] = df
        _data_cache["ts"] = time.time()

    return df


# ============================================================================
# DESIGN TOKENS  (single source of truth for both CSS and Plotly)
# ============================================================================

INK        = "#12211A"   # green-black text
PAPER      = "#F6F5EF"   # warm paper background
CARD       = "#FFFFFF"
GREEN      = "#0E7A54"   # primary — Rwanda green
GREEN_DEEP = "#0A3D2C"   # navbar / footer / display headings
GREEN_TINT = "#E4F0E9"
SUN        = "#F2B705"   # accent — sun yellow (used sparingly)
SUN_TINT   = "#FBF1D3"
CLAY       = "#C43D2E"   # urgency red
CLAY_TINT  = "#FAE9E6"
MUTED      = "#5E6E64"
LINE       = "#E3E5DD"

# Categorical palette for charts
CHART_COLORS = [GREEN, SUN, "#2E9E6F", CLAY, GREEN_DEEP, "#7FC29B", "#8C6D1F", MUTED]
GREEN_SCALE  = [[0, "#BFE3D0"], [0.5, "#2E9E6F"], [1, GREEN_DEEP]]

# Imigongo-inspired chevron band (alternating sun / green triangles)
IMIGONGO_SVG = (
    "data:image/svg+xml,"
    "%3Csvg xmlns='http://www.w3.org/2000/svg' width='28' height='10' viewBox='0 0 28 10'%3E"
    "%3Cpath d='M0 10 L7 1 L14 10 Z' fill='%23F2B705'/%3E"
    "%3Cpath d='M14 10 L21 1 L28 10 Z' fill='%230E7A54'/%3E"
    "%3C/svg%3E"
)

# ============================================================================
# APP INIT
# ============================================================================

app = dash.Dash(__name__, external_stylesheets=[
    dbc.themes.BOOTSTRAP,
    "https://use.fontawesome.com/releases/v6.1.1/css/all.css",
], suppress_callback_exceptions=True,
   meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"},
              {"name": "theme-color", "content": "#0A3D2C"}])
app.title = "Rwanda Jobs Portal"
server = app.server  # Required for Gunicorn: gunicorn multi_page_dashboard:server

app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,500;12..96,600;12..96,700;12..96,800&family=DM+Sans:opsz,wght@9..40,400;9..40,500;9..40,600;9..40,700&display=swap');

            :root {
                --ink: #12211A;
                --paper: #F6F5EF;
                --card: #FFFFFF;
                --green: #0E7A54;
                --green-deep: #0A3D2C;
                --green-tint: #E4F0E9;
                --sun: #F2B705;
                --sun-tint: #FBF1D3;
                --clay: #C43D2E;
                --clay-tint: #FAE9E6;
                --muted: #5E6E64;
                --line: #E3E5DD;
                --radius: 14px;
                --shadow-sm: 0 1px 2px rgba(18,33,26,0.05);
                --shadow-md: 0 10px 30px rgba(10,61,44,0.10);
                --display: 'Bricolage Grotesque', 'DM Sans', sans-serif;
                --body: 'DM Sans', system-ui, sans-serif;
                --pad-x: clamp(1rem, 4vw, 3.5rem);
            }

            *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
            /* Fluid type: 15px on small phones -> 16px at 1200px -> 18px at 1920px -> caps at 20px.
               Everything is sized in rem, so the whole UI scales with this one rule. */
            html { font-size: clamp(15px, 12.7px + 0.28vw, 20px); }
            body {
                background: var(--paper);
                font-family: var(--body);
                color: var(--ink);
                min-height: 100vh;
                -webkit-font-smoothing: antialiased;
            }
            h1, h2, h3, h4, h5 { font-family: var(--display); color: var(--ink); }
            a { color: var(--green); }
            :focus-visible { outline: 3px solid var(--sun); outline-offset: 2px; border-radius: 4px; }

            /* ══ SIGNATURE: imigongo chevron band ══ */
            .imigongo-band {
                height: 10px;
                background-color: var(--green-deep);
                background-image: url("''' + IMIGONGO_SVG + '''");
                background-repeat: repeat-x;
                background-position: bottom left;
            }

            /* ══ NAVBAR ══ */
            .top-navbar {
                background: var(--green-deep);
                padding: 0 var(--pad-x);
                min-height: 62px;
                display: flex;
                align-items: center;
                justify-content: space-between;
                gap: 1rem;
                position: sticky;
                top: 0;
                z-index: 1000;
                flex-wrap: wrap;
            }
            .nav-brand {
                font-family: var(--display);
                font-size: 1.15rem;
                font-weight: 700;
                color: #fff;
                text-decoration: none;
                display: flex;
                align-items: center;
                gap: 0.55rem;
                letter-spacing: -0.01em;
                padding: 0.6rem 0;
            }
            .nav-brand:hover { color: #fff; }
            .nav-brand .brand-mark {
                width: 30px; height: 30px;
                border-radius: 8px;
                background: var(--sun);
                color: var(--green-deep);
                display: flex; align-items: center; justify-content: center;
                font-size: 0.85rem;
            }
            .nav-brand .brand-accent { color: var(--sun); }
            .nav-links { display: flex; gap: 0.25rem; overflow-x: auto; }
            .nav-pill {
                color: rgba(255,255,255,0.72);
                text-decoration: none;
                font-size: 0.9rem;
                font-weight: 500;
                padding: 0.45rem 0.9rem;
                border-radius: 999px;
                transition: background 0.15s, color 0.15s;
                display: flex;
                align-items: center;
                gap: 6px;
                white-space: nowrap;
            }
            .nav-pill:hover { background: rgba(255,255,255,0.10); color: #fff; }

            /* ══ PAGE SHELL ══ */
            .page-wrap {
                width: 100%;
                max-width: 1720px;
                margin: 0 auto;
                padding: clamp(1.25rem, 3vw, 2.5rem) var(--pad-x) 0;
                min-height: calc(100vh - 72px);
            }

            /* ══ HERO ══ */
            .hero {
                position: relative;
                background: var(--card);
                border: 1px solid var(--line);
                border-radius: 20px;
                padding: clamp(1.5rem, 4vw, 3rem);
                margin-bottom: 1.5rem;
                overflow: hidden;
                box-shadow: var(--shadow-sm);
            }
            .hero::after {           /* faint chevron watermark, hero corner */
                content: "";
                position: absolute;
                right: -40px; top: -20px;
                width: 340px; height: 220px;
                background-image: url("''' + IMIGONGO_SVG + '''");
                background-size: 56px 20px;
                opacity: 0.07;
                transform: rotate(-8deg);
                pointer-events: none;
            }
            .hero-eyebrow {
                font-size: 0.78rem;
                font-weight: 700;
                letter-spacing: 0.14em;
                text-transform: uppercase;
                color: var(--green);
                margin-bottom: 0.6rem;
                display: flex; align-items: center; gap: 0.6rem;
            }
            .hero-eyebrow::before {
                content: "";
                width: 26px; height: 10px;
                background-image: url("''' + IMIGONGO_SVG + '''");
                background-size: 28px 10px;
                display: inline-block;
            }
            .hero-title {
                font-size: clamp(1.9rem, 4.5vw, 3.1rem);
                font-weight: 800;
                letter-spacing: -0.02em;
                line-height: 1.08;
                color: var(--green-deep);
                margin-bottom: 0.6rem;
            }
            .hero-title .accent {
                color: var(--green);
                font-style: italic;
            }
            .hero-sub {
                font-size: 1.02rem;
                color: var(--muted);
                margin-bottom: 1.4rem;
                max-width: 56ch;
            }

            /* Search inside hero */
            .search-hero {
                background: var(--paper);
                border: 2px solid var(--line);
                border-radius: 999px;
                padding: 0.3rem 0.3rem 0.3rem 1.25rem;
                display: flex;
                align-items: center;
                gap: 0.75rem;
                max-width: 720px;
                transition: border-color 0.2s, box-shadow 0.2s;
            }
            .search-hero:focus-within {
                border-color: var(--green);
                box-shadow: 0 0 0 4px rgba(14,122,84,0.12);
            }
            .search-hero .fa-search { color: var(--muted); flex-shrink: 0; }
            .search-hero input {
                border: none !important;
                outline: none !important;
                box-shadow: none !important;
                font-size: 1rem !important;
                font-family: var(--body);
                color: var(--ink) !important;
                background: transparent !important;
                flex: 1;
                min-width: 0;
                padding: 0.6rem 0 !important;
            }
            .search-hero input::placeholder { color: #9AA69E; }
            .search-btn {
                background: var(--green) !important;
                border: none !important;
                border-radius: 999px !important;
                color: #fff !important;
                font-weight: 600 !important;
                font-size: 0.95rem !important;
                padding: 0.65rem 1.6rem !important;
                white-space: nowrap;
                transition: background 0.2s !important;
            }
            .search-btn:hover { background: var(--green-deep) !important; }

            /* Stat strip inside hero */
            .stat-strip {
                display: flex;
                flex-wrap: wrap;
                gap: 0.5rem 2.25rem;
                margin-top: 1.4rem;
            }
            .stat-item { display: flex; align-items: baseline; gap: 0.5rem; }
            .stat-number {
                font-family: var(--display);
                font-size: clamp(1.5rem, 3vw, 2.1rem);
                font-weight: 800;
                letter-spacing: -0.02em;
                color: var(--green-deep);
                line-height: 1;
            }
            .stat-number.stat-sun  { color: #8C6D1F; }
            .stat-number.stat-clay { color: var(--clay); }
            .stat-label { font-size: 0.9rem; color: var(--muted); font-weight: 500; }

            /* ══ FILTER ROW ══ */
            .filter-row {
                background: var(--card);
                border: 1px solid var(--line);
                border-radius: var(--radius);
                padding: 0.85rem 1.1rem;
                display: flex;
                align-items: center;
                gap: 0.6rem;
                flex-wrap: wrap;
                margin-bottom: 1.5rem;
                box-shadow: var(--shadow-sm);
            }
            .filter-label {
                font-size: 0.82rem;
                font-weight: 700;
                letter-spacing: 0.1em;
                text-transform: uppercase;
                color: var(--muted);
                white-space: nowrap;
                margin-right: 0.25rem;
            }
            .filter-row > div { flex: 1 1 160px; min-width: 150px; }
            #reset-btn {
                flex: 0 0 auto !important;
                background: transparent !important;
                border: 1.5px solid var(--line) !important;
                color: var(--muted) !important;
                border-radius: 999px !important;
                font-size: 0.85rem !important;
                font-weight: 600 !important;
                padding: 0.42rem 1rem !important;
                white-space: nowrap;
                transition: border-color 0.15s, color 0.15s !important;
            }
            #reset-btn:hover { border-color: var(--green) !important; color: var(--green) !important; }

            /* ══ MAIN SPLIT: sidebar + results ══ */
            .main-split {
                display: flex;
                gap: 1.5rem;
                align-items: flex-start;
                width: 100%;
            }
            .side-col { width: clamp(220px, 18vw, 280px); flex-shrink: 0; }
            .results-col { flex: 1; min-width: 0; }

            /* ══ SIDEBAR ══ */
            .sidebar-panel {
                background: var(--card);
                border: 1px solid var(--line);
                border-radius: var(--radius);
                padding: 1.25rem;
                box-shadow: var(--shadow-sm);
            }
            .sidebar-section-title {
                font-size: 0.78rem;
                font-weight: 700;
                letter-spacing: 0.12em;
                text-transform: uppercase;
                color: var(--green);
                margin: 1.1rem 0 0.5rem;
                padding-top: 1rem;
                border-top: 1px dashed var(--line);
            }
            .sidebar-panel .sidebar-section-title:first-child { margin-top: 0; padding-top: 0; border-top: none; }
            .sidebar-panel label {
                display: flex; align-items: center;
                font-size: 0.95rem; color: var(--ink);
                padding: 0.22rem 0; cursor: pointer;
            }
            .trend-badge {
                display: inline-block;
                background: var(--green-tint);
                color: var(--green-deep);
                font-size: 0.85rem;
                font-weight: 600;
                padding: 0.3em 0.75em;
                border-radius: 999px;
                margin: 0 4px 6px 0;
                cursor: pointer;
                transition: background 0.15s, color 0.15s;
            }
            .trend-badge:hover { background: var(--green); color: #fff; }

            .contact-card {
                background: var(--green-deep);
                border-radius: var(--radius);
                padding: 1.25rem;
                margin-top: 1rem;
                background-image: url("''' + IMIGONGO_SVG + '''");
                background-repeat: repeat-x;
                background-position: bottom left;
            }
            .contact-title {
                font-size: 0.72rem;
                font-weight: 700;
                color: rgba(255,255,255,0.75);
                text-transform: uppercase;
                letter-spacing: 0.1em;
                margin-bottom: 0.8rem;
            }
            .contact-icons { display: flex; gap: 8px; padding-bottom: 0.6rem; }
            .contact-icon {
                width: 2.4rem; height: 2.4rem;
                border-radius: 10px;
                display: flex; align-items: center; justify-content: center;
                font-size: 1rem;
                color: var(--green-deep);
                background: rgba(255,255,255,0.92);
                text-decoration: none;
                transition: transform 0.15s, background 0.15s;
            }
            .contact-icon:hover { transform: translateY(-2px); background: var(--sun); color: var(--green-deep); }

            /* ══ RESULTS BAR ══ */
            .results-count {
                font-size: 0.98rem;
                font-weight: 500;
                color: var(--muted);
                margin-bottom: 1.1rem;
            }
            .results-count .count-strong {
                font-family: var(--display);
                font-weight: 700;
                color: var(--green);
            }

            /* ══ JOB CARDS ══ */
            .job-card {
                background: var(--card) !important;
                border: 1px solid var(--line) !important;
                border-radius: var(--radius) !important;
                height: 100%;
                transition: transform 0.18s ease, box-shadow 0.18s ease, border-color 0.18s ease;
                overflow: hidden;
            }
            .job-card:hover {
                transform: translateY(-3px);
                border-color: var(--green) !important;
                box-shadow: var(--shadow-md);
            }
            .job-card .card-body {
                padding: 1.4rem !important;
                display: flex;
                flex-direction: column;
                height: 100%;
            }
            .deadline-chip {
                align-self: flex-start;
                font-size: 0.8rem;
                font-weight: 700;
                padding: 0.3em 0.85em;
                border-radius: 999px;
                margin-bottom: 0.8rem;
                white-space: nowrap;
            }
            .chip-urgent  { background: var(--clay-tint); color: var(--clay); }
            .chip-warning { background: var(--sun-tint);  color: #8C6D1F; }
            .chip-ok      { background: var(--green-tint); color: var(--green); }
            .chip-none    { background: #EEF0EA; color: var(--muted); }
            .chip-urgent .fa-circle, .pulse { animation: pulse 1.5s infinite; }
            .chips-top { display: flex; flex-wrap: wrap; gap: 6px; }
            .chip-new { background: var(--sun); color: var(--green-deep); }

            .job-title {
                font-family: var(--display);
                font-size: 1.15rem !important;
                font-weight: 700 !important;
                letter-spacing: -0.01em;
                color: var(--green-deep) !important;
                line-height: 1.3 !important;
                margin-bottom: 0.25rem !important;
            }
            .job-company {
                font-size: 0.95rem;
                color: var(--muted);
                font-weight: 500;
                margin-bottom: 0.85rem;
                display: flex; align-items: center; gap: 6px;
            }
            .badges-row { display: flex; flex-wrap: wrap; gap: 6px; margin-bottom: 0.9rem; }
            .job-badge {
                font-size: 0.8rem;
                font-weight: 600;
                padding: 0.32em 0.8em;
                border-radius: 999px;
                white-space: nowrap;
                display: inline-flex; align-items: center; gap: 4px;
            }
            .badge-sector   { background: var(--green-tint); color: var(--green-deep); }
            .badge-location { background: var(--sun-tint); color: #8C6D1F; }
            .badge-source   { background: #EEF0EA; color: var(--muted); }

            .job-meta {
                font-size: 0.88rem;
                color: var(--muted);
                line-height: 1.55;
                margin-bottom: 1rem;
            }
            .job-meta strong { color: var(--ink); font-weight: 600; }

            .apply-btn {
                display: block;
                text-align: center;
                text-decoration: none;
                background: var(--green);
                color: #fff !important;
                border-radius: 10px;
                font-weight: 600;
                font-size: 0.98rem;
                padding: 0.7rem 1rem;
                margin-top: auto;
                transition: background 0.2s;
            }
            .apply-btn:hover { background: var(--green-deep); }
            .deadline-line {
                font-size: 0.8rem;
                color: #9AA69E;
                text-align: center;
                margin-top: 0.55rem;
                margin-bottom: 0;
            }

            /* ══ LOAD MORE ══ */
            .load-more-btn {
                background: transparent !important;
                border: 2px solid var(--green) !important;
                border-radius: 999px !important;
                color: var(--green) !important;
                font-weight: 600 !important;
                font-size: 0.95rem !important;
                padding: 0.75rem 1.5rem !important;
                width: 100%;
                transition: background 0.2s, color 0.2s !important;
                margin-top: 0.75rem;
            }
            .load-more-btn:hover { background: var(--green) !important; color: #fff !important; }

            /* ══ EMPTY STATE ══ */
            .empty-state {
                background: var(--card);
                border: 1px dashed var(--line);
                border-radius: var(--radius);
                padding: 3rem 1.5rem;
                text-align: center;
                color: var(--muted);
            }
            .empty-state .fa-seedling { font-size: 2rem; color: var(--green); margin-bottom: 0.8rem; }
            .empty-state h4 { color: var(--green-deep); margin-bottom: 0.4rem; }

            /* ══ INSIGHTS PAGE ══ */
            .section-title {
                font-size: clamp(1.5rem, 3vw, 2.1rem);
                font-weight: 800;
                letter-spacing: -0.02em;
                color: var(--green-deep);
                margin-bottom: 0.3rem;
            }
            .section-sub { color: var(--muted); margin-bottom: 1.5rem; }
            .metric-card {
                background: var(--card);
                border: 1px solid var(--line);
                border-radius: var(--radius);
                padding: 1.25rem 1.4rem;
                height: 100%;
                box-shadow: var(--shadow-sm);
                display: flex; align-items: center; gap: 1rem;
            }
            .metric-icon {
                width: 46px; height: 46px; border-radius: 12px;
                display: flex; align-items: center; justify-content: center;
                font-size: 1.1rem; flex-shrink: 0;
            }
            .metric-num {
                font-family: var(--display);
                font-size: 1.7rem; font-weight: 800; letter-spacing: -0.02em;
                color: var(--green-deep); line-height: 1.1;
            }
            .metric-label { font-size: 0.85rem; color: var(--muted); font-weight: 500; }
            .chart-card {
                background: var(--card);
                border: 1px solid var(--line);
                border-radius: var(--radius);
                box-shadow: var(--shadow-sm);
                height: 100%;
                overflow: hidden;
            }
            .chart-card .chart-head {
                padding: 1rem 1.4rem 0.25rem;
                font-family: var(--display);
                font-weight: 700;
                font-size: 1.05rem;
                color: var(--green-deep);
                display: flex; align-items: center; gap: 8px;
            }
            .chart-card .chart-head i { color: var(--green); font-size: 0.9rem; }

            /* ══ FOOTER ══ */
            .page-footer {
                margin-top: 3rem;
                background: var(--green-deep);
                margin-left: calc(-1 * var(--pad-x));
                margin-right: calc(-1 * var(--pad-x));
                padding: 1.4rem var(--pad-x);
                display: flex;
                justify-content: space-between;
                align-items: center;
                gap: 1rem;
                flex-wrap: wrap;
            }
            .footer-text { font-size: 0.85rem; color: rgba(255,255,255,0.65); }
            .footer-links { display: flex; gap: 1.25rem; flex-wrap: wrap; }
            .footer-link { font-size: 0.85rem; color: rgba(255,255,255,0.85); text-decoration: none; font-weight: 500; }
            .footer-link:hover { color: var(--sun); }

            /* ══ SCROLLBAR ══ */
            ::-webkit-scrollbar { width: 8px; height: 8px; }
            ::-webkit-scrollbar-track { background: var(--paper); }
            ::-webkit-scrollbar-thumb { background: #C9D4CC; border-radius: 4px; }
            ::-webkit-scrollbar-thumb:hover { background: var(--green); }

            /* ══ DASH DROPDOWNS ══ */
            .Select-control, .Select--single > .Select-control {
                border: 1.5px solid var(--line) !important;
                border-radius: 10px !important;
                font-size: 0.92rem !important;
                min-height: 40px !important;
                background: var(--paper) !important;
            }
            .Select-value-label, .Select-placeholder { font-size: 0.92rem !important; color: var(--ink) !important; line-height: 38px !important; }
            .Select-control:hover, .is-focused:not(.is-open) > .Select-control { border-color: var(--green) !important; box-shadow: none !important; }
            .Select-menu-outer { border-radius: 10px !important; border-color: var(--line) !important; font-size: 0.92rem !important; z-index: 1050 !important; }
            .VirtualizedSelectFocusedOption { background: var(--green-tint) !important; }

            /* ══ ANIMATIONS ══ */
            @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.45} }
            @keyframes fadeUp {
                from { opacity: 0; transform: translateY(10px); }
                to   { opacity: 1; transform: translateY(0); }
            }
            .job-card { animation: fadeUp 0.25s ease both; }
            @media (prefers-reduced-motion: reduce) {
                *, *::before, *::after { animation: none !important; transition: none !important; }
            }

            /* ══ RESPONSIVE ══ */
            @media (min-width: 1600px) {
                :root { --pad-x: clamp(2rem, 4vw, 5rem); }
                .side-col { width: clamp(260px, 16vw, 330px); }
                .search-hero { max-width: 860px; }
                .hero-sub { font-size: 1.1rem; }
                .main-split { gap: 2rem; }
            }
            @media (max-width: 991px) {
                .main-split { flex-direction: column; }
                .side-col { width: 100%; order: 2; }
                .results-col { width: 100%; order: 1; }
                .sidebar-panel { display: flex; flex-wrap: wrap; gap: 0 2rem; }
                .sidebar-panel > * { flex: 1 1 40%; }
                .sidebar-section-title { border-top: none; padding-top: 0; }
            }
            @media (max-width: 575px) {
                .search-btn .btn-text { display: none; }
                .search-btn { padding: 0.65rem 1rem !important; }
                .stat-strip { gap: 0.4rem 1.5rem; }
                .filter-row > div { flex: 1 1 100%; }
                .page-footer { justify-content: center; text-align: center; }
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

# ============================================================================
# NAVBAR + APP SHELL
# ============================================================================

navbar = html.Div([
    html.Div([
        html.A([
            html.Span(html.I(className="fas fa-briefcase"), className="brand-mark"),
            html.Span(["Rwanda", html.Span("Jobs", className="brand-accent"), " Portal"])
        ], href="/", className="nav-brand"),
        html.Div([
            dcc.Link([html.I(className="fas fa-search"), " Find Jobs"],
                     href="/", className="nav-pill", id="nav-jobs"),
            dcc.Link([html.I(className="fas fa-chart-line"), " Market Insights"],
                     href="/insights", className="nav-pill", id="nav-insights"),
            dcc.Link([html.I(className="fas fa-history"), " Historical"],
                     href="/historical", className="nav-pill", id="nav-historical"),
        ], className="nav-links"),
    ], className="top-navbar"),
    html.Div(className="imigongo-band"),
])

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    dcc.Store(id='cards-shown', data=9),
    navbar,
    html.Div(id='page-content'),
], style={"width": "100%", "minHeight": "100vh", "overflowX": "hidden"})


def page_footer():
    return html.Div([
        html.Span("© 2026 Rwanda Jobs Portal · Umurimo mwiza utangirira hano.", className="footer-text"),
        html.Div([
            html.A("Built by Gabriel Ntwari", href="https://www.linkedin.com/in/gabriel-ntwari/",
                   target="_blank", className="footer-link"),
            html.A([html.I(className="fab fa-whatsapp me-1"), "WhatsApp"],
                   href="https://wa.me/250782765421", target="_blank", className="footer-link"),
            html.A([html.I(className="fab fa-github me-1"), "GitHub"],
                   href="https://github.com/gabrielntwari", target="_blank", className="footer-link"),
        ], className="footer-links"),
    ], className="page-footer")


# ============================================================================
# PAGE 1: JOB SEEKER PORTAL
# ============================================================================

def create_job_seeker_page():
    df = get_jobs_data()
    active_df = df[df['days_to_deadline'].notna() & (df['days_to_deadline'] >= 0)]
    total_jobs = len(active_df)
    jobs_by_source = active_df['source'].value_counts()
    n_sources = active_df['source'].nunique()

    three_days_ago = pd.Timestamp.now() - pd.Timedelta(days=3)
    new_jobs_count = len(active_df[active_df['scraped_at'] >= three_days_ago])

    expiring_soon_count = len(active_df[
        (active_df['days_to_deadline'] <= 2) & (active_df['days_to_deadline'] >= 0)])

    return html.Div([

        # ── HERO: headline + search + stat strip ──
        html.Div([
            html.Div("Akazi · Jobs · Umurimo", className="hero-eyebrow"),
            html.H1([
                "Find work ",
                html.Span("in Rwanda", className="accent"),
            ], className="hero-title"),
            html.P(
                f"Live openings from {max(n_sources, 1)} Rwandan job boards in one place — "
                "refreshed every morning at 6 AM, duplicates removed.",
                className="hero-sub"
            ),

            # Search
            html.Div([
                html.I(className="fas fa-search"),
                dbc.Input(
                    id="search-input",
                    type="text",
                    debounce=True,
                    placeholder="Job title, company, or keyword…",
                ),
                dbc.Button([html.I(className="fas fa-search me-md-2"),
                            html.Span("Search", className="btn-text")],
                           id="search-btn-vis", className="search-btn"),
            ], className="search-hero"),

            # Stat strip
            html.Div([
                html.Div([
                    html.Span(f"{total_jobs:,}", className="stat-number"),
                    html.Span("open jobs", className="stat-label"),
                ], className="stat-item"),
                html.Div([
                    html.Span(f"{new_jobs_count}", className="stat-number stat-sun"),
                    html.Span("new in 3 days", className="stat-label"),
                ], className="stat-item"),
                html.Div([
                    html.Span(f"{expiring_soon_count}", className="stat-number stat-clay"),
                    html.Span("closing in 48 h", className="stat-label"),
                ], className="stat-item"),
                html.Div([
                    html.Span(
                        jobs_by_source.index[0] if len(jobs_by_source) else "—",
                        className="stat-number", style={'fontSize': '1.15rem', 'alignSelf': 'center'}),
                    html.Span("top source", className="stat-label"),
                ], className="stat-item"),
            ], className="stat-strip"),
        ], className="hero"),

        # ── FILTER ROW ──
        html.Div([
            html.Span("Refine", className="filter-label"),
            html.Div([
                dcc.Dropdown(id='sector-dropdown',
                    options=[{'label': 'All sectors', 'value': 'all'}] +
                            [{'label': s, 'value': s} for s in ['IT & Technology', 'Healthcare', 'Education',
                             'Finance', 'Agriculture', 'Construction', 'Logistics', 'HR', 'Legal', 'NGO / Development']],
                    value='all', clearable=False),
            ]),
            html.Div([
                dcc.Dropdown(id='district-dropdown',
                    options=[{'label': 'All locations', 'value': 'all'}] +
                            [{'label': d, 'value': d} for d in ['Kigali', 'Huye', 'Musanze', 'Rubavu', 'Nyagatare', 'Muhanga']],
                    value='all', clearable=False),
            ]),
            html.Div([
                dcc.Dropdown(id='source-dropdown',
                    options=[{'label': 'All sources', 'value': 'all'}] +
                            [{'label': s, 'value': s} for s in ['jobinrwanda', 'impactpool', 'musuratool', 'greatrwandajobs']],
                    value='all', clearable=False),
            ]),
            html.Div([
                dcc.Dropdown(id='deadline-dropdown',
                    options=[
                        {'label': 'Any deadline', 'value': 'all'},
                        {'label': 'Closing in 2 days', 'value': '2'},
                        {'label': 'Closing this week', 'value': '7'},
                        {'label': 'Closing this month', 'value': '30'},
                    ],
                    value='all', clearable=False),
            ]),
            html.Div([
                dcc.Dropdown(id='sort-dropdown',
                    options=[
                        {'label': 'Sort: closing soon', 'value': 'deadline'},
                        {'label': 'Sort: newest first', 'value': 'newest'},
                    ],
                    value='deadline', clearable=False),
            ]),
            dbc.Button([html.I(className="fas fa-rotate-left me-1"), " Reset"],
                       id="reset-btn", n_clicks=0),
        ], className="filter-row"),

        # ── MAIN SPLIT: results + sidebar ──
        html.Div([
            # RESULTS
            html.Div([
                html.Div(id="results-count"),
                dcc.Loading(html.Div(id="job-cards-container"),
                            type="circle", color=GREEN, delay_show=250),
                html.Div([
                    dbc.Button([
                        html.I(className="fas fa-chevron-down me-2"),
                        html.Span(id="load-more-label", children="Show more jobs")
                    ], id="load-more-btn", n_clicks=0, className="load-more-btn")
                ], id="load-more-container"),
            ], className="results-col"),

            # SIDEBAR
            html.Div([
                html.Div([
                    html.P("Location", className="sidebar-section-title"),
                    dcc.Checklist(
                        id='quick-location-filter',
                        options=[
                            {'label': ' Kigali', 'value': 'Kigali'},
                            {'label': ' Huye', 'value': 'Huye'},
                            {'label': ' Musanze', 'value': 'Musanze'},
                            {'label': ' Rubavu', 'value': 'Rubavu'},
                        ],
                        value=[],
                        inputStyle={'marginRight': '8px', 'accentColor': GREEN}
                    ),
                    html.P("Sector", className="sidebar-section-title"),
                    dcc.Checklist(
                        id='quick-sector-filter',
                        options=[
                            {'label': ' IT & Tech', 'value': 'IT'},
                            {'label': ' Healthcare', 'value': 'Healthcare'},
                            {'label': ' Education', 'value': 'Education'},
                            {'label': ' Finance', 'value': 'Finance'},
                        ],
                        value=[],
                        inputStyle={'marginRight': '8px', 'accentColor': GREEN}
                    ),
                    html.P("Trending", className="sidebar-section-title"),
                    html.Div([
                        html.Span(term, className="trend-badge", n_clicks=0,
                                  id={'type': 'trend-badge', 'term': term})
                        for term in ["Manager", "Developer", "Officer", "Nurse", "Driver"]
                    ]),
                ], className="sidebar-panel"),

                html.Div([
                    html.P("Get in touch", className="contact-title"),
                    html.Div([
                        html.A(html.I(className="fab fa-whatsapp"),
                               href="https://wa.me/250782765421", target="_blank",
                               className="contact-icon", title="WhatsApp"),
                        html.A(html.I(className="fas fa-envelope"),
                               href="mailto:ntwaridigabia@gmail.com",
                               className="contact-icon", title="Email"),
                        html.A(html.I(className="fab fa-linkedin-in"),
                               href="https://www.linkedin.com/in/gabriel-ntwari/", target="_blank",
                               className="contact-icon", title="LinkedIn"),
                        html.A(html.I(className="fab fa-x-twitter"),
                               href="https://x.com/ntwari_gabriel", target="_blank",
                               className="contact-icon", title="Twitter/X"),
                    ], className="contact-icons"),
                ], className="contact-card"),
            ], className="side-col"),
        ], className="main-split"),

        page_footer(),
    ], className="page-wrap")

# ============================================================================
# CALLBACKS (same IDs and logic as before)
# ============================================================================

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
     Input("sort-dropdown", "value"),
     Input("reset-btn", "n_clicks"),
     Input("search-btn-vis", "n_clicks"),
     Input("quick-location-filter", "value"),
     Input("quick-sector-filter", "value")],
    prevent_initial_call=True
)
def reset_cards_on_filter(*args):
    return 9


@callback(
    Output("search-input", "value"),
    Input({'type': 'trend-badge', 'term': ALL}, 'n_clicks'),
    prevent_initial_call=True
)
def trend_to_search(clicks):
    ctx = dash.callback_context
    if not ctx.triggered or not any(c for c in clicks if c):
        raise PreventUpdate
    triggered_id = json.loads(ctx.triggered[0]['prop_id'].rsplit('.', 1)[0])
    return triggered_id['term']


def _deadline_presentation(job):
    """Return (chip_component, deadline_text) for a job row."""
    if pd.notna(job['time_to_deadline']):
        total_seconds = job['time_to_deadline'].total_seconds()
        if total_seconds < 0:
            return None, None  # expired — caller skips
        if total_seconds < 3600:
            minutes = int(total_seconds / 60)
            chip = html.Span([html.I(className="fas fa-circle me-1", style={'fontSize': '0.5rem'}),
                              f"Closes in {minutes} min"], className="deadline-chip chip-urgent pulse")
            text = f"Deadline: {job['deadline'].strftime('%b %d, %Y %I:%M %p')}"
        elif total_seconds < 86400:
            hours = int(total_seconds / 3600)
            minutes = int((total_seconds % 3600) / 60)
            chip = html.Span([html.I(className="fas fa-circle me-1", style={'fontSize': '0.5rem'}),
                              f"Closes in {hours}h {minutes}m"], className="deadline-chip chip-urgent")
            text = f"Deadline: {job['deadline'].strftime('%b %d, %Y %I:%M %p')}"
        elif job['days_to_deadline'] <= 2:
            hours = int((total_seconds % 86400) / 3600)
            chip = html.Span([html.I(className="fas fa-circle me-1", style={'fontSize': '0.5rem'}),
                              f"Closes in {int(job['days_to_deadline'])}d {hours}h"],
                             className="deadline-chip chip-urgent")
            text = f"Deadline: {job['deadline'].strftime('%b %d, %Y')}"
        elif job['days_to_deadline'] <= 7:
            chip = html.Span([html.I(className="far fa-clock me-1"),
                              f"{int(job['days_to_deadline'])} days left"],
                             className="deadline-chip chip-warning")
            text = f"Deadline: {job['deadline'].strftime('%b %d, %Y')}"
        else:
            chip = html.Span([html.I(className="far fa-calendar-check me-1"),
                              f"{int(job['days_to_deadline'])} days left"],
                             className="deadline-chip chip-ok")
            text = f"Deadline: {job['deadline'].strftime('%b %d, %Y')}"
        return chip, text
    chip = html.Span("No deadline listed", className="deadline-chip chip-none")
    return chip, "No deadline specified"


def _job_badges(job):
    """Location / sector / source pills — skip anything empty instead of showing blank chips."""
    def has(v):
        return pd.notna(v) and str(v).strip() != '' and str(v).strip().upper() != 'EMPTY'
    badges = []
    district = job['district'] if has(job['district']) else 'Rwanda'
    badges.append(html.Span([html.I(className="fas fa-map-marker-alt"), district],
                            className="job-badge badge-location"))
    if has(job['sector']):
        badges.append(html.Span([html.I(className="fas fa-tag"), job['sector']],
                                className="job-badge badge-sector"))
    if has(job['source']):
        badges.append(html.Span(job['source'], className="job-badge badge-source"))
    return badges


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
     Input("sort-dropdown", "value"),
     Input("reset-btn", "n_clicks"),
     Input("search-btn-vis", "n_clicks"),
     Input("quick-location-filter", "value"),
     Input("quick-sector-filter", "value"),
     Input("cards-shown", "data")]
)
def update_job_cards(search, sector, district, source, deadline, sort_by, reset_clicks, search_clicks, quick_locations, quick_sectors, cards_shown):
    if cards_shown is None:
        cards_shown = 9
    df = get_jobs_data()

    ctx = dash.callback_context
    if ctx.triggered and ctx.triggered[0]['prop_id'] == 'reset-btn.n_clicks':
        filtered_df = df.copy()
    else:
        filtered_df = df.copy()

        # Only jobs with a future deadline
        filtered_df = filtered_df[
            (filtered_df['days_to_deadline'].notna()) &
            (filtered_df['days_to_deadline'] >= 0)
        ]

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

        if quick_locations:
            filtered_df = filtered_df[filtered_df['district'].isin(quick_locations)]

        if quick_sectors:
            sector_mask = filtered_df['sector'].str.contains('|'.join(quick_sectors), case=False, na=False)
            filtered_df = filtered_df[sector_mask]

    # Sort: most urgent deadlines first (default), or newest scrapes first
    if sort_by == 'newest':
        filtered_df = filtered_df.sort_values('scraped_at', ascending=False)
    else:
        filtered_df = filtered_df.sort_values('time_to_deadline', ascending=True, na_position='last')

    cards_list = []
    total_filtered = len(filtered_df)

    for idx, job in filtered_df.head(cards_shown).iterrows():
        chip, deadline_text = _deadline_presentation(job)
        if chip is None:
            continue  # expired, just in case

        education_value = job['education_level'] if (pd.notna(job['education_level']) and
                                                     str(job['education_level']).upper() != 'EMPTY' and
                                                     str(job['education_level']).strip() != '') else 'Not specified'
        experience_value = (f"{job['experience_years']} years" if (pd.notna(job['experience_years']) and
                                                                   str(job['experience_years']).upper() != 'EMPTY' and
                                                                   str(job['experience_years']).strip() != '')
                            else 'Not specified')

        is_new = pd.notna(job['scraped_at']) and \
                 (pd.Timestamp.now() - job['scraped_at']).total_seconds() < 48 * 3600
        top_chips = [chip]
        if is_new:
            top_chips.append(html.Span([html.I(className="fas fa-bolt me-1"), "New"],
                                       className="deadline-chip chip-new"))

        card = dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.Div(top_chips, className="chips-top"),
                    html.H5(job['title'], className="job-title"),
                    html.Div([
                        html.I(className="fas fa-building", style={'fontSize': '0.8rem'}),
                        html.Span(job['company'])
                    ], className="job-company"),
                    html.Div(_job_badges(job), className="badges-row"),
                    html.Div([
                        html.Div([html.Strong("Education: "), education_value]),
                        html.Div([html.Strong("Experience: "), experience_value]),
                    ], className="job-meta"),
                    html.A([
                        html.I(className="fas fa-arrow-up-right-from-square me-2"),
                        "View & apply"
                    ], href=job['source_url'], target="_blank", className="apply-btn"),
                    html.P([html.I(className="far fa-calendar me-1"), deadline_text],
                           className="deadline-line"),
                ])
            ], className="job-card border-0 h-100")
        ], xl=4, lg=6, md=6, sm=12, className="mb-3")

        cards_list.append(card)

    cards_row = dbc.Row(cards_list, className="g-3") if cards_list else []

    results_text = html.Div([
        html.Span([
            "Showing ",
            html.Span(f"{len(cards_list)}", className="count-strong"),
            f" of {total_filtered} matching jobs"
        ]),
    ], className="results-count")

    if not cards_list:
        return [
            html.Div([
                html.I(className="fas fa-seedling"),
                html.H4("No jobs match these filters"),
                html.P("Clear a filter or check back tomorrow — new jobs arrive every morning at 6 AM."),
            ], className="empty-state")
        ], html.Div([
            html.Span("0 matching jobs", className="results-count")
        ]), {'display': 'none'}, "Show more jobs"

    shown = min(cards_shown, total_filtered)
    remaining = total_filtered - shown
    btn_label = f"Show more jobs ({remaining} remaining)" if remaining > 0 else "All jobs loaded"
    btn_style = {'display': 'block' if remaining > 0 else 'none'}
    return cards_row, results_text, btn_style, btn_label

# ============================================================================
# PAGE 2: MARKET INSIGHTS
# ============================================================================

CHART_FONT = dict(family="DM Sans, system-ui, sans-serif", size=13, color=INK)

def _style_fig(fig, height=420):
    """Apply the shared chart theme."""
    fig.update_layout(
        height=height,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=CHART_FONT,
        margin=dict(l=16, r=16, t=16, b=16),
        xaxis=dict(gridcolor='rgba(18,33,26,0.08)', zeroline=False),
        yaxis=dict(gridcolor='rgba(18,33,26,0.08)', zeroline=False),
        hoverlabel=dict(font=CHART_FONT, bgcolor="#FFFFFF", bordercolor=LINE),
    )
    return fig


def _metric_card(icon, value, label, icon_bg, icon_color):
    return html.Div([
        html.Div(html.I(className=f"fas {icon}"), className="metric-icon",
                 style={'background': icon_bg, 'color': icon_color}),
        html.Div([
            html.Div(value, className="metric-num"),
            html.Div(label, className="metric-label"),
        ]),
    ], className="metric-card")


def _chart_card(icon, title, figure):
    return html.Div([
        html.Div([html.I(className=f"fas {icon}"), title], className="chart-head"),
        dcc.Graph(figure=figure, config={'displayModeBar': False}),
    ], className="chart-card")


def create_market_insights_page():
    df = get_jobs_data()
    active_df = df[df['days_to_deadline'].notna() & (df['days_to_deadline'] >= 0)]

    jobs_by_sector = active_df['sector'].value_counts().reset_index()
    jobs_by_sector.columns = ['sector', 'count']

    jobs_by_source = active_df['source'].value_counts().reset_index()
    jobs_by_source.columns = ['source', 'count']

    jobs_by_district = df['district'].value_counts().reset_index()
    jobs_by_district.columns = ['district', 'count']

    df_timeline = df.groupby(df['scraped_at'].dt.date).size().reset_index()
    df_timeline.columns = ['Date', 'Jobs']

    top_companies = df['company'].value_counts().head(10).reset_index()
    top_companies.columns = ['company', 'count']

    # ── Figures ──
    fig_sector = _style_fig(
        px.bar(jobs_by_sector.sort_values('count'), x='count', y='sector', orientation='h',
               labels={'sector': '', 'count': 'Jobs'},
               color='count', color_continuous_scale=GREEN_SCALE),
        height=460
    ).update_layout(showlegend=False, coloraxis_showscale=False) \
     .update_traces(marker_line_width=0,
                    hovertemplate='<b>%{y}</b><br>Jobs: %{x}<extra></extra>')

    fig_source = _style_fig(
        px.pie(jobs_by_source, values='count', names='source', hole=0.55,
               color_discrete_sequence=CHART_COLORS),
        height=460
    ).update_layout(legend=dict(orientation="v", yanchor="middle", y=0.5,
                                xanchor="left", x=1.02)) \
     .update_traces(textposition='inside', textinfo='percent',
                    hovertemplate='<b>%{label}</b><br>Jobs: %{value}<br>Share: %{percent}<extra></extra>',
                    marker=dict(line=dict(color='#FFFFFF', width=2)))

    fig_timeline = _style_fig(
        px.line(df_timeline, x='Date', y='Jobs', markers=True),
        height=360
    ).update_traces(line=dict(color=GREEN, width=3),
                    marker=dict(size=7, color=GREEN_DEEP, line=dict(width=2, color='white')),
                    hovertemplate='<b>%{x}</b><br>Jobs added: %{y}<extra></extra>',
                    fill='tozeroy', fillcolor='rgba(14,122,84,0.10)')

    fig_companies = _style_fig(
        px.bar(top_companies.sort_values('count'), x='count', y='company', orientation='h',
               labels={'company': '', 'count': 'Open positions'},
               color='count', color_continuous_scale=GREEN_SCALE),
        height=460
    ).update_layout(showlegend=False, coloraxis_showscale=False) \
     .update_traces(marker_line_width=0,
                    hovertemplate='<b>%{y}</b><br>Positions: %{x}<extra></extra>')

    fig_districts = _style_fig(
        px.bar(jobs_by_district.head(10).sort_values('count'), x='count', y='district', orientation='h',
               labels={'district': '', 'count': 'Jobs'},
               color='count',
               color_continuous_scale=[[0, SUN_TINT], [1, SUN]]),
        height=460
    ).update_layout(showlegend=False, coloraxis_showscale=False) \
     .update_traces(marker_line_width=0,
                    hovertemplate='<b>%{y}</b><br>Jobs: %{x}<extra></extra>')

    return html.Div([
        html.Div("Market Insights", className="hero-eyebrow"),
        html.H1("The Rwandan job market, in numbers", className="section-title"),
        html.P("What's hiring, where, and how fast — from every scrape since day one.",
               className="section-sub"),

        # Key metrics
        dbc.Row([
            dbc.Col(_metric_card("fa-briefcase", f"{len(df):,}", "Total posted jobs",
                                 GREEN_TINT, GREEN), lg=3, md=6, sm=6, xs=12, className="mb-3"),
            dbc.Col(_metric_card("fa-industry", f"{len(jobs_by_sector)}", "Job sectors",
                                 SUN_TINT, "#8C6D1F"), lg=3, md=6, sm=6, xs=12, className="mb-3"),
            dbc.Col(_metric_card("fa-globe", f"{len(jobs_by_source)}", "Job sources",
                                 GREEN_TINT, GREEN_DEEP), lg=3, md=6, sm=6, xs=12, className="mb-3"),
            dbc.Col(_metric_card("fa-building", f"{len(top_companies)}", "Top employers",
                                 CLAY_TINT, CLAY), lg=3, md=6, sm=6, xs=12, className="mb-3"),
        ], className="mb-2 g-3"),

        dbc.Row([
            dbc.Col(_chart_card("fa-chart-bar", "Jobs by sector", fig_sector),
                    md=6, className="mb-4"),
            dbc.Col(_chart_card("fa-chart-pie", "Jobs by source", fig_source),
                    md=6, className="mb-4"),
        ]),

        dbc.Row([
            dbc.Col(_chart_card("fa-chart-line", "Jobs added over time", fig_timeline),
                    md=12, className="mb-4"),
        ]),

        dbc.Row([
            dbc.Col(_chart_card("fa-building", "Top 10 hiring companies", fig_companies),
                    md=6, className="mb-4"),
            dbc.Col(_chart_card("fa-map-marker-alt", "Top 10 locations", fig_districts),
                    md=6, className="mb-4"),
        ]),

        page_footer(),
    ], className="page-wrap")


# ============================================================================
# PAGE 3: HISTORICAL JOBS (placeholder, restyled)
# ============================================================================

def create_historical_page():
    return html.Div([
        html.Div("Archive", className="hero-eyebrow"),
        html.H1("Historical jobs", className="section-title"),
        html.P("Job postings from past years, recovered from web archives.", className="section-sub"),

        html.Div([
            html.I(className="fas fa-seedling"),
            html.H4("Coming soon"),
            html.P("This page will show archived postings from the Wayback Machine, "
                   "multi-year hiring trends, company hiring history, and salary evolution."),
            html.P("In the meantime, browse today's openings on the Find Jobs page.",
                   style={'marginBottom': '0'}),
        ], className="empty-state", style={'maxWidth': '640px'}),

        page_footer(),
    ], className="page-wrap")


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
    print("\n" + "=" * 70)
    print("Opening dashboard at: http://localhost:8050")
    print("=" * 70 + "\n")
    debug_mode = os.getenv("DASH_DEBUG", "false").lower() == "true"
    app.run(debug=debug_mode, host="0.0.0.0", port=int(os.getenv("PORT", 8050)))
