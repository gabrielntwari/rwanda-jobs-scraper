"""
schema.py  —  Canonical job record schema
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Single source of truth for column order, types, and defaults.
Import and call enforce(df) at the end of every scraper.
"""

import pandas as pd

# Exact column order matching the required output format
COLUMNS = [
    "id",
    "title",
    "company",
    "description",
    "location_raw",
    "district",
    "country",
    "is_remote",
    "is_hybrid",
    "rwanda_eligible",
    "eligibility_reason",
    "confidence_score",
    "sector",
    "job_level",
    "experience_years",
    "employment_type",
    "education_level",
    "salary_min",
    "salary_max",
    "currency",
    "salary_disclosed",
    "posted_date",
    "deadline",
    "scraped_at",
    "source",
    "source_url",
    "source_job_id",
    "is_active",
    "last_checked",
    "duplicate_hash",
]

# Default values for each column (used when value is missing/None)
DEFAULTS = {
    "id":                 "",
    "title":              "",
    "company":            "",
    "description":        "",
    "location_raw":       "",
    "district":           "",
    "country":            "Rwanda",
    "is_remote":          False,
    "is_hybrid":          False,
    "rwanda_eligible":    True,
    "eligibility_reason": "",
    "confidence_score":   0,
    "sector":             "",
    "job_level":          "",
    "experience_years":   "",
    "employment_type":    "",
    "education_level":    "",
    "salary_min":         "",    # empty string (not None) when undisclosed
    "salary_max":         "",
    "currency":           "",
    "salary_disclosed":   False,
    "posted_date":        "",
    "deadline":           "",
    "scraped_at":         "",
    "source":             "",
    "source_url":         "",
    "source_job_id":      "",
    "is_active":          True,
    "last_checked":       "",
    "duplicate_hash":     "",
}


def enforce(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enforce canonical schema on a DataFrame:
      1. Add any missing columns with default values
      2. Reorder columns to exact spec
      3. Fix types (bool, empty string instead of None for salary)
      4. Strip whitespace from string columns
    """
    # Add missing columns
    for col, default in DEFAULTS.items():
        if col not in df.columns:
            df[col] = default

    # Replace None / NaN in string columns with ""
    str_cols = [c for c, d in DEFAULTS.items() if isinstance(d, str)]
    df[str_cols] = df[str_cols].fillna("").astype(str)

    # salary_min / salary_max: keep numeric when disclosed, else empty string
    for sal_col in ["salary_min", "salary_max"]:
        df[sal_col] = df[sal_col].apply(
            lambda v: "" if (v is None or (isinstance(v, float) and pd.isna(v))) else v
        )

    # Bool columns
    for bool_col in ["is_remote", "is_hybrid", "rwanda_eligible",
                     "salary_disclosed", "is_active"]:
        df[bool_col] = df[bool_col].astype(bool)

    # confidence_score: int
    df["confidence_score"] = pd.to_numeric(df["confidence_score"], errors="coerce").fillna(0).astype(int)

    # Strip whitespace from all string columns
    for col in str_cols:
        df[col] = df[col].str.strip()

    # Reorder to canonical column order
    df = df[COLUMNS]

    return df
