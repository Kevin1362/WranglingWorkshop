import pandas as pd
from datetime import date

class EmployeeCleaner:
    """Cleans employee data: missing values, invalid ranges, and normalizes text."""

    def __init__(self):
        pass

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()

        # Ensure types (keep start_date as date-safe for comparisons)
        out["position"] = out["position"].astype("string")
        out["name"] = out["name"].astype("string")

        # Track issues
        out["issue_missing_name"] = out["name"].isna()
        out["issue_missing_position"] = out["position"].isna()
        out["issue_missing_start_date"] = out["start_date"].isna()
        out["issue_missing_salary"] = out["salary"].isna()

        # Fill missing fields
        out["name"] = out["name"].fillna("Unknown")
        out["position"] = out["position"].fillna("Unknown")

        # Salary: coerce numeric, set invalid to NaN then impute with median
        out["salary"] = pd.to_numeric(out["salary"], errors="coerce")
        out["issue_invalid_salary"] = (out["salary"] < 60000) | (out["salary"] > 200000)
        out.loc[out["issue_invalid_salary"], "salary"] = pd.NA
        out["salary"] = out["salary"].fillna(out["salary"].median()).astype(int)

        # Dates: keep as python date if possible; invalid range -> NaT then impute with mode
        # Convert to datetime for easier fill, but compare using dates
        out["start_date"] = pd.to_datetime(out["start_date"], errors="coerce")
        out["issue_invalid_start_date"] = (
            (out["start_date"].dt.date < date(2015,1,1)) |
            (out["start_date"].dt.date > date(2024,12,31))
        )
        out.loc[out["issue_invalid_start_date"], "start_date"] = pd.NaT
        # Impute missing dates with most common date (mode). If mode empty, choose 2019-01-01
        if out["start_date"].dropna().empty:
            fill_dt = pd.Timestamp("2019-01-01")
        else:
            fill_dt = out["start_date"].mode().iloc[0]
        out["start_date"] = out["start_date"].fillna(fill_dt)

        # Normalize position text
        out["position"] = out["position"].str.strip().str.title()

        # Overall data quality flag
        issue_cols = [c for c in out.columns if c.startswith("issue_")]
        out["has_any_issue"] = out[issue_cols].any(axis=1)

        return out
