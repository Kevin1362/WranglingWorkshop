import pandas as pd

def add_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["start_date"] = pd.to_datetime(out["start_date"], errors="coerce")
    out["start_year"] = out["start_date"].dt.year

    today = pd.Timestamp(pd.Timestamp.today().date())
    out["years_of_service"] = ((today - out["start_date"]).dt.days / 365.25).round(2)
    return out
