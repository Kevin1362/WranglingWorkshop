import pandas as pd
from sklearn.preprocessing import MinMaxScaler

def add_salary_scaled(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    scaler = MinMaxScaler()
    out["salary_scaled"] = scaler.fit_transform(out[["salary"]])
    return out
