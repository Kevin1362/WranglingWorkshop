import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def grouped_bar_avg_salary(df: pd.DataFrame, save_path: str | None = None):
    agg = df.groupby(["position", "start_year"])["salary"].mean().reset_index()
    years = sorted([y for y in agg["start_year"].dropna().unique()])
    positions = sorted(df["position"].unique())

    pivot = agg.pivot(index="position", columns="start_year", values="salary").reindex(positions).fillna(0)

    x = np.arange(len(pivot.index))
    bar_width = 0.8 / max(1, len(years))

    plt.figure(figsize=(14, 6))
    for i, y in enumerate(years):
        plt.bar(x + i * bar_width, pivot[y].values, width=bar_width, label=str(int(y)))

    plt.xticks(x + (len(years) * bar_width)/2 - bar_width/2, pivot.index, rotation=30, ha="right")
    plt.ylabel("Average Salary")
    plt.title("Average Salary by Position and Start Year (Grouped Bar Chart)")
    plt.legend(ncol=4, fontsize=8)
    plt.tight_layout()
    if save_path:
        plt.savefig(save_path, dpi=200)
    plt.show()

def heatmap_avg_salary_dept_position(df: pd.DataFrame, save_path: str | None = None):
    heat = df.groupby(["department_name", "position"])["salary"].mean().reset_index()
    dept_order = sorted(df["department_name"].fillna("Unknown").unique())
    pos_order = sorted(df["position"].unique())
    pivot = heat.pivot(index="department_name", columns="position", values="salary").reindex(dept_order).reindex(columns=pos_order).fillna(0)

    plt.figure(figsize=(14, 6))
    plt.imshow(pivot.values, aspect="auto")
    plt.colorbar(label="Average Salary")
    plt.xticks(np.arange(len(pos_order)), pos_order, rotation=30, ha="right")
    plt.yticks(np.arange(len(dept_order)), dept_order)
    plt.title("Heatmap: Average Salary by Department and Position")
    plt.tight_layout()
    if save_path:
        plt.savefig(save_path, dpi=200)
    plt.show()
