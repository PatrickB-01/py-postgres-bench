import pandas as pd
import matplotlib

matplotlib.use("Agg")  # Use a non-GUI backend
import matplotlib.pyplot as plt
from pathlib import Path

OUTPUT_DIR = Path.cwd() / "output_data"
# Load the benchmark CSV
df = pd.read_csv(OUTPUT_DIR / "benchmark_results.csv")

# Simplify function names
df["func"] = df["name"].apply(lambda x: x.split("::")[-1])

# Convert numeric columns
numeric_cols = ["mean", "stddev", "avg_rss_kb", "peak_rss_kb", "pg_avg_commits", "pg_avg_rollbacks", "pg_avg_deadlocks", "blks_read", "blks_hit", "blk_read_time", "blk_write_time"]
df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")

# Set function as index for plotting
df.set_index("func", inplace=True)

# Plot bar charts for each metric
for metric in numeric_cols:
    plt.figure(figsize=(10, 6))
    bars = plt.bar(df.index, df[metric], color="skyblue", edgecolor="black")

    # Add values on top of bars
    for bar in bars:
        yval = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width() / 2.0,
            yval,
            f"{yval:.2e}",
            ha="center",
            va="bottom",
        )

    plt.title(f"Benchmark Comparison: {metric}")
    plt.xlabel("Function")
    plt.ylabel(metric.replace("_", " ").title())
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    plt.xticks(rotation=30)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / f"{metric}_bar_chart.png")
    # plt.show()
