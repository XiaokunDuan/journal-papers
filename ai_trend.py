"""
AI-related paper trend by journal and year (2021–2025).
Outputs: ai_trend.png
"""

import json
import re
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

# ── AI keyword matching ──────────────────────────────────────────────────────
AI_PATTERN = re.compile(
    r"\b("
    r"artificial intelligence|machine learning|deep learning|neural network|"
    r"large language model|LLM|GPT|ChatGPT|generative AI|"
    r"natural language processing|NLP|computer vision|"
    r"reinforcement learning|random forest|gradient boosting|XGBoost|"
    r"support vector machine|SVM|"
    r"algorithm(?:ic)? (?:decision|prediction|recommendation)|"
    r"predictive (?:model|analytics|algorithm)|"
    r"data.driven|AI.(?:assisted|augmented|enabled|powered)|"
    r"automation|robot(?:ic|ics)|"
    r"optimization algorithm|metaheuristic|"
    r"transformer model|BERT|text mining|"
    r"human.AI|algorithm aversion|algorithm appreciation"
    r")\b",
    re.IGNORECASE,
)

def is_ai(paper: dict) -> bool:
    text = f"{paper.get('title', '')} {paper.get('abstract', '')}"
    return bool(AI_PATTERN.search(text))

# ── Load & filter ────────────────────────────────────────────────────────────
papers = json.load(open("papers.json"))

JOURNALS = ["MS", "OR", "MSOM", "POM"]
YEARS    = list(range(2021, 2026))  # exclude 2026 (incomplete year)

# Count AI papers per journal per year
ai_counts   = defaultdict(lambda: defaultdict(int))
all_counts  = defaultdict(lambda: defaultdict(int))

for p in papers:
    yr = p["year"]
    jn = p["journal"]
    if yr not in YEARS or jn not in JOURNALS:
        continue
    all_counts[jn][yr] += 1
    if is_ai(p):
        ai_counts[jn][yr] += 1

# ── Print summary ────────────────────────────────────────────────────────────
print("AI paper counts (absolute):")
print(f"{'':6}", end="")
for yr in YEARS:
    print(f"{yr:>6}", end="")
print(f"  {'Total':>6}")
total_by_year = defaultdict(int)
for jn in JOURNALS:
    print(f"{jn:6}", end="")
    row_total = 0
    for yr in YEARS:
        n = ai_counts[jn][yr]
        row_total += n
        total_by_year[yr] += n
        print(f"{n:>6}", end="")
    print(f"  {row_total:>6}")
print(f"{'All':6}", end="")
for yr in YEARS:
    print(f"{total_by_year[yr]:>6}", end="")
print(f"  {sum(total_by_year.values()):>6}")

print("\nAI share (%):")
print(f"{'':6}", end="")
for yr in YEARS:
    print(f"{yr:>7}", end="")
print()
for jn in JOURNALS:
    print(f"{jn:6}", end="")
    for yr in YEARS:
        a = ai_counts[jn][yr]
        t = all_counts[jn][yr]
        pct = 100 * a / t if t else 0
        print(f"{pct:>6.1f}%", end="")
    print()

# ── Plot ─────────────────────────────────────────────────────────────────────
COLORS = {"MS": "#1f77b4", "OR": "#ff7f0e", "MSOM": "#2ca02c", "POM": "#d62728"}
MARKERS = {"MS": "o", "OR": "s", "MSOM": "^", "POM": "D"}

fig, axes = plt.subplots(1, 2, figsize=(14, 5.5))
fig.suptitle("AI-Related Papers in Top-4 OM Journals (2021–2025)",
             fontsize=14, fontweight="bold", y=1.02)

# Left: absolute counts
ax1 = axes[0]
for jn in JOURNALS:
    vals = [ai_counts[jn][yr] for yr in YEARS]
    ax1.plot(YEARS, vals, marker=MARKERS[jn], color=COLORS[jn],
             linewidth=2, markersize=7, label=jn)
ax1.set_title("Absolute count of AI papers", fontsize=12)
ax1.set_xlabel("Year")
ax1.set_ylabel("Number of papers")
ax1.set_xticks(YEARS)
ax1.yaxis.set_major_locator(ticker.MaxNLocator(integer=True))
ax1.legend(framealpha=0.8)
ax1.grid(axis="y", alpha=0.3)
ax1.spines[["top", "right"]].set_visible(False)

# Right: share (%)
ax2 = axes[1]
for jn in JOURNALS:
    vals = [100 * ai_counts[jn][yr] / all_counts[jn][yr]
            if all_counts[jn][yr] else 0
            for yr in YEARS]
    ax2.plot(YEARS, vals, marker=MARKERS[jn], color=COLORS[jn],
             linewidth=2, markersize=7, label=jn)
ax2.set_title("AI papers as % of journal output", fontsize=12)
ax2.set_xlabel("Year")
ax2.set_ylabel("Share (%)")
ax2.set_xticks(YEARS)
ax2.yaxis.set_major_formatter(ticker.PercentFormatter(decimals=0))
ax2.legend(framealpha=0.8)
ax2.grid(axis="y", alpha=0.3)
ax2.spines[["top", "right"]].set_visible(False)

plt.tight_layout()
plt.savefig("ai_trend.png", dpi=150, bbox_inches="tight")
print("\nSaved: ai_trend.png")
