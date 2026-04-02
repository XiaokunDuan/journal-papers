"""
Polished AI trend chart – stacked area + line overlay
"""

import json, re
from collections import defaultdict
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

# ── data ─────────────────────────────────────────────────────────────────────
AI_PATTERN = re.compile(
    r"\b(artificial intelligence|machine learning|deep learning|neural network|"
    r"large language model|LLM|GPT|ChatGPT|generative AI|"
    r"natural language processing|NLP|computer vision|"
    r"reinforcement learning|random forest|gradient boosting|XGBoost|"
    r"support vector machine|SVM|"
    r"predictive (?:model|analytics|algorithm)|"
    r"data.driven|AI.(?:assisted|augmented|enabled|powered)|"
    r"automation|robot(?:ic|ics)|"
    r"transformer model|BERT|text mining|"
    r"human.AI|algorithm aversion|algorithm appreciation"
    r")\b", re.IGNORECASE)

papers    = json.load(open("papers.json"))
JOURNALS  = ["MS", "OR", "MSOM", "POM"]
YEARS     = list(range(2021, 2026))

ai_n  = defaultdict(lambda: defaultdict(int))
all_n = defaultdict(lambda: defaultdict(int))
for p in papers:
    yr, jn = p["year"], p["journal"]
    if yr not in YEARS or jn not in JOURNALS:
        continue
    all_n[jn][yr] += 1
    if AI_PATTERN.search(f"{p.get('title','')} {p.get('abstract','')}"):
        ai_n[jn][yr] += 1

# arrays
ai_mat  = np.array([[ai_n[j][y]  for y in YEARS] for j in JOURNALS], dtype=float)
all_mat = np.array([[all_n[j][y] for y in YEARS] for j in JOURNALS], dtype=float)
pct_mat = 100 * ai_mat / all_mat

# ── style ─────────────────────────────────────────────────────────────────────
COLORS  = {"MS": "#2E86AB", "OR": "#E84855", "MSOM": "#3BB273", "POM": "#F4A261"}
CLIST   = [COLORS[j] for j in JOURNALS]
xs      = np.array(YEARS)

fig = plt.figure(figsize=(15, 6.5), facecolor="#F8F9FA")
fig.patch.set_facecolor("#F8F9FA")

# ── left: stacked bar (absolute AI papers) ────────────────────────────────────
ax1 = fig.add_subplot(1, 2, 1, facecolor="#F8F9FA")

bar_w   = 0.55
bottoms = np.zeros(len(YEARS))
bars    = []
for i, jn in enumerate(JOURNALS):
    b = ax1.bar(xs, ai_mat[i], bar_w, bottom=bottoms,
                color=CLIST[i], alpha=0.88, label=jn, zorder=3)
    bars.append(b)
    # value labels inside bars (only if tall enough)
    for xi, (bot, val) in enumerate(zip(bottoms, ai_mat[i])):
        if val >= 6:
            ax1.text(xs[xi], bot + val / 2, f"{int(val)}",
                     ha="center", va="center", fontsize=8,
                     color="white", fontweight="bold")
    bottoms += ai_mat[i]

# total label on top
for xi, total in enumerate(bottoms):
    ax1.text(xs[xi], total + 1.5, f"{int(total)}",
             ha="center", va="bottom", fontsize=9, fontweight="bold", color="#333")

ax1.set_title("AI Papers Count per Journal", fontsize=13, fontweight="bold",
              pad=12, color="#222")
ax1.set_xlabel("Year", fontsize=11, color="#444")
ax1.set_ylabel("Number of AI-related papers", fontsize=11, color="#444")
ax1.set_xticks(YEARS)
ax1.set_xlim(YEARS[0] - 0.5, YEARS[-1] + 0.5)
ax1.set_ylim(0, bottoms.max() * 1.15)
ax1.tick_params(colors="#555")
ax1.spines[["top", "right", "left"]].set_visible(False)
ax1.spines["bottom"].set_color("#ccc")
ax1.yaxis.grid(True, color="#ddd", zorder=0)
ax1.set_axisbelow(True)
ax1.legend(loc="upper left", framealpha=0, fontsize=10)

# ── right: line chart (% share) ───────────────────────────────────────────────
ax2 = fig.add_subplot(1, 2, 2, facecolor="#F8F9FA")

for i, jn in enumerate(JOURNALS):
    vals = pct_mat[i]
    ax2.plot(xs, vals, color=CLIST[i], linewidth=2.5,
             marker="o", markersize=7, label=jn, zorder=4)
    ax2.fill_between(xs, vals, alpha=0.08, color=CLIST[i])
    # end-point label
    ax2.annotate(f"{vals[-1]:.0f}%",
                 xy=(xs[-1], vals[-1]),
                 xytext=(6, 0), textcoords="offset points",
                 fontsize=9, color=CLIST[i], fontweight="bold", va="center")

ax2.set_title("AI Papers as % of Journal Output", fontsize=13, fontweight="bold",
              pad=12, color="#222")
ax2.set_xlabel("Year", fontsize=11, color="#444")
ax2.set_ylabel("Share (%)", fontsize=11, color="#444")
ax2.set_xticks(YEARS)
ax2.set_xlim(YEARS[0] - 0.3, YEARS[-1] + 0.8)
ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:.0f}%"))
ax2.tick_params(colors="#555")
ax2.spines[["top", "right", "left"]].set_visible(False)
ax2.spines["bottom"].set_color("#ccc")
ax2.yaxis.grid(True, color="#ddd", zorder=0)
ax2.set_axisbelow(True)
ax2.legend(loc="upper left", framealpha=0, fontsize=10)

# ── shared footnote ───────────────────────────────────────────────────────────
fig.text(0.5, -0.03,
         "Source: OpenAlex  ·  Journals: Management Science, Operations Research, "
         "M&SOM, Production and Operations Management  ·  2021–2025",
         ha="center", fontsize=8.5, color="#888")

plt.suptitle("AI Research Trends in Top-4 Operations Management Journals",
             fontsize=15, fontweight="bold", color="#111", y=1.03)

plt.tight_layout()
plt.savefig("ai_trend_v2.png", dpi=160, bbox_inches="tight",
            facecolor=fig.get_facecolor())
print("Saved: ai_trend_v2.png")
