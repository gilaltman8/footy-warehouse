"""Draw the dbt lineage (sources, seeds, models, singular + unit tests) to docs/lineage.png.

Reads dbt/footy_wh/target/manifest.json — run `dbt parse` first. Re-run after any model change.
"""
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

ROOT = Path(__file__).resolve().parent.parent
m = json.loads((ROOT / "dbt/footy_wh/target/manifest.json").read_text())

KIND = {"source": ("#4E8A3E", "source"), "seed": ("#C77C1E", "seed"), "model": ("#2B6A99", "model"),
        "test": ("#6B6B6B", "singular test"), "unit_test": ("#7A5BA6", "unit test")}
nodes = {}
for uid, s in m.get("sources", {}).items():
    nodes[uid] = (f"{s['source_name']}.{s['name']}", "source", [])
for uid, n in m["nodes"].items():
    if n["package_name"] != m["metadata"].get("project_name", n["package_name"]):
        continue
    rt = n["resource_type"]
    if rt in ("model", "seed") or (rt == "test" and "test_metadata" not in n):   # skip generic tests
        nodes[uid] = (n["name"], rt, n.get("depends_on", {}).get("nodes", []))
for uid, u in m.get("unit_tests", {}).items():
    nodes[uid] = (u["name"], "unit_test", u.get("depends_on", {}).get("nodes", []))
parents = {u: [p for p in deps if p in nodes] for u, (_, _, deps) in nodes.items()}

depth = {}
def d(u):
    if u not in depth:
        depth[u] = 0 if not parents[u] else 1 + max(d(p) for p in parents[u])
    return depth[u]
for u in nodes:
    d(u)

cols = {}
for u in sorted(nodes, key=lambda u: (nodes[u][1] == "test", nodes[u][0])):
    cols.setdefault(depth[u], []).append(u)
CH = 0.13                                     # width of one character, in plot units
colw = {c: max(len(nodes[u][0]) for u in us) * CH + 0.5 for c, us in cols.items()}
colx, x = {}, 0.0
for c in sorted(cols):
    colx[c] = x + colw[c] / 2
    x += colw[c] + 1.1
pos = {}
for c, us in cols.items():
    for i, u in enumerate(us):
        pos[u] = (colx[c], -(i - (len(us) - 1) / 2) * 0.9)

H = 0.5
top = max(p[1] for p in pos.values())
fig, ax = plt.subplots(figsize=(x * 0.62, (1.4 + 0.9 * max(len(v) for v in cols.values())) * 0.9), dpi=170)
for u, ps in parents.items():
    x2, y2 = pos[u]
    for p in ps:
        x1, y1 = pos[p]
        ax.annotate("", xy=(x2 - colw[depth[u]] / 2, y2), xytext=(x1 + colw[depth[p]] / 2, y1), zorder=1,
                    arrowprops=dict(arrowstyle="-|>", color="#A7B1BC", lw=0.9, shrinkA=1, shrinkB=1,
                                    connectionstyle="arc3,rad=0.06"))
for u, (name, kind, _) in nodes.items():
    xx, y = pos[u]; w = colw[depth[u]]
    ax.add_patch(FancyBboxPatch((xx - w / 2, y - H / 2), w, H, boxstyle="round,pad=0.02,rounding_size=0.08",
                                fc=KIND[kind][0], ec="white", lw=1.2, zorder=2))
    ax.text(xx, y, name, ha="center", va="center", color="white", fontsize=8, family="DejaVu Sans", zorder=3)
lx = 0.0
for k, (c, label) in KIND.items():
    if any(v[1] == k for v in nodes.values()):
        ax.add_patch(FancyBboxPatch((lx, top + 0.85), 0.3, 0.28, fc=c, ec="none",
                                    boxstyle="round,pad=0.01,rounding_size=0.05"))
        ax.text(lx + 0.45, top + 0.99, label, va="center", fontsize=8, color="#333")
        lx += 0.45 + len(label) * CH + 0.6
ax.set_xlim(-0.3, x - 0.8)
ax.set_ylim(min(p[1] for p in pos.values()) - 0.6, top + 1.4)
ax.set_aspect("auto"); ax.axis("off")
out = ROOT / "docs/lineage.png"
fig.savefig(out, bbox_inches="tight", facecolor="white")
print(f"wrote {out.relative_to(ROOT)}: {len(nodes)} nodes, {sum(map(len, parents.values()))} edges, {len(cols)} columns")
