#!/usr/bin/env python3
"""Reproduces the figures, tables and prose numbers of the empirical study from
a measurement run's JSONL logs. See README.md for the expected layout."""

import argparse
import json
from pathlib import Path

import matplotlib as mpl
import matplotlib.lines as mlines
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

NODES = {"A": "pinger-a", "B": "ponger-b", "C": "lurker-c"}
COLS = ["A", "B", "C"]
NODE_COLORS = {"A": "#56B4E9", "B": "#E69F00", "C": "#009E73"}
CASE_ORDER = ["I", "II_ok", "II_fail_III", "III"]
CASE_LABELS = {"I": "I", "II_ok": "II-A", "II_fail_III": "II-B", "III": "III"}
CASE_COLORS = {"I": "#E69F00", "II_ok": "#56B4E9", "II_fail_III": "#CC79A7", "III": "#009E73"}
MEAS_LABEL = {"A": "pool discovery", "B": "forwarding"}
STRATS = {"rt": "Routing Table", "ps": "Peerstore", "random": "Random Lookup"}
ORIGIN_STRAT = {"measurement-rt": "rt", "measurement-ps": "ps", "measurement-random": "random"}

ap = argparse.ArgumentParser(description=__doc__)
ap.add_argument("datadir", nargs="?", default=".", type=Path,
                help="run directory holding pinger-a/, ponger-b/ and lurker-c/")
ap.add_argument("--style", type=Path, help="matplotlib style file to apply")
args = ap.parse_args()

BASE = args.datadir.resolve()
missing = [d for d in NODES.values() if not (BASE / d).is_dir()]
if missing:
    ap.error(f"{BASE} has no {', '.join(missing)}")
OUT = BASE / "out"
OUT.mkdir(exist_ok=True)

if args.style:
    plt.style.use(args.style)
if mpl.rcParams["text.usetex"]:
    try:
        _f = plt.figure(); _f.text(.5, .5, "probe 123"); _f.canvas.draw(); plt.close(_f)
    except RuntimeError:
        print("WARN: no working LaTeX -> falling back to mathtext")
        mpl.rcParams["text.usetex"] = False


def load_jsonl(path):
    rows = []
    with open(path) as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                # a torn line from an interrupted write must not kill the load
                print(f"WARN {path.name}:{lineno} unparsable: {e}")
                continue
            row = {"ts": obj.get("Timestamp"), "type": obj.get("Type")}
            row.update(obj.get("Payload") or {})
            rows.append(row)
    df = pd.DataFrame(rows)
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    return df.sort_values("ts").reset_index(drop=True)


def tbl(label, stem):
    return load_jsonl(BASE / NODES[label] / f"{stem}.jsonl")


def section(t):
    print(f"\n{'=' * 72}\n# {t}\n{'=' * 72}")


def savefig(fig, name):
    fig.savefig(OUT / name, bbox_inches="tight")
    print(f"-> {OUT.name}/{name}")


pq = tbl("B", "ponger-query")
lookups = {l: tbl(l, "dht-lookup") for l in COLS}
states = {l: tbl(l, "state-sample") for l in COLS}

# ---------------------------------------------------------------- run window
section("Run window")
ts_all = pd.concat([lookups[l]["ts"] for l in COLS])
print(f"start {ts_all.min()}  end {ts_all.max()}  (UTC)")
print(f"duration {(ts_all.max() - ts_all.min()).total_seconds() / 3600:.1f} h")
print(f"probes per strategy: {pq['strategy'].value_counts().to_dict()}")

# ------------------------------------------- Table 1: peers queried per lookup
section("Table 1: peers queried per lookup")
rows = []
for l in COLS:
    df = lookups[l]
    for origin, sel in [("background", df["origin"].eq("")),
                        (MEAS_LABEL.get(l), df["origin"].ne(""))]:
        s = df.loc[sel, "num_queried"]
        if origin is None or s.empty:
            continue
        rows.append({"Node": l, "Origin": origin, "n": len(s), "p50": s.median(),
                     "mean": s.mean(), "p90": s.quantile(.9), "p99": s.quantile(.99)})
t1 = pd.DataFrame(rows).set_index(["Node", "Origin"])
print(t1.round(1).to_string())

# ---------------------------------------------- Fig 5: ECDF peers per lookup
cat_style = {"background": "-", "measurement": "--"}


def ecdf(s):
    x = np.sort(s.to_numpy(dtype=float))
    return x, np.arange(1, x.size + 1) / x.size


fig, ax = plt.subplots(figsize=(4.65, 2.0))
for l in COLS:
    df = lookups[l]
    parts = {"background": df.loc[df["origin"].eq(""), "num_queried"],
             "measurement": df.loc[df["origin"].ne(""), "num_queried"]}
    for cat, s in parts.items():
        if s.empty:
            continue
        x, y = ecdf(s)
        name = "background" if cat == "background" else MEAS_LABEL[l]
        ax.plot(x, y, cat_style[cat], color=NODE_COLORS[l], linewidth=0.9, label=f"{l} {name}")
ax.set_xlim(0, 300)
ax.set_ylim(0, 1.0)
ax.set_xlabel("peers queried per lookup")
ax.set_ylabel("cumulative share")
ax.grid(alpha=.3)
h = dict(zip(*reversed(ax.get_legend_handles_labels())))
blank = mlines.Line2D([], [], linestyle="none")
order = ["A background", "A pool discovery", "B background", "B forwarding", "C background", None]
ax.legend([h[l] if l else blank for l in order], [l if l else "" for l in order],
          ncol=3, loc="lower center", bbox_to_anchor=(0.5, 1.01),
          columnspacing=1.8, handlelength=1.9, frameon=False)
savefig(fig, "sidechannel_ecdf_num_queried.pdf")

# ------------------------------- Case III lookup breadth by selection strategy
section("Case III lookup breadth by selection strategy")
c3 = lookups["B"][lookups["B"]["origin"].isin(ORIGIN_STRAT)].copy()
c3["strategy"] = c3["origin"].map(ORIGIN_STRAT)


def breadth(df):
    return df.groupby("strategy")["num_queried"].agg(n="size", p50="median", mean="mean").reindex(STRATS)


aggregate = breadth(c3)
print(aggregate.round(1).to_string())
# stopFn fires once FindPeer has the target, so "stopped" is the reached-target subset
reached = breadth(c3[c3["terminate_reason"].eq("stopped")])
reached["reached %"] = reached["n"] / aggregate["n"] * 100
print("\nlookups that reached the target:")
print(reached.round(1).to_string())
print("\nterminate_reason counts:")
print(pd.crosstab(c3["strategy"], c3["terminate_reason"]).reindex(STRATS).to_string())

# ------------------------------------------------ Table 2: case distribution
section("Table 2: case distribution per selection strategy (%)")
ct = pd.crosstab(pq["strategy"], pq["case_path"]).reindex(index=STRATS, columns=CASE_ORDER)
print(ct.to_string(), "\n")
t2 = (pd.crosstab(pq["strategy"], pq["case_path"], normalize="index")
      .reindex(index=STRATS, columns=CASE_ORDER) * 100).rename(index=STRATS)
t2.columns = [CASE_LABELS[c] for c in CASE_ORDER]
print(t2.round(2).to_string())
print(f"\nCase I range across strategies: {t2['I'].min():.1f} to {t2['I'].max():.1f}%")

# ------------------------------------- Fig 6: case shares over time by strategy
g = (pq.set_index("ts").groupby([pd.Grouper(freq="30min"), "strategy", "case_path"])
     .size().unstack("case_path", fill_value=0).reindex(columns=CASE_ORDER, fill_value=0))
gs = g.div(g.sum(axis=1), axis=0)
t0 = gs.index.get_level_values(0).min()
fig, axes = plt.subplots(3, 1, figsize=(4.65, 5.0), sharex=True, sharey=True)
fig.subplots_adjust(hspace=0.25)
for ax, (strat, title) in zip(axes, STRATS.items()):
    sub = gs.xs(strat, level="strategy")
    sub.index = (sub.index - t0).total_seconds() / 3600
    sub.rename(columns=CASE_LABELS).plot(ax=ax, color=[CASE_COLORS[c] for c in CASE_ORDER],
                                         linewidth=0.9, legend=False)
    ax.set_title(title)
    ax.set_xlim(0, 41)
    ax.set_ylim(0, 0.85)
    ax.grid(alpha=.3)
axes[-1].set_xlabel("time (h)")
fig.supylabel("share of forwarding requests", fontsize="medium")
handles, labels = axes[0].get_legend_handles_labels()
fig.legend(handles, labels, loc="lower center",
           bbox_to_anchor=(0.5, axes[0].get_position().y1 + 0.022), ncol=len(CASE_ORDER), frameon=False)
savefig(fig, "case_distribution_by_strategy.pdf")

# ------------------------------------------ Table 3: forwarding readiness
section("Table 3: forwarding readiness latency (successful attempts; ms)")


def lat_stats(s):
    s = pd.to_numeric(s, errors="coerce").dropna()
    return pd.Series({"successful": len(s), "p50": s.median(),
                      "p90": s.quantile(.9), "p99": s.quantile(.99)})


fb_err = pq["connect_fallback_err"] if "connect_fallback_err" in pq.columns else pd.Series(pd.NA, index=pq.index)
success = pq["lookup_err"].isna() & fb_err.isna()
lat = pq[success].groupby("case_path")["total_ms"].apply(lat_stats).unstack().reindex(CASE_ORDER)
lat.insert(0, "attempts", pq.groupby("case_path").size().reindex(CASE_ORDER))
lat["fail %"] = pq.assign(failed=~success).groupby("case_path")["failed"].mean().reindex(CASE_ORDER) * 100
lat.index = lat.index.map(CASE_LABELS)
lat[["attempts", "successful"]] = lat[["attempts", "successful"]].astype(int)
print(lat.round(1).to_string())
print(f"note: II-B percentiles rest on {lat.loc['II-B', 'successful']} of "
      f"{lat.loc['II-B', 'attempts']} attempts")

section("Stale addresses (Case II)")
case2 = pq[pq["case_path"].isin(["II_ok", "II_fail_III"])].copy()
case2["failed"] = case2["case_path"] == "II_fail_III"
print(f"stale share of peerstore hits: {case2['failed'].mean() * 100:.1f}% "
      f"({case2['failed'].sum()} / {len(case2)})")
case2["age_bin"] = pd.cut(case2["peerstore_age_ms"] / 60000, bins=[0, 1, 10, 60, float("inf")],
                          labels=["<1min", "1-10min", "10-60min", ">60min"])
age = case2.groupby("age_bin", observed=True)["failed"].agg(["mean", "count"])
age["mean"] *= 100
print(age.round(2).rename(columns={"mean": "fail %", "count": "n"}).to_string())
print(f"fail rate across age bins: {age['mean'].min():.1f} to {age['mean'].max():.1f}%")

section("Case III failure by strategy")
iii = pq[pq["case_path"] == "III"].copy()
iii["lookup_failed"] = iii["lookup_err"].notna()
iii["connect_failed"] = fb_err[iii.index].notna()
iii["failed"] = iii["lookup_failed"] | iii["connect_failed"]
t = iii.groupby("strategy").agg(n=("failed", "size"), fail_pct=("failed", "mean"),
                                lookup_pct=("lookup_failed", "mean"),
                                connect_pct=("connect_failed", "mean")).reindex(STRATS)
t.loc["ALL"] = [len(iii), iii["failed"].mean(), iii["lookup_failed"].mean(), iii["connect_failed"].mean()]
t[["fail_pct", "lookup_pct", "connect_pct"]] *= 100
t["n"] = t["n"].astype(int)
print(t.round(1).to_string())

# ------------------------------------- peerstore & connections + Fig 7
section("Peerstore and connections")
for l in COLS:
    ss = states[l]
    print(f"{l}: peers_with_addrs end={ss['peers_with_addrs'].iloc[-1]}  "
          f"last-hour growth=+{ss['peers_with_addrs'].iloc[-1] - ss['peers_with_addrs'].iloc[-121]}  "
          f"conns median={ss['active_conns'].median():.0f}")
conns = pd.DataFrame({l: states[l]["active_conns"].to_numpy()[:4800] for l in COLS})
print(f"A holds most connections in {(conns['A'] >= conns[['B', 'C']].max(axis=1)).mean() * 100:.0f}% of samples")

t0 = min(states[l]["ts"].min() for l in COLS)
fig, ax = plt.subplots(figsize=(4.65, 2.2))
for l in COLS:
    ss = states[l]
    t_h = (ss["ts"] - t0).dt.total_seconds() / 3600
    ax.plot(t_h, ss["peers_with_addrs"], color=NODE_COLORS[l], lw=0.9)
    ax.plot(t_h, ss["active_conns"].rolling(8, center=True).median(), color=NODE_COLORS[l], lw=0.9)
ax.set_yscale("log")
ax.set_ylim(100, 5e4)  # reviewer: start the log axis at a clean decade
ax.set_xlabel("time (h)")
ax.set_ylabel("count")
ax.set_xlim(0, 41)
ax.grid(alpha=.3)
x_end = ((states["A"]["ts"] - t0).dt.total_seconds() / 3600).max()
ax.text(x_end, 2.0e4, "addressable peerstore", ha="right", va="top")
ax.text(x_end, 6.5e2, "active connections", ha="right", va="bottom")
ax.legend(handles=[mlines.Line2D([], [], color=NODE_COLORS[l], label=l) for l in COLS],
          loc="lower center", bbox_to_anchor=(0.5, 1), ncol=3, frameon=False)
savefig(fig, "peerstore_vs_conns.pdf")

# --------------------------------- connection lifetime (prose + footnote 3)
section("Connection lifetime")
for l in COLS:
    close_ts, open_ts = [], []
    with open(BASE / NODES[l] / "conn-close.jsonl") as f:
        for line in f:
            o = json.loads(line)
            ot = o["Payload"].get("open_time")
            if not ot:
                continue
            close_ts.append(o["Timestamp"])
            open_ts.append(ot)
    s = pd.Series((pd.to_datetime(close_ts, utc=True) - pd.to_datetime(open_ts, utc=True)).total_seconds())
    s = s[s >= 0]
    print(f"{l}: n={len(s):,}  median={s.median():.1f} s  <30s={((s < 30).mean() * 100):.1f}%")
