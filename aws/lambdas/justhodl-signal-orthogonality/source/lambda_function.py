"""
justhodl-signal-orthogonality -- the signal-redundancy auditor.

With 320+ Lambdas producing signals, redundancy is statistically
inevitable. This engine reads the calibration-fleet history (the
unified per-day score snapshot across all registered engines), builds
the pairwise correlation matrix, identifies clusters of engines that
move together, and recommends which engines to keep per cluster (by
IC) and which are candidates for retirement.

Outputs:

  - data/signal-orthogonality.json  -- the full audit:
      * pairwise correlation matrix
      * per-engine max-correlation-with-another + mean-abs-correlation
      * cluster assignments (greedy linkage at |corr| >= 0.80)
      * "keeper" per cluster (highest |IC| from fleet calibrator)
      * candidates for retirement (in a cluster with >=1 stronger sibling)
      * effective-information-rank proxy
        ( = 1 / mean_abs_pairwise_correlation, a crude but
          informative measure of how many independent dimensions of
          information the signal fleet actually carries )

Read alongside the calibration fleet, this engine answers the
question: of the engines we run, which are SAYING something
independent vs which are echoing their neighbours? The output is
intentionally conservative -- it RECOMMENDS, it does not retire.
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timezone

import boto3

S3_BUCKET = "justhodl-dashboard-live"
HIST_KEY = "data/calibration-fleet-history.json"
GSI_HIST_KEY = "data/gsi-dim-history.json"
FLEET_REPORT_KEY = "data/calibration-fleet.json"
OUT_KEY = "data/signal-orthogonality.json"
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

MIN_N = 20           # minimum paired observations to compute a correlation
HIGH_CORR = 0.80     # the redundancy threshold
MOD_CORR = 0.60      # the watchlist threshold

s3 = boto3.client("s3")


# ============== pure-python stats helpers ==============================
def rankdata(xs):
    order = sorted(range(len(xs)), key=lambda i: xs[i])
    ranks = [0.0] * len(xs)
    i = 0
    while i < len(xs):
        j = i
        while j + 1 < len(xs) and xs[order[j + 1]] == xs[order[i]]:
            j += 1
        avg = (i + j) / 2.0 + 1.0
        for k in range(i, j + 1):
            ranks[order[k]] = avg
        i = j + 1
    return ranks


def pearson(a, b):
    n = len(a)
    if n < 5:
        return None
    ma, mb = sum(a) / n, sum(b) / n
    cov = sum((a[i] - ma) * (b[i] - mb) for i in range(n))
    va = sum((x - ma) ** 2 for x in a)
    vb = sum((x - mb) ** 2 for x in b)
    if va <= 0 or vb <= 0:
        return None
    return cov / (va * vb) ** 0.5


def spearman(a, b):
    if len(a) != len(b) or len(a) < 5:
        return None
    return pearson(rankdata(a), rankdata(b))


# ============== I/O helpers ============================================
def read_json(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET,
                                        Key=key)["Body"].read())
    except Exception:
        return {}


def write_json(key, payload, cache_seconds=600):
    s3.put_object(Bucket=S3_BUCKET, Key=key,
                  Body=json.dumps(payload,
                                  default=str).encode("utf-8"),
                  ContentType="application/json",
                  CacheControl="public, max-age=%d" % cache_seconds)


def send_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = "https://api.telegram.org/bot%s/sendMessage" % TELEGRAM_TOKEN
        data = json.dumps({"chat_id": TELEGRAM_CHAT_ID, "text": msg,
                           "parse_mode": "HTML"}).encode("utf-8")
        urllib.request.urlopen(urllib.request.Request(
            url, data=data, headers={"Content-Type": "application/json"}),
            timeout=8).read()
    except Exception as e:
        print("telegram fail: %s" % e)


# ============== clustering ==============================================
def greedy_cluster(engines, corr_matrix, threshold):
    """Greedy linkage clustering: two engines join the same cluster if
    their absolute correlation is >= threshold. Returns a list of
    clusters (each a list of engine names)."""
    # union-find
    parent = {e: e for e in engines}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    for i, ei in enumerate(engines):
        for ej in engines[i + 1:]:
            c = corr_matrix.get((ei, ej))
            if c is not None and abs(c) >= threshold:
                union(ei, ej)

    clusters = {}
    for e in engines:
        clusters.setdefault(find(e), []).append(e)
    return list(clusters.values())


# ============== handler =================================================
def lambda_handler(event, context):
    t0 = time.time()

    # --- 1. read the fleet history --------------------------------------
    hist = read_json(HIST_KEY)
    snaps = hist.get("snapshots") or []
    if not snaps:
        # if the fleet hasn't accumulated anything yet, fall back to
        # gsi-dim-history which has at least gsi/dims for many dates
        gsi_hist = read_json(GSI_HIST_KEY) or {}
        gsi_snaps = gsi_hist.get("snapshots") or []
        # convert to fleet-history shape
        snaps = [{"date": s["date"],
                  "scores": {"gsi_total": s.get("gsi"),
                             **{f"gsi_{k}": v for k, v
                                in (s.get("dims") or {}).items()}}}
                 for s in gsi_snaps if s.get("gsi") is not None]

    if len(snaps) < MIN_N:
        report = {
            "as_of": datetime.now(timezone.utc).isoformat(),
            "engines_total": 0, "snapshots_total": len(snaps),
            "mode": "insufficient",
            "note": ("need %d sessions of fleet history (have %d) "
                     "before redundancy clustering is meaningful."
                     % (MIN_N, len(snaps))),
        }
        write_json(OUT_KEY, report)
        return {"statusCode": 200, "body": json.dumps(report)}

    # --- 2. assemble per-engine series ----------------------------------
    # series[engine] = [(date, score), ...] in chronological order
    snaps = sorted(snaps, key=lambda s: s.get("date") or "")
    by_date = {s["date"]: s for s in snaps}
    dates = [s["date"] for s in snaps]

    engine_set = set()
    for s in snaps:
        engine_set.update((s.get("scores") or {}).keys())
    engines = sorted(engine_set)

    series = {e: [] for e in engines}
    for d in dates:
        scores = (by_date[d].get("scores") or {})
        for e in engines:
            series[e].append(scores.get(e))   # None if absent that day

    # --- 3. pairwise Spearman correlation -------------------------------
    corr = {}
    for i, ei in enumerate(engines):
        for ej in engines[i + 1:]:
            paired = [(a, b) for a, b in zip(series[ei], series[ej])
                      if isinstance(a, (int, float))
                      and isinstance(b, (int, float))]
            if len(paired) < MIN_N:
                continue
            xs = [p[0] for p in paired]
            ys = [p[1] for p in paired]
            c = spearman(xs, ys)
            if c is not None:
                corr[(ei, ej)] = c
                corr[(ej, ei)] = c

    # --- 4. per-engine redundancy metrics -------------------------------
    fleet_report = read_json(FLEET_REPORT_KEY)
    ic_by_engine = {e.get("name"): e.get("ic_spearman")
                    for e in fleet_report.get("engines") or []}
    label_by_engine = {e.get("name"): e.get("label")
                       for e in fleet_report.get("engines") or []}

    per_engine = []
    for e in engines:
        peers = [(o, corr[(e, o)]) for o in engines
                 if o != e and (e, o) in corr]
        if not peers:
            per_engine.append({
                "name": e, "label": label_by_engine.get(e) or e,
                "max_abs_corr": None, "max_corr_with": None,
                "mean_abs_corr": None, "n_peers": 0,
                "ic_spearman": ic_by_engine.get(e),
            })
            continue
        peers_by_abs = sorted(peers, key=lambda p: -abs(p[1]))
        top = peers_by_abs[0]
        mean_abs = sum(abs(c) for _, c in peers) / len(peers)
        per_engine.append({
            "name": e, "label": label_by_engine.get(e) or e,
            "max_abs_corr": round(abs(top[1]), 3),
            "max_corr_with": top[0],
            "max_corr_signed": round(top[1], 3),
            "mean_abs_corr": round(mean_abs, 3),
            "n_peers": len(peers),
            "ic_spearman": ic_by_engine.get(e),
        })

    # --- 5. cluster + retire recommendations ----------------------------
    clusters = greedy_cluster(engines, corr, HIGH_CORR)
    cluster_out = []
    retire_recs = []
    for c in clusters:
        if len(c) <= 1:
            continue
        # rank by |IC| descending, then by predictive direction
        ranked = sorted(c, key=lambda e: -abs(ic_by_engine.get(e) or 0.0))
        keeper = ranked[0]
        retiring = ranked[1:]
        cluster_out.append({
            "members": c,
            "labels": [label_by_engine.get(e) or e for e in c],
            "size": len(c),
            "keeper": keeper,
            "keeper_label": label_by_engine.get(keeper) or keeper,
            "keeper_ic": ic_by_engine.get(keeper),
            "candidates_to_retire": retiring,
            "internal_corr": [
                {"a": a, "b": b, "corr": round(corr.get((a, b), 0.0), 3)}
                for i, a in enumerate(c) for b in c[i + 1:]
            ],
        })
        for r in retiring:
            retire_recs.append({
                "engine": r,
                "label": label_by_engine.get(r) or r,
                "redundant_with": keeper,
                "redundant_with_label": (label_by_engine.get(keeper)
                                          or keeper),
                "correlation": round(corr.get((r, keeper), 0.0), 3),
                "engine_ic": ic_by_engine.get(r),
                "keeper_ic": ic_by_engine.get(keeper),
            })

    # also watch-list pairs at the moderate threshold
    watchlist = []
    seen = set()
    for (a, b), c in corr.items():
        if a >= b:
            continue
        if (a, b) in seen:
            continue
        seen.add((a, b))
        if MOD_CORR <= abs(c) < HIGH_CORR:
            watchlist.append({
                "a": a, "a_label": label_by_engine.get(a) or a,
                "b": b, "b_label": label_by_engine.get(b) or b,
                "corr": round(c, 3),
                "ic_a": ic_by_engine.get(a),
                "ic_b": ic_by_engine.get(b),
            })
    watchlist.sort(key=lambda x: -abs(x["corr"]))

    # --- 6. effective-information-rank proxy ----------------------------
    # 1 / mean_abs_correlation -- a crude measure of how many
    # independent dimensions of information the fleet carries.
    all_corrs = [abs(corr[(a, b)]) for i, a in enumerate(engines)
                 for b in engines[i + 1:] if (a, b) in corr]
    mean_abs_all = (sum(all_corrs) / len(all_corrs)
                    if all_corrs else None)
    eff_rank = (1.0 / mean_abs_all if (mean_abs_all and mean_abs_all > 0)
                else None)

    # --- 7. publish ------------------------------------------------------
    matrix = {a: {b: round(corr.get((a, b), 0.0), 3) if a != b else 1.0
                  for b in engines}
              for a in engines}

    report = {
        "as_of": datetime.now(timezone.utc).isoformat(),
        "snapshots_total": len(snaps),
        "engines_total": len(engines),
        "engines": engines,
        "engine_labels": label_by_engine,
        "engine_ic": ic_by_engine,
        "correlation_matrix": matrix,
        "per_engine": per_engine,
        "clusters_high_redundancy": cluster_out,
        "watchlist_moderate": watchlist[:20],
        "retirement_candidates": retire_recs,
        "high_corr_threshold": HIGH_CORR,
        "moderate_corr_threshold": MOD_CORR,
        "mean_abs_pairwise_corr": (round(mean_abs_all, 3)
                                    if mean_abs_all is not None
                                    else None),
        "effective_information_rank": (round(eff_rank, 2)
                                        if eff_rank is not None
                                        else None),
        "summary": {
            "engines_in_redundancy_cluster": sum(
                len(c) for c in clusters if len(c) > 1),
            "n_clusters": sum(1 for c in clusters if len(c) > 1),
            "n_independent_singletons": sum(
                1 for c in clusters if len(c) == 1),
            "n_retire_candidates": len(retire_recs),
            "n_watchlist": len(watchlist),
        },
        "methodology": (
            "Pairwise Spearman rank correlation over the fleet's daily "
            "score history. Clusters formed by greedy union-find at "
            "|corr|>=%.2f (high redundancy). Pairs at %.2f<=|corr|<%.2f "
            "land on the moderate-redundancy watchlist. Within each "
            "high-redundancy cluster, the engine with the highest |IC| "
            "from the calibration fleet is the keeper; the others are "
            "candidates for retirement. Effective information rank "
            "= 1 / mean|pairwise correlation|, a crude but informative "
            "proxy for how many independent dimensions of information "
            "the fleet actually carries. The audit is conservative -- "
            "it recommends, it does not retire."
            % (HIGH_CORR, MOD_CORR, HIGH_CORR)),
        "duration_s": round(time.time() - t0, 1),
    }
    write_json(OUT_KEY, report)

    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "engines": len(engines),
        "snapshots": len(snaps),
        "clusters": sum(1 for c in clusters if len(c) > 1),
        "retire_candidates": len(retire_recs),
        "watchlist": len(watchlist),
        "effective_rank": (round(eff_rank, 2) if eff_rank else None),
        "elapsed_s": round(time.time() - t0, 1)})}
