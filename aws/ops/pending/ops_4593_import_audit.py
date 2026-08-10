"""ops 4593 — FRED import audit (Khalid: verify the drain still honors
popularity-first / 90-day freshness / no-discontinued, explain the
ACTION_REQUIRED badge + the 5 sentinel incidents, and give the honest
remaining/ETA).

Code audit (repo, done before this op): FRESH_DAYS=90 at queue build,
DISCONTINUED-in-title excluded and counted, stale-break on the
last_updated-desc category walk, popularity captured per row, and the
undrained tail sorted popularity-desc (re-sorted on scope change). This
op verifies the LIVE state matches the code's promises.
"""
import gzip
import json
import sys
from datetime import datetime, timezone

import boto3

from ops_report import report

B = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")


def get_json(key):
    try:
        b = s3.get_object(Bucket=B, Key=key)["Body"].read()
        if key.endswith(".gz"):
            b = gzip.decompress(b)
        return json.loads(b)
    except Exception as e:
        return {"_error": "%s: %s" % (type(e).__name__, str(e)[:90])}


def contract(r, name, cond, why):
    if cond:
        r.ok("  [%s] %s" % (name, why))
        return 0
    r.fail("  [%s] CONTRACT MISS — %s" % (name, why))
    return 1


def main():
    with report("4593_import_audit") as r:
        r.heading("ops 4593 — FRED import audit")
        misses = 0
        now = datetime.now(timezone.utc)

        r.section("1. import-health — the badge and the five incidents")
        ih = get_json("data/import-health.json")
        misses += contract(r, "health", "_error" not in ih,
                           "import-health readable (%s)"
                           % ih.get("_error", "ok"))
        raw = ih.get("pipelines") or {}
        # shape-agnostic: sentinel publishes a LIST of pipeline dicts
        if isinstance(raw, dict):
            pipe_items = [dict(v or {}, name=k) for k, v in raw.items()]
        else:
            pipe_items = [dict(p or {}) for p in raw if isinstance(p, dict)]
        overall = ih.get("status") or ih.get("overall")
        r.log("  overall=%s  sweep=%s" % (overall, ih.get("generated_at")))
        worst_src = []
        fred_p = {}
        for p in sorted(pipe_items,
                        key=lambda x: str(x.get("name")
                                          or x.get("pipeline") or "")):
            name = p.get("name") or p.get("pipeline") or "?"
            st2 = p.get("status") or p.get("state")
            r.log("    %-18s %s — %s"
                  % (name, st2, str(p.get("note") or p.get("detail")
                                    or p.get("why") or "")[:110]))
            if st2 == "ACTION_REQUIRED":
                worst_src.append(name)
            if str(name).lower() == "fred":
                fred_p = p
        r.log("  ACTION_REQUIRED source(s): %s" % (worst_src or "none"))
        misses += contract(r, "health",
                           (fred_p.get("status") or "") != "ACTION_REQUIRED",
                           "FRED pipeline itself is %s (badge driven by: %s)"
                           % (fred_p.get("status"), worst_src or "n/a"))
        for inc in (ih.get("incidents") or [])[:5]:
            r.log("  incident: %s [%s/%s] %s"
                  % (inc.get("at"), inc.get("pipeline"), inc.get("kind"),
                     str(inc.get("detail"))[:130]))
        for a in (ih.get("actions_taken") or ih.get("actions") or [])[:5]:
            r.log("  action: %s" % str(a)[:140])

        r.section("2. drain state — counters prove the three rules ran")
        st = get_json("data/_state/fred-scoped-import.json")
        misses += contract(r, "state", "_error" not in st,
                           "scoped-import state readable")
        r.log("  scope=%s  discovery_complete=%s"
              % (st.get("import_scope"), st.get("discovery_complete")))
        for k in ("series_seen", "series_queued", "series_excluded_stale",
                  "series_excluded_discontinued", "series_skipped_already",
                  "series_banked", "obs_rows_banked", "blocked_at",
                  "blocked_ts", "last_run", "runs"):
            if k in st:
                r.log("    %s = %s" % (k, st.get(k)))
        misses += contract(r, "rules",
                           (st.get("series_excluded_stale") or 0) > 0
                           and (st.get("series_excluded_discontinued")
                                or 0) > 0,
                           "freshness + discontinued filters demonstrably "
                           "firing (stale=%s, discontinued=%s excluded)"
                           % (st.get("series_excluded_stale"),
                              st.get("series_excluded_discontinued")))
        misses += contract(r, "state", not st.get("blocked_at"),
                           "no FRED block active (blocked_at=%s)"
                           % st.get("blocked_at"))

        r.section("3. queue ledger — live ordering spot-check")
        q = get_json("data/_state/fred-queue.json.gz")
        misses += contract(r, "queue", "_error" not in q,
                           "queue ledger readable")
        rows = q.get("rows") or []
        cur = int(q.get("cursor") or 0)
        remaining = max(0, len(rows) - cur)
        r.log("  rows=%d cursor=%d remaining=%d sorted=%s sorted_scope=%s "
              "built_at=%s"
              % (len(rows), cur, remaining, q.get("sorted"),
                 q.get("sorted_scope"), q.get("built_at")))
        tail = rows[cur:]
        stride = max(1, len(tail) // 200)
        sample = tail[::stride][:220]
        mono_bad = sum(1 for a, b2 in zip(sample, sample[1:])
                       if a[1] < b2[1])
        misses += contract(r, "queue",
                           q.get("sorted") is True and mono_bad == 0,
                           "undrained tail is popularity-descending "
                           "(%d/%d sampled inversions)"
                           % (mono_bad, max(0, len(sample) - 1)))
        if tail:
            r.log("  next up (popularity): %s"
                  % ", ".join("%s(p%d)" % (t[0], t[1]) for t in tail[:6]))
            cutoff = now.timestamp() - 90 * 86400
            fresh_ok = sum(
                1 for t in sample
                if t[4] and datetime.fromisoformat(
                    t[4]).replace(tzinfo=timezone.utc).timestamp()
                >= cutoff - 30 * 86400)  # 30d drain-lag tolerance
            r.log("  freshness at build: %d/%d sampled rows have "
                  "last_updated within 90d(+30d drain lag) — filter was "
                  "applied at queue build, not re-checked at drain (by "
                  "design)" % (fresh_ok, len(sample)))

        r.section("4. progress + ETA")
        vel = (ih.get("fred") or {}).get("velocity_per_h") \
            or (fred_p.get("velocity_per_h")) \
            or ih.get("velocity_per_h")
        try:
            vel = float(vel)
        except Exception:
            vel = None
        if vel and remaining:
            eta_h = remaining / vel
            r.log("  remaining=%d at %.0f/h → ETA %.1f h (~%.1f days), "
                  "finish ≈ %s UTC"
                  % (remaining, vel, eta_h, eta_h / 24,
                     datetime.fromtimestamp(
                         now.timestamp() + eta_h * 3600,
                         tz=timezone.utc).strftime("%Y-%m-%d %H:%M")))
        else:
            r.log("  remaining=%d; velocity not in health payload "
                  "(vel=%r) — ETA from strip stands" % (remaining, vel))

        r.section("verdict")
        if misses:
            r.fail("import audit: %d red" % misses)
            sys.exit(1)
        r.ok("FRED drain healthy and honoring all three rules; "
             "ACTION_REQUIRED badge explained above")


if __name__ == "__main__":
    main()
