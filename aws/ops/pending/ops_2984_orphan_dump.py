#!/usr/bin/env python3
"""ops 2984 -- U9 phase A: dump the orphan tables from the live audit
artifacts into the committed report so the container can build the
family->desk adoption map. Reads data/fleet-audit.json (+ falls back to
data/engine-wiring.json cross-ref) and emits:
  orphan_fresh: [{engine, family, outs:[...], age_h}]
  orphan_stale: [...]   orphan_dead: [...]
No mutations. PASS iff >=80 orphan-fresh rows with outs resolved.
"""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3
from ops_report import report

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]


def s3_json(key):
    return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def main():
    fails, warns = [], []
    with report("2984_orphan_dump") as rep:
        audit = s3_json("data/fleet-audit.json")
        rep.kv(audit_keys=list(audit.keys())[:25])
        table = None
        for k in ("engines", "engine_table", "fleet", "rows"):
            if isinstance(audit.get(k), list) and audit[k]:
                table = audit[k]
                break
        if table is None:
            for k, v in audit.items():
                if isinstance(v, list) and v and isinstance(v[0], dict) \
                        and ("engine" in v[0] or "name" in v[0]):
                    table = v
                    rep.kv(table_key=k)
                    break
        if not table:
            fails.append("no engine table found in audit doc")
            _w(rep, fails, warns, {})
            return
        rep.kv(table_rows=len(table),
               sample=json.dumps(table[0])[:300])

        wiring = {}
        try:
            wd = s3_json("data/engine-wiring.json")
            for k in ("wired", "rows", "engines"):
                if isinstance(wd.get(k), list):
                    for r in wd[k]:
                        nm = r.get("engine") or r.get("name")
                        if nm:
                            wiring.setdefault(nm, r)
                    break
        except Exception as e:
            warns.append("wiring doc: %s" % str(e)[:80])

        def norm(row):
            nm = row.get("engine") or row.get("name") or ""
            fam = (row.get("family") or row.get("fam")
                   or row.get("category") or "?")
            outs = (row.get("outs") or row.get("out_keys")
                    or row.get("feeds") or [])
            if isinstance(outs, str):
                outs = [outs]
            if not outs and nm in wiring:
                w = wiring[nm]
                outs = (w.get("outs") or w.get("keys")
                        or w.get("feeds") or [])
            age = (row.get("age_h") or row.get("freshest_age_h")
                   or row.get("age_hours"))
            return {"engine": nm, "family": fam,
                    "outs": outs[:6], "age_h": age}

        def status_of(row):
            s = (row.get("status") or row.get("state")
                 or row.get("category") or "").upper().replace("-", "_")
            wired_n = row.get("wired_pages") or row.get("wired") \
                or row.get("pages_n") or 0
            if isinstance(wired_n, list):
                wired_n = len(wired_n)
            return s, wired_n

        fresh, stale, dead = [], [], []
        for row in table:
            s, wn = status_of(row)
            if "ORPHAN" not in s and wn:
                continue
            if "ORPHAN_FRESH" in s or (s == "" and wn == 0
                                       and row.get("outs")):
                fresh.append(norm(row))
            elif "ORPHAN_STALE" in s or "STALE" in s:
                stale.append(norm(row))
            elif "DEAD" in s or "ORPHAN_DEAD" in s:
                dead.append(norm(row))
            elif "ORPHAN" in s:
                fresh.append(norm(row))
        rep.kv(fresh_n=len(fresh), stale_n=len(stale), dead_n=len(dead))
        if len(fresh) < 80:
            fails.append("orphan-fresh only %d (<80) -- field mapping "
                         "wrong; see sample in log" % len(fresh))
        no_outs = [r["engine"] for r in fresh if not r["outs"]]
        if len(no_outs) > 10:
            warns.append("%d fresh orphans missing outs: %s"
                         % (len(no_outs), no_outs[:8]))

        out = {"ops": 2984, "fails": fails, "warns": warns,
               "verdict": "PASS" if not fails else "FAIL",
               "ts": datetime.now(timezone.utc).isoformat(),
               "orphan_fresh": fresh, "orphan_stale": stale,
               "orphan_dead": dead}
        _w(rep, fails, warns, out)


def _w(rep, fails, warns, out):
    rp = AWS_DIR / "ops" / "reports" / "2984.json"
    rp.parent.mkdir(parents=True, exist_ok=True)
    rp.write_text(json.dumps(out, indent=1))
    rep.log("FAILS=%d WARNS=%d" % (len(fails), len(warns)))
    if fails:
        sys.exit(1)


main()
sys.exit(0)
