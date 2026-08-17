"""ops/4821 -- Fusion 2 PROBE: plumbing composite pre-wire recon.
CLAIM: this workstream (repo-board -> plumbing-composite -> risk-gate
funding enrichment) is taken by this session; parallel session should
pick a different fusion (this op is the visible claim, per the
collision-recovery doctrine).

Report-only, ZERO writes.  Probe-then-wire before the composite:
 (1) data/repo.json true shape: top-level keys, group names/counts,
     one full sample row per group (truncated) so every lookback and
     value field name is on the record.
 (2) component-family scans over series id+name for the ten composite
     candidates (fails, haircuts, SFTR, Bund-AAA scarcity, BTP/OAT
     spreads, SOFR/IORB, RRP, reserves, dealer positioning, FIMA /
     x-ccy basis) -- up to 6 matches each with latest value/date.
 (3) history depth via data/repo-history/{id}.json for the top
     candidate per family (n, first, last) -- z-scores need depth.
 (4) data/risk-gate.json: posture, composite, sizing_multiplier,
     legs keys + full funding leg dict (the enrichment target).
Hard-fails only if repo.json is unreadable/empty or fewer than 5
families match (board contract broken).
"""
import gzip
import json
import re
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")

FAMILIES = [
    ("fails", r"FAIL"),
    ("haircuts", r"HAIRCUT"),
    ("sftr", r"SFTR"),
    ("scarcity_bund_aaa", r"BUND.*AAA|AAA.*BUND|SCARC"),
    ("periphery_spreads", r"BTP|OAT|BONO"),
    ("sofr_iorb", r"SOFR|IORB"),
    ("rrp", r"RRP|WLRRAL|RREP"),
    ("reserves", r"WRESBAL|RESERV"),
    ("dealer_positioning", r"PDPOS|DEALER|^PD[-_]"),
    ("fima_xccy", r"FIMA|WREPOFOR|BASIS|XCCY"),
]


def sread(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def walk_series(doc):
    """Shape-tolerant: yield (group, row) for every dict row carrying
    an id/mnemonic, wherever the board nests them."""
    def rid(r):
        for k in ("id", "mnemonic", "series_id", "sid"):
            if isinstance(r, dict) and r.get(k):
                return str(r[k])
        return None

    if isinstance(doc.get("groups"), list):
        for g in doc["groups"]:
            gname = g.get("name") or g.get("group") or "?"
            for r in (g.get("series") or g.get("items")
                      or g.get("rows") or []):
                if rid(r):
                    yield gname, rid(r), r
        return
    for k, v in doc.items():
        if isinstance(v, list) and v and isinstance(v[0], dict) \
                and rid(v[0]):
            for r in v:
                if rid(r):
                    yield (str(r.get("group") or r.get("tier")
                               or k), rid(r), r)


def clip(obj, n=340):
    s = json.dumps(obj, default=str)
    return s[:n] + ("..." if len(s) > n else "")


def main():
    with report("ops 4821 -- plumbing composite probe "
                "(Fusion 2 claim)") as rep:
        rep.heading("1. repo.json shape")
        try:
            doc = sread("data/repo.json")
        except ClientError:
            rep.fail("data/repo.json unreadable")
            sys.exit(1)
        rep.kv(top_keys=",".join(sorted(doc)[:14]),
               as_of=doc.get("as_of") or doc.get("generated_at"),
               engine_v=doc.get("engine_v"))
        rows = list(walk_series(doc))
        if not rows:
            rep.fail("no series rows found in any known nesting")
            sys.exit(1)
        groups = {}
        for g, sid, r in rows:
            groups.setdefault(g, []).append((sid, r))
        rep.ok("series rows = %d across %d groups"
               % (len(rows), len(groups)))
        for g in sorted(groups):
            rep.log("  group %-28s n=%d" % (g[:28], len(groups[g])))
        for g in sorted(groups):
            rep.log("  SAMPLE %-24s %s"
                    % (g[:24], clip(groups[g][0][1])))

        rep.heading("2. component-family scans")
        picks = {}
        matched = 0
        for fam, pat in FAMILIES:
            rx = re.compile(pat, re.I)
            hits = []
            for g, sid, r in rows:
                name = str(r.get("name") or r.get("title") or "")
                if rx.search(sid) or rx.search(name):
                    hits.append((g, sid, r))
            if hits:
                matched += 1
                picks[fam] = hits[0][1]
                rep.ok("  %-20s %d hit(s)" % (fam, len(hits)))
                for g, sid, r in hits[:6]:
                    rep.log("    %-34s grp=%-18s last=%s @ %s"
                            % (sid[:34], g[:18],
                               r.get("latest") or r.get("last")
                               or r.get("value"),
                               r.get("latest_date") or r.get("date")
                               or r.get("as_of")))
            else:
                rep.warn("  %-20s NO MATCH -- composite must source "
                         "elsewhere (FRED leg?)" % fam)

        rep.heading("3. history depth (data/repo-history/{id}.json)")
        for fam, sid in sorted(picks.items()):
            try:
                h = sread("data/repo-history/%s.json" % sid)
                dates = h.get("dates") or []
                vals = h.get("values") or []
                rep.ok("  %-20s %-30s n=%d  %s -> %s"
                       % (fam, sid[:30], len(dates),
                          dates[0] if dates else "?",
                          dates[-1] if dates else "?"))
                if len(dates) != len(vals):
                    rep.warn("    dates/values length mismatch "
                             "%d/%d" % (len(dates), len(vals)))
            except ClientError:
                rep.warn("  %-20s %-30s history key MISSING"
                         % (fam, sid[:30]))

        rep.heading("4. risk-gate enrichment target")
        try:
            rg = sread("data/risk-gate.json")
            rep.kv(posture=rg.get("posture"),
                   composite=rg.get("composite"),
                   sizing_multiplier=rg.get("sizing_multiplier"),
                   legs=",".join(sorted(rg.get("legs") or {})))
            rep.log("  funding leg: "
                    + clip((rg.get("legs") or {}).get("funding"),
                           420))
        except ClientError:
            rep.warn("data/risk-gate.json unreadable -- wire "
                     "target must be re-probed")

        rep.heading("5. verdict")
        if matched < 5:
            rep.fail("only %d/10 families matched -- board "
                     "contract broken, composite blocked" % matched)
            sys.exit(1)
        rep.ok("probe complete: %d/10 families matched; composite "
               "spec can bind exact IDs (Fusion 2 claimed by this "
               "session)" % matched)


start = time.time()
if __name__ == "__main__":
    main()
