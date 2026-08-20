"""ops/4927 -- offshore-USD funding: source-resolution probe (no writes).

Khalid handed over a research note listing the primary sources a real
eurodollar monitor needs: NY Fed reference rates (SOFR/TGCR/BGCR/OBFR/
EFFR), FRED, BIS Locational Banking Statistics, Fed H.8 bank wholesale
funding, and the Fed Commercial Paper release. The plumbing engine
already carries most of the PRICE side. This probe establishes, with
evidence and zero guessing, exactly which of the missing pieces are
reachable and under which identifiers -- because the ECB 406 arc cost
eight months to a single wrong header, and fabricated series IDs are
the one failure this desk does not tolerate.

Probes, all read-only:

  A. eurodollar-plumbing.json -- inventory live metric ids so the new
     layer cannot duplicate an existing signal.
  B. NY Fed Markets reference-rate API direct: TGCR + BGCR are the two
     secured rates the note names that the engine does NOT carry, and
     FRED does not publish their volumes or percentiles at all. Probe
     rate + volume + percentile field names on every rate endpoint.
  C. BIS: the walker already banks every BIS dataflow to
     data/warm/bis/data/<flow>.dat.gz. If WS_LBS_D_PUB is in there,
     the USD cross-border claims/liabilities the note calls
     "the one I would NOT omit" are ALREADY in the lake and merely
     unsurfaced. Inventory the catalog + range-read the LBS object's
     header to resolve the currency/position dimensions empirically.
     Live SDMX ladder only as fallback.
  D. FRED release-walk (NOT id guessing): resolve the H.8 and
     Commercial Paper releases, then list their series and keyword-
     match large time deposits / borrowings / net due to foreign
     offices / CP outstanding / ABCP / 1M tenors. Report id +
     frequency + observation_end so staleness is visible before wiring.
  E. SOFR term structure (30/90/180d averages + SOFR Index) -- the
     free stand-in for the note's CME futures row.

Output: aws/ops/reports/4927.json + latest/*.md. Nothing is deployed
from this op. The engine that follows binds ONLY to what prints green.
"""
import gzip
import io
import json
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
UA = {"User-Agent": "JustHodl research ops4927 admin@justhodl.ai"}

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)

FINDINGS = {"generated": datetime.now(timezone.utc).isoformat(),
            "op": 4927}


def get(url, timeout=30, cap=400000, headers=None):
    """Bounded GET. Returns (status, text). Never raises."""
    h = dict(UA)
    if headers:
        h.update(headers)
    try:
        req = urllib.request.Request(url, headers=h)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read(cap)
            if resp.headers.get("Content-Encoding") == "gzip":
                raw = gzip.decompress(raw)
            return resp.status, raw.decode("utf-8", "replace")
    except Exception as e:  # noqa: BLE001
        return "%s:%s" % (type(e).__name__, str(e)[:90]), ""


def s3_json(key):
    try:
        return json.loads(s3.get_object(Bucket=B, Key=key)["Body"].read())
    except Exception:  # noqa: BLE001
        return None


def fred_key():
    """Donor pattern: lift the live FRED key off a deployed function."""
    for fn in ("dollar-strength-agent", "justhodl-eurodollar-plumbing",
               "justhodl-sp500", "justhodl-repo-monitor"):
        try:
            env = (lam.get_function_configuration(FunctionName=fn)
                   .get("Environment") or {}).get("Variables") or {}
            k = env.get("FRED_API_KEY")
            if k:
                return k, fn
        except Exception:  # noqa: BLE001
            continue
    return None, None


# ───────────────────────── A. live metric inventory ──────────────────
def probe_a(r):
    r.section("A. eurodollar-plumbing.json — live metric inventory")
    d = s3_json("data/eurodollar-plumbing.json")
    if not d:
        r.fail("could not read data/eurodollar-plumbing.json")
        FINDINGS["A"] = {"ok": False}
        return
    ids, layers = [], {}
    for lname, lay in (d.get("layers") or {}).items():
        mids = [m.get("id") for m in (lay.get("metrics") or [])]
        layers[lname] = mids
        ids += [m for m in mids if m]
    r.kv(generated=d.get("generated", "?"),
         health=d.get("plumbing_health"), verdict=d.get("verdict", "?"),
         layers=len(layers), metrics=len(ids))
    for lname, mids in layers.items():
        r.log("  %-12s %2d  %s" % (lname, len(mids), ",".join(mids)))
    FINDINGS["A"] = {"ok": True, "metric_ids": ids, "layers": layers,
                     "health": d.get("plumbing_health"),
                     "generated": d.get("generated")}


# ───────────────────────── B. NY Fed reference rates ─────────────────
NYFED = "https://markets.newyorkfed.org/api/rates"
RATE_EPS = [
    ("sofr", "%s/secured/sofr/last/1.json" % NYFED),
    ("tgcr", "%s/secured/tgcr/last/1.json" % NYFED),
    ("bgcr", "%s/secured/bgcr/last/1.json" % NYFED),
    ("effr", "%s/unsecured/effr/last/1.json" % NYFED),
    ("obfr", "%s/unsecured/obfr/last/1.json" % NYFED),
    ("all_latest", "%s/all/latest.json" % NYFED),
    ("sofr_avg", "%s/secured/sofrai/last/1.json" % NYFED),
]


def probe_b(r):
    r.section("B. NY Fed Markets reference rates (direct) — "
              "TGCR/BGCR + volumes + percentiles")
    out = {}
    for name, url in RATE_EPS:
        st, body = get(url, cap=120000)
        rec = {"status": st, "url": url}
        if st == 200 and body.strip():
            try:
                j = json.loads(body)
                rows = j.get("refRates") or j.get("refRate") or j
                if isinstance(rows, dict):
                    rows = [rows]
                if isinstance(rows, list) and rows:
                    rec["fields"] = sorted(rows[0].keys())
                    rec["sample"] = rows[0]
                    rec["n"] = len(rows)
            except Exception as e:  # noqa: BLE001
                rec["parse"] = "%s:%s" % (type(e).__name__, str(e)[:60])
        out[name] = rec
        got = rec.get("sample") or {}
        r.log("  %-11s %-6s %s" % (
            name, str(st),
            ("fields=%d rate=%s vol=%s pct=%s" % (
                len(rec.get("fields") or []),
                got.get("percentRate", got.get("rate", "-")),
                got.get("volumeInBillions", "-"),
                got.get("percentPercentile99", got.get("percentile99", "-"))))
            if rec.get("fields") else str(rec.get("parse", ""))[:70]))
    FINDINGS["B"] = out


# ───────────────────────── C. BIS LBS ────────────────────────────────
BIS_LADDER = [
    ("v1_lbs_csv",
     "https://stats.bis.org/api/v1/data/WS_LBS_D_PUB/all/all?format=csv"),
    ("v2_lbs_csv",
     "https://stats.bis.org/api/v2/data/dataflow/BIS/WS_LBS_D_PUB/1.0/"
     "all?format=csv&lastNObservations=1"),
    ("v1_lbs_dsd_avail",
     "https://stats.bis.org/api/v1/availableconstraint/WS_LBS_D_PUB/"
     "all/all/L_DENOM"),
]


def probe_c(r):
    r.section("C. BIS — is Locational Banking already in the lake?")
    res = {}

    # C1. catalog
    try:
        raw = s3.get_object(Bucket=B,
                            Key="data/warm/bis/catalog.json.gz")["Body"].read()
        cat = json.loads(gzip.decompress(raw))
        flows = cat.get("dataflows") or []
        hits = [f for f in flows
                if any(t in str(f.get("id", "")).upper()
                       for t in ("LBS", "CBS", "GLI", "DEBT_SEC"))]
        res["catalog_total"] = len(flows)
        res["catalog_hits"] = [{"id": f.get("id"), "name": f.get("name")}
                               for f in hits][:20]
        r.ok("BIS catalog: %d dataflows; %d banking-relevant"
             % (len(flows), len(hits)))
        for f in hits[:12]:
            r.log("    %-22s %s" % (f.get("id"), str(f.get("name"))[:74]))
    except Exception as e:  # noqa: BLE001
        res["catalog_error"] = "%s:%s" % (type(e).__name__, str(e)[:80])
        r.warn("BIS catalog unreadable: %s" % res["catalog_error"])

    # C2. banked objects
    banked = []
    try:
        p = s3.get_paginator("list_objects_v2")
        for page in p.paginate(Bucket=B, Prefix="data/warm/bis/data/"):
            for o in page.get("Contents", []):
                banked.append({"key": o["Key"], "mb": round(o["Size"] / 1e6, 2),
                               "modified": o["LastModified"].isoformat()})
    except Exception as e:  # noqa: BLE001
        res["list_error"] = "%s:%s" % (type(e).__name__, str(e)[:80])
    res["banked_total"] = len(banked)
    lbsish = [x for x in banked
              if any(t in x["key"].upper() for t in ("LBS", "CBS", "GLI"))]
    res["banked_hits"] = lbsish[:15]
    r.kv(bis_objects_banked=len(banked), lbs_cbs_gli_objects=len(lbsish))
    for x in lbsish[:10]:
        r.log("    %-58s %8.2f MB  %s"
              % (x["key"].split("/")[-1], x["mb"], x["modified"][:10]))

    # C3. header of the LBS object — resolve dimensions empirically
    tgt = None
    for x in lbsish:
        if "LBS" in x["key"].upper():
            tgt = x
            break
    if tgt:
        try:
            body = s3.get_object(Bucket=B, Key=tgt["key"],
                                 Range="bytes=0-3000000")["Body"].read()
            try:
                txt = gzip.GzipFile(fileobj=io.BytesIO(body)).read(2000000)
            except Exception:  # noqa: BLE001
                txt = body
            txt = txt.decode("utf-8", "replace")
            lines = txt.splitlines()
            res["lbs_object"] = tgt["key"]
            res["lbs_header"] = lines[0][:1200] if lines else ""
            res["lbs_rows_sampled"] = len(lines)
            res["lbs_sample_rows"] = lines[1:4]
            hdr = (lines[0] if lines else "").upper()
            # which column carries currency?
            cols = [c.strip().strip('"') for c in (lines[0] if lines
                                                   else "").split(",")]
            res["lbs_columns"] = cols[:40]
            res["has_usd_dim"] = ("DENOM" in hdr or "CURR" in hdr)
            usd_rows = [ln for ln in lines[1:20000] if ",USD," in ln.upper()]
            res["usd_row_count_in_sample"] = len(usd_rows)
            res["usd_sample"] = usd_rows[:3]
            r.ok("LBS object %s — %d cols, %d sampled rows, USD rows in "
                 "sample: %d" % (tgt["key"].split("/")[-1], len(cols),
                                 len(lines), len(usd_rows)))
            r.log("    header: %s" % (lines[0][:300] if lines else "(none)"))
            for ln in usd_rows[:2]:
                r.log("    USD  : %s" % ln[:300])
        except Exception as e:  # noqa: BLE001
            res["lbs_read_error"] = "%s:%s" % (type(e).__name__, str(e)[:90])
            r.warn("LBS object read failed: %s" % res["lbs_read_error"])
    else:
        r.warn("no LBS object banked — probing BIS live")

    # C4. live ladder (always run: proves reachability for a fresh engine)
    live = {}
    for name, url in BIS_LADDER:
        st, body = get(url, timeout=40, cap=300000)
        live[name] = {"status": st, "bytes": len(body),
                      "head": body[:400].replace("\n", " | ")}
        r.log("  %-18s %-6s %8db  %s"
              % (name, str(st), len(body), body[:110].replace("\n", " ")))
    res["live_ladder"] = live
    FINDINGS["C"] = res


# ───────────────────────── D. FRED release walk ──────────────────────
KW_H8 = ("large time deposit", "borrowings", "net due to related",
         "deposits, all commercial", "other deposit")
KW_CP = ("outstanding", "asset-backed", "asset backed", "financial",
         "nonfinancial", "1-month", "3-month", "overnight")


def probe_d(r):
    r.section("D. FRED release-walk — H.8 wholesale funding + "
              "Commercial Paper (discovery, not guessing)")
    key, donor = fred_key()
    if not key:
        r.fail("no FRED_API_KEY on any donor function")
        FINDINGS["D"] = {"ok": False, "reason": "no key"}
        return
    r.kv(fred_key_donor=donor)
    base = "https://api.stlouisfed.org/fred"
    st, body = get("%s/releases?api_key=%s&file_type=json&limit=1000"
                   % (base, key), cap=1500000)
    if st != 200:
        r.fail("FRED /releases -> %s" % st)
        FINDINGS["D"] = {"ok": False, "status": st}
        return
    rels = json.loads(body).get("releases", [])
    want = {}
    for rel in rels:
        n = (rel.get("name") or "").lower()
        if "assets and liabilities of commercial banks" in n:
            want["H8"] = rel
        elif n.strip() == "commercial paper":
            want["CP"] = rel
    r.kv(releases_total=len(rels),
         h8=want.get("H8", {}).get("id"), cp=want.get("CP", {}).get("id"))

    out = {"releases": {k: {"id": v.get("id"), "name": v.get("name")}
                        for k, v in want.items()}, "series": {}}
    for tag, kws in (("H8", KW_H8), ("CP", KW_CP)):
        rel = want.get(tag)
        if not rel:
            r.warn("%s release not found" % tag)
            continue
        st2, b2 = get("%s/release/series?release_id=%s&api_key=%s"
                      "&file_type=json&limit=1000&order_by=popularity"
                      "&sort_order=desc" % (base, rel["id"], key),
                      cap=3000000)
        if st2 != 200:
            r.warn("%s release/series -> %s" % (tag, st2))
            continue
        ser = json.loads(b2).get("seriess", [])
        matched = []
        for s in ser:
            t = (s.get("title") or "").lower()
            if any(k in t for k in kws):
                matched.append({
                    "id": s.get("id"), "title": s.get("title"),
                    "freq": s.get("frequency_short"),
                    "units": s.get("units_short"),
                    "start": s.get("observation_start"),
                    "end": s.get("observation_end"),
                    "updated": s.get("last_updated"),
                    "popularity": s.get("popularity")})
        matched.sort(key=lambda x: -(x.get("popularity") or 0))
        out["series"][tag] = {"release_total": len(ser),
                              "matched": matched[:40]}
        r.ok("%s release %s: %d series, %d keyword matches"
             % (tag, rel["id"], len(ser), len(matched)))
        for m in matched[:18]:
            r.log("    %-22s %-3s %-10s end=%s  %s"
                  % (m["id"], m["freq"], m["units"] or "", m["end"],
                     m["title"][:66]))
    FINDINGS["D"] = out


# ───────────────────────── E. SOFR term structure ────────────────────
def probe_e(r):
    r.section("E. SOFR term structure (free forward-funding proxy)")
    key, _ = fred_key()
    out = {}
    if not key:
        r.warn("no FRED key; skipping")
        FINDINGS["E"] = {"ok": False}
        return
    for sid in ("SOFR30DAYAVG", "SOFR90DAYAVG", "SOFR180DAYAVG",
                "SOFRINDEX", "SOFRVOL", "SOFR1", "SOFR25", "SOFR75",
                "SOFR99", "BGCR", "TGCR", "OBFR", "EFFRVOL"):
        st, body = get("https://api.stlouisfed.org/fred/series?series_id=%s"
                       "&api_key=%s&file_type=json" % (sid, key), cap=60000)
        alive = (st == 200)
        rec = {"status": st}
        if alive:
            try:
                s = json.loads(body)["seriess"][0]
                rec.update({"title": s.get("title"),
                            "freq": s.get("frequency_short"),
                            "end": s.get("observation_end"),
                            "updated": s.get("last_updated")})
            except Exception:  # noqa: BLE001
                rec["parse"] = "unexpected shape"
        out[sid] = rec
        r.log("  %-14s %-6s %-11s %s"
              % (sid, str(st), rec.get("end", "-"),
                 str(rec.get("title", ""))[:64]))
    FINDINGS["E"] = out


def main():
    t0 = datetime.now(timezone.utc)
    with report("4927-usd-funding-source-probe") as r:
        r.heading("ops 4927 — offshore-USD funding: source-resolution "
                  "probe (read-only)")
        probe_a(r)
        probe_b(r)
        probe_c(r)
        probe_d(r)
        probe_e(r)
        (ROOT / "aws" / "ops" / "reports" / "4927.json").write_text(
            json.dumps(FINDINGS, indent=1, default=str))
        r.kv(status="PROBE-COMPLETE",
             duration_s=int((datetime.now(timezone.utc) - t0)
                            .total_seconds()))


if __name__ == "__main__":
    main()
    sys.exit(0)
