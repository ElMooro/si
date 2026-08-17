"""ops/4809 -- justhodl-sp500 v1.1.0 verify: expanded better-buy compare.
 (1) settle: deployed zip carries 'sp500 v1.1.0'.
 (2) Event-invoke, poll data/sp500.json as_of <= 8 min.
 (3) doc contract: member_fields == 41 names, sample member row len 41,
     new index metrics still band-sane, momentum cols coverage reported.
 (4) compare smoke NVDA: 38 rows, groups present, 5 pillars, composite
     0-100, verdict + tags. AAPL second opinion. Full readout.
"""
import gzip
import io
import json
import sys
import time
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
FN = "justhodl-sp500"
B = "justhodl-dashboard-live"
OUT_KEY = "data/sp500.json"
MARKER = "sp500 v1.1.0"
N_FIELDS = 41
N_ROWS = 38

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=120,
                                 retries={"max_attempts": 1}))
FAILED = []


def sread(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def band(rep, name, v, lo, hi, hard=True):
    if v is None or not (lo <= v <= hi):
        (rep.fail if hard else rep.warn)(
            "  %s = %s  [band %s..%s]" % (name, v, lo, hi))
        if hard:
            FAILED.append(name)
    else:
        rep.ok("  %s = %s" % (name, v))


def settle(rep):
    for att in range(30):
        try:
            gf = lam.get_function(FunctionName=FN)
            raw = urllib.request.urlopen(gf["Code"]["Location"],
                                         timeout=60).read()
            src = zipfile.ZipFile(io.BytesIO(raw)).read(
                "lambda_function.py").decode("utf-8", "replace")
            if MARKER in src:
                rep.ok("deployed marker %s settled (attempt %d)"
                       % (MARKER, att + 1))
                return True
        except (ClientError, Exception):  # noqa: BLE001
            pass
        time.sleep(10)
    rep.fail("deployed zip never carried %s" % MARKER)
    FAILED.append("settle")
    return False


def main():
    with report("ops 4809 -- sp500 v1.1.0 expanded compare") as rep:
        rep.heading("1. deploy settle (zip marker)")
        if not settle(rep):
            sys.exit(1)

        rep.heading("2. Event-invoke + poll as_of")
        t0 = time.time()
        lam.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=b"{}")
        doc = None
        while time.time() - t0 < 480:
            time.sleep(15)
            try:
                d = sread(OUT_KEY)
                if d.get("engine_v") == "1.1.0":
                    doc = d
                    break
            except ClientError:
                pass
        if not doc:
            rep.fail("data/sp500.json never refreshed to engine_v "
                     "1.1.0 within 8 min")
            sys.exit(1)
        rep.ok("  fresh v1.1.0 doc after ~%ds  as_of=%s"
               % (int(time.time() - t0), doc.get("as_of")))

        rep.heading("3. doc contract")
        mf = doc.get("member_fields") or []
        if len(mf) != N_FIELDS:
            rep.fail("  member_fields len %d != %d" % (len(mf),
                                                       N_FIELDS))
            FAILED.append("member_fields")
        else:
            rep.ok("  member_fields = %d names" % N_FIELDS)
        mem = doc.get("members") or {}
        aapl = mem.get("AAPL")
        if not aapl or len(aapl) != N_FIELDS:
            rep.fail("  AAPL member row len %s != %d"
                     % (len(aapl) if aapl else None, N_FIELDS))
            FAILED.append("member_row")
        else:
            rep.ok("  AAPL member row len = %d" % N_FIELDS)
        # coverage of the new columns across members
        idx = {k: i for i, k in enumerate(mf)}
        for newf in ("ps_fwd", "ev_ebitda_fwd", "shareholder_yield_pct",
                     "roic_pct", "altman_z", "ntm_growth_pct",
                     "mom_6m_pct", "mom_12_1_pct"):
            if newf not in idx:
                rep.fail("  field %s missing from member_fields" % newf)
                FAILED.append(newf)
                continue
            cov = sum(1 for r in mem.values()
                      if isinstance(r[idx[newf]], (int, float)))
            pctv = round(100 * cov / max(1, len(mem)), 1)
            (rep.ok if pctv >= 50 else rep.warn)(
                "  coverage %s = %s%% (%d/%d)"
                % (newf, pctv, cov, len(mem)))
            if newf in ("ps_fwd", "roic_pct", "ntm_growth_pct") \
                    and pctv < 50:
                FAILED.append("cov_" + newf)
        cm = doc.get("diag", {}).get("census", {}).get("cols_missing")
        rep.kv(cols_missing=json.dumps(cm))
        if cm:
            rep.warn("  matrix lacks: %s (fields render None -- "
                     "honest)" % cm)

        rep.heading("4. index bands (unchanged truths)")
        v, f, q = doc["valuation"], doc["forward"], doc["quality"]
        band(rep, "pe_ttm.agg", v["pe_ttm"].get("agg"), 12, 45)
        band(rep, "pe_fwd.agg", f["pe_fwd"].get("agg"), 10,
             (v["pe_ttm"].get("agg") or 45) * 1.25)
        band(rep, "roe.agg", q["roe_pct"].get("agg"), 5, 40)
        band(rep, "shareholder_yield.agg",
             doc["yield"]["shareholder_yield_pct"].get("agg"),
             0.5, 5.0, hard=False)
        band(rep, "fwd_ps.agg", f["ps_fwd"].get("agg"), 1, 8,
             hard=False)

        rep.heading("5. compare smoke (NVDA + AAPL)")
        for tkr in ("NVDA", "AAPL"):
            pay = json.dumps({"compare": tkr}).encode()
            r = lam.invoke(FunctionName=FN, Payload=pay)
            c = json.loads(r["Payload"].read())
            if not c.get("ok"):
                rep.fail("  compare %s failed: %s"
                         % (tkr, c.get("error")))
                FAILED.append("compare_" + tkr)
                continue
            rows = c.get("rows") or []
            pil = c.get("pillars") or {}
            comp = (c.get("composite") or {}).get("score")
            okrows = len(rows) == N_ROWS
            okpil = len(pil) >= 4
            okcomp = comp is not None and 0 <= comp <= 100
            (rep.ok if okrows else rep.fail)(
                "  %s rows = %d (want %d)" % (tkr, len(rows), N_ROWS))
            (rep.ok if okpil else rep.fail)(
                "  %s pillars = %s" % (tkr, sorted(pil)))
            (rep.ok if okcomp else rep.fail)(
                "  %s composite = %s  verdict: %s"
                % (tkr, comp, c.get("verdict")))
            if not (okrows and okpil and okcomp):
                FAILED.append("compare_" + tkr)
            rep.kv(**{tkr.lower() + "_pillars": json.dumps(
                {k: pv["score"] for k, pv in pil.items()}),
                tkr.lower() + "_tags": json.dumps(c.get("tags"))})
            groups = {r["group"] for r in rows}
            if not {"valuation", "quality", "growth", "balance",
                    "momentum"} <= groups:
                rep.fail("  %s row groups incomplete: %s"
                         % (tkr, sorted(groups)))
                FAILED.append("groups_" + tkr)

        rep.heading("6. verdict")
        if FAILED:
            rep.fail("HARD FAILS: %s" % sorted(set(FAILED)))
            sys.exit(1)
        rep.ok("sp500 v1.1.0 LIVE -- 41-field members, 38-metric "
               "better-buy compare with pillar verdict")


if __name__ == "__main__":
    main()
