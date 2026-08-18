"""ops/4886 -- tape-truth v1.1 verify.
 (1) settle 'tape-truth v1.1.0'; invoke; poll v==1.1.0.
 (2) truths: SPY ledger row now carries vol/vwap/ohlc; VWAP ==
     independent recompute from fresh Polygon bars; CLV ==
     2*(c-l)/(h-l)-1 recompute; gex carries dte5 share (0..100)
     and dist_to_flip; verdict conviction == engine conviction()
     over doc's own score_parts when past WARMING, else WARMING
     honesty holds.
 (3) tape-truth.html conviction bar served; why.html tape-truth
     module served (namespaced, self-contained).
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
sys.path.insert(0, str(ROOT / "lambdas"
                       / "justhodl-tape-truth" / "source"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402
from ops_report import report  # noqa: E402
import lambda_function as engmod  # noqa: E402

REGION = "us-east-1"
FN = "justhodl-tape-truth"
B = "justhodl-dashboard-live"
OUT_KEY = "data/tape-truth.json"
MARKER = "tape-truth v1.1.0"
R3 = Path(__file__).resolve().parents[3]

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


def main():
    with report("ops 4886 -- tape v1.1 verify") as rep:
        rep.heading("1. settle + invoke")
        for _ in range(40):
            try:
                cfg = lam.get_function_configuration(
                    FunctionName=FN)
                if cfg.get("State") == "Active" and \
                        cfg.get("LastUpdateStatus") \
                        != "InProgress":
                    break
            except ClientError:
                pass
            time.sleep(6)
        settled = False
        for att in range(30):
            try:
                gf = lam.get_function(FunctionName=FN)
                raw = urllib.request.urlopen(
                    gf["Code"]["Location"],
                    timeout=60).read()
                if MARKER in zipfile.ZipFile(
                        io.BytesIO(raw)).read(
                        "lambda_function.py").decode(
                        "utf-8", "replace"):
                    rep.ok("marker settled (attempt %d)"
                           % (att + 1))
                    settled = True
                    break
            except (ClientError, Exception):  # noqa: BLE001
                pass
            time.sleep(10)
        if not settled:
            rep.fail("no marker")
            sys.exit(1)
        prev = sread(OUT_KEY).get("generated_at")
        pk = lam.get_function_configuration(
            FunctionName=FN)["Environment"]["Variables"] \
            .get("POLYGON_API_KEY")
        lam.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=b"{}")
        doc = None
        t0 = time.time()
        while time.time() - t0 < 560:
            time.sleep(15)
            try:
                dd = sread(OUT_KEY)
            except (ClientError, KeyError):
                continue
            if dd.get("generated_at") != prev \
                    and dd.get("v") == "1.1.0":
                doc = dd
                break
        if not doc:
            rep.fail("no fresh v1.1.0 doc")
            sys.exit(1)
        rep.ok("fresh in %ds" % int(time.time() - t0))

        rep.heading("2. truths")
        led = sread(engmod.CVD_LEDGER)["rows"]["SPY"]
        day = sorted(led)[-1]
        row = led[day]
        need = {"vol", "vwap", "o", "h", "l", "c"}
        if need <= set(row):
            rep.ok("ledger row carries %s (day %s)"
                   % (sorted(need), day))
        else:
            rep.fail("ledger row missing %s"
                     % (need - set(row)))
            sys.exit(1)
        j = json.loads(urllib.request.urlopen(
            urllib.request.Request(
                "https://api.polygon.io/v2/aggs/ticker/SPY/"
                "range/1/minute/%s/%s?limit=50000&apiKey=%s"
                % (day, day, pk),
                headers={"User-Agent": "ops-4886"}),
            timeout=60).read())
        vol = vn = 0.0
        for bb in j.get("results") or []:
            v = bb.get("v") or 0.0
            vol += v
            vn += (bb.get("vw") or bb.get("c") or 0.0) * v
        live_vwap = round(vn / vol, 4)
        if abs(live_vwap - row["vwap"]) < 0.0011:
            rep.ok("  VWAP %.4f == independent recompute"
                   % row["vwap"])
        else:
            rep.fail("  vwap doc=%s live=%s"
                     % (row["vwap"], live_vwap))
            FAILED.append("vwap")
        cv = doc["symbols"]["SPY"]["cvd"]
        exp_clv = round(2 * (row["c"] - row["l"])
                        / (row["h"] - row["l"]) - 1, 2) \
            if row["h"] > row["l"] else None
        if cv.get("clv_session") == exp_clv:
            rep.ok("  CLV %.2f == recompute" % exp_clv)
        else:
            rep.fail("  clv doc=%s exp=%s"
                     % (cv.get("clv_session"), exp_clv))
            FAILED.append("clv")
        rep.log("  close-vs-VWAP %+0.2f%% | vol_ratio %s"
                % (cv.get("close_vs_vwap_pct") or 0,
                   cv.get("vol_ratio_20d")))
        g = doc["symbols"]["SPY"]["gex"] or {}
        d5 = g.get("dte5_vol_share_pct")
        if g.get("status") == "LIVE" and d5 is not None \
                and 0 <= d5 <= 100 \
                and "dist_to_flip_pct" in g:
            rep.ok("  gex extras: DTE<=5 vol %.1f%%, flip "
                   "dist %s%%" % (d5,
                                  g.get("dist_to_flip_pct")))
        else:
            rep.fail("  gex extras missing: %s / %s"
                     % (d5, g.get("dist_to_flip_pct")))
            FAILED.append("gex")
        vd = doc["symbols"]["SPY"]["verdict"] or {}
        if vd.get("call") == "WARMING":
            rep.ok("  verdict still WARMING (%s) -- honest"
                   % vd.get("why"))
        elif vd.get("conviction") == engmod.conviction(
                [tuple(x) for x in
                 vd.get("score_parts", [])]):
            rep.ok("  conviction %s == recompute over "
                   "score_parts" % vd["conviction"])
        else:
            rep.fail("  conviction mismatch")
            FAILED.append("conv")

        rep.heading("3. pages")
        tt = (R3 / "tape-truth.html").read_text(
            encoding="utf-8")
        wy = (R3 / "why.html").read_text(encoding="utf-8")
        if "conviction" in tt and "tt_strip" in wy \
                and "TT_(" in wy:
            rep.ok("  committed both pages")
        else:
            rep.fail("  page tokens missing")
            sys.exit(1)
        srv = {"tape-truth.html": "conviction",
               "why.html": "tt_strip"}
        t0 = time.time()
        while time.time() - t0 < 480 and srv:
            for pg, tok in list(srv.items()):
                try:
                    req = urllib.request.Request(
                        "https://justhodl.ai/%s?t=%d"
                        % (pg, int(time.time())),
                        headers={"User-Agent": "ops-4886",
                                 "Cache-Control":
                                 "no-cache"})
                    with urllib.request.urlopen(
                            req, timeout=45) as r:
                        if tok in r.read().decode(
                                "utf-8", "replace"):
                            rep.ok("  %s SERVED (%ds)"
                                   % (pg,
                                      int(time.time() - t0)))
                            del srv[pg]
                except Exception:  # noqa: BLE001
                    pass
            if srv:
                time.sleep(30)
        if srv:
            rep.fail("  not served: %s" % list(srv))
            FAILED.append("served")

        rep.heading("4. verdict")
        if FAILED:
            rep.fail("HARD FAILS: %s" % sorted(set(FAILED)))
            sys.exit(1)
        rep.ok("v1.1 live: six new indicators, "
               "conviction-scored verdicts, research desk "
               "module wired")


if __name__ == "__main__":
    main()
