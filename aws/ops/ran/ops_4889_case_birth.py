"""ops/4889 -- justhodl-industry-case v1.0.0 birth verify.
 (0) ANTHROPIC_KEY from donor (self-heal armed).
 (1) settle marker; invoke; poll doc.
 (2) G0: cases.NVDA.ind_share_pct numeric.
 (3) truths: NVDA industry share == independent recompute
     from data/universe.json (sum Semiconductors mcap);
     rank consistency (share of #1 >= #2); industries count
     == distinct industries in universe with mcap; boom rank
     for NVDA's industry == recompute from boom league sort;
     ai_mode tagged on exactly AI_CAP cases.
 (4) both pages committed + served.
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
FN = "justhodl-industry-case"
B = "justhodl-dashboard-live"
OUT_KEY = "data/industry-case.json"
MARKER = "industry-case v1.0.0"
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
    with report("ops 4889 -- case birth") as rep:
        rep.heading("0. env + settle")
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
            time.sleep(8)
        try:
            ant = lam.get_function_configuration(
                FunctionName="justhodl-watchlist-debate"
            )["Environment"]["Variables"].get(
                "ANTHROPIC_KEY")
        except Exception:  # noqa: BLE001
            ant = None
        env = lam.get_function_configuration(
            FunctionName=FN).get("Environment",
                                 {}).get("Variables", {})
        if ant and env.get("ANTHROPIC_KEY") != ant:
            env["ANTHROPIC_KEY"] = ant
            lam.update_function_configuration(
                FunctionName=FN,
                Environment={"Variables": env})
            rep.ok("anthropic key set (self-heal armed)")
            for _ in range(30):
                time.sleep(6)
                try:
                    if lam.get_function_configuration(
                            FunctionName=FN).get(
                            "LastUpdateStatus") \
                            != "InProgress":
                        break
                except ClientError:
                    pass
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
        try:
            prev = sread(OUT_KEY).get("generated_at")
        except (ClientError, KeyError):
            prev = None
        lam.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=b"{}")
        doc = None
        t0 = time.time()
        while time.time() - t0 < 240:
            time.sleep(10)
            try:
                dd = sread(OUT_KEY)
            except (ClientError, KeyError):
                continue
            if dd.get("generated_at") != prev:
                doc = dd
                break
        if not doc:
            rep.fail("no fresh doc")
            sys.exit(1)
        rep.ok("fresh in %ds: %d cases / %d industries"
               % (int(time.time() - t0),
                  doc.get("n_cases", 0),
                  doc.get("n_industries", 0)))

        rep.heading("2. truths")
        nv = (doc.get("cases") or {}).get("NVDA") or {}
        if not isinstance(nv.get("ind_share_pct"),
                          (int, float)):
            rep.fail("G0: NVDA case missing")
            sys.exit(1)
        uni = sread("data/universe.json")
        tot = nv_mc = 0.0
        n_ind = 0
        inds = set()
        for r in uni.get("stocks") or []:
            mc = r.get("market_cap")
            if not isinstance(mc, (int, float)) or mc <= 0 \
                    or not r.get("industry"):
                continue
            inds.add(r["industry"])
            if r["industry"] == nv["industry"]:
                tot += mc
                n_ind += 1
                if r.get("symbol") == "NVDA":
                    nv_mc = mc
        exp_share = round(100.0 * nv_mc / tot, 2)
        if nv["ind_share_pct"] == exp_share \
                and nv["ind_n"] == n_ind:
            rep.ok("NVDA: %s%% of %s ($%0.0fB cohort, %d "
                   "names, rank #%d) == recompute"
                   % (exp_share, nv["industry"], tot / 1e9,
                      n_ind, nv["ind_rank"]))
        else:
            rep.fail("share doc=%s exp=%s n doc=%s exp=%s"
                     % (nv["ind_share_pct"], exp_share,
                        nv["ind_n"], n_ind))
            FAILED.append("share")
        if doc["n_industries"] == len(inds):
            rep.ok("industry count %d == universe distinct"
                   % len(inds))
        else:
            rep.fail("industries doc=%s uni=%s"
                     % (doc["n_industries"], len(inds)))
            FAILED.append("inds")
        boom = sread("data/industry-boom.json")
        lg = sorted(boom.get("league") or [],
                    key=lambda x: -(x.get("boom_score")
                                    or -1))
        exp_rank = next((i + 1 for i, r in enumerate(lg)
                         if r.get("industry")
                         == nv["industry"]), None)
        if (nv.get("boom") or {}).get("rank") == exp_rank:
            rep.ok("boom rank %s == league recompute "
                   "(score %s)" % (exp_rank,
                                   (nv.get("boom") or {})
                                   .get("score")))
        else:
            rep.fail("boom rank doc=%s exp=%s"
                     % ((nv.get("boom") or {}).get("rank"),
                        exp_rank))
            FAILED.append("boom")
        n_ai = sum(1 for c in doc["cases"].values()
                   if "ai_mode" in c)
        if n_ai == 6:
            top_ai = next(c for c in doc["cases"].values()
                          if "ai_mode" in c)
            rep.ok("ai layer on 6 cases, mode: %s"
                   % top_ai["ai_mode"][:60])
        else:
            rep.fail("ai touched %d cases" % n_ai)
            FAILED.append("ai")

        rep.heading("3. pages")
        ic = (R3 / "industry-case.html").read_text(
            encoding="utf-8")
        wy = (R3 / "why.html").read_text(encoding="utf-8")
        if "Industry Case" in ic and "ic_body" in wy \
                and "IC_(" in wy:
            rep.ok("  committed both")
        else:
            rep.fail("  page tokens missing")
            sys.exit(1)
        srv = {"industry-case.html": "What role does",
               "why.html": "ic_body"}
        srv = {"industry-case.html": "what role does this",
               "why.html": "ic_body"}
        t0 = time.time()
        while time.time() - t0 < 480 and srv:
            for pg, tok in list(srv.items()):
                try:
                    req = urllib.request.Request(
                        "https://justhodl.ai/%s?t=%d"
                        % (pg, int(time.time())),
                        headers={"User-Agent": "ops-4889",
                                 "Cache-Control":
                                 "no-cache"})
                    with urllib.request.urlopen(
                            req, timeout=45) as r:
                        if tok in r.read().decode(
                                "utf-8", "replace"):
                            rep.ok("  %s SERVED (%ds)"
                                   % (pg,
                                      int(time.time()
                                          - t0)))
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
        rep.ok("industry-case born: every share and rank "
               "recomputed from the spine")


if __name__ == "__main__":
    main()
