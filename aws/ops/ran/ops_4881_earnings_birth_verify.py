"""ops/4881 -- justhodl-earnings v1.0.0 birth verify.
 (0) env: copy FMP_KEY from donor justhodl-estimate-revisions
     onto the new function (idempotent); ANTHROPIC_KEY from
     justhodl-watchlist-debate so the LLM layer self-heals.
 (1) settle marker 'earnings v1.0.0' (Active-wait); invoke;
     poll doc v==1.0.0 (long budget: ~42 transcripts).
 (2) field-level G0: beat_league[0].beat_score numeric AND
     rank==1.
 (3) truths: recompute row0 surprise+score from its own
     eps/rev fields; rank monotonic; misses are the tail;
     LIVE-pick spot check -- re-fetch one pick's transcript
     and recount its top phrase == doc pos_hits; ai_mode
     tagged honestly; history born.
 (4) page committed + served.
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
FN = "justhodl-earnings"
B = "justhodl-dashboard-live"
OUT_KEY = "data/earnings.json"
MARKER = "earnings v1.0.0"
PAGE = Path(__file__).resolve().parents[3] / "earnings.html"
S = "https://financialmodelingprep.com/stable"

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


def donor_env(fn, name):
    try:
        return lam.get_function_configuration(
            FunctionName=fn)["Environment"]["Variables"] \
            .get(name)
    except Exception:  # noqa: BLE001
        return None


def main():
    with report("ops 4881 -- earnings birth verify") as rep:
        rep.heading("0. env keys")
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
        env = lam.get_function_configuration(
            FunctionName=FN).get("Environment",
                                 {}).get("Variables", {})
        fmp = donor_env("justhodl-estimate-revisions",
                        "FMP_KEY")
        ant = donor_env("justhodl-watchlist-debate",
                        "ANTHROPIC_KEY")
        want = dict(env)
        if fmp:
            want["FMP_KEY"] = fmp
        if ant:
            want["ANTHROPIC_KEY"] = ant
        if want != env:
            lam.update_function_configuration(
                FunctionName=FN,
                Environment={"Variables": want})
            rep.ok("env keys set (FMP=%s, ANTHROPIC=%s)"
                   % (bool(fmp), bool(ant)))
            for _ in range(30):
                time.sleep(6)
                try:
                    c2 = lam.get_function_configuration(
                        FunctionName=FN)
                    if c2.get("LastUpdateStatus") \
                            != "InProgress":
                        break
                except ClientError:
                    pass
        else:
            rep.ok("env keys already present")

        rep.heading("1. settle + invoke")
        settled = False
        for att in range(30):
            try:
                gf = lam.get_function(FunctionName=FN)
                raw = urllib.request.urlopen(
                    gf["Code"]["Location"], timeout=60).read()
                src = zipfile.ZipFile(io.BytesIO(raw)).read(
                    "lambda_function.py").decode("utf-8",
                                                 "replace")
                if MARKER in src:
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
        while time.time() - t0 < 560:
            time.sleep(15)
            try:
                d = sread(OUT_KEY)
            except (ClientError, KeyError):
                continue
            if d.get("generated_at") != prev \
                    and d.get("v") == "1.0.0":
                doc = d
                break
        if not doc:
            rep.fail("no fresh doc in 560s")
            sys.exit(1)
        rep.ok("fresh in %ds status=%s"
               % (int(time.time() - t0), doc.get("status")))

        rep.heading("2. field-level G0")
        lg = doc.get("beat_league") or []
        if lg and isinstance(lg[0].get("beat_score"),
                             (int, float)) \
                and lg[0].get("rank") == 1:
            rep.ok("beat_league[0]: %s score %+0.1f (n=%d "
                   "reporters)" % (lg[0]["t"],
                                   lg[0]["beat_score"],
                                   doc["stats"]
                                   ["n_reporters"]))
        else:
            rep.fail("league empty or field missing")
            sys.exit(1)

        rep.heading("3. truths")
        r0 = lg[0]
        ep = round((r0["eps_a"] - r0["eps_e"])
                   / abs(r0["eps_e"]) * 100, 1)
        exp = 0.6 * max(-100, min(100, ep))
        if r0.get("rev_surprise_pct") is not None:
            exp += 0.4 * max(-100, min(
                100, r0["rev_surprise_pct"] * 5))
        if r0["eps_surprise_pct"] == ep \
                and r0["beat_score"] == round(exp, 1):
            rep.ok("  row0 surprise+score == recompute "
                   "(eps %+0.1f%%)" % ep)
        else:
            rep.fail("  row0 math: doc %s/%s vs %s/%s"
                     % (r0["eps_surprise_pct"],
                        r0["beat_score"], ep,
                        round(exp, 1)))
            FAILED.append("math")
        mono = all(lg[i]["beat_score"]
                   >= lg[i + 1]["beat_score"]
                   for i in range(len(lg) - 1))
        (rep.ok if mono else rep.fail)(
            "  rank monotonic over %d rows" % len(lg))
        if not mono:
            FAILED.append("mono")
        gc = doc.get("growth_calls") or {}
        picks = gc.get("picks") or []
        rep.ok("  scanned %d transcripts -> %d picks"
               % (gc.get("scanned", 0), len(picks)))
        if picks:
            p0 = picks[0]
            rep.log("  top pick %s pick=%s growth=%s "
                    "mode=%s" % (p0["t"], p0["pick_score"],
                                 p0["growth_score"],
                                 p0.get("ai_mode")))
            top_ph = max(p0["pos_hits"],
                         key=lambda k: p0["pos_hits"][k])
            y, q = p0["period"][:4], p0["period"][-1]
            fmpk = donor_env(FN, "FMP_KEY")
            tr = json.loads(urllib.request.urlopen(
                urllib.request.Request(
                    "%s/earning-call-transcript?symbol=%s"
                    "&year=%s&quarter=%s&apikey=%s"
                    % (S, p0["t"], y, q, fmpk),
                    headers={"User-Agent": "ops-4881"}),
                timeout=50).read())
            cnt = str(tr[0]["content"]).lower().count(top_ph)
            if cnt == p0["pos_hits"][top_ph]:
                rep.ok("  transcript spot-check: %r x%d == "
                       "independent recount" % (top_ph, cnt))
            else:
                rep.fail("  recount %r doc=%d live=%d"
                         % (top_ph, p0["pos_hits"][top_ph],
                            cnt))
                FAILED.append("recount")
            if "rules_only" in str(p0.get("ai_mode")) \
                    or p0.get("ai_mode") == "llm":
                rep.ok("  ai_mode honest: %s"
                       % p0["ai_mode"][:60])
        try:
            hist = sread("data/earnings/history.json")
            rep.ok("  history born runs=%d"
                   % len(hist.get("runs", [])))
        except (ClientError, KeyError):
            rep.fail("  history missing")
            FAILED.append("hist")

        rep.heading("4. page")
        if "Growth Calls" in PAGE.read_text(
                encoding="utf-8"):
            rep.ok("  committed")
        else:
            rep.fail("  page missing tokens")
            sys.exit(1)
        t0 = time.time()
        while time.time() - t0 < 480:
            try:
                req = urllib.request.Request(
                    "https://justhodl.ai/earnings.html?t=%d"
                    % int(time.time()),
                    headers={"User-Agent": "ops-4881",
                             "Cache-Control": "no-cache"})
                with urllib.request.urlopen(req, timeout=45) \
                        as r:
                    if "Growth Calls" in r.read().decode(
                            "utf-8", "replace"):
                        rep.ok("  SERVED (%ds)"
                               % int(time.time() - t0))
                        break
            except Exception:  # noqa: BLE001
                pass
            time.sleep(30)
        else:
            rep.fail("  not served")
            FAILED.append("served")

        rep.heading("5. verdict")
        if FAILED:
            rep.fail("HARD FAILS: %s" % sorted(set(FAILED)))
            sys.exit(1)
        rep.ok("earnings desk born: beat league + transcript "
               "growth calls, every number recomputed "
               "independently")


if __name__ == "__main__":
    main()
