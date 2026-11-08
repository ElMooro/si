"""ops 6764 - verify justhodl-bank-factors 1.0.8 (ops 6763 finding: the Saturday grouped file emptied the market's returns;
justhodl-leaders refused on 2026-11-08) and heal today's book.

  G0  three-rung deploy ladder: the artifact zip carries the 1.0.8 marker ('thin_skipped'), State Active, LastUpdateStatus Successful;
  1.  invoke justhodl-bank-factors (Event) and poll the factor book's manifest until as_of.polygon-tape moves off the
      Saturday (2026-11-07) to the Friday (2026-11-06) with returns coverage back in the thousands (yesterday 12,561);
  2.  invoke justhodl-leaders (Event) and poll data/leaders.json until it is younger than this run with >= 500 leaders;
  3.  the leaders funnel over the healed matrix.
Neither engine is held (CAP / ANOM lists read first; held -> stop). Writes nothing but this report."""
from datetime import datetime, timezone
from pathlib import Path
import io
import json
import re
import sys
import time
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
GOVERNOR = "justhodl-spend-governor"
BOOK = "justhodl-bank-factors"
LEADERS = "justhodl-leaders"
MARKER = "thin_skipped"
SATURDAY = "2026-11-07"
EXPECT_LAST = "2026-11-06"
BOOK_FIELDS = ["mcap", "dollar_vol_20d", "country", "cik", "ipo_date"]
NOT_COMMON_SUFFIX = re.compile(r"-(R|RI|RT|RW|WS|WT|W|U|UN|CVR)$")


def _num(v):
    return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) and v == v else None


def is_common(r):
    if r.get("is_common") is False or r.get("is_etf") or r.get("is_otc") or r.get("is_fund") or r.get("is_preferred") or r.get("is_warrant_unit") or r.get("is_shell") or r.get("is_debt_or_structured"):
        return False
    return not NOT_COMMON_SUFFIX.search(str(r.get("symbol") or "").upper())


def candidates(rows):
    n = 0
    for r in rows:
        if is_common(r) and (_num(r.get("mcap")) or 0) > 0 and str(r.get("industry") or "").strip() and (_num(r.get("dollar_vol_20d")) or 0) > 0:
            n += 1
    return n


def selftest():
    assert candidates([{"symbol": "A", "is_common": True, "mcap": 1e9, "industry": "X", "dollar_vol_20d": 1e6},
                       {"symbol": "B", "is_common": True, "mcap": 1e9, "industry": "X", "dollar_vol_20d": None}]) == 1
    print("selftest ok")


def ready(lam, fn, r, wait_s=240):
    t0 = time.time()
    while True:
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") == "Successful":
            return c
        if time.time() - t0 > wait_s:
            r.fail("%s not ready: State %s LastUpdateStatus %s" % (fn, c.get("State"), c.get("LastUpdateStatus")))
            sys.exit(1)
        time.sleep(10)


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report
    import bank_enrich

    selftest()
    cfg = Config(retries={"max_attempts": 6, "mode": "standard"}, connect_timeout=10, read_timeout=120)
    lam = boto3.client("lambda", region_name=REGION, config=cfg)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    now = datetime.now(timezone.utc)
    facts = {}
    with report("ops_6764_bank_factors_thin_session_verify") as r:
        r.heading("ops 6764 - bank-factors 1.0.8 verify + heal today's book and the leaders desk (%s)" % now.date())

        r.section("G0. holds and the three-rung ladder")
        st = {}
        for attempt in range(4):
            try:
                st = json.loads(lam.invoke(FunctionName=GOVERNOR, Payload=b'{"action":"status"}')["Payload"].read() or b"{}")
                break
            except lam.exceptions.TooManyRequestsException:
                time.sleep(30)
        held = set(st.get("capped") or []) | {fn for fn, _ in (st.get("anomaly_held") or [])}
        r.log("governor stage %s | capped %s | anomaly %s" % (st.get("stage"), st.get("capped"), st.get("anomaly_held")))
        if BOOK in held or LEADERS in held or st.get("stage") not in ("NORMAL", None):
            r.fail("an engine is held or the stage is not NORMAL - nothing invoked")
            sys.exit(1)
        c = ready(lam, BOOK, r)
        r.log("%s: State %s / %s, last modified %s, timeout %ss" % (BOOK, c["State"], c["LastUpdateStatus"], c["LastModified"], c["Timeout"]))
        code = lam.get_function(FunctionName=BOOK)["Code"]["Location"]
        blob = urllib.request.urlopen(code, timeout=120).read()
        src = zipfile.ZipFile(io.BytesIO(blob)).read("lambda_function.py").decode("utf-8", "replace")
        ver = re.search(r'^VERSION = "([^"]+)"', src, re.M)
        r.log("artifact: %d B, lambda_function.py VERSION %s, marker '%s' %s" % (len(blob), ver.group(1) if ver else "?", MARKER, "present" if MARKER in src else "ABSENT"))
        facts["artifact_version"] = ver.group(1) if ver else None
        if MARKER not in src or (ver and ver.group(1) != "1.0.8"):
            r.fail("the deployed artifact is not 1.0.8 - deploy-lambdas has not landed; re-run this op after it does")
            sys.exit(1)

        r.section("1. bank-factors: invoke (Event) and poll the manifest")
        man0 = bank_enrich.manifest(max_age_s=0) or {}
        r.log("before: generated %s | as_of polygon-tape %s | returns %s" % (man0.get("generated_at") or man0.get("updated_at"),
                                                                             (man0.get("as_of") or {}).get("polygon-tape"), (man0.get("coverage") or {}).get("returns")))
        t_inv = time.time()
        res = lam.invoke(FunctionName=BOOK, InvocationType="Event", Payload=json.dumps({"force": True, "trigger": "ops-6764"}).encode())
        r.log("invoke status %s" % res.get("StatusCode"))
        man = {}
        while time.time() - t_inv < 840:
            time.sleep(30)
            try:
                h = s3.head_object(Bucket=LIVE, Key=bank_enrich.ROOT + "manifest.json")
                if h["LastModified"].timestamp() > t_inv:
                    man = bank_enrich.manifest(max_age_s=0) or {}
                    break
            except Exception:  # noqa: BLE001
                pass
        if not man:
            r.fail("the factor book's manifest did not refresh within 14 min of the invoke")
            sys.exit(1)
        tape = (man.get("as_of") or {}).get("polygon-tape")
        cov = (man.get("coverage") or {}).get("returns")
        srcs = (man.get("sources") or {}).get("returns") or {}
        r.log("after: generated %s | version %s | as_of polygon-tape %s | returns coverage %s | returns info %s" % (
            man.get("generated_at") or man.get("updated_at"), man.get("version"), tape, cov, json.dumps(srcs, default=str)[:400]))
        facts.update(book_version=man.get("version"), book_tape=tape, book_returns=cov)
        if tape == SATURDAY or (isinstance(cov, (int, float)) and cov < 5000):
            r.fail("the book still reads the Saturday as a session or returns coverage is thin (%s, %s)" % (tape, cov))
            sys.exit(1)
        if tape != EXPECT_LAST:
            r.warn("last session %s (expected %s - a newer complete session is fine)" % (tape, EXPECT_LAST))
        rows = bank_enrich.rows(fields=BOOK_FIELDS)
        n_cand = candidates(rows)
        r.log("leaders funnel over the healed matrix: %d rows, %d candidates (Sunday 02:50 book: 153)" % (len(rows), n_cand))
        facts["leader_candidates"] = n_cand
        if n_cand < 1000:
            r.fail("the book still yields only %d leader candidates" % n_cand)
            sys.exit(1)

        r.section("2. leaders: invoke (Event) and poll data/leaders.json")
        before = s3.head_object(Bucket=LIVE, Key="data/leaders.json")["LastModified"]
        r.log("leaders.json before: %s" % before.isoformat(timespec="seconds"))
        t_inv2 = time.time()
        res = lam.invoke(FunctionName=LEADERS, InvocationType="Event", Payload=json.dumps({"trigger": "ops-6764"}).encode())
        r.log("invoke status %s" % res.get("StatusCode"))
        doc = None
        while time.time() - t_inv2 < 300:
            time.sleep(15)
            h = s3.head_object(Bucket=LIVE, Key="data/leaders.json")
            if h["LastModified"] > before:
                doc = json.loads(s3.get_object(Bucket=LIVE, Key="data/leaders.json")["Body"].read())
                break
        if not doc:
            r.fail("leaders.json did not refresh within 5 min (the engine raised; read its log)")
            sys.exit(1)
        n_rows = len(doc.get("rows") or [])
        r.log("leaders.json after: as_of %s | %d leaders / %d industries / %d funds | bank last session %s | errors %s" % (
            doc.get("as_of"), n_rows, len(doc.get("industries") or {}), len(doc.get("etfs") or []), (doc.get("bank") or {}).get("last_session"), doc.get("n_errors")))
        facts.update(leaders_as_of=doc.get("as_of"), leaders_n=n_rows)
        if n_rows < 500:
            r.fail("only %d leaders published" % n_rows)
            sys.exit(1)
        r.kv(**{k: str(v) for k, v in facts.items()})
        r.ok("bank-factors 1.0.8 live; the book and the leaders desk are whole again")
    sys.exit(0)


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
