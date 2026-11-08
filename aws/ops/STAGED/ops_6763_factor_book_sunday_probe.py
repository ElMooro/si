"""ops 6763 - READ-ONLY probe of the factor book behind the leaders desk's Sunday refusal (ops 6762: justhodl-leaders raised
'only 58 leaders from 27109 factor-book rows: the book is short' at 03:53 UTC on 2026-11-08; Saturday's run drew 673 from
27112 rows). select_leaders needs, per row, is_common not false, mcap > 0, an industry, dollar_vol_20d > 0. This op reads
the factor book's manifest (as_of per source, coverage), counts those four conditions over the live matrix the way the
leaders engine does, and reads justhodl-bank-factors' last two log streams (redacted). Invokes nothing, changes nothing."""
from datetime import datetime, timezone
from pathlib import Path
import json
import re
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
BOOK_FIELDS = ["mcap", "dollar_vol_20d", "country", "cik", "ipo_date"]
NOT_COMMON_SUFFIX = re.compile(r"-(R|RI|RT|RW|WS|WT|W|U|UN|CVR)$")      # the leaders engine's own rule (lambda_function.py:89)
SECRET_KV = re.compile(r"(?i)\b(api[_-]?key|apikey|access[_-]?key|token|secret|password|passwd|authorization|signature|key)(\s*[=:]\s*|%3D)[^\s&\"',}]+")
LONG_TOKEN = re.compile(r"[A-Za-z0-9_\-+/=]{20,}")
AWS_KEY = re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b")
HEX32 = re.compile(r"[0-9a-fA-F]{32,}")


def _opaque(tok):
    if AWS_KEY.fullmatch(tok) or HEX32.fullmatch(tok):
        return True
    has_d, has_u, has_l = any(c.isdigit() for c in tok), any(c.isupper() for c in tok), any(c.islower() for c in tok)
    if has_d and has_u and has_l:
        return True
    return len(tok) >= 28 and has_d and (has_u or has_l) and "-" not in tok and "_" not in tok


def redact(line):
    line = SECRET_KV.sub(lambda m: m.group(1) + m.group(2) + "***", str(line))
    return LONG_TOKEN.sub(lambda m: "***" if _opaque(m.group(0)) else m.group(0), line)


def _num(v):
    return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) and v == v else None


def is_common(r):
    if r.get("is_common") is False or r.get("is_etf") or r.get("is_otc") or r.get("is_fund") or r.get("is_preferred") or r.get("is_warrant_unit") or r.get("is_shell") or r.get("is_debt_or_structured"):
        return False
    return not NOT_COMMON_SUFFIX.search(str(r.get("symbol") or "").upper())


def funnel(rows):
    """The leaders engine's candidate funnel over factor-book rows - pure, for the selftest."""
    st = {"rows": len(rows), "common": 0, "no_cap": 0, "no_industry": 0, "no_tape": 0, "candidates": 0,
          "mcap_null": 0, "dv_null": 0, "industry_blank": 0, "is_common_true": 0, "is_common_false": 0, "is_common_none": 0}
    for r in rows:
        if _num(r.get("mcap")) is None:
            st["mcap_null"] += 1
        if _num(r.get("dollar_vol_20d")) is None:
            st["dv_null"] += 1
        if not str(r.get("industry") or "").strip():
            st["industry_blank"] += 1
        ic = r.get("is_common")
        st["is_common_true" if ic is True else "is_common_false" if ic is False else "is_common_none"] += 1
        if not str(r.get("symbol") or ""):
            continue
        if not is_common(r):
            continue
        st["common"] += 1
        mcap, dv = _num(r.get("mcap")), _num(r.get("dollar_vol_20d"))
        ind = str(r.get("industry") or "").strip()
        if not mcap or mcap <= 0:
            st["no_cap"] += 1
            continue
        if not ind or ind == "Shell Companies":
            st["no_industry"] += 1
            continue
        if not dv or dv <= 0:
            st["no_tape"] += 1
            continue
        st["candidates"] += 1
    return st


def selftest():
    st = funnel([{"symbol": "AAPL", "is_common": True, "mcap": 3e12, "dollar_vol_20d": 1e10, "industry": "Consumer Electronics"},
                 {"symbol": "XYZ", "is_common": True, "mcap": None, "dollar_vol_20d": 1e6, "industry": "Banks"},
                 {"symbol": "ABC-WS", "is_common": True, "mcap": 1e9, "dollar_vol_20d": 1e6, "industry": "Banks"},
                 {"symbol": "SPY", "is_etf": True, "mcap": 1e11, "dollar_vol_20d": 1e10, "industry": ""},
                 {"symbol": "NOI", "is_common": True, "mcap": 1e9, "dollar_vol_20d": 1e6, "industry": ""}])
    assert st["candidates"] == 1 and st["common"] == 3 and st["no_cap"] == 1 and st["no_industry"] == 1 and st["mcap_null"] == 1, st
    assert redact("x apikey=abc123 y") == "x apikey=*** y"
    print("selftest ok")


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report
    import bank_enrich

    selftest()
    cfg = Config(retries={"max_attempts": 6, "mode": "standard"}, connect_timeout=10, read_timeout=60)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    logs = boto3.client("logs", region_name=REGION, config=cfg)
    now = datetime.now(timezone.utc)
    facts = {}
    with report("ops_6763_factor_book_sunday_probe") as r:
        r.heading("ops 6763 - the factor book behind the leaders desk's Sunday refusal %s (manifest, leaders funnel over the live "
                  "matrix, bank-factors log; read-only)" % now.date())

        r.section("A. factor book manifest + matrix objects")
        for key in ("manifest.json", "matrix/index.json.gz", "matrix/core.json.gz", "matrix/cols/mcap.json.gz",
                    "matrix/cols/dollar_vol_20d.json.gz", "matrix/cols/industry.json.gz", "matrix/cols/is_common.json.gz"):
            try:
                h = s3.head_object(Bucket=LIVE, Key=bank_enrich.ROOT + key)
                r.log("%s: %d B, last modified %s (%.1f h)" % (key, h["ContentLength"], h["LastModified"].isoformat(timespec="seconds"),
                                                             (now - h["LastModified"]).total_seconds() / 3600))
                if key == "manifest.json":
                    facts["manifest_age_h"] = round((now - h["LastModified"]).total_seconds() / 3600, 1)
            except Exception as exc:  # noqa: BLE001
                r.warn("%s: %s" % (key, str(exc)[:120]))
        man = bank_enrich.manifest() or {}
        r.log("manifest keys: %s" % sorted(man)[:40])
        r.log("as_of: %s" % json.dumps(man.get("as_of") or {}, default=str, sort_keys=True)[:900])
        r.log("symbols %s | generated %s | version %s" % (man.get("symbols"), man.get("generated_at") or man.get("updated_at"), man.get("version")))
        cov = man.get("coverage") or {}
        if isinstance(cov, dict):
            keep = {k: cov[k] for k in cov if any(s in k for s in ("mcap", "dollar_vol", "industry", "is_common", "profile", "returns", "identity", "symbols"))}
            r.log("coverage (selected): %s" % json.dumps(keep, default=str, sort_keys=True)[:1200])
        for k in ("degraded", "notes", "missing", "errors", "sources", "schema_found"):
            if man.get(k):
                r.log("%s: %s" % (k, redact(json.dumps(man.get(k), default=str))[:900]))
        facts["book_symbols"] = man.get("symbols")

        r.section("B. the leaders funnel over the live matrix (the engine's own conditions)")
        rows = bank_enrich.rows(fields=BOOK_FIELDS)
        st = funnel(rows)
        r.log("funnel: %s" % json.dumps(st, sort_keys=True))
        facts.update({"rows": st["rows"], "common": st["common"], "candidates": st["candidates"], "no_cap": st["no_cap"],
                      "no_industry": st["no_industry"], "no_tape": st["no_tape"], "mcap_null": st["mcap_null"], "dv_null": st["dv_null"]})
        r.log("Saturday's run (ops 6762 log): 27112 rows, 5329 common, 673 leaders; Sunday's refusal: 27109 rows, 58 leaders")
        if st["rows"] and st["candidates"] < 1000:
            r.warn("the live book yields only %d leader candidates from %d common rows: mcap missing on %d, industry blank on %d, "
                   "dollar_vol_20d missing on %d - the leaders desk refuses (correctly) until the book is whole again" % (
                       st["candidates"], st["common"], st["no_cap"], st["no_industry"], st["no_tape"]))
        else:
            r.log("the live book is whole again (%d candidates); the Sunday 02:50 book was the short one and tomorrow's 03:50 run should write" % st["candidates"])
        sample = [r_ for r_ in rows if is_common(r_) and (_num(r_.get("mcap")) is None or _num(r_.get("dollar_vol_20d")) is None)][:12]
        r.log("sample of common rows missing mcap or dollar_vol_20d: %s" % json.dumps(
            [{k: r_.get(k) for k in ("symbol", "name", "industry", "mcap", "dollar_vol_20d", "country")} for r_ in sample], default=str)[:1200])

        r.section("C. justhodl-bank-factors: last two log streams (redacted)")
        try:
            streams = logs.describe_log_streams(logGroupName="/aws/lambda/justhodl-bank-factors", orderBy="LastEventTime", descending=True,
                                                limit=2).get("logStreams", [])
            for stm in streams:
                evs = logs.get_log_events(logGroupName="/aws/lambda/justhodl-bank-factors", logStreamName=stm["logStreamName"], limit=150,
                                          startFromHead=False).get("events", [])
                starts = [i for i, e in enumerate(evs) if e["message"].startswith("START RequestId")]
                last = evs[starts[-1]:] if starts else evs
                r.log("stream %s: last event %s, %d events, last invocation %d lines" % (
                    stm["logStreamName"][-20:], datetime.fromtimestamp(stm.get("lastEventTimestamp", 0) / 1000, timezone.utc).isoformat(timespec="minutes"),
                    len(evs), len(last)))
                for e in last[-60:]:
                    stamp = datetime.fromtimestamp(e["timestamp"] / 1000, timezone.utc).strftime("%m-%d %H:%M:%S")
                    r.log("  %s %s" % (stamp, redact(" ".join(e["message"].split()))[:600]))
        except Exception as exc:  # noqa: BLE001
            r.warn("logs: %s: %s" % (type(exc).__name__, str(exc)[:160]))

        r.kv(**{k: str(v) for k, v in facts.items()})
        r.ok("read-only; nothing invoked, nothing changed")
    sys.exit(0)


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
