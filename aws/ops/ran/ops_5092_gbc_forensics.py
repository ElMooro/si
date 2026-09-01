"""ops_5092 -- global-cycle.html / justhodl-global-business-cycle forensics.

Khalid (2026-09-01): "global-cycle.html isn't working fix the engine and the
page". Before touching code this op establishes ground truth from AWS and
the edge (the sandbox cannot reach S3, justhodl.ai or Yahoo):

  S1  live feed state on S3: data/global-business-cycle.json + -history.json
      (LastModified, generated_at, fresh countries, UNKNOWN rows, per-country
      symbol/latest_date/reason, aggregate)
  S2  fleet witness: data/price-redundancy.json yahoo_success_rate
  S3  lambda config, EventBridge rule justhodl-gbc-daily, 14d CloudWatch
      Invocations/Errors/Duration/Throttles, last log streams tail
  S4  edge probes from the runner: page, history page, /data/ proxy, raw S3
      URL with Origin header (CORS), jsDelivr d3/topojson/world-atlas,
      deferred page scripts
  S5  source probes from the runner for all 34 countries: Yahoo chart API
      in the engine's EXACT request shape (probe-verified-grammar rule),
      Yahoo with a browser UA, FMP historical (key inherited from
      justhodl-equity-research), Stooq daily CSV candidates -- so the fix
      op can ship a verified multi-source chain instead of guessing
  S6  async engine invoke, poll S3 for a fresh generated_at (<= 16 min),
      then tail that run's log for per-country outcomes

Gate (sys.exit(1)): the engine did not produce a fresh feed within 16 min,
or the fresh feed has < 30/34 countries with data. A RED here is the
diagnosis, not a bug in this op.
"""
import gzip
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
FN = "justhodl-global-business-cycle"
RULE = "justhodl-gbc-daily"
LIVE_KEY = "data/global-business-cycle.json"
HIST_KEY = "data/global-business-cycle-history.json"
REDUN_KEY = "data/price-redundancy.json"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=120, retries={"max_attempts": 0}))
ev = boto3.client("events", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)

# the engine's country map (mirrors COUNTRY_MAP + SYMBOL_FALLBACKS in source)
COUNTRIES = [
    ("USA", "^GSPC"), ("CHN", "000001.SS"), ("JPN", "^N225"), ("DEU", "^GDAXI"),
    ("IND", "^BSESN"), ("GBR", "^FTSE"), ("FRA", "^FCHI"), ("ITA", "FTSEMIB.MI"),
    ("CAN", "^GSPTSE"), ("BRA", "^BVSP"), ("KOR", "^KS11"), ("AUS", "^AXJO"),
    ("ESP", "^IBEX"), ("MEX", "^MXX"), ("IDN", "^JKSE"), ("NLD", "^AEX"),
    ("TUR", "XU100.IS"), ("CHE", "^SSMI"), ("POL", "EPOL"), ("BEL", "^BFX"),
    ("SWE", "^OMX"), ("IRL", "^ISEQ"), ("AUT", "^ATX"), ("NOR", "^OSEAX"),
    ("ZAF", "^J203.JO"), ("DNK", "^OMXC25"), ("FIN", "^OMXH25"), ("CZE", "PX.PR"),
    ("HUN", "BUX.BD"), ("CHL", "ECH"), ("PRT", "PSI20.LS"), ("GRC", "GD.AT"),
    ("NZL", "^NZ50"), ("ISR", "^TA125.TA"),
]
FALLBACKS = {
    "POL": ["WIG20.WA", "^WIG"], "CZE": ["^PX", "PXTR.PR"], "HUN": ["^BUX", "OTP.BD"],
    "CHL": ["^IPSA", "^SPCLXIPSA"],
}
# Stooq candidates per country (probed, not assumed)
STOOQ = {
    "USA": ["^spx"], "CHN": ["^shc"], "JPN": ["^nkx"], "DEU": ["^dax"],
    "IND": ["^snx", "^bse", "^sensex"], "GBR": ["^ukx"], "FRA": ["^cac"],
    "ITA": ["^fmib", "^mib"], "CAN": ["^tsx", "^spxtsx"], "BRA": ["^bvp"],
    "KOR": ["^kospi"], "AUS": ["^aor", "^axjo"], "ESP": ["^ibex"],
    "MEX": ["^ipc", "^mxx"], "IDN": ["^jci", "^jkse"], "NLD": ["^aex"],
    "TUR": ["^xu100"], "CHE": ["^smi"], "POL": ["^wig20", "wig20", "^wig"],
    "BEL": ["^bel20"], "SWE": ["^omxs30", "^omxs"], "IRL": ["^iseq"],
    "AUT": ["^atx"], "NOR": ["^oseax", "^osebx"], "ZAF": ["^jalsh", "^j203"],
    "DNK": ["^omxc20", "^omxc25"], "FIN": ["^omxh25", "^hex"], "CZE": ["^px"],
    "HUN": ["^bux"], "CHL": ["^ipsa"], "PRT": ["^psi20"], "GRC": ["^athex", "^gd"],
    "NZL": ["^nz50", "^nzx50"], "ISR": ["^ta125", "^ta35"],
}
# US-listed country ETF proxies (in-house polygon-full warehouse candidates)
ETF = {
    "USA": "SPY", "CHN": "MCHI", "JPN": "EWJ", "DEU": "EWG", "IND": "INDA", "GBR": "EWU",
    "FRA": "EWQ", "ITA": "EWI", "CAN": "EWC", "BRA": "EWZ", "KOR": "EWY", "AUS": "EWA",
    "ESP": "EWP", "MEX": "EWW", "IDN": "EIDO", "NLD": "EWN", "TUR": "TUR", "CHE": "EWL",
    "POL": "EPOL", "BEL": "EWK", "SWE": "EWD", "IRL": "EIRL", "AUT": "EWO", "NOR": "NORW",
    "ZAF": "EZA", "DNK": "EDEN", "FIN": "EFNL", "CZE": None, "HUN": None, "CHL": "ECH",
    "PRT": None, "GRC": "GREK", "NZL": "ENZL", "ISR": "EIS",
}


def s3_json(key):
    try:
        o = s3.get_object(Bucket=B, Key=key)
        body = o["Body"].read()
        if key.endswith(".gz"):
            body = gzip.decompress(body)
        return json.loads(body), o.get("LastModified")
    except Exception as e:  # noqa: BLE001
        return None, str(e)[:120]


def http(url, headers=None, timeout=20, method="GET"):
    req = urllib.request.Request(url, headers=headers or {}, method=method)
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read()
            return {"status": r.status, "bytes": len(body), "ms": int((time.time() - t0) * 1000),
                    "headers": {k.lower(): v for k, v in r.headers.items()}, "body": body}
    except urllib.error.HTTPError as e:
        try:
            body = e.read()
        except Exception:  # noqa: BLE001
            body = b""
        return {"status": e.code, "bytes": len(body), "ms": int((time.time() - t0) * 1000),
                "headers": {k.lower(): v for k, v in (e.headers or {}).items()}, "body": body,
                "err": "HTTP %d" % e.code}
    except Exception as e:  # noqa: BLE001
        return {"status": None, "bytes": 0, "ms": int((time.time() - t0) * 1000),
                "headers": {}, "body": b"", "err": str(e)[:120]}


def yahoo_bars(symbol, ua, host="query1"):
    url = ("https://%s.finance.yahoo.com/v8/finance/chart/%s?range=5y&interval=1d"
           % (host, urllib.parse.quote(symbol)))
    res = http(url, headers={"User-Agent": ua, "Accept": "application/json"}, timeout=15)
    out = {"status": res["status"], "bars": 0, "last": None, "err": res.get("err")}
    if res["status"] == 200:
        try:
            d = json.loads(res["body"].decode("utf-8"))
            r0 = ((d.get("chart") or {}).get("result") or [None])[0]
            if r0:
                ts = r0.get("timestamp") or []
                q = ((r0.get("indicators") or {}).get("quote") or [{}])[0]
                cl = q.get("close") or []
                good = [(t, c) for t, c in zip(ts, cl) if c is not None]
                out["bars"] = len(good)
                if good:
                    out["last"] = datetime.fromtimestamp(good[-1][0], tz=timezone.utc).strftime("%Y-%m-%d")
            else:
                out["err"] = str((d.get("chart") or {}).get("error"))[:80]
        except Exception as e:  # noqa: BLE001
            out["err"] = "parse " + str(e)[:60]
    else:
        body = res["body"][:160].decode("utf-8", "replace") if res["body"] else ""
        out["err"] = (out["err"] or "") + " " + body.replace("\n", " ")
    return out


def fmp_bars(symbol, key):
    frm = (datetime.now(timezone.utc) - timedelta(days=5 * 365 + 10)).strftime("%Y-%m-%d")
    url = ("https://financialmodelingprep.com/api/v3/historical-price-full/%s?serietype=line&from=%s&apikey=%s"
           % (urllib.parse.quote(symbol), frm, key))
    res = http(url, headers={"User-Agent": "JH-ops5092/1.0"}, timeout=20)
    out = {"status": res["status"], "bars": 0, "last": None, "err": res.get("err")}
    if res["status"] == 200:
        try:
            d = json.loads(res["body"].decode("utf-8"))
            hist = d.get("historical") if isinstance(d, dict) else None
            if hist:
                out["bars"] = len(hist)
                out["last"] = hist[0].get("date")
            else:
                out["err"] = str(d)[:100]
        except Exception as e:  # noqa: BLE001
            out["err"] = "parse " + str(e)[:60]
    return out


def stooq_bars(sid):
    url = "https://stooq.com/q/d/l/?s=%s&i=d" % urllib.parse.quote(sid)
    res = http(url, headers={"User-Agent": "jh-series/1.0"}, timeout=20)
    out = {"status": res["status"], "bars": 0, "last": None, "err": res.get("err")}
    if res["status"] == 200:
        txt = res["body"].decode("utf-8", "replace")
        if "<" in txt[:40] or "limit" in txt[:80].lower() or "No data" in txt[:40]:
            out["err"] = txt[:60].replace("\n", " ")
            return out
        rows = [ln for ln in txt.splitlines()[1:] if ln.strip()]
        out["bars"] = len(rows)
        if rows:
            out["last"] = rows[-1].split(",")[0]
    return out


def log_tail(fn, n_streams=2, limit=200, since_ms=None):
    lines = []
    try:
        streams = logs.describe_log_streams(logGroupName="/aws/lambda/" + fn, orderBy="LastEventTime",
                                            descending=True, limit=n_streams).get("logStreams", [])
        for st in streams:
            kw = dict(logGroupName="/aws/lambda/" + fn, logStreamName=st["logStreamName"],
                      limit=limit, startFromHead=True)
            if since_ms:
                kw["startTime"] = since_ms
            evs = logs.get_log_events(**kw).get("events", [])
            lines.append("── stream %s (last event %s) ──" % (
                st["logStreamName"][-24:],
                datetime.fromtimestamp((st.get("lastEventTimestamp") or 0) / 1000, tz=timezone.utc).isoformat(timespec="seconds")))
            for e in evs:
                lines.append(e.get("message", "").rstrip()[:240])
    except Exception as e:  # noqa: BLE001
        lines.append("log tail unavailable: %s" % str(e)[:140])
    return lines


def main():
    with report("5092-gbc-forensics") as r:
        r.heading("ops 5092 -- global-cycle.html / justhodl-global-business-cycle forensics")
        now = datetime.now(timezone.utc)
        verdict = {}

        # ── S1 live feed ────────────────────────────────────────────────
        r.section("S1 live feed on S3")
        live, lm = s3_json(LIVE_KEY)
        if not live:
            r.fail("%s unreadable: %s" % (LIVE_KEY, lm))
            verdict["live_feed"] = "MISSING"
        else:
            age_h = (now - lm).total_seconds() / 3600 if hasattr(lm, "tzinfo") else None
            bc = live.get("by_country") or {}
            unk = [k for k, v in bc.items() if (v or {}).get("phase") in (None, "UNKNOWN")]
            stale = [(k, v.get("months_stale")) for k, v in bc.items() if (v.get("months_stale") or 0) > 3]
            agg = live.get("aggregate") or {}
            r.kv(s1="live", last_modified=str(lm), age_h=round(age_h, 1) if age_h else None,
                 generated_at=live.get("generated_at"), schema=live.get("schema_version"),
                 elapsed_sec=live.get("elapsed_sec"), fresh=live.get("countries_with_fresh_data"),
                 total=live.get("countries_total"), n_by_country=len(bc), unknown=len(unk),
                 global_phase=agg.get("global_phase"), avg_cli=agg.get("global_avg_cli"),
                 weight_covered=agg.get("total_weight_covered"))
            r.log("UNKNOWN rows: %s" % (unk or "none"))
            r.log("stale>3mo rows: %s" % (stale or "none"))
            for k in sorted(bc):
                v = bc[k] or {}
                r.log("  %-4s %-10s sym=%-12s latest=%s stale=%smo cli=%s 3m=%s reason=%s phys=%s" % (
                    k, v.get("phase"), v.get("yahoo_symbol"), v.get("latest_date"), v.get("months_stale"),
                    v.get("cli_level"), v.get("three_month_change"), v.get("reason"),
                    (v.get("physical") or {}).get("state")))
            pc = live.get("physical_confirmation") or {}
            r.log("physical_confirmation: %s" % json.dumps({k: pc.get(k) for k in ("countries_with_ports", "counts", "error")}))
            verdict["live_feed"] = "STALE" if (age_h is None or age_h > 36) else "FRESH"
            verdict["live_unknown"] = len(unk)
        hist, hlm = s3_json(HIST_KEY)
        if not hist:
            r.fail("%s unreadable: %s" % (HIST_KEY, hlm))
        else:
            agg_s = hist.get("aggregate_series") or hist.get("aggregate") or []
            hbc = hist.get("by_country") or {}
            r.kv(s1="history", last_modified=str(hlm), generated_at=hist.get("generated_at"),
                 n_countries=len(hbc), n_aggregate_points=len(agg_s) if isinstance(agg_s, list) else None,
                 first=(agg_s[0].get("date") if isinstance(agg_s, list) and agg_s else None),
                 last=(agg_s[-1].get("date") if isinstance(agg_s, list) and agg_s else None),
                 top_keys=sorted(hist.keys())[:12])

        # ── S2 fleet witness ───────────────────────────────────────────
        r.section("S2 fleet witness: price-redundancy yahoo_success_rate")
        red, rlm = s3_json(REDUN_KEY)
        if red:
            r.kv(s2="price-redundancy", last_modified=str(rlm),
                 yahoo_success_rate=red.get("yahoo_success_rate"),
                 stooq_success_rate=red.get("stooq_success_rate"), n=red.get("tickers_total") or red.get("n"),
                 generated_at=red.get("generated_at") or red.get("as_of"))
        else:
            r.warn("price-redundancy feed unreadable: %s" % rlm)

        # ── S3 lambda + schedule + metrics + logs ──────────────────────
        r.section("S3 lambda config / rule / metrics / logs")
        try:
            cfg = lam.get_function_configuration(FunctionName=FN)
            r.kv(s3="config", state=cfg.get("State"), last_update=cfg.get("LastUpdateStatus"),
                 runtime=cfg.get("Runtime"), timeout=cfg.get("Timeout"), memory=cfg.get("MemorySize"),
                 last_modified=cfg.get("LastModified"), env_keys=sorted((cfg.get("Environment") or {}).get("Variables", {}).keys()),
                 reserved=lam.get_function_concurrency(FunctionName=FN).get("ReservedConcurrentExecutions"))
        except Exception as e:  # noqa: BLE001
            r.fail("get_function_configuration: %s" % str(e)[:160])
        try:
            rule = ev.describe_rule(Name=RULE)
            tg = ev.list_targets_by_rule(Rule=RULE).get("Targets", [])
            r.kv(s3="rule", name=RULE, state=rule.get("State"), expr=rule.get("ScheduleExpression"),
                 targets=[t.get("Arn", "")[-40:] for t in tg])
        except Exception as e:  # noqa: BLE001
            r.fail("rule %s: %s" % (RULE, str(e)[:160]))
        try:
            end = now
            start = now - timedelta(days=14)
            for metric, stat in (("Invocations", "Sum"), ("Errors", "Sum"), ("Throttles", "Sum"), ("Duration", "Maximum")):
                dp = cw.get_metric_statistics(Namespace="AWS/Lambda", MetricName=metric,
                                              Dimensions=[{"Name": "FunctionName", "Value": FN}],
                                              StartTime=start, EndTime=end, Period=86400, Statistics=[stat]
                                              ).get("Datapoints", [])
                dp.sort(key=lambda x: x["Timestamp"])
                r.log("  %-11s 14d: %s" % (metric, ", ".join("%s=%s" % (d["Timestamp"].strftime("%m-%d"), int(d[stat])) for d in dp) or "no datapoints"))
        except Exception as e:  # noqa: BLE001
            r.warn("metrics: %s" % str(e)[:140])
        for ln in log_tail(FN, n_streams=2, limit=120):
            if any(t in ln for t in ("failed", "Task timed out", "Error", "Traceback", "REPORT", "START", "physical", "history", "fell back", "──", "v2.0 start", "written")):
                r.log("  LOG " + ln)

        # ── S4 edge probes ─────────────────────────────────────────────
        r.section("S4 edge probes from the runner")
        edge = {
            "page": ("https://justhodl.ai/global-cycle.html", {}),
            "history_page": ("https://justhodl.ai/global-cycle/", {}),
            "data_proxy": ("https://justhodl.ai/data/global-business-cycle.json", {}),
            "data_proxy_hist": ("https://justhodl.ai/data/global-business-cycle-history.json", {}),
            "raw_s3": ("https://justhodl-dashboard-live.s3.amazonaws.com/data/global-business-cycle.json",
                       {"Origin": "https://justhodl.ai"}),
            "raw_s3_hist": ("https://justhodl-dashboard-live.s3.amazonaws.com/data/global-business-cycle-history.json",
                            {"Origin": "https://justhodl.ai"}),
            "d3": ("https://cdn.jsdelivr.net/npm/d3@7", {}),
            "topojson": ("https://cdn.jsdelivr.net/npm/topojson-client@3", {}),
            "world_atlas": ("https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json", {}),
            "wss_client": ("https://justhodl.ai/wss-client.js", {}),
            "page_ai": ("https://justhodl.ai/jh-page-ai.js", {}),
            "nav_drawer": ("https://justhodl.ai/jh-nav-drawer.js", {}),
            "sidebar": ("https://justhodl.ai/sidebar.js", {}),
        }
        for name, (url, hdr) in edge.items():
            res = http(url, headers={"User-Agent": "Mozilla/5.0 (ops5092)", **hdr}, timeout=25)
            h = res["headers"]
            extra = {}
            if name.startswith("raw_s3") or name.startswith("data_proxy"):
                extra = {"acao": h.get("access-control-allow-origin"), "cache": h.get("cache-control"),
                         "ctype": h.get("content-type"), "last_mod": h.get("last-modified"), "cf": h.get("cf-cache-status")}
                if res["status"] == 200 and res["body"][:1] == b"{":
                    try:
                        j = json.loads(res["body"].decode("utf-8"))
                        extra["generated_at"] = j.get("generated_at")
                    except Exception:  # noqa: BLE001
                        extra["generated_at"] = "parse-fail"
            if name == "page" and res["status"] == 200:
                body = res["body"].decode("utf-8", "replace")
                extra = {"has_src_const": "const SRC" in body, "src_is_raw_s3": "s3.amazonaws.com/data/global-business-cycle.json" in body,
                         "has_world_topo": "world-atlas@2" in body, "title_ok": "Global Business Cycle" in body}
            r.kv(s4=name, status=res["status"], bytes=res["bytes"], ms=res["ms"], err=res.get("err"), **extra)
        # OPTIONS preflight is not what a simple GET does; browsers send GET + Origin for cross-origin fetch.
        # The ACAO header on raw_s3 is the decisive CORS fact.

        # ── S5 source probes ───────────────────────────────────────────
        r.section("S5 source probes from the runner (34 countries)")
        fmp_key = None
        try:
            env = lam.get_function_configuration(FunctionName="justhodl-equity-research").get("Environment", {}).get("Variables", {})
            fmp_key = env.get("FMP_KEY")
            r.log("FMP key inherited from justhodl-equity-research: %s" % ("yes" if fmp_key else "NO"))
        except Exception as e:  # noqa: BLE001
            r.warn("donor env: %s" % str(e)[:120])
        ENGINE_UA = "Mozilla/5.0 (compatible; JustHodlAI-GBC/2.0)"
        BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36")
        tally = {"yahoo_engine_ua": 0, "yahoo_browser_ua": 0, "fmp": 0, "stooq": 0}
        stooq_map = {}
        for iso, sym in COUNTRIES:
            ye = yahoo_bars(sym, ENGINE_UA)
            time.sleep(0.25)
            yb = yahoo_bars(sym, BROWSER_UA, host="query2") if ye["status"] != 200 or ye["bars"] < 1000 else {"status": "skip", "bars": None, "last": None, "err": None}
            fm = fmp_bars(sym, fmp_key) if fmp_key else {"status": "nokey", "bars": 0, "last": None, "err": None}
            sq = {"status": None, "bars": 0, "last": None, "err": "no candidate"}
            for cand in STOOQ.get(iso, []):
                sq = stooq_bars(cand)
                time.sleep(0.2)
                if sq["bars"] >= 250:
                    sq["id"] = cand
                    stooq_map[iso] = cand
                    break
            if ye["status"] == 200 and ye["bars"] >= 250:
                tally["yahoo_engine_ua"] += 1
            if yb.get("status") == 200 and (yb.get("bars") or 0) >= 250:
                tally["yahoo_browser_ua"] += 1
            if fm["bars"] >= 250:
                tally["fmp"] += 1
            if sq["bars"] >= 250:
                tally["stooq"] += 1
            r.kv(s5=iso, sym=sym,
                 y_eng="%s/%s/%s" % (ye["status"], ye["bars"], ye["last"]), y_eng_err=(ye["err"] or "")[:60],
                 y_brw="%s/%s/%s" % (yb.get("status"), yb.get("bars"), yb.get("last")),
                 fmp="%s/%s/%s" % (fm["status"], fm["bars"], fm["last"]), fmp_err=(fm["err"] or "")[:50],
                 stooq="%s/%s/%s/%s" % (sq.get("id"), sq["status"], sq["bars"], sq["last"]), stooq_err=(sq["err"] or "")[:40])
            time.sleep(0.2)
        # fallbacks the engine already carries
        for iso, syms in FALLBACKS.items():
            for sym in syms:
                ye = yahoo_bars(sym, ENGINE_UA)
                r.kv(s5=iso + "-fb", sym=sym, y_eng="%s/%s/%s" % (ye["status"], ye["bars"], ye["last"]), y_eng_err=(ye["err"] or "")[:60])
                time.sleep(0.25)
        r.log("tally (>=250 bars): %s" % json.dumps(tally))
        r.log("stooq verified ids: %s" % json.dumps(stooq_map))
        verdict["tally"] = tally
        # in-house polygon-full ETF proxies: does the warehouse carry them?
        try:
            pfx = "data/warm/polygon-full/grouped/"
            keys = []
            tok = None
            while True:
                kw = dict(Bucket=B, Prefix=pfx + str(now.year) + "/")
                if tok:
                    kw["ContinuationToken"] = tok
                pg = s3.list_objects_v2(**kw)
                keys += [o["Key"] for o in pg.get("Contents", [])]
                tok = pg.get("NextContinuationToken")
                if not tok:
                    break
            keys.sort()
            r.log("polygon-full grouped sessions this year: %d, latest %s" % (len(keys), keys[-1] if keys else None))
            if keys:
                body = s3.get_object(Bucket=B, Key=keys[-1])["Body"].read()
                doc = json.loads(gzip.decompress(body))
                rows = doc.get("results") if isinstance(doc, dict) else doc
                have = {row.get("T") for row in (rows or []) if isinstance(row, dict)}
                etfs = {iso: t for iso, t in ETF.items() if t}
                missing = [iso + ":" + t for iso, t in etfs.items() if t not in have]
                r.log("ETF proxies present in latest session: %d/%d; missing: %s" % (len(etfs) - len(missing), len(etfs), missing or "none"))
                r.log("latest session doc keys: %s rows=%d" % (list(doc.keys())[:8] if isinstance(doc, dict) else "list", len(rows or [])))
        except Exception as e:  # noqa: BLE001
            r.warn("polygon-full probe: %s" % str(e)[:160])

        # ── S6 real engine run ─────────────────────────────────────────
        r.section("S6 async engine run, verified on disk")
        prev_gen = (live or {}).get("generated_at") or ""
        t0 = datetime.now(timezone.utc)
        try:
            lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
            r.log("async invoke fired at %s (prev generated_at=%s)" % (t0.isoformat(timespec="seconds"), prev_gen or "none"))
        except Exception as e:  # noqa: BLE001
            r.fail("invoke: %s" % str(e)[:160])
        fresh = None
        for i in range(96):  # 16 min
            time.sleep(10)
            cur, _ = s3_json(LIVE_KEY)
            g = (cur or {}).get("generated_at") or ""
            if cur and g > prev_gen and g >= t0.isoformat(timespec="seconds")[:16]:
                fresh = cur
                break
            if i % 12 == 11:
                r.log("  poll %ds: not fresh yet" % ((i + 1) * 10))
        r.section("S6 log tail of this run")
        for ln in log_tail(FN, n_streams=1, limit=400, since_ms=int(t0.timestamp() * 1000) - 5000):
            r.log("  LOG " + ln)
        if not fresh:
            r.fail("engine did not write a fresh %s within 16 min" % LIVE_KEY)
            r.log("VERDICT %s" % json.dumps(verdict, default=str))
            sys.exit(1)
        bc = fresh.get("by_country") or {}
        with_data = [k for k, v in bc.items() if (v or {}).get("phase") not in (None, "UNKNOWN")]
        r.kv(s6="fresh_run", generated_at=fresh.get("generated_at"), elapsed_sec=fresh.get("elapsed_sec"),
             fresh=fresh.get("countries_with_fresh_data"), with_data=len(with_data), total=len(bc),
             global_phase=(fresh.get("aggregate") or {}).get("global_phase"))
        hist2, hlm2 = s3_json(HIST_KEY)
        r.kv(s6="history_after_run", last_modified=str(hlm2), generated_at=(hist2 or {}).get("generated_at"))
        verdict["fresh_with_data"] = len(with_data)
        r.log("VERDICT %s" % json.dumps(verdict, default=str))
        if len(with_data) < 30:
            r.fail("fresh feed has only %d/34 countries with data -- engine source chain is broken" % len(with_data))
            sys.exit(1)
        r.ok("engine produced a fresh feed with %d/34 countries" % len(with_data))


if __name__ == "__main__":
    main()
