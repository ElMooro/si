"""justhodl-damodaran - Damodaran Online kept in the warehouse and read into tables. Once a day.

Why: no engine read his data, and one carried a fixed 5.50% equity risk premium under his name while his own page
said 4.41% (2026-11-01). Engines now read the premium he computes at the start of each month from the warehouse.

A run:
  1. reads his home page (this month's implied premiums, the date of his last full update) and his current-data page
     (every spreadsheet he lists);
  2. asks, for the few files that move during the year (the monthly premium, the country premiums, the two histories,
     the ratings table), "has it changed?" (ETag / Last-Modified) - and for all the others only once a week, when his
     page announces a new full update, or when a file is not in the bank yet;
  3. keeps every changed file verbatim under damodaran/raw/ and the copy it replaces under damodaran/raw/_superseded/
     (he overwrites last year's files each January: the bank keeps them);
  4. reads the files into three documents and one table per industry file, written only when their content changed.

Outputs (all name him and his file):
  data/equity-risk-premium.json     the premium today (five measures), monthly since 2008-09, annual since 1960,
                                    annual returns since 1928 and the premiums worked from them
  data/country-risk-premiums.json   equity and country risk premium, rating and default spread by country
  data/industry-benchmarks.json     cost of capital, margins, returns and multiples by industry and region, and the
                                    list of every table kept under data/warm/damodaran/tables/

No engine is started from here, no event is published, nothing but his site is called. {"dry_run": true} reads and
derives and writes nothing; {"only": ["^wacc"]} narrows a run to the files whose names match; {"force_derive": true}
rebuilds the documents from the bank.
"""
from datetime import datetime, timezone
import hashlib
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request

import dam_core as C

VERSION = C.VERSION
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
RAW_PREFIX = os.environ.get("RAW_PREFIX", "damodaran/raw/")
OUT_PREFIX = os.environ.get("OUT_PREFIX", "data/warm/damodaran/")
PREMIUM_KEY = os.environ.get("PREMIUM_KEY", "data/equity-risk-premium.json")
COUNTRY_KEY = os.environ.get("COUNTRY_KEY", "data/country-risk-premiums.json")
INDUSTRY_KEY = os.environ.get("INDUSTRY_KEY", "data/industry-benchmarks.json")
MANIFEST_KEY = OUT_PREFIX + "manifest.json"
HOME = C.SITE + "New_Home_Page/home.htm"
CURRENT = C.SITE + "New_Home_Page/datacurrent.html"
UA = "Mozilla/5.0 (compatible; JustHodl.AI research data bank; +https://justhodl.ai)"
SWEEP_DAYS = int(os.environ.get("SWEEP_DAYS", "7"))
BUDGET_SECONDS = int(os.environ.get("BUDGET_SECONDS", "420"))
MAX_BYTES = 40 * 1024 * 1024
PAUSE = 0.25
RUNS_KEPT = 60
FREQUENT = re.compile(r"^(ERPbymonth\.xlsx|ctryprem[A-Za-z0-9]*\.xlsx|histimpl\.xls|histretSP\.xlsx?|ratings\.xls)$")
PREMIUM_FILES = ("ERPbymonth.xlsx", "histimpl.xls", "histretSP.xls")
COUNTRY_FILE = re.compile(r"^ctryprem[A-Za-z0-9]*\.xlsx$")
NOT_INDUSTRY = re.compile(r"^(ERP|ctryprem|histimpl|histret|S&P500|Alldata|ratings|macro|countrytaxrates|countrystats|mktcap(risk|mult))", re.I)
MONTHS = {m: i for i, m in enumerate("january february march april may june july august september october november december".split(), 1)}


# ------------------------------------------------------------------------------------------------------- network
def fetch(url, etag=None, modified=None, timeout=45, tries=3):
    """(status, headers, body). 304 when the file has not changed since the ETag / Last-Modified given."""
    headers = {"User-Agent": UA, "Accept": "*/*"}
    if etag:
        headers["If-None-Match"] = etag
    if modified:
        headers["If-Modified-Since"] = modified
    last = None
    for attempt in range(tries):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=timeout) as resp:
                return resp.status, dict(resp.headers.items()), resp.read(MAX_BYTES + 1)
        except urllib.error.HTTPError as e:
            if e.code == 304:
                return 304, dict(e.headers.items()), b""
            if e.code < 500:
                raise
            last = e
        except Exception as e:                                        # a timeout, a reset: tried again
            last = e
        time.sleep(1.5 * (attempt + 1))
    raise last


def links(html, page_url):
    """{file name: address} of every spreadsheet a page links on his site."""
    out = {}
    for m in re.finditer(r"""href\s*=\s*(?:"([^"]+)"|'([^']+)'|([^\s>]+))""", html, re.I):
        href = (m.group(1) or m.group(2) or m.group(3) or "").strip()
        if not re.search(r"\.xlsx?$", href, re.I):
            continue
        full = urllib.parse.urljoin(page_url, href)
        parts = urllib.parse.urlsplit(full)
        if parts.netloc.lower() not in ("pages.stern.nyu.edu", "www.stern.nyu.edu", "people.stern.nyu.edu") or "adamodar" not in parts.path:
            continue
        path = urllib.parse.quote(urllib.parse.unquote(parts.path), safe="/~&+,;=()'")
        name = urllib.parse.unquote(parts.path).rsplit("/", 1)[-1]
        out.setdefault(name, "https://pages.stern.nyu.edu" + path)
    return out


def home_facts(html):
    """What his home page states in words: the implied premiums at the start of the month, and his last full update."""
    plain = re.sub(r"\s+", " ", re.sub(r"&amp;", "&", re.sub(r"<[^>]+>", " ", html)))
    best = None
    for m in re.finditer(r"Implied ERP on ([A-Za-z]+) (\d{1,2}), (\d{4})\s*=\s*(.{0,420})", plain):
        mon = MONTHS.get(m.group(1).lower())
        if not mon:
            continue
        day = "%04d-%02d-%02d" % (int(m.group(3)), mon, int(m.group(2)))
        vals = [{"pct": float(v), "label": lab.strip()} for v, lab in re.findall(r"(\d+(?:\.\d+)?)\s*%\s*\(([^)]{3,60})\)", m.group(4))][:5]
        tb = re.search(r"treasury rate of (\d+(?:\.\d+)?)\s*%", m.group(4))
        if vals and (best is None or day > best["date"]):
            best = {"date": day, "values": vals, "tbond_pct": float(tb.group(1)) if tb else None}
    up = re.search(r"latest overall data update was on ([A-Za-z]+ \d{1,2}, \d{4})", plain)
    return {"premium": best, "full_update": up.group(1) if up else None}


# ---------------------------------------------------------------------------------------------------------- plan
def due(files, manifest, now, full_update):
    """The files to ask about in this run, and why the whole list is (or is not) swept."""
    banked = manifest.get("files") or {}
    sweep = manifest.get("sweep") or {}
    why = None
    if not sweep.get("at"):
        why = "the bank has never been swept"
    elif full_update and sweep.get("full_update") != full_update:
        why = "his page announces a full update (%s)" % full_update
    else:
        try:
            age = (now - datetime.fromisoformat(sweep["at"])).total_seconds() / 86400.0
        except Exception:
            age = 1e9
        if age >= SWEEP_DAYS - 0.25:
            why = "%.0f days since the last sweep" % age
    names = sorted(files, key=lambda n: (0 if FREQUENT.match(n) else 1, n.lower()))
    ask = [n for n in names if why or FREQUENT.match(n) or n not in banked]
    return ask, why


def sha(body):
    return hashlib.sha256(body).hexdigest()


def stamp(http_date, fallback):
    try:
        from email.utils import parsedate_to_datetime
        return parsedate_to_datetime(http_date).strftime("%Y%m%d")
    except Exception:
        return fallback


# -------------------------------------------------------------------------------------------------------- derive
def signature(names, files):
    return sha(json.dumps([[n, (files.get(n) or {}).get("sha256")] for n in sorted(names)]).encode() + VERSION.encode())


def source_of(name, files):
    f = files.get(name) or {}
    return {"file": name, "url": f.get("url"), "last_modified": f.get("last_modified"), "bytes": f.get("bytes")}


def premium_document(raw_of, files, home, now_iso):
    monthly = C.erp_monthly(C.book(raw_of("ERPbymonth.xlsx")))
    last = monthly["rows"][-1]
    keys = [c["key"] for c in monthly["cols"]]
    col = {k: i for i, k in enumerate(keys)}
    labels = {c["key"]: c["label"] for c in monthly["cols"]}
    measures = []
    for key, label, _pat in C.MEASURES:
        j = monthly["measures"].get(key)
        if j is not None and last[j] is not None:
            measures.append({"key": key, "label": label, "his_heading": labels[keys[j]], "value": last[j], "pct": round(last[j] * 100, 2), "column": keys[j]})
    head = next((m for m in measures if m["key"] == C.HEADLINE), None)
    tbond = last[col["t_bond_rate"]] if "t_bond_rate" in col else None

    def back(months):
        y, m = int(last[0][:4]), int(last[0][5:7]) - months
        while m <= 0:
            y, m = y - 1, m + 12
        want = "%04d-%02d" % (y, m)
        row = next((r for r in monthly["rows"] if r[0][:7] == want), None)
        j = monthly["measures"].get(C.HEADLINE)
        return {"date": row[0], "value": row[j], "pct": round(row[j] * 100, 2)} if row and j is not None and row[j] is not None else None

    us = {"as_of": last[0], "measure": C.HEADLINE, "premium": head["value"] if head else None, "premium_pct": head["pct"] if head else None,
          "over": "the ten-year US Treasury bond rate", "tbond": tbond, "tbond_pct": round(tbond * 100, 2) if tbond is not None else None,
          "sp500": last[col["s_and_p_500"]] if "s_and_p_500" in col else None,
          "expected_return_pct": round((tbond + head["value"]) * 100, 2) if head and tbond is not None else None,
          "measures": measures, "a_year_before": back(12), "a_month_before": back(1)}
    page = (home or {}).get("premium")
    check = None
    if page and head:
        same_month = page["date"][:7] == last[0][:7]
        check = {"page_date": page["date"], "page_values_pct": [v["pct"] for v in page["values"]], "page_labels": [v["label"] for v in page["values"]],
                 "file_date": last[0], "file_values_pct": [m["pct"] for m in measures],
                 "agree": bool(same_month and abs(page["values"][0]["pct"] - head["pct"]) < 0.011) if same_month else None,
                 "note": None if same_month else "the page and the file are of different months"}
    annual = C.erp_annual(C.book(raw_of("histimpl.xls")))
    returns = C.returns_by_year(C.book(raw_of("histretSP.xls")))
    return {"version": VERSION, "generated_at": now_iso, "source": C.SOURCE, "attribution": C.ATTRIBUTION, "caution": C.CAUTION,
            "unit": "fractions as in his files (0.0441 is 4.41%); fields ending _pct are in percent",
            "us": us, "home_page": check,
            "monthly": {"from": monthly["first"], "to": monthly["last"], "n": len(monthly["rows"]), "months_his_file_skips": monthly["gaps"],
                        "cols": monthly["cols"], "rows": monthly["rows"],
                        "measure_columns": {k: keys[j] for k, j in monthly["measures"].items()}},
            "annual": {"from": annual["rows"][0][0] if annual["rows"] else None, "to": annual["rows"][-1][0] if annual["rows"] else None,
                       "n": len(annual["rows"]), "cols": annual["cols"], "rows": annual["rows"]},
            "returns": {"from": returns["rows"][0][0] if returns["rows"] else None, "to": returns["rows"][-1][0] if returns["rows"] else None,
                        "n": len(returns["rows"]), "updated": returns.get("updated"), "cols": returns["cols"], "rows": returns["rows"],
                        "premiums_worked_here": C.premiums_from_returns(returns)},
            "files": [source_of(n, files) for n in PREMIUM_FILES]}


def country_document(raw_of, files, now_iso):
    best = None
    for name in sorted(n for n in files if COUNTRY_FILE.match(n)):
        try:
            t = C.country_premiums(C.book(raw_of(name)))
        except Exception as e:
            print("[damodaran] %s could not be read as country premiums: %s" % (name, type(e).__name__))
            continue
        rank = (t.get("updated") or "", stamp((files.get(name) or {}).get("last_modified"), ""))
        if len(t["rows"]) >= 50 and (best is None or rank > best[0]):
            best = (rank, name, t)
    if best is None:
        raise ValueError("no country premium file could be read")
    _rank, name, t = best
    return {"version": VERSION, "generated_at": now_iso, "source": C.SOURCE, "attribution": C.ATTRIBUTION,
            "unit": "fractions as in his file (0.0423 is 4.23%)", "as_of": t.get("updated"), "file": name,
            "mature_market_premium": t["mature_market_premium"], "us_premium": t["us_premium"], "equity_to_bond_volatility": t["equity_to_bond_volatility"],
            "inputs": t["inputs"], "n": len(t["rows"]), "cols": t["cols"], "rows": t["rows"], "frontier": t.get("frontier"),
            "other_files": sorted(n for n in files if COUNTRY_FILE.match(n) and n != name), "files": [source_of(name, files)]}


def industry_documents(raw_of, files, now_iso):
    """(headline document, {table id: table document}, [files that hold no industry table])."""
    tables, by_region, index, none = {}, {}, [], []
    for name in sorted(files, key=str.lower):
        if NOT_INDUSTRY.match(name):
            continue
        try:
            t = C.industry_table(C.book(raw_of(name)))
        except Exception as e:
            none.append({"file": name, "why": "could not be read: %s" % type(e).__name__})
            continue
        if t is None:
            none.append({"file": name, "why": "no table opening with 'Industry Name'"})
            continue
        stem = name.rsplit(".", 1)[0]
        _base, code, label = C.region_of(stem)
        topic = C.topic_of(stem)
        tid = ("%s-%s" % (topic, code)).lower()
        if tid in tables:
            tid = ("%s-%s" % (re.sub(r"[^a-z0-9]+", "", stem.lower()), code)).lower()
        key = "%stables/%s.json" % (OUT_PREFIX, tid)
        tables[tid] = {"version": VERSION, "generated_at": now_iso, "id": tid, "topic": topic, "region": code, "region_label": label, "title": t["title"],
                       "updated": t["updated"], "source": C.SOURCE, "attribution": C.ATTRIBUTION, "caution": C.CAUTION, "file": source_of(name, files),
                       "sheet": t["sheet"], "n": len(t["rows"]), "cols": t["cols"], "rows": t["rows"]}
        by_region.setdefault(code, {"label": label, "tables": {}})["tables"][topic] = t
        index.append({"id": tid, "topic": topic, "region": code, "title": t["title"], "updated": t["updated"], "file": name, "n": len(t["rows"]),
                      "columns": len(t["cols"]), "key": key})
    order = ["US", "Global"] + [c for c in sorted(by_region) if c not in ("US", "Global")]
    bench, regions = {}, []
    for code in [c for c in order if c in by_region]:
        j = C.join_industries(by_region[code]["tables"])
        bench[code] = j
        regions.append({"code": code, "label": by_region[code]["label"], "tables": len(by_region[code]["tables"]), "industries": len(j["rows"]), "missing": j["missing"]})
    updated = sorted(x["updated"] for x in index if x["updated"])
    doc = {"version": VERSION, "generated_at": now_iso, "source": C.SOURCE, "attribution": C.ATTRIBUTION, "caution": C.CAUTION,
           "unit": "fractions as in his files (0.0802 is 8.02%); multiples are multiples", "as_of": updated[-1] if updated else None,
           "regions": regions, "benchmarks": bench, "n_tables": len(index), "tables": index, "not_tables": none}
    return doc, tables, none


def body_sha(doc):
    return sha(json.dumps({k: v for k, v in doc.items() if k != "generated_at"}, sort_keys=True, separators=(",", ":")).encode())


# ----------------------------------------------------------------------------------------------------------- run
def run(s3, bucket, dry_run=False, force=False, get=fetch, now=None, sleep=time.sleep, log=print, clock=time.time, budget=BUDGET_SECONDS, only=None):
    """One run. Returns (summary, documents) - the documents this run derived, written or not.
    `only` (patterns of file names) narrows what is asked about; such a run is never counted as a sweep."""
    started = clock()
    now = now or datetime.now(timezone.utc)
    now_iso = now.isoformat(timespec="seconds")
    try:
        manifest = json.loads(s3.get_object(Bucket=bucket, Key=MANIFEST_KEY)["Body"].read())
    except Exception:
        manifest = {}
    files = dict(manifest.get("files") or {})
    trouble, changed, unchanged, fresh, docs = [], [], 0, {}, {}

    listed, home = {}, {}
    for url in (HOME, CURRENT):
        try:
            _st, _hd, body = get(url)
            html = body.decode("latin-1", "replace")
            for name, addr in links(html, url).items():
                listed.setdefault(name, addr)
            if url == HOME:
                home = home_facts(html)
        except Exception as e:
            trouble.append("%s: %s" % (url.rsplit("/", 1)[-1], type(e).__name__))
        sleep(PAUSE)
    if not listed:
        summary = {"ok": False, "version": VERSION, "dry_run": dry_run, "error": "neither page listed a file", "trouble": trouble}
        log("[damodaran] %s" % json.dumps(summary))
        return summary, docs

    ask, why = due(listed, manifest, now, home.get("full_update"))
    if only:
        pats = [re.compile(p) for p in only]
        ask = [n for n in ask if any(p.search(n) for p in pats)]
    log("[damodaran] %d files listed, %d in the bank, %d asked about (%s)" % (len(listed), len(files), len(ask), why or "the files that move, and any not yet banked"))
    stopped, new_bytes = None, 0
    for name in ask:
        if clock() - started > budget:
            stopped = "the run's time ran out after %d of %d files; the rest wait for the next run" % (ask.index(name), len(ask))
            break
        old = files.get(name) or {}
        try:
            st, hd, body = get(listed[name], None if dry_run and not old else old.get("etag"), None if dry_run and not old else old.get("last_modified"))
        except Exception as e:
            trouble.append("%s: %s" % (name, "HTTP %s" % e.code if isinstance(e, urllib.error.HTTPError) else type(e).__name__))
            sleep(PAUSE)
            continue
        sleep(PAUSE)
        if st == 304:
            unchanged += 1
            continue
        if len(body) > MAX_BYTES:
            trouble.append("%s: larger than %d MB, not banked" % (name, MAX_BYTES // 1048576))
            continue
        if body[:2] != b"PK" and body[:4] != b"\xd0\xcf\x11\xe0":
            trouble.append("%s: not a spreadsheet (%d bytes)" % (name, len(body)))
            continue
        digest = sha(body)
        fresh[name] = body
        if old.get("sha256") == digest:
            unchanged += 1
            files[name] = dict(old, etag=hd.get("ETag"), last_modified=hd.get("Last-Modified"))
            continue
        rec = {"url": listed[name], "bytes": len(body), "etag": hd.get("ETag"), "last_modified": hd.get("Last-Modified"), "sha256": digest,
               "banked_at": now_iso, "superseded": list(old.get("superseded") or [])}
        if old and not dry_run:
            kept = "%s_superseded/%s.%s" % (RAW_PREFIX, name, stamp(old.get("last_modified"), (old.get("banked_at") or now_iso)[:10].replace("-", "")))
            try:
                s3.copy_object(Bucket=bucket, Key=kept, CopySource={"Bucket": bucket, "Key": RAW_PREFIX + name})
                rec["superseded"].append({"key": kept, "sha256": old.get("sha256"), "last_modified": old.get("last_modified"), "replaced_at": now_iso})
            except Exception as e:
                trouble.append("%s: the copy it replaces could not be kept (%s); the new file was not banked" % (name, type(e).__name__))
                continue
        if not dry_run:
            s3.put_object(Bucket=bucket, Key=RAW_PREFIX + name, Body=body, ContentType="application/octet-stream")
        files[name] = rec
        changed.append(name)
        new_bytes += len(body)
    swept = bool(why) and not stopped and not only

    def raw_of(name):
        if name in fresh:
            return fresh[name]
        return s3.get_object(Bucket=bucket, Key=RAW_PREFIX + name)["Body"].read()

    derived = dict(manifest.get("derived") or {})
    wrote, groups = [], {}
    have = set(files)
    plan = [("premium", [n for n in PREMIUM_FILES if n in have], lambda: {PREMIUM_KEY: premium_document(raw_of, files, home, now_iso)}),
            ("country", sorted(n for n in have if COUNTRY_FILE.match(n)), lambda: {COUNTRY_KEY: country_document(raw_of, files, now_iso)}),
            ("industry", sorted(n for n in have if not NOT_INDUSTRY.match(n)), None)]
    for group, names, make in plan:
        sig = signature(names, files)
        need = force or (derived.get(group) or {}).get("signature") != sig
        if group == "premium" and len(names) < len(PREMIUM_FILES):
            trouble.append("the premium document waits: %s not in the bank" % ", ".join(n for n in PREMIUM_FILES if n not in have))
            continue
        if not names or not need:
            groups[group] = "unchanged"
            continue
        try:
            if group == "industry":
                doc, tables, none = industry_documents(raw_of, files, now_iso)
                out = {INDUSTRY_KEY: doc}
                out.update({"%stables/%s.json" % (OUT_PREFIX, tid): t for tid, t in tables.items()})
            else:
                out = make()
        except Exception as e:
            trouble.append("%s documents: %s: %s" % (group, type(e).__name__, str(e)[:160]))
            groups[group] = "failed"
            continue
        shas = dict((derived.get(group) or {}).get("documents") or {})
        for key, doc in out.items():
            docs[key] = doc
            h = body_sha(doc)
            if shas.get(key) == h and not force:
                continue
            if not dry_run:
                s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(doc, separators=(",", ":")).encode(), ContentType="application/json",
                              CacheControl="public, max-age=300")
            shas[key] = h
            wrote.append(key)
        derived[group] = {"signature": sig, "documents": shas, "at": now_iso}
        groups[group] = "derived"

    prem = (docs.get(PREMIUM_KEY) or {}).get("us") or {}
    try:
        import resource
        peak = round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0)
    except Exception:
        peak = None
    summary = {"ok": not any(v == "failed" for v in groups.values()) and bool(files), "version": VERSION, "dry_run": dry_run, "at": now_iso,
               "seconds": round(clock() - started, 1), "listed": len(listed), "asked": len(ask), "sweep": why, "swept": swept, "stopped": stopped,
               "changed": changed, "unchanged": unchanged, "banked_bytes": new_bytes, "in_bank": len(files), "groups": groups,
               "wrote": len(wrote), "wrote_keys": wrote[:12], "trouble": trouble[:20], "peak_memory_mb": peak,
               "premium": {"as_of": prem.get("as_of"), "pct": prem.get("premium_pct")} if prem else None,
               "home_page": home.get("premium"), "full_update": home.get("full_update")}
    if not dry_run:
        runs = list(manifest.get("runs") or [])[-(RUNS_KEPT - 1):] + [{k: summary[k] for k in ("at", "seconds", "asked", "sweep", "swept", "stopped", "changed", "unchanged", "banked_bytes", "wrote", "groups", "trouble", "peak_memory_mb")}]
        sweep = {"at": now_iso, "full_update": home.get("full_update")} if swept else (manifest.get("sweep") or {})
        s3.put_object(Bucket=bucket, Key=MANIFEST_KEY, ContentType="application/json", CacheControl="no-cache",
                      Body=json.dumps({"version": VERSION, "written_at": now_iso, "source": C.SOURCE, "site": C.SITE, "raw_prefix": RAW_PREFIX,
                                       "files": files, "sweep": sweep, "derived": derived, "home": home, "runs": runs}, separators=(",", ":")).encode())
    log("[damodaran] %s" % json.dumps({k: v for k, v in summary.items() if k not in ("wrote_keys",)})[:1800])
    return summary, docs


def lambda_handler(event=None, context=None):
    import boto3
    event = event if isinstance(event, dict) else {}
    only = event.get("only") if isinstance(event.get("only"), list) and all(isinstance(x, str) for x in event["only"]) else None
    summary, _docs = run(boto3.client("s3"), BUCKET, dry_run=bool(event.get("dry_run")), force=bool(event.get("force_derive")), only=only)
    return summary
