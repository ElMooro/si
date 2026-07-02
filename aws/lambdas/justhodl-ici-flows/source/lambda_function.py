"""justhodl-ici-flows — Fund Flows & Dry Powder (ICI weekly).

THE GAP THIS FILLS
══════════════════
The fleet's flow stack is fast-money only (ETF Global daily creations, dark
pool, options). The SLOW money — mutual-fund flows by class and MONEY MARKET
FUND assets (the $7T "cash on the sidelines" every strategist quotes) — had no
live source: FRED's weekly MMF series (WRMFSL/WIMFSL) died in the 2021 H.6
revamp (crisis-canaries still queries the corpses), leaving only quarterly Z.1.
ICI publishes both weekly, free.

SOURCES (runner-probed + S3-seeded by ops so Lambda-egress blocks can't blank
the layers; engine live-refresh is best-effort on top of stored history):
  • ici.org/research/stats/mmf      — weekly MMF assets: Total/Govt/Prime/
      Tax-exempt + Retail/Institutional ($B)
  • ici.org/research/stats/combined — weekly Combined Estimated Long-Term
      Fund Flows + ETF Net Issuance by class ($M)

INSTITUTIONAL READ (contrarian flow doctrine, Baker-Wurgler lineage):
  equity-fund capitulation WHILE money-market assets surge = dry-powder hoard
  = forward fuel (+); dry powder draining INTO equity-fund chasing = late-cycle
  deployment (−). z-scores vs own accumulated history; PROVISIONAL (<60 wk)
  clamps |signal| ≤ 1. Output: data/ici-flows.json → signal-board
  "Fund Flows (ICI)" + ici-flows.html. Cron WED,THU 16:30 UTC (ICI release
  days; upsert-idempotent).
"""
import io, json, re, time, zipfile, statistics, urllib.request
from datetime import datetime, timezone, timedelta
import boto3

BUCKET, OUT_KEY = "justhodl-dashboard-live", "data/ici-flows.json"
H_MMF, H_LTF = "data/history/ici-mmf.json", "data/history/ici-flows.json"
# ICI redesigned (ops 2709: old /research/stats/* paths 404 / JS-shell) — we
# now DISCOVER sources: harvest hub pages + sitemap for data files, score by
# tokens, try files (xls/csv) then pages-with-tables. Log-rich by design.
SEEDS = ["https://www.ici.org/research/stats",
         "https://www.ici.org/research/stats/mmf",
         "https://www.ici.org/statistical-report",
         "https://www.ici.org/research",
         "https://www.ici.org/sitemap.xml"]
TOKENS = {"mmf": ("money", "mmf", "money-market", "mm_"),
          "ltf": ("combined", "flow", "weekly", "long-term", "longterm", "trends")}
s3 = boto3.client("s3", region_name="us-east-1")
UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) jh/1"}


def _get(url, timeout=30, binary=False):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        b = r.read()
    return b if binary else b.decode("utf-8", "ignore")


def _gj(key, default=None):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return default


def _pj(key, body):
    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(body, separators=(",", ":"),
                  default=str).encode(), ContentType="application/json",
                  CacheControl="public, max-age=1800")


def _xlsx_rows(blob):
    """Coordinate-aware xlsx reader (r-attr; doc-order shifts on empty cells)."""
    z = zipfile.ZipFile(io.BytesIO(blob))
    shared = []
    if "xl/sharedStrings.xml" in z.namelist():
        sx = z.read("xl/sharedStrings.xml").decode("utf-8", "ignore")
        shared = [re.sub(r"<[^>]+>", "", m) for m in re.findall(r"<si>(.*?)</si>", sx, re.S)]
    out = []
    for sheet in [n for n in z.namelist() if re.match(r"xl/worksheets/sheet\d+\.xml", n)][:2]:
        xml = z.read(sheet).decode("utf-8", "ignore")
        for rxml in re.findall(r"<row[^>]*>(.*?)</row>", xml, re.S):
            row = {}
            for ref, ct, cv in re.findall(
                    r'<c[^>]*?r="([A-Z]+)\d+"[^>]*?(?:t="(\w+)")?[^>]*>.*?<v>(.*?)</v>', rxml, re.S):
                if ct == "s":
                    try:
                        cv = shared[int(cv)]
                    except Exception:
                        pass
                c = 0
                for ch in ref:
                    c = c * 26 + (ord(ch) - 64)
                row[c - 1] = cv
            if row:
                out.append([row.get(i, "") for i in range(max(row) + 1)])
    return out


def _cell_date(c):
    c = str(c).strip()
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(c, fmt).date().isoformat()
        except Exception:
            pass
    try:
        n = float(c)
        if 30000 < n < 60000:
            return (datetime(1899, 12, 30) + timedelta(days=n)).date().isoformat()
    except Exception:
        pass
    return None


def _num(c):
    c = str(c).strip().replace(",", "").replace("$", "")
    neg = c.startswith("(") and c.endswith(")")
    c = c.strip("()")
    try:
        v = float(c)
        return -v if neg else v
    except Exception:
        return None


def _abs(u):
    return u if u.startswith("http") else "https://www.ici.org" + (u if u.startswith("/") else "/" + u)


def _hrefs(html):
    return re.findall(r'(?:href="|<loc>)([^"<]+)', html, re.I)


def _discover(kind):
    """Harvest hubs + sitemap; return ordered candidate URLs (files first)."""
    toks = TOKENS[kind]
    files, pages, seen = [], [], set()
    frontier = list(SEEDS)
    for depth in range(2):
        nxt = []
        for url in frontier[:14]:
            try:
                html = _get(url, timeout=20)
            except Exception as e:
                print("  [%s] seed %s: %s" % (kind, url[:60], str(e)[:50]))
                continue
            for h in _hrefs(html):
                u = _abs(h.split("?")[0]) if "ici.org" in _abs(h) else None
                if not u or u in seen:
                    continue
                seen.add(u)
                low = u.lower()
                if low.endswith("sitemap.xml") and depth == 0:
                    nxt.append(u)
                elif re.search(r"\.(xlsx?|csv)$", low) and any(t in low for t in toks):
                    files.append(u)
                elif any(t in low for t in toks) and not re.search(r"\.(pdf|zip|png|jpg)$", low):
                    pages.append(u)
        frontier = nxt
    files = files[:8]
    pages = [p for p in pages if p not in files][:8]
    print("  [%s] discovered files=%d pages=%d" % (kind, len(files), len(pages)))
    for u in (files + pages)[:10]:
        print("    ·", u[:110])
    return files + pages


def _parse_blob(url, blob_or_text, want, binary):
    if binary:
        return _parse_table(_xlsx_rows(blob_or_text), want)
    if url.lower().endswith(".csv"):
        rows = [ln.split(",") for ln in blob_or_text.splitlines() if ln.strip()]
        return _parse_table(rows, want)
    rows = [[re.sub(r"<[^>]+>", " ", c).strip()
             for c in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, re.S)]
            for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", blob_or_text, re.S)]
    got = _parse_table(rows, want)
    if got:
        return got
    for h in _hrefs(blob_or_text):
        u = _abs(h.split("?")[0])
        if re.search(r"\.(xlsx?|csv)$", u.lower()):
            try:
                bin2 = u.lower().endswith((".xls", ".xlsx"))
                got = _parse_blob(u, _get(u, timeout=45, binary=bin2), want, bin2)
                if got:
                    print("    nested file hit:", u[:100])
                    return got
            except Exception:
                pass
    return {}


def _header_map(rows, want):
    """Locate header row + column indices via regex tokens; returns (start,dcol,{k:col})."""
    for ri, row in enumerate(rows[:25]):
        low = [str(c).strip().lower() for c in row]
        dcol = next((i for i, c in enumerate(low) if re.search(r"\bdate\b|week end", c)), None)
        cols = {}
        for k, pat in want.items():
            j = next((i for i, c in enumerate(low) if re.search(pat, c)), None)
            if j is not None:
                cols[k] = j
        if len(cols) >= max(2, len(want) - 2) and (dcol is not None or 0 not in cols.values()):
            return ri + 1, (dcol if dcol is not None else 0), cols
    return None, 0, {}


def _parse_table(rows, want):
    start, dcol, cols = _header_map(rows, want)
    out = {}
    if not cols:
        return out
    for row in rows[start:]:
        if dcol >= len(row):
            continue
        d = _cell_date(row[dcol])
        if not d:
            continue
        rec = {}
        for k, j in cols.items():
            if j < len(row):
                v = _num(row[j])
                if v is not None:
                    rec[k] = v
        if rec:
            out[d] = rec
    return out


MMF_WANT = {"total": r"^total(?!.*change)", "govt": r"govern", "prime": r"^prime",
            "retail": r"retail", "inst": r"^institution"}
LTF_WANT = {"eq_dom": r"domestic", "eq_world": r"world|foreign|internat",
            "hybrid": r"hybrid", "bond": r"(taxable )?bond(?!.*mun)",
            "muni": r"muni", "commodity": r"commodit", "total": r"^total"}


def _live(kind):
    want = MMF_WANT if kind == "mmf" else LTF_WANT
    for url in _discover(kind):
        try:
            binary = url.lower().endswith((".xls", ".xlsx"))
            got = _parse_blob(url, _get(url, timeout=45, binary=binary), want, binary)
            if len(got) >= 4:
                print("  [%s] parsed %d rows <- %s" % (kind, len(got), url[:100]))
                return got
        except Exception as e:
            print("  [%s] cand fail %s: %s" % (kind, url[:70], str(e)[:60]))
    return {}


def _norm_mmf(hist):
    """Normalize to $B (source sometimes $M)."""
    out = {}
    for d, r in hist.items():
        t = r.get("total")
        k = 0.001 if (t and t > 100_000) else 1.0
        out[d] = {a: round(v * k, 1) for a, v in r.items()}
    return out


def _z(series, cur, min_n=60):
    if len(series) < min_n:
        return None
    mu, sd = statistics.fmean(series), statistics.pstdev(series)
    return round((cur - mu) / sd, 2) if sd else None


def lambda_handler(event=None, context=None):
    t0 = time.time()
    # ── histories: stored (runner-seeded) + best-effort live refresh ──
    mmf_h = _gj(H_MMF, {}) or {}
    ltf_h = _gj(H_LTF, {}) or {}
    for kind, hk, store in (("mmf", H_MMF, mmf_h), ("ltf", H_LTF, ltf_h)):
        try:
            store.update(_live(kind))
        except Exception as e:
            print("  [%s] live err %s" % (kind, str(e)[:60]))
        if store:
            _pj(hk, dict(sorted(store.items())[-800:]))
    mmf_h = _norm_mmf(mmf_h)
    if not mmf_h or not ltf_h:
        raise RuntimeError("no ICI data (mmf=%d ltf=%d) — seed histories" % (len(mmf_h), len(ltf_h)))

    # ── MMF dry powder ──
    ms = sorted(mmf_h.items())
    md, m = ms[-1]
    tot = [r.get("total") for _, r in ms if r.get("total") is not None]
    d13 = round(tot[-1] - tot[-14], 1) if len(tot) > 13 else None
    z13 = _z([tot[i] - tot[i - 13] for i in range(13, len(tot))], d13) if d13 is not None else None
    yoy = round(100 * (tot[-1] / tot[-53] - 1), 1) if len(tot) > 52 else None
    mmf = {"date": md, "total_b": m.get("total"),
           "govt_b": m.get("govt"), "prime_b": m.get("prime"),
           "retail_b": m.get("retail"), "inst_b": m.get("inst"),
           "govt_share_pct": round(100 * m["govt"] / m["total"], 1) if m.get("govt") and m.get("total") else None,
           "wow_b": round(tot[-1] - tot[-2], 1) if len(tot) > 1 else None,
           "chg_13w_b": d13, "z_13w": z13, "yoy_pct": yoy, "weeks_n": len(ms),
           "history": [{"date": d, "value": r.get("total")} for d, r in ms[-520:]]}

    # ── long-term flows by class ($M weekly) ──
    ls = sorted(ltf_h.items())
    classes = {}
    for k in ("eq_dom", "eq_world", "hybrid", "bond", "muni", "commodity", "total"):
        vals = [(d, r[k]) for d, r in ls if r.get(k) is not None]
        if not vals:
            continue
        v = [x for _, x in vals]
        s4 = round(sum(v[-4:]), 0)
        s4_hist = [sum(v[i - 4:i]) for i in range(4, len(v))]
        classes[k] = {"latest_w_m": round(v[-1], 0), "sum_4w_m": s4,
                      "z_4w": _z(s4_hist, s4), "date": vals[-1][0], "weeks_n": len(v)}
    eq4 = (classes.get("eq_dom", {}).get("sum_4w_m") or 0) + (classes.get("eq_world", {}).get("sum_4w_m") or 0)
    eqz = [z for z in (classes.get("eq_dom", {}).get("z_4w"), classes.get("eq_world", {}).get("z_4w")) if z is not None]
    eq_z = round(statistics.fmean(eqz), 2) if eqz else None
    bd_z = classes.get("bond", {}).get("z_4w")
    rot = round(eq_z - bd_z, 2) if eq_z is not None and bd_z is not None else None

    provisional = mmf["weeks_n"] < 60 or min((c["weeks_n"] for c in classes.values()), default=0) < 60
    ez, mz = eq_z if eq_z is not None else 0, z13 if z13 is not None else 0
    if ez <= -1.5 and mz >= 1.0:
        regime, sig = "CAPITULATION_HOARD", 2
    elif mz <= -1.5 and ez >= 1.5:
        regime, sig = "FULL_DEPLOYMENT", -2
    elif ez >= 1.0 and mz <= -0.5:
        regime, sig = "CHASE", -1
    elif ez <= -0.75 or mz >= 0.75:
        regime, sig = "DEFENSIVE_ROTATION", 1
    else:
        regime, sig = "NEUTRAL", 0
    if provisional:
        sig = max(-1, min(1, sig))

    doc = {"version": "1.0.0", "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
           "source": "ICI weekly (ici.org): MMF assets + combined long-term fund flows & ETF net issuance",
           "mmf": mmf,
           "long_term": {"classes": classes, "equity_sum_4w_m": round(eq4, 0),
                         "equity_z_4w": eq_z, "eq_minus_bond_4w_z": rot},
           "regime": regime, "signal": sig, "provisional": provisional,
           "method": ("Contrarian flow doctrine: equity-fund capitulation while MMF assets surge "
                      "= dry-powder hoard (+); MMF drain into equity chasing = late deployment (−). "
                      "z vs own accumulated weekly history; provisional (<60w) clamps |signal|<=1. "
                      "Slow-money complement to the ETF Global daily-flow stack."),
           "duration_s": round(time.time() - t0, 1)}
    _pj(OUT_KEY, doc)
    print("  wrote %s mmf=$%.0fB z13=%s eq4w=$%.0fM eqZ=%s -> %s sig=%+d n(m/l)=%d/%d%s"
          % (OUT_KEY, mmf["total_b"] or 0, z13, eq4, eq_z, regime, sig,
             mmf["weeks_n"], len(ls), " PROV" if provisional else ""))
    return {"ok": True, "regime": regime, "signal": sig, "mmf_total_b": mmf["total_b"],
            "equity_4w_m": eq4, "provisional": provisional}
