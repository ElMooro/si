"""ICI weekly releases through a real browser (2026-11-11).

ici.org sits behind an Akamai bot wall that answers 403 to every datacenter HTTP client: the justhodl-ici-flows Lambda
never acquired a release after 2026-09-26 (its request journals: 403 on every scheduled run) and was switched off on
2026-10-20 as a broken writer. Headless Chrome on a GitHub runner is let through (ops 6781: all three release pages
HTTP 200, both native parsers green, every reconciliation identity within rounding). This lane is the writer now:

  1. acquires the three weekly release pages with Chrome - the exact main-document bytes, status and final URL;
  2. hands /mmf and /combined_flows to the NATIVE engine unchanged (ici_store.run with the browser transport in
     place of its urllib acquire): retained private originals, sealed packet, independent replay, CAS publication
     of data/ici-flows.json (ici-research.v1);
  3. parses /flows (Estimated Long-Term Mutual Fund Flows, millions of dollars) with the same strict table reader
     and merges the weeks into data/ici-fund-flows.json - the Inflows & outflows desk feed - together with the
     combined (mutual funds + ETF net issuance) and money-market observations as accumulating weekly histories.

Every number is a cell of an ICI release table; a week that ICI revises is replaced in place (fresh wins). Nothing is
estimated or carried forward. The Lambda trigger stays DISABLED by design: it cannot pass the wall.
"""
from contextlib import contextmanager
from datetime import date, datetime, timezone
from pathlib import Path
import hashlib
import json
import re
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "aws" / "lambdas" / "justhodl-ici-flows" / "source")]
import ici_research as research  # noqa: E402
import ici_store as store  # noqa: E402

LIVE = "justhodl-dashboard-live"
DESK_KEY = "data/ici-fund-flows.json"
ENGINE = "ici-flows-lane"
FLOWS_URL = "https://www.ici.org/research/stats/flows"
MF_LABELS = [("Total equity", "equity"), ("Domestic", "equity_domestic"), ("World", "equity_world"), ("Hybrid", "hybrid"),
             ("Total bond", "bond"), ("Taxable", "bond_taxable"), ("Municipal", "bond_municipal"), ("Total", "total")]
MF_UNIT_TEXT = "Millions of dollars"
COMBINED_FIELDS = [series for _, series in research.FLOW]
MMF_FIELDS = ["total", "government", "prime", "tax_exempt", "retail", "institutional"]
SEED_SOURCE = "seed:86w(2023-02-15..2026-09-23)"
KINDS = ("flows", "combined_flows", "mmf")
URLS = {"flows": FLOWS_URL, "combined_flows": research.SOURCES["combined_flows"]["url"], "mmf": research.SOURCES["mmf"]["url"]}


def now():
    return datetime.now(timezone.utc).isoformat()


def parse_mutual_fund_flows(raw, acquired_at):
    """The /research/stats/flows table (Estimated Long-Term Mutual Fund Flows), as strictly as the native parser reads
    its two sources: one unambiguous release date, one uniquely identified classification table, a declared unit,
    dated observation columns only, full-width rows, reviewed numerals."""
    parser = research.Tables().finish(raw)
    text = research.clean(" ".join(parser.text))
    clock = research.stamp(acquired_at)
    dates = set(re.findall(r"Washington,?\s+DC;?\s+([A-Z][a-z]+ \d{1,2}, \d{4})", text))
    if len(dates) != 1:
        raise ValueError("One unambiguous publisher release date required")
    release = datetime.strptime(next(iter(dates)), "%B %d, %Y").date()
    if release > clock.date():
        raise ValueError("Future source release")
    wanted = [label.casefold() for label, _ in MF_LABELS]
    candidates = [t for t in parser.tables if len(t["rows"]) > 1 and [research.clean(r[0]).casefold() if r else "" for r in t["rows"][1:]] == wanted]
    if len(candidates) != 1:
        raise ValueError("Complete uniquely identified classification table required")
    table = candidates[0]
    rows = table["rows"]
    header = rows[0]
    context = table["prefix"] + " " + research.clean(" ".join(table["text"]))
    if MF_UNIT_TEXT.casefold() not in context.casefold():
        raise ValueError("Source unit declaration missing")
    if not header or header[0] not in ("", " "):
        raise ValueError("Reviewed observation header required")
    columns = []
    for column, label in enumerate(header[1:], 1):
        day = research.observation(label)
        if not day:
            raise ValueError("Unknown observation column")
        if date.fromisoformat(day) > release:
            raise ValueError("Observation after release date")
        columns.append((column, day))
    if not columns or len({d for _, d in columns}) != len(columns):
        raise ValueError("Unique dated observations required")
    weeks = {}
    for index, (_, series) in enumerate(MF_LABELS, 1):
        row = rows[index]
        if len(row) != len(header):
            raise ValueError("Source row width differs")
        for column, day in columns:
            value = research.number(row[column])
            weeks.setdefault(day, {})[series] = float(value) if value is not None else None
    return {"source_id": "ICI:flows", "url": FLOWS_URL, "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest(),
            "acquired_at": acquired_at, "release_date": release.isoformat(), "observation_dates": sorted(weeks),
            "unit": "usd_mn", "weeks": weeks}


def native_observations(parsed, fields):
    """{date: {series: value}} from a native parse_release packet, for the desk feed's accumulating histories"""
    out = {}
    for obs in parsed["observations"]:
        if obs["series"] in fields:
            out.setdefault(obs["date"], {})[obs["series"]] = obs["value"]
    return out


def merge_rows(existing, fresh, fields):
    """existing list of {week_ending,...} + fresh {date: {series: value}} -> sorted rows, fresh wins per week"""
    rows = {r["week_ending"]: dict(r) for r in (existing or []) if isinstance(r, dict) and r.get("week_ending")}
    for day, values in fresh.items():
        row = rows.setdefault(day, {"week_ending": day})
        for field in fields:
            if field in values:
                row[field] = values[field]
    return [rows[k] for k in sorted(rows)]


def desk_feed(existing, flows, combined, mmf, generated_at, run_id):
    """The Inflows & outflows desk feed: the seeded mutual-fund history extended by every acquired release, plus the
    combined and money-market tables as their own weekly histories and ETF net issuance as ICI's own identity
    (combined estimated long-term flows = mutual fund flows + ETF net issuance, both in millions of dollars)."""
    existing = existing if isinstance(existing, dict) else {}
    mf_fields = [series for _, series in MF_LABELS]
    history = merge_rows(existing.get("history"), flows["weeks"], mf_fields)
    combined_history = merge_rows(existing.get("combined_history"), native_observations(combined, COMBINED_FIELDS), COMBINED_FIELDS)
    mmf_history = merge_rows(existing.get("money_market_history"), native_observations(mmf, MMF_FIELDS), MMF_FIELDS)
    mf_by_week = {r["week_ending"]: r for r in history}
    etf = []
    for row in combined_history:
        mf = mf_by_week.get(row["week_ending"])
        if not mf:
            continue
        out = {"week_ending": row["week_ending"]}
        for field in ("equity", "equity_domestic", "equity_world", "hybrid", "bond", "bond_taxable", "bond_municipal", "total"):
            a, b = row.get(field), mf.get(field)
            out[field] = round(a - b, 3) if a is not None and b is not None else None
        etf.append(out)
    latest = dict(history[-1]) if history else {}
    latest_week = {"week_ending": latest.get("week_ending")}
    for field in mf_fields:
        latest_week[field] = latest.get(field)
    latest_week["money_market"] = mmf_history[-1].get("total") if mmf_history else None
    latest_week["money_market_week_ending"] = mmf_history[-1].get("week_ending") if mmf_history else None
    latest_week["etf_net_issuance"] = etf[-1].get("total") if etf else None
    latest_week["etf_net_issuance_week_ending"] = etf[-1].get("week_ending") if etf else None
    sources = [s for s in (existing.get("sources") or []) if isinstance(s, str) and not s.startswith("chrome-lane:")]
    if SEED_SOURCE not in sources and any(r["week_ending"] <= "2026-09-23" for r in history):
        sources.insert(0, SEED_SOURCE)
    sources.append("chrome-lane:%s:flows %dw,combined %dw,mmf %dw" % (flows["release_date"], len(flows["weeks"]), len(combined["observation_dates"]), len(mmf["observation_dates"])))
    return {
        "generated_at": generated_at, "engine": ENGINE, "latest_week": latest_week, "history": history, "count": len(history),
        "units": "millions of dollars", "combined_history": combined_history, "money_market_history": mmf_history,
        "money_market_units": "billions of dollars", "etf_net_issuance_history": etf,
        "definitions": {
            "history": "ICI Estimated Long-Term Mutual Fund Flows (weekly, $ millions): " + FLOWS_URL,
            "combined_history": "ICI Combined Estimated Long-Term Flows and ETF Net Issuance (weekly, $ millions): " + URLS["combined_flows"],
            "money_market_history": "ICI Money Market Fund Assets (weekly, $ billions): " + URLS["mmf"],
            "etf_net_issuance_history": "combined minus mutual fund flows for the same week, ICI's own identity; null where either side is missing"},
        "sources": sources, "notes": ["Weeks revise in place when ICI revises them (fresh release wins); seeded weeks before 2026-09-30 came from archived releases.",
                                      "Acquired with headless Chrome on GitHub Actions (ici.org refuses datacenter HTTP clients); the Lambda trigger stays disabled."],
        "lane": {"run_id": run_id, "acquired_at": flows["acquired_at"], "release_dates": {"flows": flows["release_date"], "combined_flows": combined["release_date"], "mmf": mmf["release_date"]},
                 "originals_sha256": {"flows": flows["sha256"], "combined_flows": combined["sha256"], "mmf": mmf["sha256"]}},
    }


@contextmanager
def browser_transport(originals):
    """the native store's acquire(kind) answered from the browser-acquired originals, restored afterwards"""
    previous = store.acquire

    def acquire(kind):
        if kind not in originals:
            raise ValueError("Browser original unavailable for " + kind)
        return originals[kind]
    store.acquire = acquire
    try:
        yield
    finally:
        store.acquire = previous


def parse_check(kind, raw, acquired_at):
    """raises when the bytes do not carry the kind's release table"""
    if kind == "flows":
        parse_mutual_fund_flows(raw, acquired_at)
    else:
        research.parse_release(raw, kind, acquired_at)


def acquire_with_chrome(kinds=KINDS, log=print, playwright=None, check=parse_check):
    """kind -> (raw, source): the exact main-document bytes of each release page from a real headless Chrome; when
    the server bytes do not carry the table (assembled in the page), the rendered document is retained instead and
    the source says so"""
    if playwright is None:
        from playwright.sync_api import sync_playwright as playwright
    originals = {}
    with playwright() as p:
        try:
            browser = p.chromium.launch(channel="chrome", headless=True)
        except Exception:  # noqa: BLE001 - the runner image without Google Chrome falls back to Playwright's chromium
            browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1366, "height": 900}, locale="en-US")
        page = ctx.new_page()
        try:
            for kind in kinds:
                url = URLS[kind]
                response = page.goto(url, wait_until="domcontentloaded", timeout=60000)
                try:
                    page.wait_for_selector("table", timeout=15000)
                except Exception:  # noqa: BLE001
                    pass
                acquired = now()
                status = response.status if response else None
                raw = response.body() if response else b""
                headers = {k: response.headers.get(k.lower()) for k in ("Content-Type", "Content-Length", "ETag", "Last-Modified")} if response else {}
                if status != 200 or page.url != url or not raw:
                    log(json.dumps({"kind": kind, "http_status": status, "final_url": page.url[:80], "bytes": len(raw), "status": "refused"}))
                    raise ValueError("Official release unavailable through the browser")
                transport = "chrome"
                try:
                    check(kind, raw, acquired)
                except ValueError:
                    rendered = page.content().encode("utf-8")
                    check(kind, rendered, acquired)
                    raw, transport = rendered, "chrome-rendered-document"
                originals[kind] = (raw, {"url": url, "http_status": status, "acquired_at": acquired, "response_headers": headers, "transport": transport})
                log(json.dumps({"kind": kind, "http_status": status, "bytes": len(raw), "acquired_at": acquired, "transport": transport}))
        finally:
            ctx.close()
            browser.close()
    return originals


def read_json(client, bucket, key):
    try:
        return json.loads(client.get_object(Bucket=bucket, Key=key)["Body"].read())
    except Exception as exc:  # noqa: BLE001
        if store.missing(exc):
            return None
        raise


def run(client, bucket, run_id, acquire=acquire_with_chrome, clock=now, log=print):
    """one lane run: acquire, native publication, desk feed; returns the summary that the workflow prints"""
    summary = {"run_id": run_id, "started_at": clock(), "native": None, "desk": None}
    originals = acquire(KINDS, log)
    parsed = {}
    for kind in ("combined_flows", "mmf"):
        raw, source = originals[kind]
        parsed[kind] = research.parse_release(raw, kind, source["acquired_at"])
    raw, source = originals["flows"]
    flows = parse_mutual_fund_flows(raw, source["acquired_at"])
    log(json.dumps({"parsed": {k: {"release_date": v["release_date"], "dates": v["observation_dates"]} for k, v in parsed.items()},
                    "flows": {"release_date": flows["release_date"], "dates": flows["observation_dates"]}}))
    try:
        with browser_transport({k: originals[k] for k in ("combined_flows", "mmf")}):
            result = store.run(client, bucket, "chrome-lane:" + str(run_id))
        summary["native"] = {"published": result.get("published"), "generated_at": result.get("generated_at"),
                             "provider_requests": result.get("provider_requests"), "status": result.get("status")}
    except Exception as exc:  # noqa: BLE001 - the native packet is reported, the desk feed still proceeds
        summary["native"] = {"published": False, "error_type": type(exc).__name__}
    existing = read_json(client, bucket, DESK_KEY)
    feed = desk_feed(existing, flows, parsed["combined_flows"], parsed["mmf"], clock(), run_id)
    body = json.dumps(feed, separators=(",", ":"), allow_nan=False).encode()
    client.put_object(Bucket=bucket, Key=DESK_KEY, Body=body, ContentType="application/json", CacheControl="no-store")
    before = {r["week_ending"] for r in (existing or {}).get("history", []) if isinstance(r, dict)} if existing else set()
    summary["desk"] = {"key": DESK_KEY, "weeks": feed["count"], "latest_week": feed["latest_week"]["week_ending"],
                       "new_weeks": sorted(set(flows["weeks"]) - before), "combined_weeks": len(feed["combined_history"]),
                       "money_market_weeks": len(feed["money_market_history"]), "bytes": len(body)}
    summary["finished_at"] = clock()
    return summary
