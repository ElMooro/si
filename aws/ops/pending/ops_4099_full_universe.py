"""ops_4099 — every TradingView indicator on tradingview.html + cftc diag.

Khalid: "make sure every single tradingview indicator is here". The vault
carried 1,358 of the 10,319 symbols he imported, because its registry came
only from brain-tagged notes plus aliases. v3.14.0 admits EVERY imported
symbol as a row so the page mirrors TradingView.

Resolution stays bounded (RESOLVE_BUDGET/run) and un-attempted rows are
marked PENDING_RESOLUTION rather than NO_FREE_SOURCE — the latter would be
a claim we have not earned AND would trip the 27-day retry gate, freezing
those symbols out for a month.

Also diagnoses the cftc rows, which still read NO_FREE_SOURCE on the live
page despite 65 verified aliases. Two candidate causes and I am measuring
rather than picking: the alias key may not match the vault's symbol form,
or the CFTC call itself may be failing. The op tests both directly.
"""
import io, json, sys, time, urllib.parse, urllib.request, zipfile as zf
from collections import Counter
from pathlib import Path
import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=120, retries={"max_attempts": 0}))
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-tradingview"
MARK = "tradingview-vault v3.14.1 ops4099 wallclock-guard"


def gj(k, d=None):
    try: return json.loads(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read())
    except Exception: return d


def main():
    with report("4099_full_universe") as rep:
        rep.heading("ops 4098 — full TradingView universe on the vault page")
        checks = []

        # ── cftc diagnosis FIRST: no point shipping on a wrong theory ──
        rep.section("A. why are the cftc rows still NO_FREE_SOURCE?")
        sa = gj("data/symbol-aliases.json", {}) or {}
        aliases = sa.get("aliases") or {}
        cot_keys = [k for k, r in (sa.get("detail") or {}).items()
                    if r.get("route") == "cot-verified"]
        rep.log(f"  cot aliases in artifact: {len(cot_keys)}")
        v = gj("data/tradingview.json", {}) or {}
        vsyms = {r.get("symbol") for r in (v.get("symbols") or [])}
        matched = [k for k in cot_keys if k in vsyms]
        rep.log(f"  cot alias keys that match a vault symbol EXACTLY: {len(matched)}")
        rep.log(f"  sample alias key : {cot_keys[0] if cot_keys else '(none)'}")
        rep.log(f"  in aliases map   : {cot_keys[0] in aliases if cot_keys else '-'}")
        if cot_keys:
            tgt = aliases.get(cot_keys[0], "")
            rep.log(f"  alias value      : {tgt}")
            # test the exact call the vault makes
            try:
                did, code, col = tgt.split(":", 1)[1].split(":", 2)
                url = ("https://publicreporting.cftc.gov/resource/%s.json?" % did
                       + urllib.parse.urlencode({
                           "cftc_contract_market_code": code,
                           "$select": f"report_date_as_yyyy_mm_dd,{col}",
                           "$order": "report_date_as_yyyy_mm_dd DESC",
                           "$limit": 1}))
                req = urllib.request.Request(url, headers={
                    "User-Agent": "justhodl-ops/4098", "Accept": "application/json"})
                rows_ = json.loads(urllib.request.urlopen(req, timeout=25).read())
                rep.log(f"  LIVE CFTC CALL   : {rows_}")
                rep.log("  → if this returns a value, the API and column are fine "
                        "and the fault is in how the vault reaches the alias")
            except Exception as e:
                rep.log(f"  LIVE CFTC CALL FAILED: {str(e)[:140]}")
                rep.log("  → the adapter's own request shape is wrong; fix that "
                        "before blaming the alias lookup")

        # ── ship the full universe ──
        rep.section("B. deploy vault v3.14.1")
        src = (ROOT / "lambdas" / FN / "source" / "lambda_function.py").read_text()
        assert MARK in src, "marker parity broken"
        buf = io.BytesIO()
        with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
            z.writestr("lambda_function.py", src)
        for _ in range(6):
            try:
                lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue(),
                                         Publish=True); break
            except lam.exceptions.ResourceConflictException:
                time.sleep(12)
        settled = False
        for a in range(24):
            try:
                c = lam.get_function_configuration(FunctionName=FN)
                if c.get("State") == "Active" and c.get("LastUpdateStatus") != "InProgress":
                    loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
                    dep = zf.ZipFile(io.BytesIO(urllib.request.urlopen(loc, timeout=60).read())
                                     ).read("lambda_function.py").decode()
                    if MARK in dep:
                        settled = True; rep.log(f"  ✓ settled (attempt {a+1})"); break
            except Exception as e:
                rep.log(f"  settle {a+1}: {str(e)[:55]}")
            time.sleep(10)
        checks.append(("v3.14.1 settled by marker", settled))
        if not settled:
            rep.log("✗ stale"); sys.exit(1)

        rep.section("C. run it (async + poll)")
        before_gen = v.get("generated_at")
        before_n = len(v.get("symbols") or [])
        r = lam.invoke(FunctionName=FN, InvocationType="Event",
                       Payload=b'{"source":"ops4099"}')
        checks.append(("async accepted", r["StatusCode"] == 202))
        after = None
        for a in range(50):
            time.sleep(20)
            cur = gj("data/tradingview.json")
            if cur and cur.get("generated_at") != before_gen:
                after = cur; rep.log(f"  ✓ artifact moved after {(a+1)*20}s"); break
        if after is None:
            rep.log("✗ never moved"); sys.exit(1)

        rows = after.get("symbols") or []
        st = Counter(str(x.get("status")) for x in rows)
        rep.section("D. did the universe actually widen?")
        rep.log(f"  rows {before_n} → {len(rows)}")
        rep.log(f"  from_watchlist : {after.get('n_from_watchlist')}")
        rep.log(f"  LIVE {after.get('n_live')} · pending {after.get('n_pending')} "
                f"· unresolved {after.get('n_unresolved')}")
        rep.log(f"  status histogram: {dict(st)}")
        cft = [x for x in rows if str(x.get("resolved_via") or "").startswith("cftc:")
               and x.get("value") is not None]
        rep.log(f"  cftc rows with a value: {len(cft)}")
        rep.kv(rows=len(rows), live=after.get("n_live"),
               pending=after.get("n_pending"), cftc_live=len(cft))

        # The universe must reach the imported truth.
        wl = gj("data/tv-watchlists.json", {}) or {}
        truth = {str(x).strip() for l in (wl.get("watchlists") or wl.get("lists") or [])
                 for x in (l.get("symbols") or []) if str(x).strip()}
        rep.log(f"  imported unique symbols: {len(truth)}")
        checks.append(("vault universe grew past 8,000 rows", len(rows) > 8000))
        checks.append(("watchlist symbols are flagged", (after.get("n_from_watchlist") or 0) > 5000))
        checks.append(("un-attempted rows are PENDING, not NO_FREE_SOURCE",
                       st.get("PENDING_RESOLUTION", 0) > 0))
        checks.append(("existing LIVE population not regressed",
                       (after.get("n_live") or 0) >= 900))
        checks.append(("every row serialises exchanges as a list",
                       all(isinstance(x.get("exchanges"), list) for x in rows)))

        rep.section("VERDICT")
        for n, o in checks: rep.log(f"  {'✓' if o else '✗'} {n}")
        bad = [n for n, o in checks if not o]
        if bad:
            rep.log(f"✗ FAILED: {bad}"); sys.exit(1)
        rep.log(f"✅ PASS_ALL — {len(rows)} rows on tradingview.html "
                f"({after.get('n_live')} live, {after.get('n_pending')} queued). "
                f"Resolution accretes at RESOLVE_BUDGET/run.")


if __name__ == "__main__":
    main()
