"""ops 6936 - DRY RUN, read only: the Risk Map engine 2.0.0 built on the runner from the real warehouse, and checked.

Khalid 2026-11-24: "improve this engine so that it covers all asset classes ... make sure whats booming is actually
booming and whats destroyed is actually destroyed".

The engine's build() is run here, on the runner, against the live warehouse. Nothing is written to S3 and the deployed
engine is not invoked. Then, with arithmetic written in this file and not taken from the engine:

  1. what would be published: counts, dates, what is left out and why; the first map's 38 instruments as the live
     1.0.0 feed has them beside what 2.0.0 gives;
  2. every instrument called BOOMING or DESTROYED, with its record, held to the rule by this op's own tests;
  3. every row's changes recomputed from its anchors; every state recomputed from its score and record;
  4. an independent provider: each fund's anchor closes (the close every change is measured from) and its previous
     close are looked up in the FMP price bank's own history of the same fund, which FMP split-adjusts itself - so a
     split the Polygon bank still holds unadjusted and this engine failed to rebase would show here;
  5. coins beside CoinMarketCap's own 7 / 30 / 90-day changes; currencies beside the Federal Reserve's H.10 rates.

The document it would publish, and a cut of the real bars it read, are embedded for the engine's and the page's tests.
Re-runnable.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import base64
import gzip
import json
import sys
import time
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "aws" / "lambdas" / "justhodl-regime-map" / "source"
sys.path[:0] = [str(ROOT / "aws" / "ops"), str(SRC)]
RESOLVER = "https://justhodl-data-proxy.raafouis.workers.dev/series?id="
FRED_FX = {"EUR": ("DEXUSEU", False), "JPY": ("DEXJPUS", True), "GBP": ("DEXUSUK", False), "CHF": ("DEXSZUS", True), "CAD": ("DEXCAUS", True),
           "AUD": ("DEXUSAL", False), "MXN": ("DEXMXUS", True), "BRL": ("DEXBZUS", True), "INR": ("DEXINUS", True), "KRW": ("DEXKOUS", True)}
FIXTURE_FUNDS = ("QQQ", "VIXM", "VIXY", "SMH", "KRE", "EWJ", "SHY", "CPER")
FIXTURE_COINS = ("BTC", "ETH", "SOL", "XRP", "DOGE", "ADA")
FIXTURE_FMP = ("EURUSD", "USDJPY", "USDCHF", "USDMXN", "DX-Y.NYB", "^VIX", "KCUSX")


# ---------------------------------------------------------------- this op's own arithmetic (not the engine's)
def own_months_back(d, n):
    y, m = d.year, d.month - n
    while m <= 0:
        y, m = y - 1, m + 12
    for day in range(d.day, 0, -1):
        try:
            return date(y, m, day)
        except ValueError:
            continue


def own_target(last_iso, window):
    last = date.fromisoformat(last_iso)
    if window == "w":
        return (last - timedelta(days=7)).isoformat()
    if window == "ytd":
        return date(last.year - 1, 12, 31).isoformat()
    return own_months_back(last, {"m": 1, "q": 3, "h": 6, "y": 12}[window]).isoformat()


def own_state(score, r1m, r3m, v50, v200):
    """The state by the published rule, from the published score and record."""
    if score >= 45:
        return "BOOMING" if (r1m > 0 and r3m > 0 and max(r1m, r3m) >= 5 and v50 > 0 and v200 > 0) else "RISK-ON"
    if score >= 15:
        return "RISK-ON"
    if score > -15:
        return "NEUTRAL"
    if score > -45:
        return "RISK-OFF"
    return "DESTROYED" if (r1m < 0 and r3m < 0 and min(r1m, r3m) <= -5 and v50 < 0 and v200 < 0) else "RISK-OFF"


def own_score(r1m, r3m, r6m, v50, v200, ma50_over_200):
    pts = (1 if v50 > 0 else 0) + (1 if v200 > 0 else 0) + (1 if ma50_over_200 else 0)
    raw = 2.2 * (0.45 * r3m + 0.30 * r6m + 0.25 * r1m) + (pts - 1.5) / 1.5 * 25.0
    return max(-100.0, min(100.0, raw))


def selftest():
    assert own_months_back(date(2026, 3, 31), 1) == date(2026, 2, 28) and own_target("2026-11-24", "q") == "2026-08-24" and own_target("2026-01-05", "ytd") == "2025-12-31"
    assert own_state(60, 1, 5, 2, 3) == "BOOMING" and own_state(60, -1, 5, 2, 3) == "RISK-ON" and own_state(-60, -1, -5, -2, -3) == "DESTROYED" and own_state(-60, 1, -5, -2, -3) == "RISK-OFF"
    assert own_state(60, 3.4, 1.1, 2, 3) == "RISK-ON" and own_state(60, 5, 1.1, 2, 3) == "BOOMING" and own_state(-60, -4.9, -4.9, -2, -3) == "RISK-OFF"
    assert own_state(44, 1, 1, 1, 1) == "RISK-ON" and own_state(-15, 0, 0, 0, 0) == "RISK-OFF" and own_state(-14, 0, 0, 0, 0) == "NEUTRAL"
    assert round(own_score(10, 20, 30, 1, 1, True)) == round(2.2 * (9 + 9 + 2.5) + 25)


def main():
    selftest()
    import boto3
    from botocore.config import Config
    from ops_report import report
    import lambda_function as L
    import risk_math as M
    import risk_universe as U
    real = boto3.client("s3", region_name="us-east-1", config=Config(retries={"max_attempts": 4, "mode": "standard"}, max_pool_connections=64))

    class Recorder:
        """The S3 client handed to the engine: every object it reads is kept, so the fixture is cut from the very
        bytes this build saw (the forming-bar file is rewritten every five minutes)."""
        def __init__(self):
            self.seen, self.listed = {}, {}

        def get_object(self, Bucket, Key):
            raw = real.get_object(Bucket=Bucket, Key=Key)["Body"].read()
            self.seen[Key] = raw
            import io
            return {"Body": io.BytesIO(raw)}

        def list_objects_v2(self, **kw):
            out = real.list_objects_v2(**kw)
            self.listed.setdefault(kw.get("Prefix"), []).extend(o["Key"] for o in out.get("Contents") or [])
            return out

        def doc(self, key):
            raw = self.seen.get(key)
            return None if raw is None else json.loads(gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw)
    s3 = Recorder()

    def get(key):
        try:
            raw = real.get_object(Bucket=L.BUCKET, Key=key)["Body"].read()
            return json.loads(gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw)
        except Exception:  # noqa: BLE001
            return None

    with report("ops_6936_risk_map_dry_run") as r:
        now = datetime.now(timezone.utc)
        r.heading("ops 6936 - Risk Map 2.0.0, dry run on the real warehouse (nothing written, engine not invoked), %s" % now.isoformat(timespec="minutes"))
        fails = []

        def ck(ok, msg):
            if not ok:
                fails.append(msg)
                r.fail(msg)
            return ok

        # ------------------------------------------------------------ 1. the build
        r.section("1. What 2.0.0 would publish")
        live, prev = get(L.OUT_KEY) or {}, get(L.PREV_KEY)
        t0 = time.time()
        doc = L.build(s3, now=now, prev=prev, log=r.log)
        body = json.dumps(doc, separators=(",", ":")).encode()
        m = doc["map"]
        tiles = m["tiles"]
        by_t = {t["t"]: t for t in tiles}
        r.log("built in %.1f s on the runner; %d bytes (%d gzip); %d of %d instruments measured: %s" % (time.time() - t0, len(body), len(gzip.compress(body)), m["n"], m["n_universe"], m["counts"]))
        r.log("dates: %s" % doc["as_of"])
        r.log("left out (%d): %s" % (len(m["left_out"]), "; ".join("%s - %s" % (x["t"], x["why"]) for x in m["left_out"]) or "none"))
        r.log("notes: %s" % (doc["notes"] or "none"))
        late = [(t["t"], t["d"], t["late"]) for t in tiles if t.get("late")]
        r.log("a bar behind their kind's newest (%d): %s" % (len(late), late))
        r.log("splits rebased: %s" % [(t["t"], t["splits"]) for t in tiles if t.get("splits")])
        r.log("unexplained one-day moves: %s" % [(t["t"], t["jump"]) for t in tiles if t.get("jump")])
        r.log("regime (first map's 38, same rule): %s - %s" % (doc["regime"]["label"], {k: v for k, v in doc["regime"].items() if k not in ("label", "summary")}))
        r.log("live 1.0.0 feed (%s): %s - %s" % (str(live.get("generated_at"))[:16], (live.get("regime") or {}).get("label"), {k: v for k, v in (live.get("regime") or {}).items() if k not in ("label", "summary")}))
        r.log("cross-asset: %s - %s" % (doc["cross_asset"]["label"], doc["cross_asset"]["summary"]))
        r.log("first-map lists by 2.0.0: booming %s | destroyed %s | strongest %s | weakest %s" % (
            [x["ticker"] for x in doc["booming"]], [x["ticker"] for x in doc["destroyed"]], [x["ticker"] for x in doc["strongest"]], [x["ticker"] for x in doc["weakest"]]))
        ck(len(doc["ranked"]) == 38, "the first map's instruments: %d of 38" % len(doc["ranked"]))
        # Turns: this op's own reading of each one's direction against the name the engine gave it.
        rank = {"DESTROYED": 0, "RISK-OFF": 1, "NEUTRAL": 2, "RISK-ON": 3, "BOOMING": 4}
        up, down = {"BOTTOMING / RECOVERING", "ACCELERATING", "FLIPPED RISK-ON", "STRENGTHENING"}, {"ROLLING OVER", "CAPITULATING", "FLIPPED RISK-OFF", "WEAKENING"}
        ref = L.reference(prev, now)
        ck(doc.get("turns_since") == ref.get("generated_at"), "the run the turns are measured from: %s" % doc.get("turns_since"))
        for x in doc["transitions"] + m["transitions"]:
            a, b = rank[x["from"]], rank[x["to"]]
            rose, fell = b > a or (b == a and x["delta"] > 0), b < a or (b == a and x["delta"] < 0)
            ck((x["kind"] in up and rose) or (x["kind"] in down and fell), "%s is called %s but went %s -> %s (%+d)" % (x["ticker"], x["kind"], x["from"], x["to"], x["delta"]))
        r.log("turns since the run of %s: %d on the first map %s | %d on the whole map %s" % (
            str(doc.get("turns_since"))[:16], doc["n_transitions"], [(x["ticker"], x["from"], x["to"], x["delta"], x["kind"]) for x in doc["transitions"]],
            m["n_transitions"], [(x["ticker"], x["from"], x["to"], x["delta"], x["kind"]) for x in m["transitions"][:12]]))
        snap = L.snapshot(doc, prev)
        r.log("the snapshot the next run would be compared with: %d first-map states, %d tiles, kept beside it %s" % (
            len(snap["states"]), len(snap["tiles"]), (snap.get("base") or {}).get("generated_at") or "nothing (no run of an earlier day is on record yet)"))
        need = ["booming", "destroyed", "elapsed_s", "engine", "generated_at", "n_instruments", "n_transitions", "ok", "prev_regime", "ranked", "regime", "regime_changed", "sleeves", "thesis", "transitions", "version", "windows"]
        ck(all(k in doc for k in need), "a key the feed's contract requires is missing: %s" % [k for k in need if k not in doc])
        ck(set(doc["sleeves"]) == set(live.get("sleeves") or doc["sleeves"]), "sleeve names changed: %s" % sorted(doc["sleeves"]))
        old = {x["ticker"]: x for x in live.get("ranked") or []}
        r.log("  %-5s | live 1.0.0: score state 1m 3m 6m | 2.0.0: score state 1m 3m 6m (by the calendar, last bar)" % "name")
        for x in doc["ranked"]:
            o = old.get(x["ticker"])
            r.log("  %-5s | %s | %+4d %-9s %+6.1f %+6.1f %+6.1f (%s%s)" % (
                x["ticker"], ("%+4d %-9s %+6.1f %+6.1f %+6.1f" % (o["risk_on"], o["state"], o["r1m"], o["r3m"], o["r6m"])) if o else "-", x["risk_on"], x["state"], x["r1m"], x["r3m"], x["r6m"],
                x["as_of"], ", forming" if by_t[x["ticker"]].get("f") else ""))

        # ------------------------------------------------------------ 2. classes
        r.section("2. Every asset class")
        for c in m["classes"]:
            st = m["stats"]["classes"][c["id"]]
            r.log("%-14s %3d instruments | average score %s | rising %s%% | %s | median change: day %s, week %s, month %s, 3 months %s, 6 months %s, year %s" % (
                c["name"], st["n"], st["score"], st["rising_pct"], " ".join("%s %d" % (k.lower(), v) for k, v in st["states"].items() if v),
                st["r"]["d"], st["r"]["w"], st["r"]["m"], st["r"]["q"], st["r"]["h"], st["r"]["y"]))
            for g in c["groups"]:
                gs = m["stats"]["groups"][g["id"]]
                r.log("      %-48s %3d | score %6s | rising %4s%% | 3 months %7s | best %s, worst %s" % (g["name"], gs["n"], gs["score"], gs["rising_pct"], gs["r"]["q"], gs["best"], gs["worst"]))

        # ------------------------------------------------------------ 3. booming and destroyed, held to the rule
        r.section("3. What is called BOOMING and DESTROYED, with the record")
        for name in ("booming", "destroyed"):
            r.log("%s (%d):" % (name.upper(), len(m[name])))
            for tk in m[name]:
                t = by_t[tk]
                r.log("    %-6s %-34s score %+4d | month %+7.2f%% | 3 months %+7.2f%% | 6 months %+7.2f%% | year %s | vs 50-day %+6.2f%% | vs 200-day %+6.2f%% | from 52-week high %+6.2f%% | %s%s" % (
                    tk, t["n"][:34], t["score"], t["r"]["m"], t["r"]["q"], t["r"]["h"], ("%+7.2f%%" % t["r"]["y"]) if "y" in t["r"] else "n/a", t["v50"], t["v200"], t["off_hi"], t["d"], " (late %d)" % t["late"] if t.get("late") else ""))
        held = [t for t in tiles if t.get("held")]
        r.log("scores of 45 or more, or -45 or less, NOT called booming / destroyed (%d):" % len(held))
        for t in held:
            r.log("    %-6s %-30s -> %s: %s (month %+.2f%%, 3 months %+.2f%%, vs 50-day %+.2f%%, vs 200-day %+.2f%%)" % (t["t"], t["n"][:30], t["state"], t["held"], t["r"]["m"], t["r"]["q"], t["v50"], t["v200"]))
        for t in tiles:
            if t["state"] == "BOOMING":
                ck(t["score"] >= 45 and t["r"]["m"] > 0 and t["r"]["q"] > 0 and max(t["r"]["m"], t["r"]["q"]) >= 5 and t["v50"] > 0 and t["v200"] > 0, "%s is called BOOMING against its record" % t["t"])
            if t["state"] == "DESTROYED":
                ck(t["score"] <= -45 and t["r"]["m"] < 0 and t["r"]["q"] < 0 and min(t["r"]["m"], t["r"]["q"]) <= -5 and t["v50"] < 0 and t["v200"] < 0, "%s is called DESTROYED against its record" % t["t"])
        ck(set(m["booming"]) == {t["t"] for t in tiles if t["state"] == "BOOMING"}, "the booming list is not exactly the instruments in that state")
        ck(set(m["destroyed"]) == {t["t"] for t in tiles if t["state"] == "DESTROYED"}, "the destroyed list is not exactly the instruments in that state")
        ck(all(x["state"] == "BOOMING" for x in doc["booming"]) and all(x["state"] == "DESTROYED" for x in doc["destroyed"]), "a first-map list holds an instrument in another state")

        # ------------------------------------------------------------ 4. every row by this op's arithmetic
        r.section("4. Every row recomputed here")
        n_ret = n_anchor = n_state = 0
        for t in tiles:
            for k, (ad, ac, nb) in t["a"].items():
                n_ret += 1
                mine = (t["c"] / ac - 1) * 100
                ck(abs(mine - t["r"][k]) <= 0.011, "%s %s: published %s, close over anchor gives %.4f" % (t["t"], k, t["r"][k], mine))
                if k != "d":
                    n_anchor += 1
                    want = own_target(t["d"], k)
                    ck(ad <= want and (date.fromisoformat(want) - date.fromisoformat(ad)).days <= 10, "%s %s: anchor %s for a window that starts %s" % (t["t"], k, ad, want))
                else:
                    ck(ad == t["pd"] and abs(ac - t["p"]) <= 1e-9 * max(1, abs(ac)), "%s: the day's anchor is not the previous close" % t["t"])
            edge = min(abs(t["r"]["m"]), abs(t["r"]["q"]), abs(t["v50"]), abs(t["v200"])) < 0.006     # a figure that rounds to 0.00 cannot be signed from the document
            if not t.get("jump") and not edge:
                n_state += 1
                ck(own_state(t["score"], t["r"]["m"], t["r"]["q"], t["v50"], t["v200"]) == t["state"], "%s: state %s, the rule gives %s" % (
                    t["t"], t["state"], own_state(t["score"], t["r"]["m"], t["r"]["q"], t["v50"], t["v200"])))
            lo_s = min(own_score(t["r"]["m"], t["r"]["q"], t["r"]["h"], t["v50"], t["v200"], b) for b in (True, False))
            hi_s = max(own_score(t["r"]["m"], t["r"]["q"], t["r"]["h"], t["v50"], t["v200"], b) for b in (True, False))
            ck(lo_s - 0.6 <= t["score"] <= hi_s + 0.6, "%s: score %s outside what its record allows (%.1f .. %.1f)" % (t["t"], t["score"], lo_s, hi_s))
        r.log("%d changes equal the close over their anchor; %d anchors sit on or within ten days before the day their window starts; %d states equal the rule; every score is one its record allows" % (n_ret, n_anchor, n_state))

        # ------------------------------------------------------------ 5. an independent provider for the funds
        r.section("5. Funds: the closes every change is measured from, against FMP's own history of the same fund")
        funds = [t for t in tiles if t["k"] == "fund"]

        def fmp_closes(t):
            d = get(L.fmp_key(t["sym"]))
            rows = d if isinstance(d, list) else (d or {}).get("rows") or (d or {}).get("historical") or []
            return t["t"], {str(x.get("date"))[:10]: x.get("close") for x in rows if isinstance(x, dict) and x.get("close")}
        with ThreadPoolExecutor(max_workers=32) as pool:
            fmp = dict(pool.map(fmp_closes, funds))
        compared = worst = 0
        absent, off, short, stale_fmp = [], [], [], []
        for t in funds:
            have = fmp.get(t["t"]) or {}
            if not have:
                absent.append(t["t"])
                continue
            pts = [(k, v[0], v[1]) for k, v in t["a"].items() if k != "d"] + ([("prev", t["pd"], t["p"])] if not t.get("f") else []) + ([("last", t["d"], t["c"])] if not t.get("f") else [])
            for k, d, c in pts:
                f = have.get(d)
                if f is None:
                    short.append("%s %s %s" % (t["t"], k, d))
                    continue
                compared += 1
                ratio = float(f) / c
                factors = [1.0 / float(e["factor"]) for e in t.get("splits") or [] if d < e["date"]]
                if any(abs(ratio / x - 1) * 100 <= 0.6 for x in factors):
                    stale_fmp.append("%s %s %s: this engine %.4f (split-adjusted), FMP's bank %.4f = %.4g times it, the split's own factor" % (t["t"], k, d, c, float(f), ratio))
                    continue
                diff = abs(ratio - 1) * 100
                worst = max(worst, diff)
                if diff > 0.6:
                    off.append("%s %s %s: bank %.4f, FMP %.4f (%.2f%%)" % (t["t"], k, d, c, float(f), diff))
        r.log("%d funds; %d closes compared on the same dates; largest difference %.3f%%; beyond 0.6%%: %d" % (len(funds), compared, worst, len(off)))
        for line in off[:40]:
            r.log("    " + line)
        r.log("closes where FMP's bank still holds the price from before a split this engine rebased (%d) - FMP's file is merged day by day and its older rows are never re-adjusted:" % len(stale_fmp))
        for line in stale_fmp[:24]:
            r.log("    " + line)
        r.log("funds FMP holds no history for: %s" % (absent or "none"))
        r.log("dates FMP has no row for (%d): %s" % (len(short), short[:20]))
        ck(compared >= 0.8 * 6 * (len(funds) - len(absent)), "too few closes could be compared with FMP: %d" % compared)
        split_funds = [t["t"] for t in funds if t.get("splits")]
        r.log("funds with a split rebased here, and how they compare: %s" % [(tk, max([float(fmp[tk].get(v[0]) or 0) / v[1] for k, v in by_t[tk]["a"].items() if k != "d" and fmp.get(tk, {}).get(v[0])] or [0])) for tk in split_funds])
        ck(not off, "%d of %d closes differ from FMP's by more than 0.6%% for no split: %s" % (len(off), compared, off[:6]))

        # ------------------------------------------------------------ 6. coins and currencies beside another source
        r.section("6. Coins beside CoinMarketCap; currencies beside the Federal Reserve's H.10")
        listing = None
        try:
            keys = sorted(s3.listed.get(L.CMC_ROOT) or [])
            ld = s3.doc(keys[-1]) if keys else None
            data = ld.get("data") if isinstance(ld, dict) else ld
            listing = data.get("data") if isinstance(data, dict) and "data" in data else data
            r.log("CoinMarketCap listing %s" % keys[-1][len(L.CMC_ROOT):])
        except Exception as e:  # noqa: BLE001
            r.log("CoinMarketCap listing not read: %s" % type(e).__name__)
        cmc = {}
        for c in listing or []:
            cmc.setdefault(str(c.get("symbol")).upper(), (c.get("quote") or {}).get("USD") or {})
        sign_off = []
        for t in [x for x in tiles if x["k"] == "coin"]:
            q = cmc.get(t["t"]) or {}
            pair = [(t["r"].get("w"), q.get("percent_change_7d")), (t["r"].get("m"), q.get("percent_change_30d")), (t["r"].get("q"), q.get("percent_change_90d"))]
            r.log("    %-7s %-18s score %+4d %-9s | bars to %s: week %s, month %s, 3 months %s | CoinMarketCap 7d %s, 30d %s, 90d %s | market value $%sB" % (
                t["t"], t["n"][:18], t["score"], t["state"], t["d"], *["%+7.1f" % a if a is not None else "    n/a" for a, _ in pair], *["%+7.1f" % float(b) if b is not None else "    n/a" for _, b in pair],
                ("%.2f" % (t["mc"] / 1e9)) if t.get("mc") else "n/a"))
            for (a, b), w in zip(pair[1:], ("month", "3 months")):
                if a is not None and b is not None and abs(a) > 12 and abs(float(b)) > 12 and (a > 0) != (float(b) > 0):
                    sign_off.append("%s %s: %+.1f here, %+.1f at CoinMarketCap" % (t["t"], w, a, float(b)))
        r.log("coins whose month or three-month change points the other way from CoinMarketCap's (both beyond 12%%): %s" % (sign_off or "none"))
        ck(not sign_off, "coins disagree in direction with CoinMarketCap: %s" % sign_off)
        fx_off = []
        for cur, (sid, per_dollar) in FRED_FX.items():
            t = by_t.get(cur)
            if not t:
                continue
            time.sleep(0.6)
            try:
                with urllib.request.urlopen(urllib.request.Request(RESOLVER + urllib.parse.quote("fred:" + sid, safe=""), headers={"User-Agent": "jh-ops-probe"}), timeout=45) as resp:
                    obs = {d: float(v) for d, v in (json.loads(resp.read()).get("obs") or []) if isinstance(v, (int, float))}
            except Exception as e:  # noqa: BLE001
                r.log("    %s: H.10 %s not read (%s)" % (cur, sid, type(e).__name__))
                continue
            rows = []
            for k in ("q", "h", "y"):
                if k not in t["a"]:
                    continue
                ad, ac, _ = t["a"][k]
                near = max((d for d in obs if d <= ad), default=None)
                if not near or (date.fromisoformat(ad) - date.fromisoformat(near)).days > 5:
                    continue
                fed = (1.0 / obs[near]) if per_dollar else obs[near]
                rows.append((k, ad, ac, near, fed, abs(fed / ac - 1) * 100))
            last_fed = max(obs) if obs else None
            r.log("    %s (%s%s): %s | H.10 runs to %s" % (cur, sid, ", turned over" if per_dollar else "", "; ".join("%s anchor %s %.6g vs Fed %s %.6g (%.2f%%)" % x for x in rows) or "no common dates", last_fed))
            fx_off += ["%s %s %.2f%%" % (cur, x[0], x[5]) for x in rows if x[5] > 1.5]
        r.log("currency anchors more than 1.5%% from the Federal Reserve's rate for the same day: %s" % (fx_off or "none"))
        ck(not fx_off, "currency closes differ from the Federal Reserve's: %s" % fx_off)

        # ------------------------------------------------------------ 7. fixtures
        r.section("7. Fixtures (the document; a cut of the real bars)")
        want_funds = sorted(set(U.legacy_tickers()) - set(U.LEGACY_COINS) | set(FIXTURE_FUNDS))
        bank = {}
        for tk in want_funds:
            d = s3.doc(L.bank_key(tk)) or {}
            bank[tk] = [[x[0]] + [None if v is None else float("%.7g" % v) for v in x[1:6]] for x in (d.get("rows") or [])[-470:] if isinstance(x, list) and len(x) >= 6]
        splits = {}
        for key in sorted(k for k in s3.seen if k.startswith(L.SPLITS_ROOT)):
            d = s3.doc(key)
            rows = d if isinstance(d, list) else (d or {}).get("results") or []
            splits[key[len(L.SPLITS_ROOT):-len(".json.gz")]] = [{k: x.get(k) for k in ("ticker", "execution_date", "split_from", "split_to")} for x in rows if isinstance(x, dict) and str(x.get("ticker")).upper() in bank]
        today = s3.doc(L.TODAY_KEY) or {}
        coins = {}
        for c in FIXTURE_COINS:
            d = s3.doc(L.COIN_ROOT + c + ".json.gz") or {}
            coins[c] = {"symbol": c, "source": d.get("source"), "rows": [[x[0]] + [float("%.7g" % v) for v in x[1:6]] for x in (d.get("rows") or [])[-820:]]}
        fmp_fix = {}
        for sym in FIXTURE_FMP:
            d = s3.doc(L.fmp_key(sym))
            rows = d if isinstance(d, list) else (d or {}).get("rows") or (d or {}).get("historical") or []
            fmp_fix[sym] = sorted(({"date": str(x.get("date"))[:10], "close": x.get("close")} for x in rows if isinstance(x, dict)), key=lambda x: x["date"])[-640:]
        cmc_key = sorted(s3.listed.get(L.CMC_ROOT) or [""])[-1]
        cmc_fix = [{"symbol": c.get("symbol"), "name": c.get("name"), "cmc_rank": c.get("cmc_rank"), "tags": (c.get("tags") or [])[:8], "quote": {"USD": {"market_cap": ((c.get("quote") or {}).get("USD") or {}).get("market_cap")}}} for c in (listing or [])[:80]]
        r.log("the engine read %d objects; the cut keeps %d funds, %d coins, %d price-bank series, %d split files, the forming-bar file as of %s and the first 80 rows of %s" % (
            len(s3.seen), len(bank), len(coins), len(fmp_fix), len(splits), today.get("as_of"), cmc_key[len(L.CMC_ROOT):]))
        fixture = {"cut_at": now.isoformat(timespec="seconds"), "bank": bank, "splits": splits,
                   "today": {"session": today.get("session"), "as_of": today.get("as_of"), "rows": {k: v for k, v in (today.get("rows") or {}).items() if k in bank}},
                   "coins": coins, "fmp": fmp_fix, "cmc": cmc_fix, "cmc_day": cmc_key[len(L.CMC_ROOT):-len(".json.gz")], "prev": prev,
                   "expect": {"regime": doc["regime"], "ranked": doc["ranked"], "booming": doc["booming"], "destroyed": doc["destroyed"], "transitions": doc["transitions"],
                              "tiles": {tk: by_t[tk] for tk in list(bank) + list(FIXTURE_COINS) + ["EUR", "JPY", "CHF", "MXN", "DXY", "VIX", "KC"] if tk in by_t}}}
        for name, obj in (("document", doc), ("warehouse-cut", fixture)):
            raw = gzip.compress(json.dumps(obj, separators=(",", ":")).encode(), 9)
            b64 = base64.b64encode(raw).decode("ascii")
            r.log("%s: %d bytes gzip" % (name, len(raw)))
            r.lines.append("<!-- FIXTURE %s %d bytes" % (name, len(raw)))
            r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
            r.lines.append("FIXTURE-END -->")

        r.kv(tiles=m["n"], universe=m["n_universe"], booming=len(m["booming"]), destroyed=len(m["destroyed"]), left_out=len(m["left_out"]),
             regime=doc["regime"]["label"], cross_asset=doc["cross_asset"]["label"], failed_checks=len(fails))
        if fails:
            raise SystemExit(1)
        r.ok("DRY RUN: %d instruments in %d asset classes; %d booming and %d destroyed, each held to its record; nothing written" % (m["n"], len(m["classes"]), len(m["booming"]), len(m["destroyed"])))


if __name__ == "__main__":
    main()
