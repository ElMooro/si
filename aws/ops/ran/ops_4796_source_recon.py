"""ops/4796 -- four-source recon (read-only, evidence verbatim):
 A. ICMA RepoFunds Rate page: hunt the FREE backing data endpoint
    behind the 'RFR by Sovereign Issuer' widget (regex the HTML + any
    linked JS for json/csv/api URLs; probe top candidates, print
    shapes). CME DataMine = paid entitlement; this hunts the free path.
 B. ICMA SFTR public-data page: list downloadable xlsx/csv links;
    fetch the newest, print size + sheet names / header.
 C. DTCC daily Treasury fails: probe the chart page + candidate data
    endpoints; print what answers.
 D. FIMA/foreign-official RRP FRED id: read FRED_API_KEY from
    justhodl-repo's live env (never logged), FRED search
    'reverse repurchase foreign official', print top ids+titles.
 E. TE key location: confirm which lambda env holds TE_API_KEY/TE_KEY
    (names only, values never logged)."""
import gzip
import json
import re
import sys
import urllib.request
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from ops_report import report  # noqa: E402

UA = {"User-Agent": "Mozilla/5.0 (research; raafouis@gmail.com)"}
lam = boto3.client("lambda", region_name="us-east-1",
                    config=Config(read_timeout=30, retries={"max_attempts": 1}))


def fetch(url, timeout=40, binary=False):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return raw if binary else raw.decode("utf-8", "replace")


def main():
    with report("4796_source_recon") as rep:
        rep.heading("ops 4796 -- RFR / SFTR / DTCC / FIMA / TE recon")

        rep.section("A. ICMA RepoFunds Rate free endpoint hunt")
        try:
            html = fetch("https://www.icmagroup.org/market-practice-and-"
                          "regulatory-policy/repo-and-collateral-markets/"
                          "market-data/euro-repo-funds-rate/")
            urls = sorted(set(re.findall(
                r"https?://[^\s\"'<>]+?\.(?:json|csv|xlsx)[^\s\"'<>]*",
                html)))[:12]
            apis = sorted(set(re.findall(
                r"[\"'](/[^\"']*(?:api|data|chart)[^\"']*)[\"']",
                html)))[:12]
            rep.kv(check="rfr_page_bytes", value=len(html))
            for u in urls:
                rep.log("  fileurl: " + u[:160])
            for a in apis:
                rep.log("  relpath: " + a[:160])
            for u in urls[:3]:
                try:
                    body = fetch(u, binary=True)
                    rep.ok(f"  probe {u[:90]} -> {len(body)}B "
                            f"head={body[:80]!r}")
                except Exception as e:
                    rep.log(f"  probe {u[:90]} -> {type(e).__name__}")
        except Exception as e:
            rep.warn(f"RFR page: {type(e).__name__}: {str(e)[:100]}")

        rep.section("B. ICMA SFTR public data files")
        try:
            html = fetch("https://www.icmagroup.org/market-practice-and-"
                          "regulatory-policy/repo-and-collateral-markets/"
                          "market-data/sftr-public-data/")
            files = sorted(set(re.findall(
                r"href=[\"']([^\"']+\.(?:xlsx|csv|zip))[\"']", html)))[:10]
            rep.kv(check="sftr_file_links", value=len(files))
            for f0 in files:
                rep.log("  " + f0[:170])
            if files:
                u = files[-1]
                if u.startswith("/"):
                    u = "https://www.icmagroup.org" + u
                body = fetch(u, binary=True)
                rep.ok(f"  newest fetched: {len(body)}B "
                        f"magic={body[:4]!r}")
        except Exception as e:
            rep.warn(f"SFTR page: {type(e).__name__}: {str(e)[:100]}")

        rep.section("C. DTCC daily Treasury fails probes")
        for u in ("https://www.dtcc.com/charts/daily-total-us-treasury-"
                   "trade-fails",
                   "https://www.dtcc.com/-/media/Files/Downloads/"
                   "Settlement-Asset-Services/Settlement/"
                   "treasury-fails.csv",
                   "https://www.dtcc.com/api/charts/treasury-fails"):
            try:
                body = fetch(u, binary=True)
                head = body[:120]
                rep.ok(f"  {u[:80]} -> {len(body)}B head={head[:70]!r}")
                if b"json" in head.lower() or head[:1] in (b"{", b"["):
                    rep.log("    ^ JSON-ish!")
                if b"csv" in u.encode() and b"," in head:
                    rep.log("    ^ CSV-ish!")
                txt = body[:40000].decode("utf-8", "replace")
                emb = sorted(set(re.findall(
                    r"[\"'](/[^\"']*(?:api|data|json|csv)[^\"']*)[\"']",
                    txt)))[:8]
                for a in emb:
                    rep.log("    embedded: " + a[:140])
            except Exception as e:
                rep.log(f"  {u[:80]} -> {type(e).__name__}")

        rep.section("D. FIMA / foreign-official RRP FRED id")
        try:
            env = lam.get_function_configuration(
                FunctionName="justhodl-repo")["Environment"]["Variables"]
            fk = env.get("FRED_API_KEY", "")
            rep.kv(check="fred_key_present", value=bool(fk))
            if fk:
                import urllib.parse as up
                u = ("https://api.stlouisfed.org/fred/series/search?" +
                      up.urlencode({"search_text":
                                     "reverse repurchase foreign official",
                                     "api_key": fk, "file_type": "json",
                                     "limit": 8}))
                js = json.loads(fetch(u))
                for s0 in js.get("seriess") or []:
                    rep.ok(f"  {s0['id']}: {s0['title'][:90]} "
                            f"[{s0.get('frequency_short')}] "
                            f"{s0.get('observation_start')}->")
        except Exception as e:
            rep.warn(f"FIMA search: {type(e).__name__}: {str(e)[:100]}")

        rep.section("E. TE key holder (names only)")
        for fn in ("justhodl-dxy-predict", "justhodl-blackswan-watch",
                    "justhodl-bottom-signals"):
            try:
                ev = lam.get_function_configuration(
                    FunctionName=fn)["Environment"]["Variables"]
                ks = [k for k in ev if "TE" in k.upper()
                       and "KEY" in k.upper()]
                rep.kv(**{f"te_{fn.split('-')[-1]}":
                           ",".join(ks) or "none"})
            except Exception as e:
                rep.log(f"  {fn}: {type(e).__name__}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
