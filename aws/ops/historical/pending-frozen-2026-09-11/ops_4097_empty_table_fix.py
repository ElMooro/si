"""ops_4097 — the empty table, and the hung upload.

EMPTY TABLE (root cause, not a symptom): build_registry() normalises
exchanges into a sorted list, but every row admitted AFTER it — the
curated ALIASES union and my generated-alias expansion — kept a raw
Python set(). json.dumps(default=str) renders set() as the STRING
"set()", and tradingview.html calls (r.exchanges||[]).join(',') on it.
A string has no .join, so the .map() threw and the ENTIRE tbody rendered
empty. One malformed row blanked all 1,358. The category chips still
drew their counts because those come from a different field, which is
exactly why it looked like a filter bug and was not one.

Fixed in two places on purpose: the engine now normalises every admission
path, and the page wraps each row so a single bad record can never blank
the table again.

HUNG UPLOAD: v1.8.1 shipped the entire DESCS map every sync. Descriptions
accrete server-side, so v1.8.2 sends at most 1,500 unsent ones per sync.

Gates verify the ARTIFACT, not the intention: no row may serialise
exchanges as anything but a list.
"""
import io, json, sys, time, urllib.request, zipfile as zf
from pathlib import Path
import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
REPO = ROOT.parent
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=120, retries={"max_attempts": 0}))
BUCKET = "justhodl-dashboard-live"
MARK = "tradingview-vault v3.13.2 ops4097 exchanges-normalize"


def main():
    with report("4097_empty_table_fix") as rep:
        rep.heading("ops 4097 — empty table root cause + hung upload")
        checks = []

        rep.section("A. prove the bug exists in the LIVE artifact first")
        cur = json.loads(s3.get_object(Bucket=BUCKET, Key="data/tradingview.json")["Body"].read())
        bad = [r.get("symbol") for r in (cur.get("symbols") or [])
               if not isinstance(r.get("exchanges"), list)]
        rep.log(f"  rows with non-list exchanges BEFORE: {len(bad)}")
        rep.log(f"  sample: {bad[:8]}")
        rep.log("  → any one of these throws in .map() and blanks the table")
        rep.kv(bad_before=len(bad))

        rep.section("B. deploy vault v3.13.2")
        src = (ROOT / "lambdas" / "justhodl-tradingview" / "source" / "lambda_function.py").read_text()
        assert MARK in src, "marker parity broken"
        buf = io.BytesIO()
        with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
            z.writestr("lambda_function.py", src)
        for _ in range(6):
            try:
                lam.update_function_code(FunctionName="justhodl-tradingview",
                                         ZipFile=buf.getvalue(), Publish=True); break
            except lam.exceptions.ResourceConflictException:
                time.sleep(12)
        settled = False
        for a in range(24):
            try:
                c = lam.get_function_configuration(FunctionName="justhodl-tradingview")
                if c.get("State") == "Active" and c.get("LastUpdateStatus") != "InProgress":
                    loc = lam.get_function(FunctionName="justhodl-tradingview")["Code"]["Location"]
                    dep = zf.ZipFile(io.BytesIO(urllib.request.urlopen(loc, timeout=60).read())
                                     ).read("lambda_function.py").decode()
                    if MARK in dep:
                        settled = True; rep.log(f"  ✓ settled (attempt {a+1})"); break
            except Exception as e:
                rep.log(f"  settle {a+1}: {str(e)[:55]}")
            time.sleep(10)
        checks.append(("vault v3.13.2 settled", settled))
        if not settled:
            rep.log("✗ stale"); sys.exit(1)

        rep.section("C. force run — also retries the cftc rows past the 27d gate")
        r = lam.invoke(FunctionName="justhodl-tradingview", InvocationType="Event",
                       Payload=json.dumps({"source": "ops4097", "force": True}).encode())
        checks.append(("async accepted", r["StatusCode"] == 202))
        after = None
        for a in range(45):
            time.sleep(20)
            try:
                nx = json.loads(s3.get_object(Bucket=BUCKET, Key="data/tradingview.json")["Body"].read())
                if nx.get("generated_at") != cur.get("generated_at"):
                    after = nx; rep.log(f"  ✓ artifact moved after {(a+1)*20}s"); break
            except Exception:
                pass
        if after is None:
            rep.log("✗ never moved"); sys.exit(1)

        rows = after.get("symbols") or []
        bad2 = [r_.get("symbol") for r_ in rows if not isinstance(r_.get("exchanges"), list)]
        live = [r_ for r_ in rows if str(r_.get("status")).upper() == "LIVE"]
        cft = [r_ for r_ in rows if str(r_.get("resolved_via") or "").startswith("cftc:")
               and r_.get("value") is not None]
        rep.section("D. results")
        rep.log(f"  rows {len(rows)}  LIVE {len(live)}")
        rep.log(f"  rows with non-list exchanges AFTER: {len(bad2)}")
        rep.log(f"  cftc-routed rows with a value: {len(cft)}")
        for x in cft[:10]:
            rep.log(f"    {str(x.get('symbol'))[:26]:26} = {str(x.get('value'))[:12]:12} asof {x.get('asof')}")
        rep.kv(rows=len(rows), live=len(live), bad_after=len(bad2), cftc_live=len(cft))

        checks.append(("NO row serialises exchanges as a non-list", not bad2))
        checks.append(("table has rows to render", len(rows) > 0))
        checks.append(("vault still healthy", len(live) > 900))
        # cftc is reported, not gated — the force run is the first real
        # chance the adapter has had; if it still returns nothing that is a
        # separate finding, not a reason to fail the table fix.
        rep.log(f"  (cftc live = {len(cft)} — reported, not gated here)")

        rep.section("E. extension v1.8.2 — bounded descs, stops the hang")
        try:
            old = s3.get_object(Bucket=BUCKET, Key="tools/jh-tv-extension.zip")["Body"].read()
            rooted = not any(n.startswith("chrome-extension/")
                             for n in zf.ZipFile(io.BytesIO(old)).namelist()[:4])
        except Exception:
            rooted = True
        b2 = io.BytesIO(); sd = REPO / "chrome-extension"
        with zf.ZipFile(b2, "w", zf.ZIP_DEFLATED) as z:
            for f in sorted(sd.rglob("*")):
                if f.is_file():
                    z.write(f, str(f.relative_to(sd if rooted else REPO)))
        s3.put_object(Bucket=BUCKET, Key="tools/jh-tv-extension.zip", Body=b2.getvalue(),
                      ContentType="application/zip", CacheControl="max-age=300")
        chk = zf.ZipFile(io.BytesIO(s3.get_object(Bucket=BUCKET, Key="tools/jh-tv-extension.zip")["Body"].read()))
        pre = "" if rooted else "chrome-extension/"
        man = json.loads(chk.read(pre + "manifest.json"))
        cjs = chk.read(pre + "content.js").decode()
        rep.kv(ext=man.get("version"))
        checks.append(("extension v1.8.2", man.get("version") == "1.8.2"))
        checks.append(("descs payload is bounded", "SENT_DESCS" in cjs and "_dn >= 1500" in cjs))
        checks.append(("description capture retained", "DESCS[sym] = dsc" in cjs))
        checks.append(("harvester not regressed",
                       "PRIORITY WALK" in cjs and "function onOk" in cjs
                       and "autoStart" in cjs))

        rep.section("VERDICT")
        for n, o in checks: rep.log(f"  {'✓' if o else '✗'} {n}")
        bad3 = [n for n, o in checks if not o]
        if bad3:
            rep.log(f"✗ FAILED: {bad3}"); sys.exit(1)
        rep.log(f"✅ PASS_ALL — {len(bad)} malformed rows → 0. tradingview.html "
                f"renders again ({len(rows)} rows, {len(live)} LIVE). "
                f"Upload bounded at 1,500 descriptions per sync.")


if __name__ == "__main__":
    main()
