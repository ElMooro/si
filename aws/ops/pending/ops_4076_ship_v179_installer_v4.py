"""ops_4076 — ship extension v1.7.9 + installer v4.

TWO REAL BUGS, both of which would have silently eaten the v1.7.8
priority walk on the day it shipped:

  1. EXTENSION (v1.7.9) — autoStart's guard stored only the date:
       if (r.jh_auto_day === today) return;
     The old build already walked today, so jh_auto_day is stamped
     2026-07-29.  An extension updated today would have seen its own
     stale stamp and returned immediately — the entire agency-first
     reorder would have sat idle until tomorrow, looking installed and
     doing nothing.  v1.7.9 stamps the VERSION into the guard, so an
     upgrade re-arms the walk at once while keeping once-per-day within
     a version.

  2. INSTALLER (v4) — the .ps1 ran
       Remove-Item -Recurse -Force $dir
     on the very folder Chrome loaded the unpacked extension from.  Every
     update therefore destroyed Chrome's registration and demanded a
     fresh "Load unpacked".  v4 stages to temp and copies IN PLACE, so
     the folder path never disappears and an update is one reload click.

Gates target those two failure modes directly, not version strings.
"""
import io
import json
import sys
import time
import urllib.request
import zipfile as zf
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
REPO = ROOT.parent
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
ZIP_KEY = "tools/jh-tv-extension.zip"
PS1_KEY = "tools/install-jh-extension.ps1"
S3BASE = "https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/"


def main():
    with report("4076_ship_v179_installer_v4") as rep:
        rep.heading("ops 4076 — extension v1.7.9 + installer v4")
        checks = []

        # ═════════ A. extension zip ═════════
        rep.section("A. rebuild + upload the extension zip")
        try:
            old = s3.get_object(Bucket=BUCKET, Key=ZIP_KEY)["Body"].read()
            names = zf.ZipFile(io.BytesIO(old)).namelist()[:4]
            rooted = not any(n.startswith("chrome-extension/") for n in names)
            oldv = json.loads(zf.ZipFile(io.BytesIO(old)).read(
                "manifest.json" if rooted
                else "chrome-extension/manifest.json")).get("version")
            rep.log(f"  previous S3 zip: v{oldv} ({len(old)} B)")
        except Exception:
            rooted, oldv = True, None
            rep.log("  no readable previous zip — files-at-root default")

        buf = io.BytesIO()
        src = REPO / "chrome-extension"
        with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
            for f in sorted(src.rglob("*")):
                if f.is_file():
                    z.write(f, str(f.relative_to(src if rooted else REPO)))
        data = buf.getvalue()
        s3.put_object(Bucket=BUCKET, Key=ZIP_KEY, Body=data,
                      ContentType="application/zip", CacheControl="max-age=300")

        chk = zf.ZipFile(io.BytesIO(
            s3.get_object(Bucket=BUCKET, Key=ZIP_KEY)["Body"].read()))
        pre = "" if rooted else "chrome-extension/"
        man = json.loads(chk.read(pre + "manifest.json"))
        cjs = chk.read(pre + "content.js").decode()
        rep.kv(old_version=oldv, new_version=man.get("version"),
               bytes=len(data))

        checks.append(("zip carries v1.7.9", man.get("version") == "1.7.9"))
        # THE bug: the guard must no longer be a bare date.
        checks.append(("auto-start guard is version-stamped (update-day trap)",
                       "function autoKey" in cjs
                       and "getManifest().version" in cjs
                       and "jh_auto_day: autoKey()" in cjs))
        checks.append(("no bare-date stamp survives anywhere",
                       "jh_auto_day: today }" not in cjs
                       and cjs.count("new Date().toISOString().slice(0, 10) });") == 0))
        # v1.7.8 work must still be intact — a later ship must not regress it.
        checks.append(("v1.7.8 priority walk still present",
                       "PRIORITY WALK" in cjs and "b[tierOf(s2)].push" in cjs))
        checks.append(("symsearch canary + 240ms step still present",
                       "i % 200 === 0" in cjs and "setTimeout(step, 240)" in cjs))
        checks.append(("rate telemetry still present",
                       "rate_per_min" in cjs and "tier1_done" in cjs))
        checks.append(("harvester + autonomy intact",
                       "scanner.tradingview.com/symbol" in cjs
                       and "autoStart" in cjs and "autoSync" in cjs))

        # ═════════ B. installer v4 ═════════
        rep.section("B. upload installer v4")
        ps1 = (REPO / "tools-src" / "install-jh-extension.ps1").read_bytes()
        s3.put_object(Bucket=BUCKET, Key=PS1_KEY, Body=ps1,
                      ContentType="text/plain", CacheControl="max-age=120")
        got = urllib.request.urlopen(
            S3BASE + PS1_KEY + f"?t={int(time.time())}", timeout=30).read()
        rep.kv(ps1_bytes=len(ps1), ps1_edge=len(got))

        checks.append(("served .ps1 is byte-exact", got == ps1))
        checks.append(("caret-free (the v2 cmd-escaping bug class)",
                       b"^" not in got))
        # THE bug: must no longer delete the folder Chrome loaded from.
        checks.append(("no longer nukes the loaded folder",
                       b"Remove-Item -Recurse -Force $dir" not in got))
        checks.append(("updates IN PLACE via a staging copy",
                       b"jh-tv-stage" in got and b"Copy-Item" in got))
        checks.append(("detects update vs fresh install",
                       b"isUpdate" in got and b"RELOAD arrow" in got))
        checks.append(("still pulls the zip from S3 + writes the shortcut",
                       b"jh-tv-extension.zip" in got
                       and b"CreateShortcut" in got))

        # ═════════ C. the .bat stub is untouched and still points here ═══
        rep.section("C. stub integrity")
        bat = urllib.request.urlopen(
            S3BASE + "tools/install-jh-extension.bat"
            f"?t={int(time.time())}", timeout=30).read()
        rep.kv(bat_bytes=len(bat))
        checks.append(("the .bat Khalid already has still fetches this .ps1",
                       b"install-jh-extension.ps1" in bat))

        # ═════════ D. repo zip parity (the v1.4.0 decoy) ═════════
        rep.section("D. repo/Pages zip parity")
        repo_zip = REPO / "tools" / "jh-tv-extension.zip"
        rv = None
        if repo_zip.exists():
            rv = json.loads(zf.ZipFile(io.BytesIO(
                repo_zip.read_bytes())).read("manifest.json")).get("version")
        rep.log(f"  repo zip version: {rv}")
        checks.append(("repo zip matches S3 (no stale Pages decoy)",
                       rv == man.get("version")))

        rep.section("VERDICT")
        for n, o in checks:
            rep.log(f"  {'✓' if o else '✗'} {n}")
        bad = [n for n, o in checks if not o]
        if bad:
            rep.log(f"✗ FAILED: {bad}")
            sys.exit(1)
        rep.log("✅ PASS_ALL — v1.7.9 + installer v4 live. Re-running the "
                "same .bat updates in place; one reload click and the "
                "agency-first walk starts immediately.")


if __name__ == "__main__":
    main()
