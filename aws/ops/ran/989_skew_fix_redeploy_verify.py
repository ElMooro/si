"""
ops 989 - Skew-Tail-Hedging Fix Redeploy + Tier-4 Final Verify
===============================================================

After ops 988 the Tier-4 cluster had 5/6 engines live. Only
skew-tail-hedging failed (state=None, error="SKEW quote unavailable
from FMP and AlphaVantage"). Root cause: FMP doesn't carry ^SKEW.

Commit b91fb0ae patched the source to add FRED + Yahoo as primary
fallbacks (FRED is the proven SKEW data path used by anomaly-detector
and options-flow). Now redeploy that one Lambda + re-invoke all 6
Tier-4 + verify everything end-to-end.

  1. Update skew-tail-hedging code (zip + update_function_code)
  2. wait_for_settled
  3. Re-invoke skew-tail-hedging, sleep 5s, read S3, verify state
     is now populated (not None)
  4. Invoke all 6 Tier-4 engines fresh (long timeout) + read S3
  5. Re-invoke signal-board, verify 48/48 live
  6. Fetch retail-edges.html, verify 27 markers
  7. Telegram summary with full scorecard
"""
import io
import json
import os
import sys
import time
import traceback
import urllib.request
import zipfile
from pathlib import Path

import boto3
from botocore.config import Config

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"
REPO_ROOT = Path(__file__).resolve().parents[3]
FIX_TARGET = "justhodl-skew-tail-hedging"

ENGINES = [
    ("justhodl-post-earnings-mean-rev", "data/post-earnings-mean-rev.json"),
    ("justhodl-insider-sell-cluster",   "data/insider-sell-cluster.json"),
    ("justhodl-vix9d-vix-inversion",    "data/vix9d-vix-inversion.json"),
    ("justhodl-breadth-divergence",     "data/breadth-divergence.json"),
    ("justhodl-skew-tail-hedging",      "data/skew-tail-hedging.json"),
    ("justhodl-dxy-equity-divergence",  "data/dxy-equity-divergence.json"),
]
SIGNAL_BOARD_FN = "justhodl-signal-board"

PAGE_MARKERS = [
    "earnings-iv-crush", "stealth-accumulation", "failed-pattern-reversal",
    "squeeze-pretrigger", "catalyst-skew-premove", "crypto-etf-arb", "lockup-expiration",
    "precatalyst-vol-expansion", "cef-discount", "reit-nav-discount", "divcut-warning",
    "rating-change-cluster", "multi-tf-convergence", "52wk-quality-breakout", "spac-floor-warrant",
    "vvix-vov-regime", "sympathetic-momentum", "insider-buyback-confluence",
    "gap-fill-confirm", "13f-price-divergence", "credit-equity-divergence",
    "post-earnings-mean-rev", "insider-sell-cluster", "vix9d-vix-inversion",
    "breadth-divergence", "skew-tail-hedging", "dxy-equity-divergence",
]

TELEGRAM_BOT_TOKEN = "8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs"
TELEGRAM_CHAT_ID = "8678089260"


def long_client():
    cfg = Config(connect_timeout=10, read_timeout=900,
                 retries={"max_attempts": 0})
    return boto3.client("lambda", region_name=REGION, config=cfg)


def zip_source_dir(src_dir):
    buf = io.BytesIO()
    src_path = Path(src_dir)
    if not src_path.is_dir():
        return None, f"source dir not found: {src_dir}"
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for fp in src_path.rglob("*"):
            if fp.is_file() and "__pycache__" not in fp.parts:
                zf.write(fp, fp.relative_to(src_path))
    buf.seek(0)
    return buf.getvalue(), None


def wait_settled(lambda_c, name, max_wait=180):
    deadline = time.time() + max_wait
    last = {}
    while time.time() < deadline:
        try:
            r = lambda_c.get_function_configuration(FunctionName=name)
            state = r.get("State")
            last_status = r.get("LastUpdateStatus")
            last = {"state": state, "last_status": last_status}
            if state == "Active" and last_status in ("Successful", None):
                return {"ok": True, **last}
            if state == "Failed" or last_status == "Failed":
                return {"ok": False, **last, "fatal": True}
        except Exception as e:
            last = {"err": str(e)[:200]}
        time.sleep(3)
    return {"ok": False, **last, "timeout": True}


def invoke(name):
    lc = long_client()
    try:
        r = lc.invoke(FunctionName=name, InvocationType="RequestResponse",
                      Payload=b"{}")
        outer = r["StatusCode"]
        fn_err = r.get("FunctionError")
        body = r["Payload"].read().decode("utf-8", errors="ignore")
        inner = None
        parsed = None
        try:
            parsed = json.loads(body)
            inner = parsed.get("statusCode")
        except Exception:
            parsed = {"raw": body[:500]}
        return {
            "ok": outer == 200 and not fn_err
                  and (inner is None or inner < 400),
            "outer_status": outer, "inner_status": inner,
            "function_error": fn_err,
            "body_preview": str(parsed)[:500],
        }
    except Exception as e:
        return {"ok": False, "error": str(e)[:400]}


def read_s3(s3, key):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        data = json.loads(obj["Body"].read())
        is_err = "error" in data and not data.get("state")
        return {
            "ok": not is_err and bool(data.get("state"))
                  and data.get("state") != "ERROR"
                  and data.get("state") != "DATA_UNAVAILABLE",
            "size": obj["ContentLength"],
            "last_modified": obj["LastModified"].isoformat(),
            "engine": data.get("engine"),
            "state": data.get("state"),
            "signal_strength": data.get("signal_strength"),
            "as_of": data.get("as_of"),
            "n_picks": (data.get("n_setups") or data.get("n_tickets")
                        or data.get("n_clusters") or 0),
            "sources_used": (data.get("current_metrics") or {}).get(
                "sources_used"),
            "error_in_payload": data.get("error") if is_err else None,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}


def fetch_page(url="https://justhodl.ai/retail-edges.html"):
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "ops989/1.0",
                          "Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=20) as r:
            html = r.read().decode("utf-8", errors="ignore")
        markers = {n: n in html for n in PAGE_MARKERS}
        return {
            "ok": True, "status": r.status, "size": len(html),
            "markers_found": sum(markers.values()),
            "expected": len(PAGE_MARKERS),
            "missing": [n for n, v in markers.items() if not v],
        }
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}


def verify_signal_board(s3):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key="data/signal-board.json")
        data = json.loads(obj["Body"].read())
        return {
            "ok": True,
            "n_engines": data.get("n_engines"),
            "n_live": data.get("n_live"),
            "n_stale": data.get("n_stale"),
            "composite_posture": data.get("composite_posture"),
            "composite_signal": data.get("composite_signal"),
            "expects_48": data.get("n_engines") == 48,
            "all_48_live": data.get("n_live") == 48,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}


def telegram_alert(text):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = json.dumps({
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text, "parse_mode": "Markdown",
        }).encode("utf-8")
        req = urllib.request.Request(
            url, data=data,
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status == 200
    except Exception as e:
        print(f"[telegram] {type(e).__name__}: {str(e)[:120]}")
        return False


def main():
    print("=" * 70)
    print("ops 989 -- skew-tail-hedging fix redeploy + Tier-4 final verify")
    print("=" * 70)
    lambda_c = boto3.client("lambda", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    report = {
        "ops": 989,
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    try:
        # 1. Update skew-tail-hedging code from patched source
        print(f"\n=== Step 1: redeploy {FIX_TARGET} ===")
        src_dir = REPO_ROOT / "aws" / "lambdas" / FIX_TARGET / "source"
        zip_bytes, err = zip_source_dir(str(src_dir))
        if err:
            report["fix_deploy"] = {"ok": False, "error": err}
            print(f"FATAL: {err}")
        else:
            print(f"  zip: {len(zip_bytes)} bytes")
            try:
                lambda_c.update_function_code(
                    FunctionName=FIX_TARGET, ZipFile=zip_bytes)
                settled = wait_settled(lambda_c, FIX_TARGET, max_wait=180)
                print(f"  wait_settled: {settled}")
                report["fix_deploy"] = {
                    "ok": settled.get("ok"),
                    "zip_bytes": len(zip_bytes),
                    "settled": settled,
                }
            except Exception as e:
                report["fix_deploy"] = {"ok": False, "error": str(e)[:300],
                                        "tb": traceback.format_exc()[-600:]}
                print(f"  FATAL: {e}")

        # 2. Re-invoke all 6 Tier-4 engines fresh
        print("\n=== Step 2: invoke all 6 Tier-4 engines ===")
        report["engines"] = {}
        for name, key in ENGINES:
            print(f"\n--- {name} ---")
            inv = invoke(name)
            print(f"  invoke ok={inv.get('ok')} outer={inv.get('outer_status')} "
                  f"inner={inv.get('inner_status')} "
                  f"err={inv.get('function_error')}")
            if inv.get("body_preview"):
                print(f"  body: {inv['body_preview'][:280]}")
            time.sleep(3)
            s3v = read_s3(s3, key)
            print(f"  s3: ok={s3v.get('ok')} state={s3v.get('state')} "
                  f"strength={s3v.get('signal_strength')} "
                  f"picks={s3v.get('n_picks')} size={s3v.get('size')} "
                  f"sources={s3v.get('sources_used')}")
            if s3v.get("error_in_payload"):
                print(f"  PAYLOAD ERR: {s3v['error_in_payload'][:200]}")
            report["engines"][name] = {"invoke": inv, "s3": s3v}

        # 3. Re-invoke signal-board
        print("\n=== Step 3: signal-board re-invoke ===")
        sb_inv = invoke(SIGNAL_BOARD_FN)
        print(f"  invoke ok={sb_inv.get('ok')} inner={sb_inv.get('inner_status')}")
        if sb_inv.get("body_preview"):
            print(f"  body: {sb_inv['body_preview'][:400]}")
        report["signal_board_invoke"] = sb_inv
        time.sleep(4)
        sb_v = verify_signal_board(s3)
        print(f"  n_engines={sb_v.get('n_engines')} n_live={sb_v.get('n_live')} "
              f"posture={sb_v.get('composite_posture')} "
              f"signal={sb_v.get('composite_signal')}")
        report["signal_board"] = sb_v

        # 4. Page check
        print("\n=== Step 4: page check ===")
        page = fetch_page()
        print(f"  ok={page.get('ok')} "
              f"markers={page.get('markers_found')}/{page.get('expected')}")
        if page.get("missing"):
            print(f"  MISSING: {page['missing']}")
        report["page"] = page

        # 5. Scorecard
        engines = report["engines"]
        n_inv = sum(1 for e in engines.values() if e["invoke"].get("ok"))
        n_s3 = sum(1 for e in engines.values() if e["s3"].get("ok"))
        n_real = sum(1 for e in engines.values()
                     if e["s3"].get("state")
                     and e["s3"]["state"] not in ("ERROR", "DATA_UNAVAILABLE"))
        sb_ok = sb_v.get("expects_48") and sb_v.get("all_48_live")
        page_ok = page.get("ok") and page.get("markers_found") == 27
        scorecard = {
            "fix_deploy_ok": report.get("fix_deploy", {}).get("ok", False),
            "n_engines_total": len(ENGINES),
            "n_invoke_ok": n_inv,
            "n_s3_ok": n_s3,
            "n_real_state": n_real,
            "signal_board_48_48_ok": sb_ok,
            "page_27_markers_ok": page_ok,
            "all_pass": (report.get("fix_deploy", {}).get("ok", False)
                         and n_inv == 6 and n_s3 == 6 and n_real == 6
                         and sb_ok and page_ok),
        }
        report["scorecard"] = scorecard
        report["ended_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        print("\n" + "=" * 70)
        print(f"SCORECARD: {json.dumps(scorecard, indent=2)}")
        print("=" * 70)

        # 6. Telegram
        emoji = "✅" if scorecard["all_pass"] else "⚠️"
        engine_lines = []
        for name, e in engines.items():
            short = name.replace("justhodl-", "")
            state = e.get("s3", {}).get("state") or "—"
            strength = e.get("s3", {}).get("signal_strength")
            picks = e.get("s3", {}).get("n_picks", 0)
            engine_lines.append(
                f"`{short}` {state} (s={strength}, n={picks})")
        msg = (
            f"{emoji} *Tier-4 retail edges -- DEPLOY COMPLETE*\n"
            f"fix_deploy: {scorecard['fix_deploy_ok']}\n"
            f"invoke {n_inv}/6 | s3 {n_s3}/6 | real {n_real}/6\n"
            f"signal-board: {sb_v.get('n_engines')}/{sb_v.get('n_live')} live "
            f"({sb_v.get('composite_posture')} {sb_v.get('composite_signal')})\n"
            f"page: {page.get('markers_found')}/27\n\n"
            + "\n".join(engine_lines)
            + f"\n\nall_pass: *{scorecard['all_pass']}*"
        )
        sent = telegram_alert(msg)
        report["telegram_sent"] = sent
        print(f"\nTelegram: {sent}")
    finally:
        out_dir = REPO_ROOT / "aws" / "ops" / "reports"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "989.json"
        try:
            out_path.write_text(json.dumps(report, indent=2, default=str))
            print(f"\nReport: {out_path.relative_to(REPO_ROOT)}")
        except Exception as wex:
            print(f"\nReport write FAILED: {wex}")
        print("\n=== FULL REPORT JSON ===")
        print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception as e:
        print(f"FATAL: {e}\n{traceback.format_exc()}")
        sys.exit(1)
