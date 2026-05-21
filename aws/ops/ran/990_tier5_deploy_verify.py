"""
ops 990 - Tier-5 Retail Edges End-to-End Verify
=================================================

After commit 3e3fb865 pushed 6 new Tier-5 retail-edge Lambdas + commit
3a7f7f4a wired them into signal-board (48->54) + retail-edges.html
(27->33 cards). This ops verifies the whole chain end-to-end.

  1. Wait for CI to materialize all 6 new Lambdas (poll up to 10 min)
  2. Inject env vars from proven donor justhodl-earnings-pead (FMP_KEY,
     FRED_KEY, ALPHA_VANTAGE_KEY, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID).
     Saga lesson from Tier-4 ops 988: don't trust deploy-lambdas.yml to
     inject everything correctly for brand-new Lambdas; patch directly.
  3. wait_for_settled, then invoke each Lambda fresh (long timeout for
     slow scanners: earnings-quality=720s, buyback-yield=540s)
  4. Read S3 outputs, verify state populated and not error-stub
  5. Re-invoke signal-board, verify 54/54 engines live
  6. Fetch retail-edges.html, verify 33 markers present
  7. Telegram scorecard summary

Doctrines applied (from Tier-3+4 saga):
- read_timeout=900 (longest scanner is 720s; need margin)
- Inner statusCode parse (catches handler 500s vs outer transport 200)
- "error_stub" detection: data has "error" key but no "state" key
- Graceful DATA_UNAVAILABLE state OK (not pass, not fail; reported)
- try/finally with stdout JSON dump (report always written)
- Direct env injection from donor (deploy-lambdas.yml unreliable for new)
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

DONOR_FN = "justhodl-earnings-pead"  # has FMP_KEY, FRED_KEY, AV, TG
BASELINE_KEYS = [
    "FMP_KEY", "FRED_KEY", "ALPHA_VANTAGE_KEY",
    "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID", "TELEGRAM_TOKEN",
]

ENGINES = [
    ("justhodl-gold-equity-rotation",   "data/gold-equity-rotation.json"),
    ("justhodl-buyback-yield-ranking",  "data/buyback-yield-ranking.json"),
    ("justhodl-put-call-extreme",       "data/put-call-extreme.json"),
    ("justhodl-cta-trend-exhaust",      "data/cta-trend-exhaust.json"),
    ("justhodl-ndx-spx-spread",         "data/ndx-spx-spread.json"),
    ("justhodl-earnings-quality",       "data/earnings-quality.json"),
]
SIGNAL_BOARD_FN = "justhodl-signal-board"

# All 33 page markers (existing 27 from Tier 1-4 + 6 new Tier-5)
PAGE_MARKERS = [
    # Tier 1
    "earnings-iv-crush", "stealth-accumulation", "failed-pattern-reversal",
    "squeeze-pretrigger", "catalyst-skew-premove", "crypto-etf-arb",
    "lockup-expiration",
    # Tier 2
    "precatalyst-vol-expansion", "cef-discount", "reit-nav-discount",
    "divcut-warning", "rating-change-cluster", "multi-tf-convergence",
    "52wk-quality-breakout", "spac-floor-warrant",
    # Tier 3
    "vvix-vov-regime", "sympathetic-momentum", "insider-buyback-confluence",
    "gap-fill-confirm", "13f-price-divergence", "credit-equity-divergence",
    # Tier 4
    "post-earnings-mean-rev", "insider-sell-cluster", "vix9d-vix-inversion",
    "breadth-divergence", "skew-tail-hedging", "dxy-equity-divergence",
    # Tier 5
    "gold-equity-rotation", "buyback-yield-ranking", "put-call-extreme",
    "cta-trend-exhaust", "ndx-spx-spread", "earnings-quality",
]

TELEGRAM_BOT_TOKEN = "8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs"
TELEGRAM_CHAT_ID = "8678089260"


def long_client():
    cfg = Config(connect_timeout=10, read_timeout=900,
                 retries={"max_attempts": 0})
    return boto3.client("lambda", region_name=REGION, config=cfg)


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


def wait_for_lambdas_to_exist(lambda_c, names, max_wait=600):
    """Poll for all Lambdas to materialize from CI deploy."""
    deadline = time.time() + max_wait
    pending = set(names)
    iteration = 0
    while pending and time.time() < deadline:
        iteration += 1
        ready = []
        for name in list(pending):
            try:
                lambda_c.get_function_configuration(FunctionName=name)
                ready.append(name)
            except lambda_c.exceptions.ResourceNotFoundException:
                pass
            except Exception:
                pass
        for r in ready:
            pending.discard(r)
        if pending:
            print(f"  [poll {iteration}] {len(names)-len(pending)}/{len(names)} ready, "
                  f"waiting on: {sorted(pending)}")
            time.sleep(20)
    return {"ok": not pending, "missing": sorted(pending),
            "elapsed_s": round(max_wait - (deadline - time.time()), 0)}


def get_donor_env(lambda_c):
    """Read donor's env vars to seed new Lambdas."""
    try:
        cfg = lambda_c.get_function_configuration(FunctionName=DONOR_FN)
        env = (cfg.get("Environment") or {}).get("Variables") or {}
        donor = {k: env[k] for k in BASELINE_KEYS if k in env}
        return donor
    except Exception as e:
        return {"_ERROR": str(e)[:300]}


def patch_env(lambda_c, name, donor_env):
    """Merge donor env into Lambda's existing env (don't overwrite Lambda's own keys)."""
    try:
        cfg = lambda_c.get_function_configuration(FunctionName=name)
        existing = (cfg.get("Environment") or {}).get("Variables") or {}
        merged = dict(donor_env)
        merged.update(existing)  # existing wins (e.g. S3_BUCKET set in config.json)
        # Filter out any error markers
        merged = {k: v for k, v in merged.items() if not k.startswith("_")}
        keys_changed = sorted(set(merged.keys()) - set(existing.keys()))
        if not keys_changed:
            return {"ok": True, "no_change": True}
        lambda_c.update_function_configuration(
            FunctionName=name, Environment={"Variables": merged})
        settled = wait_settled(lambda_c, name, max_wait=120)
        return {"ok": settled.get("ok"), "keys_added": keys_changed,
                "settled": settled}
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}


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
        is_err_stub = "error" in data and not data.get("state")
        state = data.get("state")
        is_real = (state and state not in
                   ("ERROR", "DATA_UNAVAILABLE") and not is_err_stub)
        return {
            "ok": not is_err_stub and bool(state),  # S3 ok = has state
            "real_state": is_real,                  # real_state = not DATA_UNAVAILABLE
            "size": obj["ContentLength"],
            "last_modified": obj["LastModified"].isoformat(),
            "engine": data.get("engine"),
            "state": state,
            "signal_strength": data.get("signal_strength"),
            "as_of": data.get("as_of"),
            "n_picks": (data.get("n_setups") or data.get("n_tickets")
                        or data.get("n_clusters") or 0),
            "error_in_payload": data.get("error") if is_err_stub else None,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}


def fetch_page(url="https://justhodl.ai/retail-edges.html"):
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "ops990/1.0",
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
            "expects_54": data.get("n_engines") == 54,
            "all_54_live": data.get("n_live") == 54,
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
    print("ops 990 -- Tier-5 retail edges end-to-end verify")
    print("=" * 70)
    lambda_c = boto3.client("lambda", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    report = {
        "ops": 990,
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "donor": DONOR_FN,
        "baseline_env_keys": BASELINE_KEYS,
        "engines_expected": [e[0] for e in ENGINES],
    }
    try:
        # 1. Wait for CI to materialize Lambdas
        print("\n=== Step 1: wait for CI deploy ===")
        names = [e[0] for e in ENGINES]
        # Initial CI wait (deploy-lambdas.yml takes ~3-5 min)
        print("  initial sleep 180s (CI deploy window)...")
        time.sleep(180)
        exist = wait_for_lambdas_to_exist(lambda_c, names, max_wait=480)
        print(f"  exist: ok={exist['ok']} elapsed={exist['elapsed_s']}s "
              f"missing={exist['missing']}")
        report["ci_wait"] = exist
        if not exist["ok"]:
            raise RuntimeError(
                f"CI deploy incomplete; missing: {exist['missing']}")

        # 2. Inject env vars from donor
        print(f"\n=== Step 2: env injection from donor {DONOR_FN} ===")
        donor_env = get_donor_env(lambda_c)
        present = sorted([k for k in donor_env.keys() if not k.startswith("_")])
        print(f"  donor env keys: {present}")
        report["donor_env_keys_present"] = present
        report["env_patches"] = {}
        for name in names:
            res = patch_env(lambda_c, name, donor_env)
            print(f"  patch {name}: ok={res.get('ok')} "
                  f"keys_added={res.get('keys_added')}")
            report["env_patches"][name] = res

        # 3. Invoke each engine + read S3
        print("\n=== Step 3: invoke 6 engines + verify S3 ===")
        report["engines"] = {}
        for name, key in ENGINES:
            print(f"\n--- {name} ---")
            inv = invoke(name)
            print(f"  invoke ok={inv.get('ok')} outer={inv.get('outer_status')} "
                  f"inner={inv.get('inner_status')} "
                  f"err={inv.get('function_error')}")
            if inv.get("body_preview"):
                print(f"  body: {inv['body_preview'][:300]}")
            time.sleep(3)
            s3v = read_s3(s3, key)
            print(f"  s3: ok={s3v.get('ok')} state={s3v.get('state')} "
                  f"real={s3v.get('real_state')} "
                  f"strength={s3v.get('signal_strength')} "
                  f"picks={s3v.get('n_picks')} size={s3v.get('size')}")
            if s3v.get("error_in_payload"):
                print(f"  PAYLOAD ERR: {s3v['error_in_payload'][:200]}")
            report["engines"][name] = {"invoke": inv, "s3": s3v}

        # 4. Re-invoke signal-board (it must have been redeployed by 3a7f7f4a)
        print("\n=== Step 4: signal-board re-invoke + verify 54/54 ===")
        # Wait a moment for signal-board to also be deployed by CI
        sb_exist = wait_for_lambdas_to_exist(lambda_c, [SIGNAL_BOARD_FN],
                                              max_wait=60)
        sb_settled = wait_settled(lambda_c, SIGNAL_BOARD_FN, max_wait=90)
        print(f"  signal-board settled: {sb_settled}")
        sb_inv = invoke(SIGNAL_BOARD_FN)
        print(f"  invoke ok={sb_inv.get('ok')} inner={sb_inv.get('inner_status')}")
        if sb_inv.get("body_preview"):
            print(f"  body: {sb_inv['body_preview'][:400]}")
        report["signal_board_invoke"] = sb_inv
        time.sleep(5)
        sb_v = verify_signal_board(s3)
        print(f"  n_engines={sb_v.get('n_engines')} n_live={sb_v.get('n_live')} "
              f"posture={sb_v.get('composite_posture')} "
              f"signal={sb_v.get('composite_signal')}")
        report["signal_board"] = sb_v

        # 5. Page check (33 markers)
        print("\n=== Step 5: page check 33 markers ===")
        page = fetch_page()
        print(f"  ok={page.get('ok')} "
              f"markers={page.get('markers_found')}/{page.get('expected')}")
        if page.get("missing"):
            print(f"  MISSING: {page['missing']}")
        report["page"] = page

        # 6. Scorecard
        engines = report["engines"]
        n_inv = sum(1 for e in engines.values() if e["invoke"].get("ok"))
        n_s3 = sum(1 for e in engines.values() if e["s3"].get("ok"))
        n_real = sum(1 for e in engines.values() if e["s3"].get("real_state"))
        sb_ok = sb_v.get("expects_54") and sb_v.get("all_54_live")
        page_ok = page.get("ok") and page.get("markers_found") == 33
        scorecard = {
            "ci_wait_ok": exist["ok"],
            "n_engines_total": len(ENGINES),
            "n_invoke_ok": n_inv,
            "n_s3_ok": n_s3,
            "n_real_state": n_real,
            "signal_board_54_54_ok": sb_ok,
            "page_33_markers_ok": page_ok,
            "all_pass": (exist["ok"]
                         and n_inv == 6 and n_s3 == 6 and n_real == 6
                         and sb_ok and page_ok),
        }
        report["scorecard"] = scorecard
        report["ended_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        print("\n" + "=" * 70)
        print(f"SCORECARD: {json.dumps(scorecard, indent=2)}")
        print("=" * 70)

        # 7. Telegram
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
            f"{emoji} *Tier-5 retail edges -- DEPLOY COMPLETE*\n"
            f"CI wait: {scorecard['ci_wait_ok']}\n"
            f"invoke {n_inv}/6 | s3 {n_s3}/6 | real {n_real}/6\n"
            f"signal-board: {sb_v.get('n_engines')}/{sb_v.get('n_live')} live "
            f"({sb_v.get('composite_posture')} {sb_v.get('composite_signal')})\n"
            f"page: {page.get('markers_found')}/33\n\n"
            + "\n".join(engine_lines)
            + f"\n\nall_pass: *{scorecard['all_pass']}*"
        )
        sent = telegram_alert(msg)
        report["telegram_sent"] = sent
        print(f"\nTelegram: {sent}")
    except Exception as ex:
        report["fatal_error"] = str(ex)[:600]
        report["fatal_trace"] = traceback.format_exc()[-1500:]
        print(f"\nFATAL: {ex}")
    finally:
        out_dir = REPO_ROOT / "aws" / "ops" / "reports"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "990.json"
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
