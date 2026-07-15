"""ops 3342 — carry-surface v1.2.0: crypto revived (OKX, Binance was geo-blocked) +
commodity roll-yield winsorized (kill the +87/-80 USO/UNG blowups).

WHY:
  - CRYPTO n=0: compute_crypto_carry hit fapi.binance.com, which is geo-blocked from AWS
    Lambda IPs — all 10 errored, class showed empty. Re-sourced onto OKX public API
    (www.okx.com/api/v5/public/funding-rate), the proven-reachable source already used by
    justhodl-crypto-funding. Universe -> bare coins; MATIC dropped (delisted/rebranded POL),
    LTC added. Symbols now render as e.g. BTC-PERP.
  - COMMODITY ±80: old code annualized a 30-day ETF-vs-spot basis by a flat ×12, turning one
    volatile month into ±80% carry that owned the top and bottom of every board. Fixed:
    steadier 60-day window, annualize by actual window length (252/win) not ×12, winsorize
    the observed basis to ±40, anchor 75% structural / 25% observed, and a final ±70 hard cap
    (VXX structural -65 is the legitimate floor).

  Offline-tested (test_v12.py): USO -2.85 (was blowup), UNG -28.75 (near structural), quiet
  GLD ~0, extreme month winsorized to band; OKX 8h->annual math correct. ALL PASS.

VERIFY: redeploy, full invoke, read data/carry-surface.json and assert:
  (a) crypto count >= 6 (OKX reachable), (b) no commodity |carry| > 70, (c) version 1.2.0.
"""
import json
import time
from pathlib import Path

import boto3

from ops_report import report
from _lambda_deploy_helpers import deploy_lambda

FN = "justhodl-carry-surface"
SRC = Path(f"aws/lambdas/{FN}/source")
CFG = json.loads(Path(f"aws/lambdas/{FN}/config.json").read_text())
ENV = CFG["env"]
BUCKET = ENV["S3_BUCKET"]
OUT_KEY = ENV["OUT_KEY"]
DESCRIPTION = (CFG.get("description") or "")[:256]  # AWS 256-char cap

with report("3342_carry_surface_crypto_commodity") as r:
    r.section("Deploy carry-surface v1.2.0")
    smoke = deploy_lambda(
        report=r, function_name=FN, source_dir=SRC, env_vars=ENV,
        eb_rule_name=CFG["schedule"]["rule_name"], eb_schedule=CFG["schedule"]["cron"],
        timeout=CFG["timeout"], memory=CFG["memory"], description=DESCRIPTION,
        create_function_url=True, smoke=True,
    )
    r.log(f"smoke: {json.dumps(smoke)[:200] if smoke else 'None'}")

    r.section("Verify: invoke + inspect")
    lam = boto3.client("lambda", region_name=CFG["region"])
    inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
    r.log(f"invoke status={inv.get('StatusCode')} err={inv.get('FunctionError')}")

    time.sleep(3)
    s3 = boto3.client("s3", region_name=CFG["region"])
    payload = json.loads(s3.get_object(Bucket=BUCKET, Key=OUT_KEY)["Body"].read().decode())

    assets = payload.get("all_assets", [])
    crypto = [a for a in assets if a.get("asset_class") == "crypto" and a.get("carry_pct") is not None]
    commod = [a for a in assets if a.get("asset_class") == "commodity" and a.get("carry_pct") is not None]
    over70 = [a for a in commod if abs(a["carry_pct"]) > 70]
    r.log(f"version={payload.get('version')} n_assets={payload.get('n_assets')}")
    r.log(f"crypto live: {len(crypto)} -> {[(a['symbol'], round(a['carry_pct'],1)) for a in crypto[:5]]}")
    r.log(f"commodity: {[(a['symbol'], a['carry_pct']) for a in commod]}")

    if len(crypto) >= 6:
        r.ok(f"CRYPTO REVIVED — {len(crypto)} perps live via OKX (was 0).")
    else:
        r.fail(f"crypto still short: {len(crypto)} live (OKX reachability?).")

    if not over70:
        r.ok(f"COMMODITY WINSORIZED — no |carry| > 70 (max={max((abs(a['carry_pct']) for a in commod), default=0):.1f}).")
    else:
        r.fail(f"commodity blowup persists: {[(a['symbol'], a['carry_pct']) for a in over70]}")

    if payload.get("version") == "1.2.0":
        r.ok("version 1.2.0 live")
    else:
        r.fail(f"version mismatch: {payload.get('version')}")
