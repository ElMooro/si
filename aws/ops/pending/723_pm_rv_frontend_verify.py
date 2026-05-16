"""ops/723 — verify the two new frontend pages end-to-end.

commit "feat: PM Decision cockpit + Cross-Asset RV frontend pages" added
pm-decision.html and cross-asset-rv.html. Both fetch S3 JSON directly.

This script:
  1. Invokes justhodl-pm-decision and justhodl-cross-asset-rv so fresh
     output exists in S3.
  2. Reads data/pm-decision.json + data/cross-asset-rv.json and confirms
     every field the frontends render is present.
  3. Fetches the live GitHub Pages URLs and scans for page markers, and
     confirms index.html links both pages.
"""
import json, os, time, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config

BUCKET = "justhodl-dashboard-live"
cfg = Config(read_timeout=300, connect_timeout=20, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1")

report = {"ops": 723, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "pm-decision + cross-asset-rv frontend pages"}


def invoke(fn):
    try:
        r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse",
                       Payload=b"{}")
        body = r["Payload"].read().decode("utf-8", "replace") if r.get("Payload") else ""
        return {"status": r.get("StatusCode"), "fn_error": r.get("FunctionError"),
                "response": body[:500]}
    except Exception as e:
        return {"status": "error", "err": str(e)[:250]}


def read_s3(key):
    try:
        o = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(o["Body"].read()), o["LastModified"].isoformat()
    except Exception as e:
        return None, str(e)[:200]


def fetch(url):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl-ops/723"})
        with urllib.request.urlopen(req, timeout=25) as r:
            return r.status, r.read().decode("utf-8", "replace")
    except Exception as e:
        return None, str(e)[:200]


# ── 1. invoke both engines ──
report["invoke_pm"] = invoke("justhodl-pm-decision")
report["invoke_rv"] = invoke("justhodl-cross-asset-rv")
time.sleep(5)

# ── 2. read + validate S3 sidecars ──
pm, pm_lm = read_s3("data/pm-decision.json")
rv, rv_lm = read_s3("data/cross-asset-rv.json")
report["pm_last_modified"] = pm_lm
report["rv_last_modified"] = rv_lm

pm_checks, rv_checks = {}, {}
if isinstance(pm, dict):
    a = pm.get("actions") or {}
    mf = pm.get("macro_frame") or {}
    pm_checks = {
        "has_posture": bool(pm.get("posture")),
        "has_posture_word": bool(pm.get("posture_word")),
        "has_headline": bool(pm.get("headline")),
        "macro_frame_ok": all(k in mf for k in
                              ("regime", "defcon_level", "leading_markets_signal")),
        "actions_keys_ok": all(k in a for k in ("trim", "add", "hedge")),
        "has_portfolio": isinstance(pm.get("portfolio"), dict),
        "has_triggers": isinstance(pm.get("triggers"), list),
        "has_inputs_used": isinstance(pm.get("inputs_used"), dict),
    }
    report["pm_summary"] = {
        "posture_word": pm.get("posture_word"),
        "headline": (pm.get("headline") or "")[:160],
        "n_trim": len(a.get("trim") or []),
        "n_add": len(a.get("add") or []),
        "n_hedge": len(a.get("hedge") or []),
        "n_triggers": len(pm.get("triggers") or []),
        "defcon_level": mf.get("defcon_level"),
    }
else:
    report["pm_error"] = pm_lm

if isinstance(rv, dict):
    rels = rv.get("relationships") or []
    rv_checks = {
        "has_rv_state": bool(rv.get("rv_state")),
        "has_rv_read": bool(rv.get("rv_read")),
        "has_relationships": len(rels) > 0,
        "rel_fields_ok": all(
            all(k in r for k in ("key", "label", "state"))
            for r in rels) if rels else False,
        "n_dislocated_present": rv.get("n_dislocated") is not None,
        "n_stretched_present": rv.get("n_stretched") is not None,
        "has_interpretation": bool(rv.get("interpretation")),
    }
    scored = [r for r in rels if r.get("state") != "NO_DATA"]
    report["rv_summary"] = {
        "rv_state": rv.get("rv_state"),
        "n_relationships": len(rels),
        "n_scored": len(scored),
        "n_dislocated": rv.get("n_dislocated"),
        "n_stretched": rv.get("n_stretched"),
        "top_abs_z": max((r.get("abs_z") or 0 for r in scored), default=None),
    }
else:
    report["rv_error"] = rv_lm

# ── 3. live frontend pages ──
pages = {}
for name, marker in [("pm-decision.html", "PM Decision Layer"),
                     ("cross-asset-rv.html", "Cross-Asset Relative Value")]:
    st, html = fetch(f"https://justhodl.ai/{name}")
    pages[name] = {
        "http": st,
        "marker_found": (isinstance(html, str) and marker in html),
        "fetches_s3": (isinstance(html, str) and "justhodl-dashboard-live.s3" in html),
        "bytes": len(html) if isinstance(html, str) else 0,
    }
st, idx = fetch("https://justhodl.ai/index.html")
pages["index_links"] = {
    "http": st,
    "links_pm": (isinstance(idx, str) and "/pm-decision.html" in idx),
    "links_rv": (isinstance(idx, str) and "/cross-asset-rv.html" in idx),
}
report["pages"] = pages

# ── verdict ──
checks = {
    "pm_invoke_ok": report["invoke_pm"].get("status") == 200
                    and report["invoke_pm"].get("fn_error") is None,
    "rv_invoke_ok": report["invoke_rv"].get("status") == 200
                    and report["invoke_rv"].get("fn_error") is None,
    "pm_sidecar_valid": bool(pm_checks) and all(pm_checks.values()),
    "rv_sidecar_valid": bool(rv_checks) and all(rv_checks.values()),
    "pm_page_live": pages["pm-decision.html"]["marker_found"],
    "rv_page_live": pages["cross-asset-rv.html"]["marker_found"],
    "index_links_both": pages["index_links"]["links_pm"]
                        and pages["index_links"]["links_rv"],
}
report["pm_checks"] = pm_checks
report["rv_checks"] = rv_checks
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    "VERIFIED — both engines produce valid output, both pages render it live"
    if report["all_pass"]
    else "REVIEW — see failed checks (GH Pages can lag ~1min after push)")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/723_pm_rv_frontend_verify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/723_pm_rv_frontend_verify.json")
