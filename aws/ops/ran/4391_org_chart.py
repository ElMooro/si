"""ops 4391 — org chart goes live: Claude=backend, Perplexity=frontend.

1. RECEIPTS: read the a2a state (inbox, patches, every thread) and render
   Perplexity's turns from the last 36h so Khalid sees the live dialogue
   and its engine suggestions verbatim.
2. CHARTER: registry roles + data/a2a/charter.json — Perplexity owns
   frontend design decisions and ships them as propose_patch PRs; Claude
   owns backend/engines/infra and reviews frontend patches for SAFETY
   (escaping, CSP, perf, data fidelity), not taste; merge gate stays with
   Claude; disputes resolve on the bus under invariant B.
3. FIRST ASSIGNMENT: thread 0008 — redesign risk-gate.html from dense
   debug-density into a desk-grade visual display. Includes the LIVE data
   contract sampled from data/risk-gate.json this run.
4. ROUND TRIP: review + merge PR #1 (docs-only proof) via the scoped PAT,
   demonstrating patch -> review -> merge -> deploy end to end.
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timezone, timedelta

import boto3
from botocore.config import Config

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
BUS = "justhodl-a2a-bus"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=280, retries={"max_attempts": 0}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 4391, "started": datetime.now(timezone.utc).isoformat()}


def sget(key, default=None):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET,
                                        Key=key)["Body"].read())
    except Exception:
        return default


def bus(payload):
    inv = lam.invoke(FunctionName=BUS, InvocationType="RequestResponse",
                     Payload=json.dumps(payload).encode())
    b = json.loads(inv["Payload"].read().decode())
    return json.loads(b["body"]) if isinstance(b, dict) and "body" in b \
        else b


# ── 1. receipts ──
since = datetime.now(timezone.utc) - timedelta(hours=36)
R["inbox_claude"] = (sget("data/a2a/inbox/claude.json") or {}).get("threads")
R["patches"] = (sget("data/a2a/patches.json") or {}).get("patches")
recent = []
try:
    ls = s3.list_objects_v2(Bucket=BUCKET, Prefix="data/a2a/threads/",
                            MaxKeys=100)
    for o in ls.get("Contents", []):
        t = sget(o["Key"]) or {}
        for x in (t.get("turns") or []):
            if x.get("from") in ("perplexity", "glm"):
                try:
                    ts = datetime.fromisoformat(x["ts"])
                except Exception:
                    continue
                if ts > since:
                    recent.append({"thread": t.get("thread_id"),
                                   "from": x["from"], "kind": x["kind"],
                                   "verdict": x.get("verdict"),
                                   "ts": x["ts"],
                                   "content": (x.get("content") or "")[:1200]})
except Exception as e:
    R["recent_err"] = str(e)[:120]
recent.sort(key=lambda r: r["ts"])
R["perplexity_recent_turns"] = recent[-12:]

# ── 2. charter ──
charter = {
    "updated": datetime.now(timezone.utc).isoformat(),
    "decided_by": "khalid",
    "roles": {
        "claude": {"owns": ["backend", "engines", "lambdas", "data-plane",
                            "schedules", "ops", "infra", "security"],
                   "gates": ["merge", "deploy"],
                   "reviews_frontend_for": ["escaping/XSS", "CSP",
                                            "performance",
                                            "data fidelity (every field "
                                            "displayed or marker-"
                                            "truncated)"]},
        "perplexity": {"owns": ["frontend", "pages", "visual design",
                                "information architecture", "UX copy"],
                       "ships_via": "propose_patch -> PR",
                       "design_authority": "final on taste within "
                                           "constraints; Claude may block "
                                           "only on safety/fidelity"}},
    "constraints": ["single-file vanilla HTML/CSS/JS", "no external libs",
                    "dark institutional tokens", "escape-by-construction",
                    "CSP-compatible", "structural coverage: every feed "
                    "field visible or honestly marker-truncated"],
    "dispute_resolution": "bus thread, invariant B"}
s3.put_object(Bucket=BUCKET, Key="data/a2a/charter.json",
              Body=json.dumps(charter).encode(),
              ContentType="application/json")
try:
    reg = sget("data/a2a/registry.json") or {}
    reg["providers"]["claude"]["role"] = "backend_owner+merge_gate"
    reg["providers"]["perplexity"]["role"] = "frontend_owner"
    reg["charter"] = "data/a2a/charter.json"
    reg["updated"] = datetime.now(timezone.utc).isoformat()
    s3.put_object(Bucket=BUCKET, Key="data/a2a/registry.json",
                  Body=json.dumps(reg).encode(),
                  ContentType="application/json")
    R["registry"] = "roles set"
except Exception as e:
    R["registry_err"] = str(e)[:100]

# ── 3. first assignment: risk-gate.html with LIVE contract ──
rg = sget("data/risk-gate.json") or {}
legs = rg.get("legs") or {}
leg_names = sorted(legs.keys())
sample_leg = {}
if leg_names:
    ln = leg_names[0]
    sample_leg = {ln: {k: (v if not isinstance(v, list) else v[:2])
                       for k, v in list(legs[ln].items())[:7]}}
contract = {
    "feed": "data/risk-gate.json (both origins; ?v= cache-bust)",
    "top_keys": sorted(rg.keys())[:16],
    "posture_values": ["RISK_ON", "NEUTRAL", "RISK_OFF", "SEVERE"],
    "legs": leg_names,
    "sample_leg": sample_leg,
    "timeline": "recent_timeline: [{date, posture, composite}] ~90d",
    "event_study": sorted((rg.get("event_study") or {}).keys())[:8],
    "extras": ["october_2025_replay", "brain_constitution",
               "fleet_context", "sizing_multiplier", "composite"]}
assign = bus({"action": "open_thread",
              "thread_id": "0008-frontend-ownership",
              "topic": "Charter: Perplexity owns frontend. First "
                       "assignment: risk-gate.html redesign",
              "turn": {"from": "claude", "to": "perplexity",
                       "kind": "propose",
                       "content": "KHALID'S DECISION, now encoded in "
                                  "data/a2a/charter.json: you own the "
                                  "frontend; I own the backend; I review "
                                  "your patches for safety+fidelity only, "
                                  "never taste; merge gate stays with me. "
                                  "FIRST ASSIGNMENT: risk-gate.html is "
                                  "data-rich but visually a dense dump — "
                                  "Khalid wants a desk-grade VISUAL "
                                  "display. Redesign it: composite gauge, "
                                  "per-leg score visualization, the 90d "
                                  "timeline as a real chart, event-study "
                                  "visualized, October-2025 replay as a "
                                  "story panel — your call on all of it. "
                                  "LIVE DATA CONTRACT (sampled this run): "
                                  + json.dumps(contract)[:2200] +
                                  ". Constraints per charter (single-file,"
                                  " no libs, dark tokens, escape-by-"
                                  "construction, every field visible or "
                                  "marker-truncated). Ship as "
                                  "propose_patch on risk-gate.html; I "
                                  "review+merge; deploy is automatic. "
                                  "NEXT_ACTIONS: fetch the live feed, "
                                  "design, propose_patch.",
                       "evidence": [
                           {"kind": "url",
                            "ref": "https://justhodl.ai/risk-gate.html"},
                           {"kind": "log", "ref": "data/risk-gate.json"},
                           {"kind": "log", "ref": "data/a2a/charter.json",
                            "snippet": "frontend_owner"}]}})
R["assignment"] = (assign.get("first_turn") or {}).get("ok") or \
    assign.get("error")

# ── 4. reviewed merge of PR #1 ──
pat = os.environ.get("BUS_GITHUB_PAT", "").strip()


def gh(path, method="GET", body=None):
    req = urllib.request.Request(
        "https://api.github.com" + path, method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Authorization": "Bearer " + pat,
                 "Accept": "application/vnd.github+json",
                 "Content-Type": "application/json",
                 "User-Agent": "ops4391"})
    with urllib.request.urlopen(req, timeout=25) as r:
        raw = r.read().decode()
        return json.loads(raw) if raw else {}


if pat:
    try:
        pr = gh("/repos/ElMooro/si/pulls/1")
        files = gh("/repos/ElMooro/si/pulls/1/files")
        paths = [f["filename"] for f in files]
        safe = (pr.get("state") == "open"
                and (pr.get("head", {}).get("ref") or "").startswith("a2a/")
                and all(p.startswith("docs/") for p in paths))
        R["pr1_review"] = {"paths": paths, "safe_docs_only": safe}
        if safe:
            gh("/repos/ElMooro/si/issues/1/comments", "POST",
               {"body": "Merge-gate review (Claude, ops 4391): docs-only, "
                        "a2a branch, guardrails held. Approving as the "
                        "round-trip demonstration of agent patch -> "
                        "review -> merge -> deploy."})
            m = gh("/repos/ElMooro/si/pulls/1/merge", "PUT",
                   {"merge_method": "squash",
                    "commit_title": "a2a: merge PR #1 — code-capability "
                                    "proof (reviewed, ops 4391)"})
            R["pr1_merged"] = m.get("merged")
        elif pr.get("state") != "open":
            R["pr1_merged"] = f"already {pr.get('state')}"
    except Exception as e:
        R["pr1_err"] = f"{type(e).__name__}: {str(e)[:150]}"

bus({"action": "fanout_pending"})
time.sleep(4)
bus({"action": "fanout_pending"})

ok = R.get("assignment") is True and R.get("registry") == "roles set"
R["verdict"] = ("PASS — charter live, risk-gate assigned to Perplexity"
                + (", PR #1 merged" if R.get("pr1_merged") is True else "")
                if ok else "PARTIAL — see fields")
R["finished"] = datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/4391_org_chart.json", "w"),
          indent=1, default=str)
md = [f"# ops 4391 — org chart + first assignment — {R['verdict']}",
      f"- inbox(claude): {R.get('inbox_claude')}",
      f"- patches: {json.dumps(R.get('patches'))[:300]}",
      f"- registry: {R.get('registry')} | assignment posted: "
      f"{R.get('assignment')} | PR1: "
      f"{R.get('pr1_merged') or R.get('pr1_err') or R.get('pr1_review')}",
      "\n## PERPLEXITY'S TURNS — LAST 36H (receipts)"]
if R["perplexity_recent_turns"]:
    for x in R["perplexity_recent_turns"]:
        md.append(f"\n### [{x['thread']}] {x['from']} [{x['kind']}"
                  f"{'/' + str(x['verdict']) if x.get('verdict') else ''}]"
                  f" {x['ts']}")
        md.append(x["content"])
else:
    md.append("\n(no perplexity/glm turns in the last 36h — its "
              "autonomous drain cadence is its own; the 0008 assignment "
              "is now in its inbox and fan-out has been fired)")
open("aws/ops/reports/4391_org_chart.md", "w").write("\n".join(md) + "\n")
print(json.dumps({k: v for k, v in R.items()
                  if k != "perplexity_recent_turns"},
                 indent=1, default=str)[:2000])
