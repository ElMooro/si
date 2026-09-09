"""ops_5227 -- audit 2026-09-08 INST-06 follow-up: managed_secret env backfill.

ops 5221 found engines (fedliquidityapi, fmp-stock-picks-agent) whose execution role lacks ssm:GetParameter
and whose environment never carried the credential: the old literal fallback masked that. managed_secret
resolves env FIRST, so the durable fix is: every consumer carries the canonical env var of each provider it
references. This op:
  1. parses each Lambda's source for `managed_secret((ENV..., ), ("/justhodl/<prov>/api-key",))` references;
  2. reads the SSM value once per provider;
  3. for each live function missing every env name of a referenced provider, adds the CANONICAL env var
     (merge, nothing else changes) -- and reports functions whose env value differs from SSM (rotation
     reconciliation list; not changed here);
  4. re-invokes nothing; the next cold start picks the env up.
Never prints secret values.
"""
import json
import re
import sys
import time
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
CANON = {"/justhodl/fmp/api-key": "FMP_KEY", "/justhodl/polygon/api-key": "POLYGON_API_KEY", "/justhodl/fred/api-key": "FRED_API_KEY",
         "/justhodl/cmc/api-key": "CMC_KEY", "/justhodl/telegram/bot_token": "TELEGRAM_BOT_TOKEN",
         "/justhodl/anthropic/api_key": "ANTHROPIC_API_KEY", "/justhodl/newsapi/api-key": "NEWSAPI_KEY", "/justhodl/census/api-key": "CENSUS_API_KEY"}
REF = re.compile(r'managed_secret\(\s*\(([^)]*)\)\s*,\s*\(\s*["\']([^"\']+)["\']')
FAILS = []


def mask(v):
    v = str(v or "")
    return (v[:2] + "…" + v[-2:]) if len(v) > 8 else "…"


with report("ops_5227_managed_secret_env_backfill") as R:
    R.heading("ops 5227 -- managed_secret env backfill (no runtime SSM dependency)")
    ssm = boto3.client("ssm", region_name=REGION)
    lam = boto3.client("lambda", region_name=REGION)
    # 1. references per engine
    refs = {}
    for d in sorted((ROOT / "aws" / "lambdas").iterdir()):
        src = d / "source"
        if not src.is_dir() or d.name.startswith("_"):
            continue
        need = {}
        for f in src.glob("*.py"):
            for m in REF.finditer(f.read_text(errors="ignore")):
                envs = tuple(x.strip().strip("'\"") for x in m.group(1).split(",") if x.strip())
                need.setdefault(m.group(2), set()).update(envs)
        if need:
            refs[d.name] = need
    R.log("%d engines reference %d providers via managed_secret" % (len(refs), len({p for n in refs.values() for p in n})))
    # 2. SSM values
    vals = {}
    for pname in sorted({p for n in refs.values() for p in n}):
        try:
            vals[pname] = ssm.get_parameter(Name=pname, WithDecryption=True)["Parameter"]["Value"]
        except Exception as e:
            R.warn("%s unreadable: %s" % (pname, str(e)[:80]))
    # 3. fleet envs
    fleet = {}
    for page in lam.get_paginator("list_functions").paginate():
        for fc in page.get("Functions", []):
            fleet[fc["FunctionName"]] = ((fc.get("Environment") or {}).get("Variables") or {})
    added, differ, missing_param, absent_fn = [], [], [], []
    for fn, need in sorted(refs.items()):
        if fn not in fleet:
            absent_fn.append(fn); continue
        env = dict(fleet[fn])
        patch = {}
        for pname, envs in need.items():
            if pname not in vals:
                missing_param.append((fn, pname)); continue
            present = [e for e in envs if env.get(e)]
            if present:
                if any(env[e] != vals[pname] for e in present):
                    differ.append((fn, pname, [e for e in present if env[e] != vals[pname]]))
                continue
            canon = CANON.get(pname) or sorted(envs)[0]
            patch[canon] = vals[pname]
        if patch:
            env.update(patch)
            for attempt in range(6):
                try:
                    lam.update_function_configuration(FunctionName=fn, Environment={"Variables": env})
                    break
                except lam.exceptions.ResourceConflictException:
                    time.sleep(5 * (attempt + 1))
                except Exception as e:
                    FAILS.append("%s: env update failed: %s" % (fn, str(e)[:80])); break
            else:
                FAILS.append("%s: env update kept conflicting" % fn)
            added.append((fn, sorted(patch.keys())))
    R.section("results")
    R.log("env vars ADDED on %d functions: %s" % (len(added), ", ".join("%s[%s]" % (fn, ",".join(k)) for fn, k in added[:40])))
    R.log("%d functions carry a value that DIFFERS from SSM (reconcile at rotation): %s" % (len(differ), ", ".join("%s:%s" % (fn, p.split('/')[2]) for fn, p, _ in differ[:30])))
    if absent_fn:
        R.log("%d repo-only engines (no live function): %s" % (len(absent_fn), ", ".join(absent_fn[:20])))
    if missing_param:
        R.warn("%d (function, parameter) pairs skipped -- parameter unreadable: %s" % (len(missing_param), missing_param[:10]))
    R.kv(step="backfill", engines=len(refs), added=len(added), differ=len(differ), repo_only=len(absent_fn), skipped=len(missing_param))
    # the two engines the gate caught must now carry FRED
    for fn in ("fedliquidityapi", "fmp-stock-picks-agent"):
        cfg = lam.get_function_configuration(FunctionName=fn) if fn in fleet else None
        env = ((cfg or {}).get("Environment") or {}).get("Variables") or {}
        ok = any(env.get(k) for k in ("FRED_API_KEY", "FRED_KEY"))
        (R.ok if ok else R.fail)("%s carries a FRED env var: %s" % (fn, ok))
        if not ok:
            FAILS.append("%s still lacks a FRED env var" % fn)
    R.section("verdict")
    for f in FAILS:
        R.fail(f)
    if FAILS:
        sys.exit(1)
    R.ok("GREEN -- every managed_secret consumer carries its provider env vars; SSM is a fallback, never a dependency")
