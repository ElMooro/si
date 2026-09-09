"""Move remaining published provider credentials to managed configuration.

Uses existing SSM values first; legacy values are read in memory from the audit
baseline solely to preserve service during migration. No value is logged.
This does not revoke/rotate credentials at external provider accounts.
"""
import json
import os
import re
import subprocess
import sys
from pathlib import Path
import boto3
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report

PROVIDERS = {
    "AlphaVantage": ("/justhodl/alphavantage/api-key", ("AV_KEY", "ALPHAVANTAGE_KEY", "ALPHA_VANTAGE_API_KEY", "ALPHAVANTAGE_API_KEY")),
    "BLS": ("/justhodl/bls/api-key", ("BLS_API_KEY",)),
    "BEA": ("/justhodl/bea/api-key", ("BEA_API_KEY",)),
    "Census": ("/justhodl/census/api-key", ("CENSUS_API_KEY",)),
}
TARGETS = {
    "alphavantage-market-agent": "AlphaVantage", "alphavantage-technical-analysis": "AlphaVantage",
    "justhodl-bloomberg-v8": "AlphaVantage", "justhodl-fleet-monitor": "AlphaVantage",
    "justhodl-options-flow": "AlphaVantage", "justhodl-stock-analyzer": "AlphaVantage",
    "bls-employment-api-v2": "BLS",
}
with report("ops_5229_audit_provider_configuration") as rep:
    rep.heading("Managed provider configuration migration")
    ssm = boto3.client("ssm", region_name="us-east-1")
    lam = boto3.client("lambda", region_name="us-east-1")
    baseline = subprocess.check_output(["git", "show", "125a68a89268c5ff8303dfba4177026dcf1e3c21:SYSTEM_CATALOG.md"], text=True)
    values = {}
    failures = []
    for provider, (parameter, aliases) in PROVIDERS.items():
        try:
            value = ssm.get_parameter(Name=parameter, WithDecryption=True)["Parameter"]["Value"]
            action = "existing managed value preserved"
        except ssm.exceptions.ParameterNotFound:
            match = re.search(r"^- " + re.escape(provider) + r": `([^`]+)`", baseline, re.M)
            if not match:
                failures.append(provider + ": no existing configuration")
                continue
            value = match.group(1)
            ssm.put_parameter(Name=parameter, Value=value, Type="SecureString", Overwrite=False,
                Description="Managed provider credential; external rotation still required")
            action = "migrated legacy configuration; provider rotation remains required"
        values[provider] = value
        rep.kv(provider=provider, parameter=parameter, action=action)
    for function, provider in TARGETS.items():
        if provider not in values:
            failures.append(function + ": provider unavailable")
            continue
        try:
            config = lam.get_function_configuration(FunctionName=function)
        except lam.exceptions.ResourceNotFoundException:
            rep.kv(function=function, status="repo-only; no live function")
            continue
        env = dict((config.get("Environment") or {}).get("Variables") or {})
        aliases = PROVIDERS[provider][1]
        if not any(env.get(alias) for alias in aliases):
            env[aliases[0]] = values[provider]
            lam.update_function_configuration(FunctionName=function, RevisionId=config["RevisionId"], Environment={"Variables": env})
            lam.get_waiter("function_updated_v2").wait(FunctionName=function)
            rep.kv(function=function, status="managed environment added", variable=aliases[0])
        else:
            rep.kv(function=function, status="existing managed environment preserved")
        final = lam.get_function_configuration(FunctionName=function)
        actual = (final.get("Environment") or {}).get("Variables") or {}
        if not any(actual.get(alias) for alias in aliases):
            failures.append(function + ": environment validation failed")
    for failure in failures:
        rep.fail(failure)
    if failures:
        sys.exit(1)
    rep.ok("Managed configuration is available before code migration")
    rep.warn("External provider revocation/rotation is not performed by this migration")
