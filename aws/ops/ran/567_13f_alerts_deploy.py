#!/usr/bin/env python3
"""567 — Ensure justhodl-13f-positions has TELEGRAM env vars, force-invoke,
parse log output to confirm alert section ran (no Q1 filings expected yet, so
no actual telegram should send — just verify the code path is reached)."""
import io, json, os, time as _time, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/567_13f_alerts_deploy.json"
NAME = "justhodl-13f-positions"

lam = boto3.client("lambda", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # Wait for any in-flight auto-deploy to finish
    for i in range(15):
        try:
            cfg = lam.get_function(FunctionName=NAME)["Configuration"]
            if cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") == "Successful":
                out["lambda_last_modified"] = cfg.get("LastModified")
                break
            out[f"wait_{i}"] = cfg.get("LastUpdateStatus")
        except Exception: pass
        _time.sleep(5)

    # Ensure Telegram creds in env
    try:
        cfg = lam.get_function_configuration(FunctionName=NAME)
        env = (cfg.get("Environment") or {}).get("Variables", {}) or {}
        had_token = "TELEGRAM_TOKEN" in env and bool(env.get("TELEGRAM_TOKEN"))
        had_chat = "TELEGRAM_CHAT_ID" in env and bool(env.get("TELEGRAM_CHAT_ID"))
        out["pre_env"] = {"had_TG_token": had_token, "had_TG_chat": had_chat,
                          "keys": sorted(env.keys())}
        patched = False
        if not had_token:
            try:
                env["TELEGRAM_TOKEN"] = ssm.get_parameter(
                    Name="/justhodl/telegram/token", WithDecryption=True
                )["Parameter"]["Value"]
                patched = True
            except Exception as e:
                out["ssm_token_err"] = str(e)[:120]
        if not had_chat:
            try:
                env["TELEGRAM_CHAT_ID"] = ssm.get_parameter(
                    Name="/justhodl/telegram/chat_id", WithDecryption=True
                )["Parameter"]["Value"]
                patched = True
            except Exception as e:
                out["ssm_chat_err"] = str(e)[:120]
        if patched:
            lam.update_function_configuration(FunctionName=NAME,
                                                Environment={"Variables": env})
            lam.get_waiter("function_updated").wait(FunctionName=NAME)
            out["env_patched"] = "OK"
    except Exception as e:
        out["env_err"] = str(e)[:200]

    _time.sleep(2)

    # Force-invoke with log capture
    try:
        resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse",
                           LogType="Tail", Payload=b"{}")
        out["invoke_status"] = resp.get("StatusCode")
        out["fn_error"] = resp.get("FunctionError")
        body = resp["Payload"].read().decode("utf-8")
        try:
            p = json.loads(body)
            out["response"] = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
        except: out["raw"] = body[:600]
        if resp.get("LogResult"):
            log = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")
            out["log_size"] = len(log)
            # Filter to interesting lines
            keep_lines = []
            for line in log.split("\n"):
                if any(tok in line for tok in [
                    "13F positions", "13f-alert", "QUARTER", "alert", "filings",
                    "Traceback", "Error", "exception",
                ]):
                    keep_lines.append(line.strip()[:280])
            out["log_relevant"] = keep_lines[-30:]
            out["log_tail_raw"] = log[-2500:]
    except Exception as e:
        out["invoke_err"] = str(e)[:200]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
