"""1107 — create synthesis Lambda + invoke + verify. Description now fits 256-char limit."""
import io, json, pathlib, time, traceback, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1107_create.json"


def phase(out, name, fn):
    try:
        r = fn()
        out["phases"].append({"name": name, "status": "ok", "result": r})
        return r
    except Exception as e:
        out["phases"].append({
            "name":      name,
            "status":    "ERROR",
            "error":     str(e)[:400],
            "traceback": traceback.format_exc()[:1500],
        })
        return None


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "phases": []}
    lam = boto3.client("lambda", region_name="us-east-1",
                        config=Config(read_timeout=300))
    s3 = boto3.client("s3", region_name="us-east-1")
    fn = "justhodl-ai-website-synthesis"

    def check():
        try:
            info = lam.get_function(FunctionName=fn)
            return {"exists": True, "state": info["Configuration"]["State"],
                     "last_modified": info["Configuration"]["LastModified"]}
        except lam.exceptions.ResourceNotFoundException:
            return {"exists": False}

    state = phase(out, "check", check)

    def create():
        cfg = json.load(open(f"aws/lambdas/{fn}/config.json"))
        src = lam.get_function_configuration(FunctionName="justhodl-ai-chat")
        api_key = src["Environment"]["Variables"]["ANTHROPIC_API_KEY"]
        buf = io.BytesIO()
        seen = set()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            srcdir = pathlib.Path(f"aws/lambdas/{fn}/source")
            for f in sorted(srcdir.iterdir()):
                if f.is_file():
                    zf.write(f, arcname=f.name); seen.add(f.name)
            shdir = pathlib.Path("aws/shared")
            if shdir.is_dir():
                for f in sorted(shdir.iterdir()):
                    if f.is_file() and f.suffix == ".py" and f.name not in seen:
                        zf.write(f, arcname=f.name); seen.add(f.name)
        resp = lam.create_function(
            FunctionName=fn,
            Runtime=cfg["runtime"], Role=cfg["role_arn"],
            Handler=cfg["handler"], Code={"ZipFile": buf.getvalue()},
            Timeout=cfg["timeout"], MemorySize=cfg["memory"],
            Description=cfg["description"],
            Environment={"Variables": {"ANTHROPIC_API_KEY": api_key}},
        )
        for _ in range(60):
            info = lam.get_function(FunctionName=fn)
            if info["Configuration"]["State"] == "Active":
                break
            time.sleep(1)
        return {"arn": resp["FunctionArn"], "state": "Active",
                 "desc_len": len(cfg["description"]),
                 "files_in_zip": sorted(seen)}

    if not state or not state.get("exists"):
        phase(out, "create_lambda", create)
    else:
        out["phases"].append({"name": "create_lambda", "status": "skipped — already exists"})

    def invoke():
        t0 = time.time()
        r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse", Payload=b"{}")
        elapsed = round(time.time() - t0, 1)
        body = r["Payload"].read().decode("utf-8", errors="replace")
        try:
            p = json.loads(body)
            if isinstance(p.get("body"), str):
                try:
                    return {"elapsed_s": elapsed, "summary": json.loads(p["body"])}
                except Exception:
                    return {"elapsed_s": elapsed, "body": p["body"][:400]}
            return {"elapsed_s": elapsed, "p": str(p)[:300]}
        except Exception:
            return {"elapsed_s": elapsed, "raw": body[:400]}

    phase(out, "invoke", invoke)

    def read_output():
        obj = s3.get_object(Bucket="justhodl-dashboard-live",
                              Key="data/ai-website-synthesis.json")
        d = json.loads(obj["Body"].read())
        out_ = {
            "size_kb":         round(obj["ContentLength"]/1024, 1),
            "last_modified":   obj["LastModified"].isoformat(),
            "status":          d.get("status", "ok"),
            "model":           d.get("model"),
            "engines_loaded":  d.get("engines_loaded"),
            "engines_total":   d.get("engines_total"),
            "claude_elapsed":  d.get("claude_elapsed_sec"),
            "alert_info":      d.get("alert_info"),
        }
        if d.get("status") != "error":
            syn = d.get("synthesis", {})
            out_.update({
                "global_posture": syn.get("global_posture"),
                "headline":       syn.get("headline"),
                "thesis":         (syn.get("thesis") or "")[:400],
                "decisive_call":  syn.get("decisive_call"),
                "key_drivers":    syn.get("key_drivers", [])[:5],
                "key_dissonances": syn.get("key_dissonances", [])[:3],
                "watch_list":     syn.get("watch_list", [])[:5],
                "per_page_focus_keys": list((syn.get("per_page_focus") or {}).keys()),
                "macro_focus":    (syn.get("per_page_focus") or {}).get("macro-frontrun"),
                "auction_focus":  (syn.get("per_page_focus") or {}).get("auction-crisis"),
            })
            with open("aws/ops/reports/1107_full.json", "w") as f:
                json.dump(d, f, indent=2, default=str)
        else:
            out_["error"] = d.get("error")
        return out_

    phase(out, "read_output", read_output)

    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1107] DONE")


if __name__ == "__main__":
    main()
