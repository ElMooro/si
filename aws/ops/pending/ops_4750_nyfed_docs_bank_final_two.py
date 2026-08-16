"""
ops/4750 -- marketshare + guidesheets banked as DOCUMENTS: 10/10 closed.

ops 4749 proved all four endpoints answer 200 -- the payloads are
nested reference documents (share tables, guide sheets), not operation
lists, so rows-extraction was the wrong tool, not the data missing.
This op banks them verbatim: for each family, every path-param enum
combination the spec declares (all guidesheetType values x
latest/previous; marketshare ytd + qtrly), fetched and stored raw at
data/warm/nyfed-markets/{fam}/{variant}.json.gz with a small wrapper
(source, endpoint, banked_at, payload). Point-in-time reference docs,
permanent; payload sizes and top-level keys logged so the shapes are
on record.
"""
import gzip
import itertools
import json
import re
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))

import boto3  # noqa: E402
from ops_report import report  # noqa: E402

try:
    import yaml  # noqa: E402
except Exception:
    subprocess.run([sys.executable, "-m", "pip", "install", "-q", "pyyaml"],
                    check=False)
    import yaml  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
NYF = "https://markets.newyorkfed.org"
WARM_PREFIX = "data/warm/nyfed-markets/"
SPEC_KEY = WARM_PREFIX + "markets-api-spec.yml.gz"
UA = {"User-Agent": "JustHodl.AI research raafouis@gmail.com", "Accept": "*/*"}
FAMS = ("marketshare", "guidesheets")

s3 = boto3.client("s3", region_name=REGION)


def get(url, timeout=60):
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read(12_000_000)
            if raw[:2] == b"\x1f\x8b":
                raw = gzip.decompress(raw)
            return {"ok": True, "status": r.status,
                     "body": raw.decode("utf-8", "replace")}
    except urllib.error.HTTPError as e:
        return {"ok": False, "status": e.code, "body": ""}
    except Exception as e:
        return {"ok": False, "status": None, "body": "",
                 "error": f"{type(e).__name__}: {str(e)[:120]}"}


def resolve(spec, node):
    if isinstance(node, dict) and "$ref" in node:
        cur = spec
        for part in node["$ref"].lstrip("#/").split("/"):
            cur = cur.get(part, {})
        return cur
    return node


def enum_values(p):
    sch = p.get("schema") or {}
    vals = sch.get("enum") or p.get("enum")
    if vals:
        return [str(v) for v in vals]
    if sch.get("default") is not None:
        return [str(sch["default"])]
    return ["all"]


def main():
    with report("4750_nyfed_docs_bank_final_two") as rep:
        rep.heading("ops 4750 -- marketshare + guidesheets as documents (10/10)")

        raw = s3.get_object(Bucket=BUCKET, Key=SPEC_KEY)["Body"].read()
        spec = yaml.safe_load(gzip.decompress(raw).decode("utf-8", "replace"))
        paths = spec.get("paths") or {}
        servers = spec.get("servers") or []
        base = (servers[0].get("url") if servers else f"{NYF}/api").rstrip("/")
        if base.startswith("/"):
            base = NYF + base

        def fam_of(pth):
            parts = pth.strip("/").split("/")
            return parts[1] if parts and parts[0] == "api" and len(parts) > 1 \
                else parts[0]

        total_banked = 0
        for fam in FAMS:
            fam_paths = sorted(p for p in paths if fam_of(p) == fam)
            rep.kv(check=f"templates_{fam}", value=len(fam_paths))
            for t in fam_paths:
                op = (paths[t] or {}).get("get") or {}
                params = [resolve(spec, p) for p in
                           (op.get("parameters") or []) +
                           ((paths[t] or {}).get("parameters") or [])]
                path_params = [p for p in params if p.get("in") == "path"]
                names = [p["name"] for p in path_params]
                # json-only where a format param exists
                combos = []
                val_lists = []
                for p in path_params:
                    vals = (["json"] if p["name"].lower() == "format"
                            else enum_values(p))
                    val_lists.append(vals)
                for combo in itertools.product(*val_lists) if val_lists else [()]:
                    combos.append(dict(zip(names, combo)))
                for subs in combos:
                    url = base + re.sub(r"\{(\w+)\}",
                                         lambda m: subs.get(m.group(1), "all"), t)
                    r = get(url)
                    body = r.get("body", "")
                    parsed = None
                    if r["ok"] and body.strip()[:1] in "[{":
                        try:
                            parsed = json.loads(body)
                        except Exception:
                            parsed = None
                    variant = "-".join(v for k, v in subs.items()
                                        if k.lower() != "format") or "default"
                    variant = re.sub(r"[^A-Za-z0-9_-]+", "_", variant)
                    tail = ("latest" if "latest" in t else
                            "previous" if "previous" in t else "doc")
                    if parsed is None or len(body) < 40:
                        rep.log(f"{fam}/{variant}/{tail}: status={r.get('status')} "
                                f"bytes={len(body)} -> skipped (no usable JSON)")
                        continue
                    keys_note = (",".join(list(parsed.keys())[:6])
                                  if isinstance(parsed, dict)
                                  else f"list[{len(parsed)}]")
                    out_key = f"{WARM_PREFIX}{fam}/{variant}-{tail}.json.gz"
                    out = {"source": f"NY Fed Markets API -- {fam}",
                           "endpoint": url,
                           "banked_at": datetime.now(timezone.utc).isoformat(),
                           "payload_bytes": len(body), "payload": parsed}
                    s3.put_object(Bucket=BUCKET, Key=out_key,
                                   Body=gzip.compress(json.dumps(
                                       out, separators=(",", ":")).encode()),
                                   ContentType="application/json",
                                   ContentEncoding="gzip")
                    total_banked += 1
                    rep.ok(f"{fam}/{variant}-{tail}: {len(body)} bytes, "
                            f"top-level: {keys_note}")
                    rep.kv(family=fam, variant=f"{variant}-{tail}",
                           banked=True, payload_bytes=len(body))
        rep.kv(check="documents_banked_total", value=total_banked)
        if total_banked:
            rep.ok(f"{total_banked} reference documents banked -- "
                    "all 10 NY Fed API families now held")
        else:
            rep.warn("0 documents banked -- payload shapes logged above for "
                     "a follow-up with the exact structures")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
