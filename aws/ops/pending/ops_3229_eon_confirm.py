"""ops 3229 — 3228's report truncated at section 5's header (CI success,
body missing), so EON's presence in the LIVE map is unconfirmed. Confirm
or repair — the nightly must see it."""
import json
import sys
from datetime import datetime, timezone

import boto3

from ops_report import report

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
SYM = "ICEEUR:EON2!"
ENTRY = {"source": "FRED", "id": "ECBESTRVOLWGTTRMDMNRT",
         "confidence": 0.85,
         "note": "€STR (FRED, post-storm retry) (ops 3228/3229)"}

with report("3229_eon_confirm") as rep:
    fails = []
    rep.heading("ops 3229 — EON in the live map, confirmed or repaired")
    prev = json.loads(S3.get_object(Bucket=BUCKET,
                                    Key="data/symbol-map.json")
                      ["Body"].read())
    mp = prev.get("map") or {}
    cur = prev.get("curated") or {}
    if SYM in mp:
        rep.ok(f"{SYM} already in map → {mp[SYM].get('id')} "
               "(3228's write DID land; truncation was cosmetic)")
    else:
        mp[SYM] = ENTRY
        cur[SYM] = ENTRY
        prev["map"], prev["curated"] = mp, cur
        prev["generated_at"] = datetime.now(timezone.utc).isoformat()
        prev["note"] = "ops 3229: EON repair"
        S3.put_object(Bucket=BUCKET, Key="data/symbol-map.json",
                      Body=json.dumps(prev),
                      ContentType="application/json")
        rep.ok(f"{SYM} repaired into map → {ENTRY['id']}")
    idx = json.loads(S3.get_object(Bucket=BUCKET,
                                   Key="data/wl-engines.json")
                     ["Body"].read())
    act = sum(1 for e in (idx.get("engines") or [])
              if str(e.get("state")) == "ACTIVE")
    rep.kv(index_generated=str(idx.get("generated_at"))[:19],
           active_now=act, verdict="PASS")
