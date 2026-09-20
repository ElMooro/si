"""justhodl-narrative-vs-tape — contrarian alpha. Finds where the LOUD narrative
(news velocity / sentiment) disagrees with what the TAPE shows (capital flow,
institutional accumulation, dislocation). Two edges:

  • CROWDED & FADING: high news buzz but institutions distributing / price
    rolling over → the story is priced in, smart money leaving.
  • QUIET ACCUMULATION: low/negative narrative but institutions accumulating /
    cheap-and-inflecting → unloved names smart money is buying.

OUTPUT: data/narrative-vs-tape.json · SCHEDULE: every 4h.
"""
import json, time
from datetime import datetime, timezone
import boto3
from capital_research_boundary import context as capital_context

REGION = "us-east-1"; BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/narrative-vs-tape.json"
s3 = boto3.client("s3", region_name=REGION)


def rj(key, default=None):
    try: return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception: return default


def lambda_handler(event=None, context=None):
    t0 = time.time()
    nv = rj("data/news-velocity.json") or {}
    nsent = rj("data/news-sentiment.json") or {}
    cf = rj("data/capital-flow.json") or {}
    disl = rj("data/dislocations.json") or {}

    # Build narrative buzz map: ticker -> buzz score (velocity + |sentiment|)
    buzz = {}
    for item in (nv.get("trending") or nv.get("by_ticker") or nv.get("velocity") or []):
        if isinstance(item, dict):
            tk = (item.get("ticker") or item.get("symbol") or "").upper()
            v = item.get("velocity") or item.get("velocity_z") or item.get("count") or item.get("score")
            if tk and v is not None:
                try: buzz[tk] = float(v)
                except (ValueError, TypeError): pass
    # sentiment map
    sent = {}
    sd = nsent.get("by_ticker") or {}
    if isinstance(sd, dict):
        for tk, v in sd.items():
            s = (v.get("sentiment") if isinstance(v, dict) else v)
            try: sent[tk.upper()] = float(s)
            except (ValueError, TypeError): pass

    cheap_inflecting = set()
    for r in [*(disl.get("buy_the_laggard") or []), *(disl.get("top_dislocations") or [])]:
        t = (r.get("ticker") or "").upper()
        if t and r.get("cheap_and_inflecting"):
            cheap_inflecting.add(t)

    crowded_fading = []   # loud narrative + tape negative
    quiet_accum = []      # quiet/negative narrative + tape positive

    buzz_vals = sorted(buzz.values())
    hi_buzz = buzz_vals[int(len(buzz_vals) * 0.7)] if len(buzz_vals) >= 5 else 1e9

    # The retained dislocation screen is not evidence of institutional trading.
    for tk in cheap_inflecting:
        b = buzz.get(tk, 0); sv = sent.get(tk, 0)
        if b < hi_buzz and (b == 0 or sv <= 0):
            quiet_accum.append({"ticker": tk, "buzz": round(b, 2), "sentiment": round(sv, 2),
                                "tape": "Upstream dislocation screen: cheap & inflecting",
                                "edge": "Low narrative coverage alongside an unqualified dislocation screen; no institutional buying or expected-return inference."})

    out = {"engine": "narrative-vs-tape", "version": "1.0",
           "capital_flow_exclusion": capital_context(cf),
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "duration_s": round(time.time() - t0, 1),
           "crowded_fading": crowded_fading[:15],
           "quiet_accumulation": sorted(quiet_accum, key=lambda x: x["buzz"])[:15],
           "n_buzz_tracked": len(buzz),
           "note": "Narrative and dislocation research. The legacy quiet_accumulation key is retained for compatibility; its records do not establish buying. Crowded-fading stock-flow evidence is unavailable."}
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[narrative-vs-tape] {len(crowded_fading)} crowded-fading, {len(quiet_accum)} quiet-accum")
    return {"statusCode": 200, "body": json.dumps({"crowded": len(crowded_fading), "quiet": len(quiet_accum)})}
