"""justhodl-imf-full v1.0.0 -- COMPLETE IMF SDMX 2.1 warehouse.

Built on the ops-4961 grammar probe: legacy dataservices.imf.org is
DNS-dead; the live platform api.imf.org/external/sdmx/2.1 publishes
~222 dataflows, many VINTAGE-SCOPED (e.g. CPI_WCA_2026_MAY_VINTAGE)
-- each vintage is a first-class snapshot worth keeping. Doctrine:

  DISCOVER  parse /dataflow/IMF XML (format param ignored at the
            source) -> every flow id + name; vintages accumulate --
            new vintages join the queue, old ones are retained
  DRAIN     per flow: /data/{FLOW} full-pull streamed to /tmp,
            gzipped, uploaded verbatim (RAM-safe); on refusal ->
            ?lastNObservations=200000 fallback (tagged partial);
            still refused -> NAMED failure, 3-strike quarantine
  REFRESH   daily rediscovery (new vintages auto-queue); weekly
            redrain of NON-vintage flows (vintages are snapshots,
            re-pulling them is waste); {"redrain":true} forces all
  CHAIN     MIDAS self-chain, ~640s links, save-first
Keys data/warm/imf-full/src/{FLOW}.xml.gz; manifest -> imf-note-v2.
"""
import gzip
import json
import os
import re
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

import boto3

ENGINE_VERSION = "justhodl-imf-full v1.0.1 ops4967 loose-parse"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
API = "https://api.imf.org/external/sdmx/2.1"
ROOT = "data/warm/imf-full/"
STATE_KEY = ROOT + "_state/state.json"
MANIFEST_KEY = ROOT + "manifest.json"
UA = {"User-Agent": "JustHodl Research (raafouis@gmail.com)",
      "Accept": "application/xml"}
BUDGET_S = int(os.environ.get("IMF_BUDGET_S", "640"))
SPACING = 0.35
CHAIN_DEPTH_MAX = 40
LASTN = 200000

s3 = boto3.client("s3", region_name="us-east-1")
_t0 = time.time()


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _j(key, default=None):
    try:
        return json.loads(
            s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return default


def _put_json(key, obj):
    s3.put_object(Bucket=BUCKET, Key=key,
                  Body=json.dumps(obj, indent=1).encode(),
                  ContentType="application/json")


def discover(state):
    req = urllib.request.Request(API + "/dataflow/IMF", headers=UA)
    with urllib.request.urlopen(req, timeout=120) as r:
        xml = r.read(12_000_000).decode("utf-8", "replace")
    # ops-4961 proven parse: loose id capture (the source emits
    # namespaced/self-closing forms a closing-tag anchor misses --
    # that anchor produced 0 flows live in 4967 v1)
    ids = re.findall(
        r'Dataflow[^>]*\bid="([A-Za-z0-9_.\-]+)"', xml)
    flows = {}
    for fid in ids:
        if fid in flows:
            continue
        nm = re.search(
            r'id="%s"[^>]*>\s*<[^>]*Name[^>]*>([^<]+)<'
            % re.escape(fid), xml)
        flows[fid] = (nm.group(1).strip() if nm else "")[:120]
    if not flows:
        raise RuntimeError("catalog parse yielded 0 flows")
    state["universe"] = flows
    q = state.setdefault("queue", [])
    queued = {x[0] for x in q}
    added = 0
    for fid in sorted(flows):
        if fid not in state.get("have", {}) and fid not in queued \
                and fid not in state.get("failures", {}):
            q.append([fid, 0])
            added += 1
    state["last_discover"] = {"at": _now(), "flows": len(flows),
                              "queued_new": added}
    return len(flows)


def pull(url):
    """Stream to /tmp gz. -> (path, raw_bytes, obs_hint)"""
    tmp = "/tmp/imf_pull.xml.gz"
    n, obs = 0, 0
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=280) as r, \
            gzip.open(tmp, "wb") as f:
        while True:
            c = r.read(1 << 20)
            if not c:
                break
            n += len(c)
            obs += c.count(b"<Obs")
            f.write(c)
    if n < 200:
        raise RuntimeError("empty body (%dB)" % n)
    return tmp, n, obs


def drain_one(state, fid):
    key = ROOT + "src/%s.xml.gz" % fid
    modes = [("full", API + "/data/" + fid),
             ("lastN", API + "/data/" + fid +
              "?lastNObservations=%d" % LASTN)]
    last = "none"
    for mode, url in modes:
        try:
            tmp, raw, obs = pull(url)
            s3.upload_file(tmp, BUCKET, key, ExtraArgs={
                "ContentType": "application/gzip",
                "Metadata": {"engine": "imf-full", "flow": fid[:80],
                             "mode": mode, "obs_hint": str(obs)}})
            gzb = os.path.getsize(tmp)
            os.remove(tmp)
            state["have"][fid] = {
                "raw_bytes": raw, "gz_bytes": gzb, "obs_hint": obs,
                "mode": mode, "vintage": "_VINTAGE" in fid,
                "at": _now()}
            state["failures"].pop(fid, None)
            return True
        except urllib.error.HTTPError as e:
            last = "%s HTTP %s" % (mode, e.code)
        except Exception as e:
            last = "%s %s: %s" % (mode, type(e).__name__,
                                  str(e)[:60])
        time.sleep(SPACING)
    fl = state["failures"]
    tries = (fl.get(fid) or {}).get("tries", 0) + 1
    fl[fid] = {"err": last[:110], "tries": tries}
    return tries >= 3


def write_manifest(state):
    have = state.get("have") or {}
    _put_json(MANIFEST_KEY, {
        "as_of": _now(), "engine": "justhodl-imf-full",
        "version": "1.0.0",
        "flows_catalog": len(state.get("universe") or {}),
        "flows_banked": len(have),
        "vintages_banked": sum(1 for v in have.values()
                               if v.get("vintage")),
        "gb": round(sum(v.get("raw_bytes") or 0
                        for v in have.values()) / 1e9, 2),
        "obs_hint": sum(v.get("obs_hint") or 0
                        for v in have.values()),
        "lastN_partial": sum(1 for v in have.values()
                             if v.get("mode") == "lastN"),
        "queue_left": len(state.get("queue") or []),
        "failures": len(state.get("failures") or {}),
        "phase": state.get("phase"),
        "note": ("complete IMF SDMX-2.1 warehouse on api.imf.org "
                 "(legacy host DNS-dead): every dataflow verbatim, "
                 "vintage snapshots retained, daily rediscovery + "
                 "weekly non-vintage redrain")})


def lambda_handler(event, ctx=None):
    global _t0
    _t0 = time.time()
    event = event or {}
    state = _j(STATE_KEY, None) or {
        "version": "1.0.0", "phase": "DISCOVER", "queue": [],
        "have": {}, "failures": {}, "universe": {}}
    if float(state.get("lease_until") or 0) > time.time():
        return {"skipped": "lease_held"}
    state["lease_until"] = time.time() + BUDGET_S + 200
    _put_json(STATE_KEY, state)

    disc_age = 0
    try:
        disc_age = time.time() - datetime.fromisoformat(
            (state.get("last_discover") or {}).get("at")
        ).timestamp()
    except Exception:
        disc_age = 10**9
    if state["phase"] == "DISCOVER" or event.get("rediscover") or \
            disc_age > 20 * 3600:
        try:
            discover(state)
            if state["phase"] == "DISCOVER" or state.get("queue"):
                state["phase"] = "DRAIN"
        except Exception as e:
            state["failures"]["_discover"] = {"err": str(e)[:110]}
        _put_json(STATE_KEY, state)

    if event.get("redrain") and state.get("phase") == "COMPLETE":
        vint_skip = not event.get("include_vintages")
        state["queue"] = [
            [fid, 0] for fid in sorted(state["have"])
            if not (vint_skip and
                    state["have"][fid].get("vintage"))]
        state["phase"] = "DRAIN"

    if state["phase"] == "DRAIN" and state["queue"]:
        while state["queue"]:
            if time.time() - _t0 > BUDGET_S - 60:
                break
            fid, att = state["queue"][0]
            if att >= 3:
                state["queue"].pop(0)
                _put_json(STATE_KEY, state)
                continue
            state["queue"][0][1] = att + 1
            _put_json(STATE_KEY, state)
            if drain_one(state, fid):
                state["queue"].pop(0)
            _put_json(STATE_KEY, state)
            time.sleep(SPACING)
        if not state["queue"]:
            state["phase"] = "COMPLETE"

    n_left = len(state["queue"])
    depth = int(event.get("chain_depth") or 0)
    chain = bool(n_left and depth < CHAIN_DEPTH_MAX
                 and not event.get("no_chain"))
    state["lease_until"] = 0
    state["as_of"] = _now()
    state["n_banked"] = len(state["have"])
    _put_json(STATE_KEY, state)
    write_manifest(state)
    if chain:
        try:
            boto3.client("lambda", region_name="us-east-1").invoke(
                FunctionName=os.environ.get(
                    "AWS_LAMBDA_FUNCTION_NAME",
                    "justhodl-imf-full"),
                InvocationType="Event",
                Payload=json.dumps(
                    {"chain_depth": depth + 1}).encode())
        except Exception:
            chain = False
    return {"ok": True, "phase": state["phase"],
            "banked": state["n_banked"], "queue_left": n_left,
            "chained": chain,
            "elapsed_s": round(time.time() - _t0, 1)}
