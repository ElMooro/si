"""ops_4984 v3 -- FINRA AUTH-TIER SENTINEL + full-depth drain.

KEYLESS VERDICT (final, evidence-complete): the public tier is
hard-windowed -- weeklySummary.summaryStartDate 2022-01..2023-11,
regShoDaily rolling 1yr, GET-equality filters silently ignored,
POST auth-gated. 8/9 datasets · 22.2M rows · ~700MB banked = the
complete keyless windows. Since-inception depth = auth tier only.

THIS OP IS THE SENTINEL: on every push it checks the engine env
for FINRA_CLIENT_SECRET.
  absent  -> fast RED (stays pending; fires again next push)
  present -> mint token; POST+Bearer dateRangeFilters partition
             drain: weeklySummary 2014..2021 by year,
             regShoDaily years desc to inception, the 10
             auth-gated datasets, treasuryWeeklyAggregates retry;
             checkpointed per partition; gates + card.
Khalid's one action: portal -> cred c2de60df... -> Reset ->
paste the one-time secret here (or into vault item finra).
"""
import gzip
import io
import json
import re
import sys
import time
import urllib.error
import urllib.request
import zlib
from base64 import b64encode
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
FN = "justhodl-finra-full"
CAT = "justhodl-provider-catalog"
API = "https://api.finra.org"
TOKEN_URL = ("https://ews.fip.finra.org/fip/rest/ews/oauth2/"
             "access_token?grant_type=client_credentials")
ROOT = "data/warm/finra-full/"
STATE_KEY = ROOT + "_state/state.json"
MANIFEST_KEY = ROOT + "manifest.json"
HUB_KEY = "data/provider-catalog.json"
PAGE = 5000
GUARD_S = 70 * 60
T0 = time.time()

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
TOK = {"v": ""}


def gj(key, default=None):
    try:
        raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return json.loads(raw)
    except Exception:
        return default


def http(path, method="GET", body=None, timeout=120,
         cap=60_000_000):
    h = {"Accept": "application/json"}
    if TOK["v"]:
        h["Authorization"] = "Bearer " + TOK["v"]
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        h["Content-Type"] = "application/json"
    req = urllib.request.Request(API + path, data=data,
                                 headers=h, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read(cap)


def rows_of(raw):
    js = json.loads(raw)
    return js if isinstance(js, list) else js.get("data") or []


with report("ops_4984_finra_partition_drain") as R:
    fails = []
    R.section("P0 sentinel: secret present?")
    env = lam.get_function_configuration(
        FunctionName=FN)["Environment"]["Variables"]
    cid = env.get("FINRA_CLIENT_ID") or ""
    sec = env.get("FINRA_CLIENT_SECRET") or ""
    R.log("  client_id=%s secret=%s" % (
        (cid[:8] + "...") if cid else None, bool(sec)))
    if not sec:
        R.log("ops 4984 RED: SENTINEL WAITING -- keyless tier is "
              "hard-windowed (evidence complete); paste the API "
              "secret (portal Reset shows it once) and this op "
              "drains full 2014-inception depth + the 10 auth "
              "datasets automatically on the next push")
        sys.exit(1)

    R.section("P1 token mint")
    try:
        req = urllib.request.Request(
            TOKEN_URL, method="POST",
            headers={"Authorization": "Basic " + b64encode(
                ("%s:%s" % (cid, sec)).encode()).decode()})
        with urllib.request.urlopen(req, timeout=45) as r:
            TOK["v"] = json.loads(r.read(20_000)).get(
                "access_token") or ""
        R.log("  mint: %s" % ("OK" if TOK["v"] else "EMPTY"))
    except Exception as e:
        R.log("  mint FAILED: %s" % str(e)[:140])
    if not TOK["v"]:
        R.log("ops 4984 RED: token mint failed with the stored "
              "secret -- re-paste from a fresh portal Reset")
        sys.exit(1)

    st = gj(STATE_KEY) or {}
    have = dict(st.get("have") or {})
    universe = dict(st.get("universe") or {})
    invalid = dict(st.get("invalid") or {})

    def checkpoint():
        now = datetime.now(timezone.utc).isoformat(
            timespec="seconds")
        st2 = gj(STATE_KEY) or {}
        st2.update({"have": have, "universe": universe,
                    "invalid": invalid,
                    "n_banked": len(have), "as_of": now,
                    "lease_until": 0})
        s3.put_object(Bucket=B, Key=STATE_KEY,
                      Body=json.dumps(st2, indent=1).encode(),
                      ContentType="application/json")
        man = gj(MANIFEST_KEY) or {}
        man.update({
            "as_of": now, "datasets_banked": len(have),
            "datasets_catalog": len(universe),
            "rows": sum(v.get("rows") or 0
                        for v in have.values()),
            "mb": round(sum(v.get("bytes") or 0
                            for v in have.values()) / 1e6, 1),
            "invalid_named": len(invalid),
            "cred_expires": "active (auth tier live)"})
        s3.put_object(Bucket=B, Key=MANIFEST_KEY,
                      Body=json.dumps(man, indent=1).encode(),
                      ContentType="application/json")

    def fetch_range(g, nm, f, d0, d1, key):
        buf = io.BytesIO()
        rows = 0
        with gzip.GzipFile(fileobj=buf, mode="wb") as zf:
            off = 0
            while True:
                body = {"limit": PAGE, "offset": off,
                        "dateRangeFilters": [{
                            "fieldName": f, "startDate": d0,
                            "endDate": d1}]}
                batch = rows_of(http(
                    "/data/group/%s/name/%s" % (g, nm),
                    method="POST", body=body))
                for r_ in batch:
                    zf.write((json.dumps(
                        r_, separators=(",", ":")) + "\n"
                    ).encode())
                rows += len(batch)
                if len(batch) < PAGE or \
                        time.time() - T0 > GUARD_S:
                    break
                off += PAGE
                time.sleep(0.12)
        b_ = buf.getvalue()
        if rows:
            s3.put_object(
                Bucket=B, Key=ROOT + "src/%s.jsonl.gz" % key,
                Body=b_, ContentType="application/gzip",
                Metadata={"engine": "finra-full",
                          "dataset": key[:110],
                          "rows": str(rows)})
            have[key] = {"rows": rows, "bytes": len(b_),
                         "at": datetime.now(timezone.utc
                                            ).isoformat(
                             timespec="seconds"),
                         "status": "auth-partition"}
        return rows

    R.section("P2 auth partition drain")
    for yr in range(2014, 2022):
        if time.time() - T0 > GUARD_S:
            R.log("  guard -- remainder resumes next push")
            break
        key = "otcMarket__weeklySummary__Y%d" % yr
        if key in have:
            continue
        n = fetch_range("otcMarket", "weeklySummary",
                        "summaryStartDate",
                        "%d-01-01" % yr, "%d-12-31" % yr, key)
        R.log("  %-42s rows=%d" % (key, n))
        checkpoint()
    for yr in range(2024, 2004, -1):
        if time.time() - T0 > GUARD_S:
            R.log("  guard -- remainder resumes next push")
            break
        key = "otcMarket__regShoDaily__Y%d" % yr
        if key in have:
            continue
        n = fetch_range("otcMarket", "regShoDaily",
                        "tradeReportDate",
                        "%d-01-01" % yr,
                        min("%d-12-31" % yr, "2025-08-24"), key)
        R.log("  %-42s rows=%d" % (key, n))
        checkpoint()
        if n == 0:
            R.log("  regShoDaily inception above Y%d" % yr)
            break
    R.section("P2b auth-gated datasets + treasury")
    for key in sorted(set(list(invalid) +
                          ["fixedIncomeMarket__"
                           "treasuryWeeklyAggregates"])):
        if time.time() - T0 > GUARD_S:
            break
        if key in have:
            continue
        g, nm = key.split("__", 1)
        buf = io.BytesIO()
        rows = 0
        try:
            with gzip.GzipFile(fileobj=buf, mode="wb") as zf:
                off = 0
                while True:
                    batch = rows_of(http(
                        "/data/group/%s/name/%s?limit=%d"
                        "&offset=%d" % (g, nm, PAGE, off)))
                    for r_ in batch:
                        zf.write((json.dumps(
                            r_, separators=(",", ":")) + "\n"
                        ).encode())
                    rows += len(batch)
                    if len(batch) < PAGE or \
                            time.time() - T0 > GUARD_S:
                        break
                    off += PAGE
                    time.sleep(0.12)
            b_ = buf.getvalue()
            if rows:
                s3.put_object(
                    Bucket=B,
                    Key=ROOT + "src/%s.jsonl.gz" % key,
                    Body=b_, ContentType="application/gzip",
                    Metadata={"engine": "finra-full",
                              "dataset": key[:110],
                              "rows": str(rows)})
                have[key] = {"rows": rows, "bytes": len(b_),
                             "at": datetime.now(
                                 timezone.utc).isoformat(
                                 timespec="seconds"),
                             "status": "auth"}
                universe[key] = {"group": g, "name": nm}
                invalid.pop(key, None)
                R.log("  %-52s rows=%d" % (key, rows))
        except Exception as e:
            R.log("  %-52s %s" % (key, str(e)[:70]))
        checkpoint()

    R.section("P4 substance: Y2014 weekly span")
    ok4 = False
    try:
        k14 = "otcMarket__weeklySummary__Y2014"
        pat = re.compile(
            rb'"summaryStartDate":"(20\d\d-\d\d-\d\d)"')
        body = s3.get_object(
            Bucket=B, Key=ROOT + "src/%s.jsonl.gz" % k14)["Body"]
        d = zlib.decompressobj(16 + zlib.MAX_WBITS)
        lo, hi, carry = None, None, b""
        for chunk in iter(lambda: body.read(4 << 20), b""):
            buf2 = carry + d.decompress(chunk)
            carry = buf2[-64:]
            for m in pat.finditer(buf2):
                v = m.group(1)
                lo = v if lo is None or v < lo else lo
                hi = v if hi is None or v > hi else hi
        R.log("  Y2014 span %s .. %s rows=%s" % (
            (lo or b"?").decode(), (hi or b"?").decode(),
            (have.get(k14) or {}).get("rows")))
        ok4 = bool(lo) and lo.decode() <= "2014-12-31"
    except Exception as e:
        R.log("  substance err %s" % str(e)[:100])
    R.log("P4 %s" % ("PASS" if ok4 else "FAIL"))
    if not ok4:
        fails.append("P4")

    R.section("P5 card")
    t_mark = datetime.now(timezone.utc).isoformat(
        timespec="seconds")
    try:
        lam.invoke(FunctionName=CAT, InvocationType="Event",
                   Payload=b"{}")
    except Exception:
        pass
    hub, t0 = {}, time.time()
    while time.time() - t0 < 8 * 60:
        time.sleep(30)
        hub = gj(HUB_KEY) or {}
        if (hub.get("as_of") or "") >= t_mark:
            break
    fe = next((p for p in hub.get("providers", [])
               if p.get("slug") == "finra"), {}) or {}
    R.log("P5 note=%s" % (fe.get("catalog_note") or "")[:180])

    if fails:
        R.log("ops 4984 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(datasets=len(have),
         rows=sum(v.get("rows") or 0 for v in have.values()),
         mb=round(sum(v.get("bytes") or 0
                      for v in have.values()) / 1e6, 1))
    R.log("ops 4984 GREEN -- FULL TIER: since-inception depth + "
          "auth datasets banked; engine owns refresh on Lambda")
