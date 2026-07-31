"""ops_4192 — MPRYY config retry (PPI mask, freq variants, vintages) +
other-bare 595 identification + label-wave-2 sizing."""
import json
import re
import sys
import urllib.request
from collections import Counter
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=290,
                                 retries={"max_attempts": 1}))
BUCKET = "justhodl-dashboard-live"
UA = {"User-Agent": "Mozilla/5.0"}


def fetch(url, timeout=90):
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", "ignore")
    except Exception as e:
        return -1, str(e)[:140]


def mask_countries(flow, mask):
    st, x = fetch("https://api.imf.org/external/sdmx/2.1/data/"
                  f"{flow}/{mask}?lastNObservations=1", 90)
    return len(set(re.findall(r'COUNTRY="([A-Z]{3})"', x)))


def main():
    with report("4192_mpryy_census") as rep:
        rep.heading("ops 4192 — MPRYY retry + bare census + wave-2 sizing")
        checks = []

        rep.section("A. MPRYY: PPI flow, freq variants, vintages")
        proven = None
        for flow in ("PPI", "PPI_2026_MAY_VINTAGE"):
            st, x = fetch("https://api.imf.org/external/sdmx/2.1/data/"
                          f"{flow}/all?detail=serieskeysonly", 150)
            m = re.search(r"<Series[^>]*>", x)
            if not m:
                rep.log(f"  {flow}: no series ({st})")
                continue
            attrs = list(dict(re.findall(
                r'([A-Za-z0-9_]+)="[^"]*"', m.group(0))))
            trs = sorted(set(re.findall(
                r'TYPE_OF_TRANSFORMATION="([A-Za-z0-9_]+)"', x)))
            fqs = sorted(set(re.findall(
                r'FREQUENCY="([A-Za-z0-9]+)"', x)))
            rep.log(f"  {flow}: attrs={attrs} trs={json.dumps(trs)[:200]}"
                    f" fqs={fqs}")
            tr = next((t for t in ("YOY_PCH_PA_PT", "PCH_PA_PT")
                       if t in trs), None)
            if not tr:
                continue
            for fq in ("M", ""):
                parts = [fq if k.upper() == "FREQUENCY"
                         else (tr if k == "TYPE_OF_TRANSFORMATION"
                               else "") for k in attrs]
                mask = ".".join(parts)
                nc = mask_countries(flow, mask)
                rep.log(f"  {flow} {tr} fq='{fq}': {nc} countries")
                if nc >= 40:
                    proven = (flow, mask)
                    break
            if proven:
                break
        rep.kv(mpryy_proven=bool(proven))

        if proven:
            d = json.loads(s3.get_object(
                Bucket=BUCKET, Key="data/family-defs.json")["Body"].read())
            fams = d.get("families") or {}
            fams["MPRYY"] = {"kind": "imf_mask", "flow": proven[0],
                             "mask": proven[1]}
            d["families"] = fams
            d["marker"] = "family-defs v2 ops4192"
            s3.put_object(Bucket=BUCKET, Key="data/family-defs.json",
                          Body=json.dumps(d).encode(),
                          ContentType="application/json",
                          CacheControl="max-age=300")
            rep.ok(f"  defs updated: {sorted(fams)}")
            lam.invoke(FunctionName="justhodl-families-feed",
                       InvocationType="RequestResponse", Payload=b"{}")
            fd = json.loads(s3.get_object(
                Bucket=BUCKET, Key="data/families.json")["Body"].read())
            c = fd.get("counts") or {}
            rep.kv(MPRYY=c.get("MPRYY"))
            checks.append(("MPRYY count >= 40",
                           (c.get("MPRYY") or 0) >= 40))
            lam.invoke(FunctionName="justhodl-tradingview",
                       InvocationType="Event", Payload=b"{}")
            rep.ok("  vault fired")
        else:
            rep.log("  MPRYY: no mask proved — honest, config untouched")

        rep.section("B. other-bare identification (verbatim)")
        v = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/tradingview.json")["Body"].read())
        others = []
        shapes = Counter()
        pend_sfx = Counter()
        for r in v.get("symbols") or []:
            if r.get("status") != "PENDING_RESOLUTION":
                continue
            sy = str(r.get("symbol"))
            if ":" in sy:
                shapes["prefixed"] += 1
                continue
            if re.match(r"^\d{6}_", sy):
                shapes["cot"] += 1
                continue
            if sy.endswith("!"):
                shapes["futures"] += 1
                continue
            m2 = re.match(r"^([A-Z]{2})([A-Z0-9]{1,12})$", sy)
            if m2:
                shapes["econ-bare"] += 1
                pend_sfx[m2.group(2)] += 1
                continue
            shapes["other"] += 1
            if len(others) < 44:
                others.append(sy)
        rep.log("  shapes: " + json.dumps(dict(shapes.most_common())))
        rep.log("  other samples: " + json.dumps(others)[:900])

        rep.section("C. label-wave-2 sizing (PENDING suffixes)")
        pmi = sum(pend_sfx.get(s, 0)
                  for s in ("MPMI", "COMPPMI", "SPMI"))
        dead = sum(pend_sfx.get(s, 0) for s in
                   ("CU", "CCI", "LEI", "CLI", "TH", "CS", "IC", "LPS",
                    "FYGDPG", "COP", "TVS", "DUSD", "MGDPYY", "GDPQQ",
                    "GFCF", "FO", "CPR", "IPMM"))
        rep.kv(pmi_pending=pmi, deadcode_pending=dead)
        rep.log("  top pending suffixes: " + json.dumps(
            dict(pend_sfx.most_common(24)))[:500])

        failed = [l for k2 in [0] for l, k in checks if not k]
        for l, k in checks:
            (rep.ok if k else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"4192 DONE — mpryy={bool(proven)} pmi={pmi} dead={dead}")


if __name__ == "__main__":
    main()
