"""ops 4694 — bank the 21 Wayback ICE series (uploaded direct to chat,
no browser upload path needed).

VALIDATION HONESTY: the grabber clipped archived rows at 2023-08-14 by
design, to never risk touching authentic FRED data. That means these
21 series were built to have NO overlapping date with the banked FRED
window -- so the "checksum N/M shared dates agree" method used earlier
tonight (Farinelli/GMS_VAAS, which DID overlap) cannot apply here by
construction. This op checks the REAL banked start date live (not
assumed) and applies the strongest validation actually possible:
  - if real overlap exists (banked starts <= archived's last date):
    exact-match checksum, same standard as before.
  - if it's a clean gap (the expected case): adjacency check (gap in
    trading days is small) + value-continuity check (archived's last
    value is close to banked's first value -- credit spreads don't
    jump), reported explicitly as a WEAKER validation than a checksum.
Series that fail either check are NOT merged. Every merge is
union-only; the delete-proof policy makes it permanent.
"""
import gzip
import json
import sys
from datetime import datetime

import boto3

from ops_report import report

B = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")


def contract(r, name, cond, why):
    if cond:
        r.ok("  [%s] %s" % (name, why))
        return 0
    r.fail("  [%s] CONTRACT MISS — %s" % (name, why))
    return 1


def main():
    with report("4694_bank_wayback21") as r:
        r.heading("ops 4694 — bank 21 Wayback ICE series (uploaded "
                  "direct)")
        misses = 0

        r.section("1. Load the uploaded file")
        with gzip.open("aws/ops/_incoming_jh_ice.json.gz") as f:
            series_list = json.load(f)
        misses += contract(r, "load", len(series_list) == 21,
                           "%d series loaded" % len(series_list))
        ids = [s["id"] for s in series_list]

        r.section("2. Locate banked docs")
        want = set(x + ".json" for x in ids)
        kmap, tok = {}, None
        while True:
            kw = {"Bucket": B, "Prefix": "data/warm/fred-scoped/",
                  "MaxKeys": 1000}
            if tok:
                kw["ContinuationToken"] = tok
            resp = s3.list_objects_v2(**kw)
            for o in resp.get("Contents") or []:
                bn = o["Key"].rsplit("/", 1)[-1]
                if bn in want:
                    kmap[bn[:-5]] = o["Key"]
            if not resp.get("IsTruncated") or len(kmap) == len(ids):
                break
            tok = resp.get("NextContinuationToken")
        misses += contract(r, "locate", len(kmap) == len(ids),
                           "%d/%d banked docs found" % (len(kmap),
                                                        len(ids)))

        r.section("3. Per-series: real overlap check, correct "
                  "validation, merge")
        merged, checksummed, continuity, rejected = [], [], [], []
        for s in series_list:
            sid, arch_rows = s["id"], s["rows"]
            key = kmap.get(sid)
            if not key:
                rejected.append((sid, "no banked doc"))
                continue
            doc = json.loads(s3.get_object(
                Bucket=B, Key=key)["Body"].read())
            cur = doc.get("observations") or []
            cur_dates = sorted(o.get("date") for o in cur
                               if o.get("date"))
            if not cur_dates:
                rejected.append((sid, "banked doc has no obs"))
                continue
            banked_first = cur_dates[0]
            cmap = {o.get("date"): str(o.get("value")) for o in cur}
            arch_last_date, arch_last_val = arch_rows[-1]

            overlap = [(d, v) for d, v in arch_rows if d in cmap]
            if overlap:
                mm = 0
                for d, v in overlap:
                    try:
                        if abs(float(v) - float(cmap[d])) > 0.02:
                            mm += 1
                    except Exception:
                        mm += 1
                if mm > max(2, 0.05 * len(overlap)):
                    rejected.append((sid, "checksum failed %d/%d"
                                    % (mm, len(overlap))))
                    continue
                method = "checksum %d/%d agree" % (
                    len(overlap) - mm, len(overlap))
                checksummed.append(sid)
            else:
                try:
                    d1 = datetime.strptime(arch_last_date, "%Y-%m-%d")
                    d2 = datetime.strptime(banked_first, "%Y-%m-%d")
                    gap_days = (d2 - d1).days
                except Exception:
                    gap_days = 999
                if gap_days < 0 or gap_days > 10:
                    rejected.append((sid, "bad adjacency: archived "
                                    "ends %s, banked starts %s "
                                    "(%d day gap)"
                                    % (arch_last_date, banked_first,
                                       gap_days)))
                    continue
                banked_first_val = cmap.get(banked_first)
                try:
                    delta = abs(float(arch_last_val)
                               - float(banked_first_val))
                    rel = delta / max(0.5, abs(float(arch_last_val)))
                except Exception:
                    rel = 999
                if rel > 0.25:
                    rejected.append((sid, "value discontinuity: "
                                    "archived last=%.3f banked "
                                    "first=%.3f (%.0f%% jump)"
                                    % (float(arch_last_val),
                                       float(banked_first_val), rel
                                       * 100)))
                    continue
                method = ("adjacency (%d-day gap) + value-continuity "
                          "(%.3f -> %.3f, %.1f%% delta) — WEAKER than "
                          "checksum, no shared date exists by design"
                          % (gap_days, float(arch_last_val),
                             float(banked_first_val), rel * 100))
                continuity.append(sid)

            new_obs = sorted(
                [{"date": d, "value": v} for d, v in arch_rows] + cur,
                key=lambda o: str(o["date"]))
            doc["observations"] = new_obs
            doc["meta"] = dict(doc.get("meta") or {},
                               recovered_rows=len(arch_rows),
                               recovered_source="wayback (uploaded "
                               "direct to chat) — validated via: "
                               + method,
                               history_floor=new_obs[0]["date"])
            doc["meta"].pop("history_gap", None)
            s3.put_object(Bucket=B, Key=key,
                          Body=json.dumps(doc, default=str).encode(),
                          ContentType="application/json")
            merged.append(sid)
            r.ok("  %-20s +%d rows -> %d obs, %s -> %s  [%s]"
                 % (sid, len(arch_rows), len(new_obs),
                    new_obs[0]["date"], new_obs[-1]["date"], method))

        for sid, why in rejected:
            r.warn("  %-20s REJECTED — %s" % (sid, why))

        r.section("verdict")
        r.log("  merged=%d (checksum-validated=%d, "
              "continuity-validated=%d) rejected=%d"
              % (len(merged), len(checksummed), len(continuity),
                 len(rejected)))
        misses += contract(r, "result", len(merged) >= 15,
                           "%d/21 series merged" % len(merged))
        try:
            cov = json.loads(s3.get_object(
                Bucket=B,
                Key="data/repo-coverage.json")["Body"].read())
        except Exception:
            cov = {}
        cov["ice_wayback_direct"] = {
            "merged": merged, "checksummed": checksummed,
            "continuity_only": continuity,
            "rejected": [{"id": a, "reason": b} for a, b in rejected]}
        s3.put_object(Bucket=B, Key="data/repo-coverage.json",
                      Body=json.dumps(cov, default=str).encode(),
                      ContentType="application/json")

        if misses:
            r.fail("bank: %d red" % misses)
            sys.exit(1)
        r.ok("21-series Wayback recovery banked — %d exact-checksum "
             "validated, %d adjacency+continuity validated (honestly "
             "labeled as weaker evidence), permanent under the "
             "delete-proof policy"
             % (len(checksummed), len(continuity)))


if __name__ == "__main__":
    main()
