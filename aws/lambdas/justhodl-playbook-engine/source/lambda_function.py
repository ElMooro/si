"""justhodl-playbook-engine v1.0 — ops 3263.

Khalid's 3,300+ TradingView notes contain TESTED RULES he refined over
years ("ECONOMY CRASH LAGS YIELD CURVE INVERSION BY 30 MONTHS", "IF YOU
GONNA BUY OPTIONS ... WHEN ...", "X top marks Y bottom"). This engine
extracts them deterministically (no LLM dependency) and evaluates the
flagship timing rule against LIVE data.

INPUT   data/tradingview-notes.json (the mirror)
OUTPUT  data/playbook-rules.json
  · rules[]: {id, symbol, family, text, params}
  · families: counts per extraction family
  · flagship.yield_curve: T10Y2Y inversion onset, months elapsed,
    Khalid's 30-month lag marker date — FACTS, stated plainly.

Families:
  TIMING      "...lags/leads ... by N months/weeks"
  INVARIANT   emphatic ALWAYS / NEVER claims
  TURN        top/bottom marks|signals|before|after ...
  CONDITIONAL when|if ... then/expect/→ ...
"""
import json
import re
import sys
from datetime import datetime, timezone

import boto3

sys.path.insert(0, "/var/task")
import series_source as SS  # bundled shared

BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name="us-east-1")
OUT_KEY = "data/playbook-rules.json"

FAMS = [
    ("TIMING", re.compile(
        r"(lag|lead)s?\b.{0,50}?\bby\s+(\d{1,3})\s*"
        r"(month|week|day)s?", re.I)),
    ("INVARIANT", re.compile(r"\b(ALWAYS|NEVER)\b")),
    ("TURN", re.compile(
        r"\b(top|bottom)s?\b.{0,60}?\b(mark|signal|before|after|"
        r"precede)s?\b", re.I)),
    ("CONDITIONAL", re.compile(
        r"\b(when|if)\b.{8,90}?\b(then|expect|means|→|->)\b", re.I)),
]


def extract(notes):
    rules, seen = [], set()
    for n in notes:
        t = str(n.get("text") or "").strip()
        if len(t) < 30:
            continue
        for fam, rx in FAMS:
            m = rx.search(t)
            if not m:
                continue
            key = (fam, t[:120].lower())
            if key in seen:
                continue
            seen.add(key)
            sym = "UNTAGGED"
            ms = re.match(r"\[TV:([^\]]+)\]", t)
            if ms:
                sym = ms.group(1)[:24]
            params = {}
            if fam == "TIMING":
                params = {"direction": m.group(1).lower(),
                          "n": int(m.group(2)),
                          "unit": m.group(3).lower()}
            rules.append({"id": n.get("id"), "symbol": sym,
                          "family": fam, "params": params,
                          "text": t[:240]})
            break
    return rules


def flagship_yield_curve():
    """His rule: economy crash LAGS yield-curve inversion by 30 months.
    Facts from T10Y2Y: onset of the most recent inversion episode,
    months elapsed, the 30-month marker date."""
    try:
        w = SS.fetch("FRED", "T10Y2Y")
        ks = sorted(w)
        vals = [(k, float(w[k])) for k in ks]
        onset = None
        for i, (k, v) in enumerate(vals):
            if v < 0 and (i == 0 or vals[i - 1][1] >= 0):
                onset = k
        if not onset:
            return {"status": "no inversion episode in series"}
        d0 = datetime.fromisoformat(onset[:10])
        nowd = datetime.now(timezone.utc).replace(tzinfo=None)
        months = round((nowd - d0).days / 30.44, 1)
        marker = d0.replace(
            year=d0.year + (d0.month + 29) // 12,
            month=(d0.month + 29) % 12 + 1)
        cur = vals[-1]
        return {"series": "FRED T10Y2Y (10y-2y)",
                "latest": {"week": cur[0], "value": round(cur[1], 3)},
                "most_recent_inversion_onset": onset,
                "months_elapsed": months,
                "khalid_lag_months": 30,
                "lag_marker_date": marker.date().isoformat(),
                "note": "his tested rule: crash lags inversion by ~30 "
                        "months — facts above, no forecast dressing"}
    except Exception as e:
        return {"status": f"eval error: {str(e)[:80]}"}


def lambda_handler(event, context):
    notes = json.loads(S3.get_object(
        Bucket=BUCKET, Key="data/tradingview-notes.json")
        ["Body"].read()).get("notes") or []
    rules = extract(notes)
    fams = {}
    for r in rules:
        fams[r["family"]] = fams.get(r["family"], 0) + 1
    rules.sort(key=lambda r: (r["family"] != "TIMING",
                              r["family"], -len(r["text"])))
    doc = {"generated_at": datetime.now(timezone.utc).isoformat(),
           "source_notes": len(notes),
           "n_rules": len(rules),
           "families": fams,
           "flagship": {"yield_curve": flagship_yield_curve()},
           "rules": rules[:250],
           "note": "Khalid's tested playbook, extracted from his own "
                   "notes (deterministic, ops 3263)"}
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(doc, ensure_ascii=False),
                  ContentType="application/json")
    print(f"[playbook] {len(rules)} rules from {len(notes)} notes "
          f"({fams})")
    return {"ok": True, "n_rules": len(rules)}
