#!/usr/bin/env python3
"""Keep accounting projections strict. The pane shows reported dollars separately."""
from pathlib import Path
import hashlib
import json

ROOT = Path(__file__).resolve().parents[3]
JS = ROOT / "jh-chart-buyback.js"
TRAN = ROOT / "tests/fixtures/buyback-evidence/transition.json"
STRICT = r"""  function observations(row){
    const source=row?.measurements?.cashflow_observations;
    if(!Array.isArray(source))return {status:'missing_or_invalid_observations',rows:[]};
    return {status:'reported_observations',rows:source.map((received,index)=>{
      const issues=[];const observation=object(received)?received:{};
      const end=day(observation.date),start=day(observation.start_date),unit=observation.reported_currency;
      const comparable=row?.measurement_contract===CONTRACT&&observation.eligible===true&&observation.reported_calendar_duration_aligned===true&&start!==null&&end!==null&&start<=end&&typeof unit==='string'&&/^[A-Z]{3}$/.test(unit);
      if(!comparable)issues.push('Quarter identity, duration or currency is unqualified.');
      const read=(key,field,sign)=>{
        const metric=observation.metrics?.[key];
        if(!comparable||!object(metric)||metric.status!=='reported_value'||metric.source_field!==field||metric.sign!==sign||metric.unit!==unit)return null;
        return number(metric.value);
      };
      // The producer already converted net issuance to signed net repurchases.
      const net=read('net_common_repurchases','netCommonStockIssuance','negative');
      const grossValue=read('gross_common_repurchases','commonStockRepurchased','magnitude');
      const gross=grossValue!==null&&grossValue>=0?grossValue:null;
      if(net===null)issues.push('Reported net repurchases unavailable; gross is separate.');
      const cap=number(row?.market_cap);
      const aligned=cap!==null&&cap>0&&day(row?.market_cap_asof)===end&&row?.market_cap_unit===unit;
      let ratio=null;
      if(net!==null&&aligned){ratio=number(net/cap*100);if(net!==0&&ratio===0)ratio=null;}
      if(!aligned)issues.push('Market cap date and currency do not match this quarter.');
      if(net!==null&&aligned&&ratio===null)issues.push('Ratio exceeds supported numeric precision.');
      return {source_index:index,start_date:start,end_date:end,unit:typeof unit==='string'?unit:null,net,gross,ratio,issues,received};
    })};
  }
"""
PAIRS = [
    ("  function observations(row){", "  function reported(row){"),
    ("  function reported(row){", STRICT + "  function reported(row){"),
    ("const projected=observations(row);", "const projected=reported(row);"),
]

def main():
    text = JS.read_text()
    done = (
        "const projected=reported(row);" in text
        and text.count("function observations(row)") == 1
        and text.count("function reported(row)") == 1
    )
    if not done:
        for before, after in PAIRS:
            count = text.count(before)
            if count == 1:
                text = text.replace(before, after, 1)
                continue
            if count == 0 and text.count(after) == 1:
                continue
            raise SystemExit("needle missing or repeated: " + str(count))
    if "const projected=reported(row);" not in text or text.count("function observations(row)") != 1:
        raise SystemExit("split missing")
    doc = json.loads(TRAN.read_text())
    block = doc["changes"]["jh-chart-buyback.js"]
    known = {item["after"] for item in block["edits"]}
    for before, after in PAIRS:
        if after not in known:
            block["edits"].append({"before": before, "after": after})
            known.add(after)
    digest = hashlib.sha256(text.encode()).hexdigest()
    changed = False
    if text != JS.read_text():
        JS.write_text(text)
        changed = True
    if block["after_sha256"] != digest:
        block["after_sha256"] = digest
        TRAN.write_text(json.dumps(doc, indent=2) + "\n")
        changed = True
    raw = text
    for item in reversed(block["edits"]):
        if raw.count(item["after"]) != 1:
            raise SystemExit("preservation after-string is not unique")
        raw = raw.replace(item["after"], item["before"], 1)
    back = hashlib.sha256(raw.encode()).hexdigest()
    if back != block["before_sha256"]:
        raise SystemExit("preservation reverse hash mismatch " + back)
    print("ok", digest, "changed" if changed else "unchanged")

if __name__ == "__main__":
    main()
