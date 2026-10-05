#!/usr/bin/env python3
"""Revert the provider-as-candle pull. Pages rejected observations-as-OHLC."""
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
ENGINE = ROOT / "jh-chart-engine.js"
LEDGER = ROOT / "tests/fixtures/chart-oecd-additional/transition.json"
MARKER = "chart-provider-series.v1"
RESTORED = "00a5b3e1a22395ae36a776844ac0ca1509cc4c21745c649d98289997b3412594"
START = "    // One /series pull for every namespaced id."
END = "    var ws=warehouseSpec(tfId);"
WALL_MARK = "    // Namespaced ids, including every data provider, take the warehouse pull first.\n"
OLD_BLOCK = "    // Preserve the complete exchange id. This endpoint reports daily OHLC;\n    // observations without OHLC cannot be manufactured into market candles.\n    if(String(sym).indexOf(\":\")>=0 && !/^(DATA|provider|DESK|CQSNAP|CQARM|CQDOC):/i.test(String(sym))){\n      var seriesUrl=PROXY+\"/series?id=\"+encodeURIComponent(sym),tvRaw=null;\n      try{\n        if(!/^(1d|2d|3d|5d|1w|2w|1M|3M)$/.test(sp[0]))return [];\n        tvRaw=await fetchJson(seriesUrl);\n        if(!tvRaw||typeof tvRaw!==\"object\"||typeof tvRaw.id!==\"string\"||tvRaw.id.toUpperCase()!==String(sym).toUpperCase()||tvRaw.via||tvRaw.routing_evidence||tvRaw.freq!==\"D\"||!Array.isArray(tvRaw.ohlc))return [];\n        var tvBars=[],seenDays=new Set(),ordinals=[];\n        for(var rowIndex=0;rowIndex<tvRaw.ohlc.length;rowIndex++){\n          var row=tvRaw.ohlc[rowIndex];\n          if(!Array.isArray(row)||(row.length!==5&&row.length!==6)||typeof row[0]!==\"string\"||!/^\\d{4}-\\d{2}-\\d{2}$/.test(row[0]))return [];\n          var stamp=Date.parse(row[0]+\"T00:00:00Z\");\n          if(!Number.isFinite(stamp)||new Date(stamp).toISOString().slice(0,10)!==row[0]||seenDays.has(row[0]))return [];\n          for(var priceIndex=1;priceIndex<=4;priceIndex++)if(typeof row[priceIndex]!==\"number\"||!Number.isFinite(row[priceIndex]))return [];\n          if(row[2]<Math.max(row[1],row[4])||row[3]>Math.min(row[1],row[4])||row[2]<row[3])return [];\n          seenDays.add(row[0]);ordinals.push(rowIndex);\n          tvBars.push({time:stamp/1000,open:row[1],high:row[2],low:row[3],close:row[4],volume:reportedVolume(row[5])});\n        }\n        tvBars.sort(function(a,b){return a.time-b.time;});\n        if(tvBars.length<8)return [];\n        var shown=resampleToTf(tvBars,tfId);\n        if(!shown||shown.length<2||!barsFitTf(shown,tfId))return [];\n        var tvSrc=\"Warehouse OHLC \u00b7 provider identity and historical completeness unverified\";\n        identifyBars(shown,sym,tfId,tvSrc);\n        barEvidence.get(shown).market_history={contract:\"chart-market-series.v1\",requested_id:sym,reported_id:tvRaw.id,request_url:seriesUrl,source_frequency:\"D\",display_interval:tfId,whole_packet:tvRaw,source_row_ordinals:ordinals,\n          provider_identity_verified:false,full_history_verified:false,raw_upstream_replay_verified:false,point_in_time_verified:false,calls_eligible:false,sizing_eligible:false,\n          scope:\"Complete received parsed warehouse packet retained. Full id binding is checked; upstream instrument identity is not independently verified. OHLC values are not reconstructed from closes. Missing volume remains unavailable. Display aggregation retains the original rows in this packet.\"};\n        if(!quiet&&sym===active&&tfId===tf)lastSource=tvSrc;\n        return shown;\n      }catch(eTv){return [];}\n    }\n"
WALL_OLD = "    // A measurement ID cannot become a similarly named exchange ticker.\n    if(scalar){\n      // Scalar download completion cannot publish a label for a superseded selection.\n      return [];\n    }\n"


def sha256(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def main():
    raw = ENGINE.read_text(encoding="utf-8")
    ledger = json.loads(LEDGER.read_text(encoding="utf-8"))
    row = ledger["changes"]["jh-chart-engine.js"]
    if MARKER not in raw:
        if sha256(raw) != RESTORED:
            raise SystemExit("provider pull is gone but the engine is not the restored hash")
        print("provider candle pull already reverted")
        return
    start = raw.find(START)
    end = raw.find(END, start)
    wall_at = raw.find(WALL_MARK)
    if start < 0 or end < 0 or wall_at < 0 or raw.find(START, start + 1) >= 0:
        raise SystemExit("revert markers are not unique")
    # The wall if() is the three lines after the marker comment.
    wall_end = raw.find("    }\n", wall_at)
    if wall_end < 0:
        raise SystemExit("scalar wall end missing")
    wall_end += len("    }\n")
    wall_start = raw.rfind("    // A measurement ID cannot become", 0, wall_at)
    if wall_start < 0:
        raise SystemExit("scalar wall start missing")
    text = raw[:wall_start] + WALL_OLD + raw[wall_end:start] + OLD_BLOCK + raw[end:]
    if MARKER in text or sha256(text) != RESTORED:
        raise SystemExit("revert did not restore 00a5b3e1: " + sha256(text))
    added = row["replacements"][-1]
    if "Namespaced ids, including every data provider" not in added.get("after", ""):
        raise SystemExit("ledger wall replacement is not the one this pull added")
    row["replacements"].pop()
    row["replacements"][0]["after"] = OLD_BLOCK
    row["after_sha256"] = RESTORED
    check = text
    for edit in reversed(row["replacements"]):
        if check.count(edit["after"]) != 1:
            raise SystemExit("restored replacement after is not unique")
        check = check.replace(edit["after"], edit["before"], 1)
    if sha256(check) != row["before_sha256"]:
        raise SystemExit("restored ledger does not reverse to the snapshot")
    ENGINE.write_text(text, encoding="utf-8")
    LEDGER.write_text(json.dumps(ledger, indent=2) + "\n", encoding="utf-8")
    print("provider candle pull reverted", RESTORED)


if __name__ == "__main__":
    main()
