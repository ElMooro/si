#!/usr/bin/env python3
"""Plot /series observations for every data provider.

The warehouse pull kept only exact daily OHLC and returned nothing for
provider obs. Namespaced measurement ids also returned before that pull.
Keep the OHLC rules. When /series returns observations for the requested
id, or for a series inside that id, plot those values. Do not manufacture
candles from closes. A miss on a measurement id still cannot become a ticker.
"""
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
ENGINE = ROOT / "jh-chart-engine.js"
LEDGER = ROOT / "tests/fixtures/chart-oecd-additional/transition.json"
MARKER = "chart-provider-series.v1"
EXPECTED = "00a5b3e1a22395ae36a776844ac0ca1509cc4c21745c649d98289997b3412594"
START = "    // Preserve the complete exchange id."
END = "    var ws=warehouseSpec(tfId);"
WALL_OLD = "    // A measurement ID cannot become a similarly named exchange ticker.\n    if(scalar){\n      // Scalar download completion cannot publish a label for a superseded selection.\n      return [];\n    }\n"
WALL_NEW = "    // A measurement ID cannot become a similarly named exchange ticker.\n    // Namespaced ids, including every data provider, take the warehouse pull first.\n    if(scalar && String(sym).indexOf(\":\")<0){\n      // Scalar download completion cannot publish a label for a superseded selection.\n      return [];\n    }\n"
BLOCK_NEW = "    // One /series pull for every namespaced id. Data providers plot the\n    // observations the warehouse returned. Daily OHLC is drawn only from\n    // reported OHLC and is never manufactured from closes. A measurement\n    // the warehouse does not return still cannot become an exchange ticker.\n    if(String(sym).indexOf(\":\")>=0 && !/^(DATA|provider|DESK|CQSNAP|CQARM|CQDOC):/i.test(String(sym))){\n      var seriesUrl=PROXY+\"/series?id=\"+encodeURIComponent(sym),tvRaw=null;\n      try{\n        tvRaw=await fetchJson(seriesUrl);\n        var want=String(sym).toUpperCase();\n        var got=tvRaw&&typeof tvRaw.id===\"string\"?tvRaw.id.toUpperCase():\"\";\n        var bound=!!got&&(got===want||got.indexOf(want+\":\")===0);\n        var prov=tvRaw&&tvRaw.provider?String(tvRaw.provider).toLowerCase():\"\";\n        var marketProv=prov===\"equity\"||prov===\"tv\"||prov===\"instrument\"||prov===\"polygon\";\n        if(bound&&!marketProv&&Array.isArray(tvRaw.obs)){\n          var obsBars=[],seenObs={},obsOk=true;\n          for(var oi=0;oi<tvRaw.obs.length;oi++){\n            var orow=tvRaw.obs[oi];\n            if(!Array.isArray(orow)||orow.length<2){obsOk=false;break;}\n            var od=orow[0],ov=orow[1];\n            if(ov===null||ov===\"\")continue;\n            var on=typeof ov===\"number\"?ov:+ov;\n            if(!isFinite(on)){obsOk=false;break;}\n            var ot=null;\n            if(typeof od===\"string\"&&/^\\d{4}-\\d{2}-\\d{2}$/.test(od)){\n              var oms=Date.parse(od+\"T00:00:00Z\");\n              if(isFinite(oms)&&new Date(oms).toISOString().slice(0,10)===od)ot=oms/1000;\n            }else if(typeof od===\"string\"&&/^(\\d{4})-(\\d{2})$/.test(od)){\n              var om=/^(\\d{4})-(\\d{2})$/.exec(od);\n              ot=Date.UTC(+om[1],+om[2],0)/1000;\n            }else if(typeof od===\"string\"&&/^\\d{4}$/.test(od)){\n              ot=Date.UTC(+od,0,1)/1000;\n            }\n            if(ot===null){obsOk=false;break;}\n            if(Object.prototype.hasOwnProperty.call(seenObs,ot)&&seenObs[ot]!==on)continue;\n            if(!Object.prototype.hasOwnProperty.call(seenObs,ot)){\n              seenObs[ot]=on;\n              obsBars.push({time:ot,open:on,high:on,low:on,close:on,volume:null});\n            }\n          }\n          if(obsOk&&obsBars.length>=8){\n            obsBars.sort(function(a,b){return a.time-b.time;});\n            var obsSrc=(tvRaw.provider_name||tvRaw.provider||\"series\")+\" \u00b7 \"+obsBars.length+\" observations\";\n            if(tvRaw.via)obsSrc+=\" \u00b7 \"+sym+\" \u2192 \"+tvRaw.via;\n            else if(got!==want)obsSrc+=\" \u00b7 \"+tvRaw.id;\n            identifyBars(obsBars,sym,tfId,obsSrc);\n            try{barEvidence.get(obsBars).market_history={contract:\"chart-provider-series.v1\",requested_id:sym,reported_id:tvRaw.id,request_url:seriesUrl,source_frequency:tvRaw.freq||null,display_interval:tfId,via:tvRaw.via||null,provider_identity_verified:false,full_history_verified:false,calls_eligible:false,sizing_eligible:false,scope:\"Scalar observations from /series for this id. Not market OHLC. Values are not reconstructed into candles.\"};}catch(eEv){}\n            if(!quiet&&sym===active&&tfId===tf)lastSource=obsSrc;\n            return obsBars;\n          }\n        }\n        var dailyTf=/^(1d|2d|3d|5d|1w|2w|1M|3M)$/.test(sp[0]);\n        if(bound&&dailyTf&&!tvRaw.via&&!tvRaw.routing_evidence&&tvRaw.freq===\"D\"&&Array.isArray(tvRaw.ohlc)){\n          var tvBars=[],seenDays=new Set(),ordinals=[],ohlcOk=true;\n          for(var rowIndex=0;rowIndex<tvRaw.ohlc.length;rowIndex++){\n            var row=tvRaw.ohlc[rowIndex];\n            if(!Array.isArray(row)||(row.length!==5&&row.length!==6)||typeof row[0]!==\"string\"||!/^\\d{4}-\\d{2}-\\d{2}$/.test(row[0])){ohlcOk=false;break;}\n            var stamp=Date.parse(row[0]+\"T00:00:00Z\");\n            if(!isFinite(stamp)||new Date(stamp).toISOString().slice(0,10)!==row[0]||seenDays.has(row[0])){ohlcOk=false;break;}\n            for(var priceIndex=1;priceIndex<=4;priceIndex++)if(typeof row[priceIndex]!==\"number\"||!isFinite(row[priceIndex])){ohlcOk=false;break;}\n            if(!ohlcOk)break;\n            if(row[2]<Math.max(row[1],row[4])||row[3]>Math.min(row[1],row[4])||row[2]<row[3]){ohlcOk=false;break;}\n            seenDays.add(row[0]);ordinals.push(rowIndex);\n            tvBars.push({time:stamp/1000,open:row[1],high:row[2],low:row[3],close:row[4],volume:reportedVolume(row[5])});\n          }\n          if(ohlcOk){\n            tvBars.sort(function(a,b){return a.time-b.time;});\n            if(tvBars.length>=8){\n              var shown=resampleToTf(tvBars,tfId);\n              if(shown&&shown.length>=2&&barsFitTf(shown,tfId)){\n                var tvSrc=\"Warehouse OHLC \u00b7 provider identity and historical completeness unverified\";\n                identifyBars(shown,sym,tfId,tvSrc);\n                barEvidence.get(shown).market_history={contract:\"chart-market-series.v1\",requested_id:sym,reported_id:tvRaw.id,request_url:seriesUrl,source_frequency:\"D\",display_interval:tfId,whole_packet:tvRaw,source_row_ordinals:ordinals,\n                  provider_identity_verified:false,full_history_verified:false,raw_upstream_replay_verified:false,point_in_time_verified:false,calls_eligible:false,sizing_eligible:false,\n                  scope:\"Complete received parsed warehouse packet retained. Full id binding is checked; upstream instrument identity is not independently verified. OHLC values are not reconstructed from closes. Missing volume remains unavailable. Display aggregation retains the original rows in this packet.\"};\n                if(!quiet&&sym===active&&tfId===tf)lastSource=tvSrc;\n                return shown;\n              }\n            }\n          }\n        }\n      }catch(eTv){}\n      if(scalar)return [];\n    }\n"


def sha256(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def main():
    raw = ENGINE.read_text(encoding="utf-8")
    ledger = json.loads(LEDGER.read_text(encoding="utf-8"))
    row = ledger["changes"]["jh-chart-engine.js"]
    if MARKER in raw:
        if sha256(raw) != row.get("after_sha256"):
            raise SystemExit("marker present but engine hash does not match the ledger")
        if WALL_NEW not in raw or raw.count(BLOCK_NEW) != 1:
            raise SystemExit("marker present but the provider pull is not the recorded block")
        print("provider series pull already present")
        return
    if sha256(raw) != EXPECTED:
        raise SystemExit("engine hash moved; restage before applying")
    start = raw.find(START)
    end = raw.find(END, start)
    if start < 0 or end < 0 or raw.find(START, start + 1) >= 0:
        raise SystemExit("series block markers are not unique")
    old_block = raw[start:end]
    if row["replacements"][0]["after"] != old_block:
        raise SystemExit("ledger series block is not the engine block")
    if raw.count(WALL_OLD) != 1:
        raise SystemExit("scalar wall is not unique")
    text = raw.replace(old_block, BLOCK_NEW, 1).replace(WALL_OLD, WALL_NEW, 1)
    if text.count(BLOCK_NEW) != 1 or text.count(WALL_NEW) != 1 or MARKER not in text:
        raise SystemExit("provider pull did not land once")
    back = text.replace(WALL_NEW, WALL_OLD, 1).replace(BLOCK_NEW, old_block, 1)
    if back != raw:
        raise SystemExit("reverse did not restore the engine")
    row["replacements"][0]["after"] = BLOCK_NEW
    row["replacements"].append({"before": WALL_OLD, "after": WALL_NEW})
    row["after_sha256"] = sha256(text)
    check = text
    for edit in reversed(row["replacements"]):
        if check.count(edit["after"]) != 1:
            raise SystemExit("replacement after is not unique during reverse")
        check = check.replace(edit["after"], edit["before"], 1)
    if sha256(check) != row["before_sha256"]:
        raise SystemExit("full reverse does not restore the recorded engine snapshot")
    ENGINE.write_text(text, encoding="utf-8")
    LEDGER.write_text(json.dumps(ledger, indent=2) + "\n", encoding="utf-8")
    print("provider series pull applied", row["after_sha256"])


if __name__ == "__main__":
    main()
