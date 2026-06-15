import json, urllib.request
d=json.loads(urllib.request.urlopen(urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev/data/bottleneck-boom-research.json",headers={"User-Agent":"x"}),timeout=20).read())
bt=d.get("by_ticker",{})
def c(f): return sum(1 for v in bt.values() if v.get(f) is not None)
def cl(f): return sum(1 for v in bt.values() if v.get(f))
print("FEED:")
print(f"  cash_conv {c('cash_conv')} accruals {c('accruals')} | nde {c('net_debt_ebitda')} int_cov {c('int_cov')} cur {c('cur_ratio')}")
print(f"  insider {c('insider_sig')} | sector_mom {c('sector_mom')} | peg {c('peg')} ev_ebitda {c('ev_ebitda')}")
print(f"  bear {cl('bear')} | scorecard {c('score_bull')} | seg_conc {c('seg_conc')} acq {c('acq_pct')}")
p=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/bottleneck-boom.html",headers={"User-Agent":"Mozilla/5.0"}),timeout=20).read().decode("utf-8","ignore")
checks={"scorecard":"scol bull" in p,"bear box":"bearbox" in p,"quality section":"Quality &amp; solvency" in p,
 "insider badge":"Insiders" in p,"sector":"Sector 1M" in p,"PEG":"PEG <b>" in p or "PEG" in p,
 "bottleneck-trend":"Bottleneck" in p and "intensifying" in p,"row score chip":"score_bull!=null" in p,
 "clean filter":"Few red flags" in p,"header fixed":"['ticker','Name','l']" in p}
print("\nPAGE live:")
for k,val in checks.items(): print(f"  {'OK' if val else 'MISS'}  {k}")
# show a couple scorecards
for t in ("VST","DELL"):
    v=bt.get(t,{})
    print(f"\n{t}: {v.get('score_bull')}▲/{v.get('score_bear')}▼  bull={v.get('flags_bull')}  bear={v.get('flags_bear')}")
