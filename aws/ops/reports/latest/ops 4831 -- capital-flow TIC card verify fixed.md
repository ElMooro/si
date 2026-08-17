# G0. live JSON contract for every card binding

**Status:** success  
**Duration:** 0.2s  
**Finished:** 2026-08-17T17:02:54+00:00  

## Log
- `17:02:54` ✅   flows_bn.total latest=+262.8 12m=+1771.9 z=1.61
- `17:02:54` ✅   flows_bn.treas latest=+17.0 12m=+334.8 z=-0.16
- `17:02:54` ✅   flows_bn.equity latest=+134.3 12m=+901.7 z=1.27
- `17:02:54` ✅   flows_bn.corp latest=+52.5 12m=+448.8 z=1.17
- `17:02:54` ✅   flows_bn.agency latest=+19.1 12m=+128.9 z=0.67
- `17:02:54` ✅   flows_bn.tbills latest=-43.5 12m=+52.6 z=-1.63
- `17:02:54` ✅   signals.risk_appetite latest=+206.0B z=1.6
- `17:02:54` ✅   signals.safe_haven latest=-117.3B z=-1.2
- `17:02:54` ✅   signals.total_demand latest=+223.0B z=1.22
- `17:02:54` ✅   signals.official_private latest=+230.7B z=1.84
- `17:02:54` ✅   holder_splits.lt_total status=OK gap=0.0
- `17:02:54` ✅   split OK: private +246.8B vs official +16.1B
- `17:02:54` ✅   country_lt_treasury: 5 OK rows (e.g. china +608.5B, other-gap 6.1)
# 1. committed-HTML asserts

- `17:02:54` ✅   token 'id="tic-ff-card"'           x1
- `17:02:54` ✅   token '/data/foreign-flows.json'   x1
- `17:02:54` ✅   token 'country_lt_treasury'        x1
- `17:02:54` ✅   token 'holder_splits'              x1
- `17:02:54` ✅   token 'official_private'           x1
- `17:02:54` ✅   token 'custodial bias'             x1
# 2. served check (<=8 min; CF purge-on-success live)

- `17:02:54` ✅ card SERVED at https://justhodl.ai/capital-flow.html after 0s
# 3. verdict

- `17:02:54` ✅ capital-flow.html now surfaces the TIC organ: six flows, four signals incl private-vs-official, reconciled split, country decomposition with the honest 'other' gap
