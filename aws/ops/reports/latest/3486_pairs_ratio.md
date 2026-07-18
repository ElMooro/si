# ops 3486 — pairs ratio builder

**Status:** success  
**Duration:** 142.9s  
**Finished:** 2026-07-18T23:57:08+00:00  

## Log
- `23:54:45` FAIL  T1_ratio_micro — /tmp/tmpqbp8y40s.js:272
  var R=FGChart.ratio(A,B);
        ^

ReferenceError: FGChart is not defined
    at /tmp/tmpqbp8y40s.js:272:9
    at Object.<anonymous> (/tmp/tmpqbp8y40s.js:277:3)
    at Module._compile (node:in
- `23:57:08` PASS  T2_served_core — {'node_ok': [True, True, True, True]}
- `23:57:08` PASS  T3_flagship_v21 — {'ops3486': True, 'rtbtn': True, 'ratioRaw': True, 'chip_x': True, 'whales_intact': True, 'flags_intact': True, 'marks_intact': True, 'ern_intact': True}
# RESULT: FAILS: ['T1_ratio_micro']

