# ops 6919 - the World tab's histories, dry run (nothing written), 2026-11-22T18:23+00:00

**Status:** success  
**Duration:** 24.3s  
**Finished:** 2026-11-22T18:24:20+00:00  

## Data

| cds_measured | cds_reported | countries | errors | failed_checks | measures | seconds | yield_monthly | yield_reported |
|---|---|---|---|---|---|---|---|---|
| 25 | 38 | 44 | 0 | 0 | 137 | 24 | 30 | 44 |

## Log
## 1. The run

- `18:24:20` receipt: {"published": true, "generated_at": "2026-11-22T18:23:56.700009+00:00", "coverage": {"countries": 44, "measures": 137, "by_source": {"oecd": 25, "reported": 82, "dtcc": 25, "ecb": 5}, "errors": 0, "status": "measured"}, "sets": {"yield": 30, "cds": 25, "yield_reported": 44, "cds_reported": 38}, "publications_folded": 71, "series_asked": 30, "seconds": 23.6}
- `18:24:20` 24s | objects read from the bucket 145 (of which the world document's kept publications and receipts: 140) | writes kept in memory: ['data/ciss-research/sovereign/world-state.json', 'data/ciss-sovereign-world.json']
- `18:24:20` monthly series asked of the resolver: 30 | RU:ok/0.1s PL:ok/0.1s ZA:ok/0.1s CZ:ok/0.1s MX:ok/0.1s FR:ok/0.1s CL:ok/0.1s IT:ok/0.1s HU:ok/0.1s IL:ok/0.1s GR:ok/0.1s BE:ok/0.1s NZ:ok/0.1s JP:ok/0.1s ES:ok/0.1s CA:ok/0.1s US:ok/0.1s PT:ok/0.1s GB:ok/0.1s FI:ok/0.1s AT:ok/0.1s AU:ok/0.1s IE:ok/0.1s KR:ok/0.0s NL:ok/0.1s DE:ok/0.1s SE:ok/0.1s DK:ok/0.1s CH:ok/0.0s NO:ok/0.1s
- `18:24:20` document sovereign-world.v1 1.0.0 | 726085 bytes, 122167 gzip | state 485220 bytes | errors {}
## 2. The four sets

- `18:24:20` yield           30 of 44 countries | sources {'oecd': 25, 'ecb': 5} | status {'ended': 1, 'current': 29} | earliest start 1953-04-30 | newest 2026-10-31
- `18:24:20`                 not held for: Turkey, Brazil, Colombia, Vietnam, Peru, Indonesia, India, Thailand, Philippines, Taiwan, Hong Kong, Malaysia, China, Singapore
- `18:24:20` cds             25 of 44 countries | sources {'dtcc': 25} | status {'current': 15, 'late': 10} | earliest start 2024-09-03 | newest 2026-11-20
- `18:24:20`                 not held for: Russia, Poland, Czech Republic, Vietnam, India, Hungary, Greece, Taiwan, New Zealand, Canada, Hong Kong, Portugal, Finland, Ireland, Singapore, Sweden, Denmark, Switzerland, Norway
- `18:24:20` yield_reported  44 of 44 countries | sources {'reported': 44} | status {'current': 44} | earliest start 2026-09-26 | newest 2026-11-22
- `18:24:20`                 not held for: nobody
- `18:24:20` cds_reported    38 of 44 countries | sources {'reported': 38} | status {'current': 38} | earliest start 2026-09-26 | newest 2026-11-22
- `18:24:20`                 not held for: Russia, Czech Republic, Vietnam, Peru, Thailand, Taiwan
- `18:24:20`   Russia          y oecd 1999-01..2018-06-30 n234 last 7.62 | c - | yr reported 2026-09..2026-11-22 n53 last 17.104 | cr -
- `18:24:20`   Turkey          y - | c dtcc 2024-09..2026-11-20 n526 last 260.38 | yr reported 2026-09..2026-11-22 n53 last 33.55 | cr reported 2026-09..2026-11-22 n53 last 244.51
- `18:24:20`   Brazil          y - | c dtcc 2024-09..2026-11-20 n542 last 179.5 | yr reported 2026-09..2026-11-22 n53 last 15.663 | cr reported 2026-09..2026-11-22 n53 last 175.36
- `18:24:20`   Colombia        y - | c dtcc 2024-09..2026-11-20 n538 last 184.0 | yr reported 2026-09..2026-11-22 n53 last 12.848 | cr reported 2026-09..2026-11-22 n53 last 149.16
- `18:24:20`   Poland          y ecb 2001-01..2026-10-31 n310 last 6.32 | c - | yr reported 2026-09..2026-11-22 n53 last 6.291 | cr reported 2026-09..2026-11-22 n53 last 151.0
- `18:24:20`   South Africa    y oecd 1957-01..2026-10-31 n838 last 8.94 | c dtcc 2024-09..2026-11-20 n526 last 147.91 | yr reported 2026-09..2026-11-22 n53 last 8.845 | cr reported 2026-09..2026-11-22 n53 last 134.51
- `18:24:20`   Czech Republic  y oecd 2000-04..2026-10-31 n319 last 5.3725 | c - | yr reported 2026-09..2026-11-22 n53 last 5.26 | cr -
- `18:24:20`   Vietnam         y - | c - | yr reported 2026-09..2026-11-22 n53 last 4.568 | cr -
- `18:24:20`   Peru            y - | c dtcc 2024-09..2026-11-20 n497 last 76.03 | yr reported 2026-09..2026-11-22 n53 last 6.166 | cr -
- `18:24:20`   Mexico          y oecd 2001-07..2026-10-31 n238 last 9.25 | c dtcc 2024-09..2026-11-20 n541 last 101.5 | yr reported 2026-09..2026-11-22 n53 last 9.348 | cr reported 2026-09..2026-11-22 n53 last 98.84
- `18:24:20`   France          y oecd 1960-01..2026-10-31 n802 last 4.89 | c dtcc 2025-10..2026-11-19 n51 last 89.87 | yr reported 2026-09..2026-11-22 n53 last 4.873 | cr reported 2026-09..2026-11-22 n53 last 90.23
- `18:24:20`   Indonesia       y - | c dtcc 2024-09..2026-11-20 n520 last 102.8 | yr reported 2026-09..2026-11-22 n53 last 7.711 | cr reported 2026-09..2026-11-22 n53 last 103.08
- `18:24:20`   India           y - | c - | yr reported 2026-09..2026-11-22 n53 last 7.183 | cr reported 2026-09..2026-11-22 n53 last 87.67
- `18:24:20`   Chile           y oecd 2004-07..2026-10-31 n268 last 6.05 | c dtcc 2024-09..2026-11-20 n472 last 57.0 | yr reported 2026-09..2026-11-22 n53 last 6.105 | cr reported 2026-09..2026-11-22 n53 last 67.03
- `18:24:20`   Thailand        y - | c dtcc 2024-09..2026-11-20 n4 last 36.36 | yr reported 2026-09..2026-11-22 n53 last 1.94 | cr -
- `18:24:20`   Italy           y ecb 1991-03..2026-10-31 n428 last 4.68 | c dtcc 2024-09..2026-10-19 n44 last 66.1 | yr reported 2026-09..2026-11-22 n53 last 4.407 | cr reported 2026-09..2026-11-22 n53 last 63.23
- `18:24:20`   Hungary         y oecd 1999-02..2026-10-31 n333 last 5.57 | c - | yr reported 2026-09..2026-11-22 n53 last 5.57 | cr reported 2026-09..2026-11-22 n53 last 85.59
- `18:24:20`   Philippines     y - | c dtcc 2024-09..2026-11-20 n486 last 64.18 | yr reported 2026-09..2026-11-22 n53 last 7.75 | cr reported 2026-09..2026-11-22 n53 last 57.35
- `18:24:20`   Israel          y oecd 1997-01..2026-10-31 n358 last 4.25 | c dtcc 2024-09..2026-10-15 n9 last 50.85 | yr reported 2026-09..2026-11-22 n53 last 4.216 | cr reported 2026-09..2026-11-22 n53 last 60.07
- `18:24:20`   Greece          y ecb 1992-10..2026-10-31 n408 last 4.41 | c - | yr reported 2026-09..2026-11-22 n53 last 4.256 | cr reported 2026-09..2026-11-22 n53 last 50.41
- `18:24:20`   Taiwan          y - | c - | yr reported 2026-09..2026-11-22 n53 last 1.87 | cr -
- `18:24:20`   Belgium         y oecd 1955-01..2026-10-31 n862 last 4.38 | c dtcc 2026-09..2026-11-10 n9 last 43.97 | yr reported 2026-09..2026-11-22 n53 last 4.302 | cr reported 2026-09..2026-11-22 n53 last 46.18
- `18:24:20`   New Zealand     y oecd 1970-01..2026-10-31 n681 last 5.01 | c - | yr reported 2026-09..2026-11-22 n53 last 4.82 | cr reported 2026-09..2026-11-22 n53 last 21.4
- `18:24:20`   Japan           y oecd 1989-01..2026-10-31 n454 last 2.755 | c dtcc 2025-09..2026-10-13 n51 last 29.56 | yr reported 2026-09..2026-11-22 n53 last 2.789 | cr reported 2026-09..2026-11-22 n53 last 28.67
- `18:24:20`   Spain           y oecd 1980-01..2026-10-31 n562 last 4.0715 | c dtcc 2025-02..2026-11-13 n29 last 28.41 | yr reported 2026-09..2026-11-22 n53 last 3.908 | cr reported 2026-09..2026-11-22 n53 last 29.63
- `18:24:20`   Canada          y oecd 1955-01..2026-10-31 n862 last 3.8748 | c - | yr reported 2026-09..2026-11-22 n53 last 3.709 | cr reported 2026-09..2026-11-22 n53 last 39.6
- `18:24:20`   United States   y oecd 1953-04..2026-10-31 n883 last 5.13 | c dtcc 2025-04..2026-11-05 n19 last 48.24 | yr reported 2026-09..2026-11-22 n53 last 4.912 | cr reported 2026-09..2026-11-22 n53 last 35.3
- `18:24:20`   Hong Kong       y - | c - | yr reported 2026-09..2026-11-22 n53 last 3.913 | cr reported 2026-09..2026-11-22 n53 last 36.2
- `18:24:20`   Portugal        y ecb 1993-07..2026-10-31 n400 last 3.917 | c - | yr reported 2026-09..2026-11-22 n53 last 3.792 | cr reported 2026-09..2026-11-22 n53 last 24.72
- `18:24:20`   United Kingdom  y oecd 1960-01..2026-10-31 n802 last 5.3393 | c dtcc 2024-09..2026-11-10 n59 last 22.18 | yr reported 2026-09..2026-11-22 n53 last 5.243 | cr reported 2026-09..2026-11-22 n53 last 25.37
- `18:24:20`   Malaysia        y - | c dtcc 2024-09..2026-11-19 n361 last 35.39 | yr reported 2026-09..2026-11-22 n53 last 3.82 | cr reported 2026-09..2026-11-22 n53 last 57.19
- `18:24:20`   Finland         y oecd 1988-01..2026-10-31 n465 last 3.8212 | c - | yr reported 2026-09..2026-11-22 n53 last 3.695 | cr reported 2026-09..2026-11-22 n53 last 15.16
- `18:24:20`   Austria         y oecd 1990-01..2026-10-31 n442 last 3.6898 | c dtcc 2026-06..2026-09-16 n2 last 10.21 | yr reported 2026-09..2026-11-22 n53 last 3.548 | cr reported 2026-09..2026-11-22 n53 last 18.29
- `18:24:20`   Australia       y oecd 1969-07..2026-10-31 n688 last 5.265 | c dtcc 2026-10..2026-11-05 n4 last 16.88 | yr reported 2026-09..2026-11-22 n53 last 4.959 | cr reported 2026-09..2026-11-22 n53 last 16.38
- `18:24:20`   China           y - | c dtcc 2024-09..2026-11-20 n488 last 34.49 | yr reported 2026-09..2026-11-22 n53 last 1.626 | cr reported 2026-09..2026-11-22 n53 last 34.35
- `18:24:20`   Ireland         y oecd 1970-12..2026-10-31 n671 last 3.5525 | c - | yr reported 2026-09..2026-11-22 n53 last 3.441 | cr reported 2026-09..2026-11-22 n53 last 17.12
- `18:24:20`   South Korea     y oecd 2000-10..2026-10-31 n313 last 4.21 | c dtcc 2024-09..2026-11-20 n393 last 23.4 | yr reported 2026-09..2026-11-22 n53 last 3.867 | cr reported 2026-09..2026-11-22 n53 last 22.03
- `18:24:20`   Singapore       y - | c - | yr reported 2026-09..2026-11-22 n53 last 2.271 | cr reported 2026-09..2026-11-22 n53 last 25.56
- `18:24:20`   Netherlands     y oecd 1959-01..2026-10-31 n814 last 3.535 | c dtcc 2025-11..2026-09-16 n2 last 7.23 | yr reported 2026-09..2026-11-22 n53 last 3.386 | cr reported 2026-09..2026-11-22 n53 last 13.49
- `18:24:20`   Germany         y oecd 1956-05..2026-10-31 n846 last 3.4159 | c dtcc 2024-09..2026-10-29 n17 last 12.06 | yr reported 2026-09..2026-11-22 n53 last 3.303 | cr reported 2026-09..2026-11-22 n53 last 12.91
- `18:24:20`   Sweden          y oecd 1986-12..2026-10-31 n479 last 3.1683 | c - | yr reported 2026-09..2026-11-22 n53 last 3.018 | cr reported 2026-09..2026-11-22 n53 last 9.69
- `18:24:20`   Denmark         y ecb 1983-05..2026-10-31 n522 last 3.228 | c - | yr reported 2026-09..2026-11-22 n53 last 3.126 | cr reported 2026-09..2026-11-22 n53 last 10.07
- `18:24:20`   Switzerland     y oecd 1955-01..2026-10-31 n862 last 0.51 | c - | yr reported 2026-09..2026-11-22 n53 last 0.532 | cr reported 2026-09..2026-11-22 n53 last 7.0
- `18:24:20`   Norway          y oecd 1985-01..2026-10-31 n502 last 4.433 | c - | yr reported 2026-09..2026-11-22 n53 last 4.274 | cr reported 2026-09..2026-11-22 n53 last 12.9
## 3. Checks

- `18:24:20` a second run on what the first kept: folded 1 publication(s), asked 0 series more, 0.5s
- `18:24:20` ✅ the World tab's histories compile from the live stores; nothing was written
