# ops 6919 - the World tab's histories, dry run (nothing written), 2026-11-22T18:15+00:00

**Status:** failure  
**Duration:** 66.6s  
**Finished:** 2026-11-22T18:16:43+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/STAGED/ops_6919_world_history_dry_run.py", line 138, in main
    ck(us and us["source"] == "oecd" and us["start"] == "1953-04-30" and us["freq"] == "M", "United States monthly yield: %s" % (us and (us["source"], us["start"])))
                                                                                            ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~^~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
TypeError: not all arguments converted during string formatting

```

## Log
## 1. The run

- `18:16:43` receipt: {"published": true, "generated_at": "2026-11-22T18:15:36.850494+00:00", "coverage": {"countries": 44, "measures": 137, "by_source": {"oecd": 25, "reported": 82, "dtcc": 25, "ecb": 5}, "errors": 0, "status": "measured"}, "sets": {"yield": 30, "cds": 25, "yield_reported": 44, "cds_reported": 38}, "publications_folded": 70, "series_asked": 30, "seconds": 66.2}
- `18:16:43` 66s | objects read from the bucket 143 (of which the world document's kept publications and receipts: 138) | writes kept in memory: ['data/ciss-research/sovereign/world-state.json', 'data/ciss-sovereign-world.json']
- `18:16:43` monthly series asked of the resolver: 30 | RU:ok/0.8s PL:ok/0.5s ZA:ok/0.5s CZ:ok/0.5s MX:ok/0.5s FR:ok/20.4s CL:ok/0.4s IT:ok/1.0s HU:ok/0.4s IL:ok/0.4s GR:ok/0.8s BE:ok/1.0s NZ:ok/0.5s ES:ok/1.0s CA:ok/0.4s JP:ok/0.4s US:ok/0.5s PT:ok/1.0s GB:ok/0.3s FI:ok/0.8s AT:ok/0.8s AU:ok/0.2s IE:ok/0.9s KR:ok/0.2s NL:ok/0.7s DE:ok/0.2s SE:ok/0.2s DK:ok/0.2s CH:ok/0.2s NO:ok/0.2s
- `18:16:43` document sovereign-world.v1 1.0.0 | 723395 bytes, 121899 gzip | state 483669 bytes | errors {}
## 2. The four sets

- `18:16:43` yield           30 of 44 countries | sources {'oecd': 25, 'ecb': 5} | status {'ended': 1, 'current': 29} | earliest start 1953-04-30 | newest 2026-10-31
- `18:16:43`                 not held for: Turkey, Brazil, Colombia, Vietnam, Peru, Indonesia, India, Thailand, Philippines, Taiwan, Hong Kong, Malaysia, China, Singapore
- `18:16:43` cds             25 of 44 countries | sources {'dtcc': 25} | status {'current': 15, 'late': 10} | earliest start 2024-09-03 | newest 2026-11-20
- `18:16:43`                 not held for: Russia, Poland, Czech Republic, Vietnam, India, Hungary, Greece, Taiwan, New Zealand, Canada, Hong Kong, Portugal, Finland, Ireland, Singapore, Sweden, Denmark, Switzerland, Norway
- `18:16:43` yield_reported  44 of 44 countries | sources {'reported': 44} | status {'current': 44} | earliest start 2026-09-26 | newest 2026-11-21
- `18:16:43`                 not held for: nobody
- `18:16:43` cds_reported    38 of 44 countries | sources {'reported': 38} | status {'current': 38} | earliest start 2026-09-26 | newest 2026-11-21
- `18:16:43`                 not held for: Russia, Czech Republic, Vietnam, Peru, Thailand, Taiwan
- `18:16:43`   Russia          y oecd 1999-01..2018-06-30 n234 last 7.62 | c - | yr reported 2026-09..2026-11-21 n52 last 17.104 | cr -
- `18:16:43`   Turkey          y - | c dtcc 2024-09..2026-11-20 n526 last 260.38 | yr reported 2026-09..2026-11-21 n52 last 33.55 | cr reported 2026-09..2026-11-21 n52 last 244.51
- `18:16:43`   Brazil          y - | c dtcc 2024-09..2026-11-20 n542 last 179.5 | yr reported 2026-09..2026-11-21 n52 last 15.663 | cr reported 2026-09..2026-11-21 n52 last 175.36
- `18:16:43`   Colombia        y - | c dtcc 2024-09..2026-11-20 n538 last 184.0 | yr reported 2026-09..2026-11-21 n52 last 12.848 | cr reported 2026-09..2026-11-21 n52 last 149.16
- `18:16:43`   Poland          y ecb 2001-01..2026-10-31 n310 last 6.32 | c - | yr reported 2026-09..2026-11-21 n52 last 6.287 | cr reported 2026-09..2026-11-21 n52 last 151.0
- `18:16:43`   South Africa    y oecd 1957-01..2026-10-31 n838 last 8.94 | c dtcc 2024-09..2026-11-20 n526 last 147.91 | yr reported 2026-09..2026-11-21 n52 last 8.845 | cr reported 2026-09..2026-11-21 n52 last 134.51
- `18:16:43`   Czech Republic  y oecd 2000-04..2026-10-31 n319 last 5.3725 | c - | yr reported 2026-09..2026-11-21 n52 last 5.259 | cr -
- `18:16:43`   Vietnam         y - | c - | yr reported 2026-09..2026-11-21 n52 last 4.568 | cr -
- `18:16:43`   Peru            y - | c dtcc 2024-09..2026-11-20 n497 last 76.03 | yr reported 2026-09..2026-11-21 n52 last 6.166 | cr -
- `18:16:43`   Mexico          y oecd 2001-07..2026-10-31 n238 last 9.25 | c dtcc 2024-09..2026-11-20 n541 last 101.5 | yr reported 2026-09..2026-11-21 n52 last 9.348 | cr reported 2026-09..2026-11-21 n52 last 98.84
- `18:16:43`   France          y oecd 1960-01..2026-10-31 n802 last 4.89 | c dtcc 2025-10..2026-11-19 n51 last 89.87 | yr reported 2026-09..2026-11-21 n52 last 4.873 | cr reported 2026-09..2026-11-21 n52 last 90.23
- `18:16:43`   Indonesia       y - | c dtcc 2024-09..2026-11-20 n520 last 102.8 | yr reported 2026-09..2026-11-21 n52 last 7.711 | cr reported 2026-09..2026-11-21 n52 last 103.08
- `18:16:43`   India           y - | c - | yr reported 2026-09..2026-11-21 n52 last 7.183 | cr reported 2026-09..2026-11-21 n52 last 87.67
- `18:16:43`   Chile           y oecd 2004-07..2026-10-31 n268 last 6.05 | c dtcc 2024-09..2026-11-20 n472 last 57.0 | yr reported 2026-09..2026-11-21 n52 last 6.105 | cr reported 2026-09..2026-11-21 n52 last 67.03
- `18:16:43`   Thailand        y - | c dtcc 2024-09..2026-11-20 n4 last 36.36 | yr reported 2026-09..2026-11-21 n52 last 1.94 | cr -
- `18:16:43`   Italy           y ecb 1991-03..2026-10-31 n428 last 4.68 | c dtcc 2024-09..2026-10-19 n44 last 66.1 | yr reported 2026-09..2026-11-21 n52 last 4.407 | cr reported 2026-09..2026-11-21 n52 last 63.23
- `18:16:43`   Hungary         y oecd 1999-02..2026-10-31 n333 last 5.57 | c - | yr reported 2026-09..2026-11-21 n52 last 5.57 | cr reported 2026-09..2026-11-21 n52 last 85.59
- `18:16:43`   Philippines     y - | c dtcc 2024-09..2026-11-20 n486 last 64.18 | yr reported 2026-09..2026-11-21 n52 last 7.75 | cr reported 2026-09..2026-11-21 n52 last 57.35
- `18:16:43`   Israel          y oecd 1997-01..2026-10-31 n358 last 4.25 | c dtcc 2024-09..2026-10-15 n9 last 50.85 | yr reported 2026-09..2026-11-21 n52 last 4.216 | cr reported 2026-09..2026-11-21 n52 last 60.07
- `18:16:43`   Greece          y ecb 1992-10..2026-10-31 n408 last 4.41 | c - | yr reported 2026-09..2026-11-21 n52 last 4.256 | cr reported 2026-09..2026-11-21 n52 last 50.41
- `18:16:43`   Taiwan          y - | c - | yr reported 2026-09..2026-11-21 n52 last 1.87 | cr -
- `18:16:43`   Belgium         y oecd 1955-01..2026-10-31 n862 last 4.38 | c dtcc 2026-09..2026-11-10 n9 last 43.97 | yr reported 2026-09..2026-11-21 n52 last 4.302 | cr reported 2026-09..2026-11-21 n52 last 46.18
- `18:16:43`   New Zealand     y oecd 1970-01..2026-10-31 n681 last 5.01 | c - | yr reported 2026-09..2026-11-21 n52 last 4.82 | cr reported 2026-09..2026-11-21 n52 last 21.4
- `18:16:43`   Spain           y oecd 1980-01..2026-10-31 n562 last 4.0715 | c dtcc 2025-02..2026-11-13 n29 last 28.41 | yr reported 2026-09..2026-11-21 n52 last 3.906 | cr reported 2026-09..2026-11-21 n52 last 29.63
- `18:16:43`   Canada          y oecd 1955-01..2026-10-31 n862 last 3.8748 | c - | yr reported 2026-09..2026-11-21 n52 last 3.709 | cr reported 2026-09..2026-11-21 n52 last 39.6
- `18:16:43`   Japan           y oecd 1989-01..2026-10-31 n454 last 2.755 | c dtcc 2025-09..2026-10-13 n51 last 29.56 | yr reported 2026-09..2026-11-21 n52 last 2.751 | cr reported 2026-09..2026-11-21 n52 last 28.67
- `18:16:43`   United States   y oecd 1953-04..2026-10-31 n883 last 5.13 | c dtcc 2025-04..2026-11-05 n19 last 48.24 | yr reported 2026-09..2026-11-21 n52 last 4.941 | cr reported 2026-09..2026-11-21 n52 last 35.3
- `18:16:43`   Hong Kong       y - | c - | yr reported 2026-09..2026-11-21 n52 last 3.913 | cr reported 2026-09..2026-11-21 n52 last 36.2
- `18:16:43`   Portugal        y ecb 1993-07..2026-10-31 n400 last 3.917 | c - | yr reported 2026-09..2026-11-21 n52 last 3.782 | cr reported 2026-09..2026-11-21 n52 last 24.72
- `18:16:43`   United Kingdom  y oecd 1960-01..2026-10-31 n802 last 5.3393 | c dtcc 2024-09..2026-11-10 n59 last 22.18 | yr reported 2026-09..2026-11-21 n52 last 5.243 | cr reported 2026-09..2026-11-21 n52 last 25.37
- `18:16:43`   Malaysia        y - | c dtcc 2024-09..2026-11-19 n361 last 35.39 | yr reported 2026-09..2026-11-21 n52 last 3.82 | cr reported 2026-09..2026-11-21 n52 last 57.19
- `18:16:43`   Finland         y oecd 1988-01..2026-10-31 n465 last 3.8212 | c - | yr reported 2026-09..2026-11-21 n52 last 3.691 | cr reported 2026-09..2026-11-21 n52 last 15.16
- `18:16:43`   Austria         y oecd 1990-01..2026-10-31 n442 last 3.6898 | c dtcc 2026-06..2026-09-16 n2 last 10.21 | yr reported 2026-09..2026-11-21 n52 last 3.548 | cr reported 2026-09..2026-11-21 n52 last 18.29
- `18:16:43`   China           y - | c dtcc 2024-09..2026-11-20 n488 last 34.49 | yr reported 2026-09..2026-11-21 n52 last 1.576 | cr reported 2026-09..2026-11-21 n52 last 34.35
- `18:16:43`   Australia       y oecd 1969-07..2026-10-31 n688 last 5.265 | c dtcc 2026-10..2026-11-05 n4 last 16.88 | yr reported 2026-09..2026-11-21 n52 last 4.916 | cr reported 2026-09..2026-11-21 n52 last 16.38
- `18:16:43`   Ireland         y oecd 1970-12..2026-10-31 n671 last 3.5525 | c - | yr reported 2026-09..2026-11-21 n52 last 3.437 | cr reported 2026-09..2026-11-21 n52 last 17.12
- `18:16:43`   South Korea     y oecd 2000-10..2026-10-31 n313 last 4.21 | c dtcc 2024-09..2026-11-20 n393 last 23.4 | yr reported 2026-09..2026-11-21 n52 last 3.864 | cr reported 2026-09..2026-11-21 n52 last 22.03
- `18:16:43`   Singapore       y - | c - | yr reported 2026-09..2026-11-21 n52 last 2.271 | cr reported 2026-09..2026-11-21 n52 last 25.56
- `18:16:43`   Netherlands     y oecd 1959-01..2026-10-31 n814 last 3.535 | c dtcc 2025-11..2026-09-16 n2 last 7.23 | yr reported 2026-09..2026-11-21 n52 last 3.386 | cr reported 2026-09..2026-11-21 n52 last 13.49
- `18:16:43`   Germany         y oecd 1956-05..2026-10-31 n846 last 3.4159 | c dtcc 2024-09..2026-10-29 n17 last 12.06 | yr reported 2026-09..2026-11-21 n52 last 3.303 | cr reported 2026-09..2026-11-21 n52 last 12.91
- `18:16:43`   Sweden          y oecd 1986-12..2026-10-31 n479 last 3.1683 | c - | yr reported 2026-09..2026-11-21 n52 last 3.018 | cr reported 2026-09..2026-11-21 n52 last 9.69
- `18:16:43`   Denmark         y ecb 1983-05..2026-10-31 n522 last 3.228 | c - | yr reported 2026-09..2026-11-21 n52 last 3.126 | cr reported 2026-09..2026-11-21 n52 last 10.07
- `18:16:43`   Switzerland     y oecd 1955-01..2026-10-31 n862 last 0.51 | c - | yr reported 2026-09..2026-11-21 n52 last 0.532 | cr reported 2026-09..2026-11-21 n52 last 7.0
- `18:16:43`   Norway          y oecd 1985-01..2026-10-31 n502 last 4.433 | c - | yr reported 2026-09..2026-11-21 n52 last 4.274 | cr reported 2026-09..2026-11-21 n52 last 12.9
## 3. Checks

- `18:16:43` ✗ the document's countries are not the table's
