# ops 6608 - S3 cost by usage type, last 8 days (read-only), 2026-10-22 11:29 UTC

**Status:** success  
**Duration:** 0.9s  
**Finished:** 2026-10-22T11:29:49+00:00  

## Data

| days | latest_day | latest_usd | read |
|---|---|---|---|
| 8 | 2026-10-21 | 18.91 | True |

## Log
- `11:29:49` 2026-10-14  S3 $83.44  top: TimedStorage-ByteHrs $42.33 (1841); Requests-Tier1 $30.79 (6157189); Requests-Tier2 $5.55 (13876448); DataTransfer-Out-Bytes $4.60 (51); TimedStorage-SIA-ByteHrs $0.07 (6); USE1-StorageLens-ObjCount $0.06 (293980)
- `11:29:49` 2026-10-15  S3 $116.04  top: TimedStorage-ByteHrs $73.25 (3185); Requests-Tier1 $30.64 (6127488); DataTransfer-Out-Bytes $6.52 (72); Requests-Tier2 $5.42 (13549123); USE1-StorageLens-ObjCount $0.08 (390488); TimedStorage-SIA-ByteHrs $0.07 (6)
- `11:29:49` 2026-10-16  S3 $137.00  top: TimedStorage-ByteHrs $100.57 (4373); Requests-Tier1 $27.46 (5492217); Requests-Tier2 $4.79 (11986583); DataTransfer-Out-Bytes $3.94 (44); USE1-StorageLens-ObjCount $0.11 (527462); TimedStorage-SIA-ByteHrs $0.07 (6)
- `11:29:49` 2026-10-17  S3 $7.31  top: DataTransfer-Out-Bytes $5.28 (59); TimedStorage-ByteHrs $1.57 (68); Requests-Tier1 $0.18 (36587); USE1-StorageLens-ObjCount $0.12 (595796); TimedStorage-SIA-ByteHrs $0.07 (6)
- `11:29:49` 2026-10-18  S3 $12.71  top: DataTransfer-Out-Bytes $11.59 (129); TimedStorage-ByteHrs $0.79 (34); Requests-Tier1 $0.11 (21294); TimedStorage-SIA-ByteHrs $0.07 (6); Requests-Tier2 $0.06 (147597)
- `11:29:49` 2026-10-19  S3 $8.49  top: DataTransfer-Out-Bytes $7.41 (82); TimedStorage-ByteHrs $0.79 (34); Requests-Tier1 $0.11 (21236); TimedStorage-SIA-ByteHrs $0.07 (6)
- `11:29:49` 2026-10-20  S3 $12.83  top: DataTransfer-Out-Bytes $11.51 (128); TimedStorage-ByteHrs $0.79 (34); BatchOperations-Jobs $0.25 (1); TimedStorage-SIA-ByteHrs $0.07 (6); Requests-Tier1 $0.06 (12718); Requests-Tier2 $0.06 (147863)
- `11:29:49` 2026-10-21  S3 $18.91  top: Requests-SIA-Tier1 $13.55 (1354833); DataTransfer-Out-Bytes $2.10 (23); BatchOperations-Objects $1.28 (1282030); TimedStorage-ByteHrs $0.77 (34); Requests-Tier1 $0.51 (102856); BatchOperations-Jobs $0.25 (1); Requests-Tier2 $0.22 (552854); TimedStorage-SIA-ByteHrs $0.07 (6)
## usage types with any day >= $0.50

- `11:29:49` TimedStorage-ByteHrs: [["2026-10-14", [42.333, 1840.5]], ["2026-10-15", [73.253, 3184.9]], ["2026-10-16", [100.571, 4372.7]], ["2026-10-17", [1.566, 68.1]], ["2026-10-18", [0.789, 34.3]], ["2026-10-19", [0.789, 34.3]], ["2026-10-20", [0.789, 34.3]], ["2026-10-21", [0.771, 33.5]]]
- `11:29:49` Requests-Tier1: [["2026-10-14", [30.786, 6157189.0]], ["2026-10-15", [30.637, 6127488.0]], ["2026-10-16", [27.461, 5492217.0]], ["2026-10-17", [0.183, 36587.0]], ["2026-10-18", [0.106, 21294.0]], ["2026-10-19", [0.106, 21236.0]], ["2026-10-20", [0.064, 12718.0]], ["2026-10-21", [0.514, 102856.0]]]
- `11:29:49` Requests-SIA-Tier1: [["2026-10-20", [0.0, 1.0]], ["2026-10-21", [13.548, 1354833.0]]]
- `11:29:49` DataTransfer-Out-Bytes: [["2026-10-14", [4.599, 51.1]], ["2026-10-15", [6.52, 72.4]], ["2026-10-16", [3.939, 43.8]], ["2026-10-17", [5.28, 58.7]], ["2026-10-18", [11.589, 128.8]], ["2026-10-19", [7.406, 82.3]], ["2026-10-20", [11.514, 127.9]], ["2026-10-21", [2.102, 23.4]]]
- `11:29:49` Requests-Tier2: [["2026-10-14", [5.551, 13876448.0]], ["2026-10-15", [5.42, 13549123.0]], ["2026-10-16", [4.795, 11986583.0]], ["2026-10-17", [0.031, 78625.0]], ["2026-10-18", [0.059, 147597.0]], ["2026-10-19", [0.04, 99776.0]], ["2026-10-20", [0.059, 147863.0]], ["2026-10-21", [0.221, 552854.0]]]
- `11:29:49` BatchOperations-Objects: [["2026-10-21", [1.282, 1282030.0]]]
- `11:29:49` ✅ read-only; one Cost Explorer call
