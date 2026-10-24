# ops 6642 - the S3 cost anomaly of Oct 21-22: cause, is it over, AWS feedback

**Status:** success  
**Duration:** 1.4s  
**Finished:** 2026-10-24T14:30:02+00:00  

## Data

| anomalies | backfill_anomalies | batch_jobs_active | batch_jobs_total | marked | oct23_batch_ops_usd | oct23_sia_put_usd | oct23_sia_puts | problems |
|---|---|---|---|---|---|---|---|---|
| 5 | ["4ba9ae1d-eb31-40f2-8d22-5fb356d0c860"] | 0 | 3 | ["4ba9ae1d-eb31-40f2-8d22-5fb356d0c860"] | 0.0 | 0.83 | 83148 | 0 |

## Log
## A. Cost Anomaly Detection - anomalies since Oct 15

- `14:30:01` 4ba9ae1d-eb3 2026-10-21 -> 2026-10-22 | max $20.55/day, total $38.88 (actual $48.38 vs expected $9.5) | feedback None | Amazon Simple Storage Service | Amazon Simple Storag/Requests-SIA-Tier1 $21.83; Amazon Simple Storag/BatchOperations-Objects $2.09; Amazon Simple Storag/Retrieval-SIA $1.73
- `14:30:01` 2eb00391-c97 2026-10-16 -> 2026-10-16 | max $0.69/day, total $0.69 (actual $2.84 vs expected $2.15) | feedback None | Amazon Simple Queue Service | Amazon Simple Queue /? $0.69
- `14:30:01` 309894c5-3f8 2026-10-12 -> 2026-10-16 | max $448.89/day, total $1994.69 (actual $2008.31 vs expected $13.62) | feedback None | AWS Lambda | AWS Lambda/Lambda-GB-Second $1978.31
- `14:30:01` a66945ec-901 2026-10-12 -> 2026-10-16 | max $8.13/day, total $36.33 (actual $36.33 vs expected $0.0) | feedback None | CloudWatch Events | CloudWatch Events/USE1-Event-64K-Chunks $36.32
- `14:30:01` fd3c7dcd-1d4 2026-10-12 -> 2026-10-16 | max $136.57/day, total $421.66 (actual $423.76 vs expected $2.1) | feedback None | Amazon Simple Storage Service | Amazon Simple Storag/TimedStorage-ByteHrs $230.52; Amazon Simple Storag/Requests-Tier1 $131.94; Amazon Simple Storag/Requests-Tier2 $13.32; Amazon Simple Storag/DataTransfer-Out-Bytes $11.12
## B. Cost Explorer - S3 by usage type per day, account total per day

- `14:30:01` 2026-10-19 S3 $  8.49 | TimedStorage-SIA-ByteHrs $0.07 (6), TimedStorage-ByteHrs $0.79 (34), Requests-Tier1 $0.11 (21236), Requests-Tier2 $0.04 (99776), DataTransfer-Out-Bytes $7.41 (82)
- `14:30:01` 2026-10-20 S3 $ 12.83 | BatchOperations-Jobs $0.25 (1), TimedStorage-SIA-ByteHrs $0.07 (6), TimedStorage-ByteHrs $0.79 (34), Requests-Tier1 $0.06 (12718), Requests-Tier2 $0.06 (147863), DataTransfer-Out-Bytes $11.51 (128)
- `14:30:01` 2026-10-21 S3 $ 18.91 | Requests-SIA-Tier1 $13.55 (1354833), BatchOperations-Objects $1.28 (1282030), BatchOperations-Jobs $0.25 (1), Retrieval-SIA $0.02 (2), TimedStorage-SIA-ByteHrs $0.07 (6), TimedStorage-ByteHrs $0.77 (34), Requests-Tier1 $0.51 (102856), Requests-Tier2 $0.22 (552854), DataTransfer-Out-Bytes $2.10 (23)
- `14:30:01` 2026-10-22 S3 $ 29.47 | Requests-SIA-Tier1 $21.83 (2183028), BatchOperations-Objects $2.09 (2086159), BatchOperations-Jobs $0.25 (1), Retrieval-SIA $1.74 (174), TimedStorage-SIA-ByteHrs $0.25 (20), TimedStorage-ByteHrs $0.78 (34), Requests-Tier1 $0.52 (103348), Requests-Tier2 $0.21 (515906), DataTransfer-Out-Bytes $1.63 (18)
- `14:30:01` 2026-10-23 S3 $  6.75 | Requests-SIA-Tier1 $0.83 (83148), Retrieval-SIA $0.02 (2), TimedStorage-SIA-ByteHrs $0.49 (39), TimedStorage-ByteHrs $0.79 (34), Requests-Tier1 $0.61 (122484), Requests-Tier2 $0.23 (578527), DataTransfer-Out-Bytes $3.61 (40)
- `14:30:01` 2026-10-24 (partial) S3 $  0.32 | Requests-SIA-Tier1 $0.08 (8182), Retrieval-SIA $0.02 (2), Requests-Tier1 $0.06 (11748), Requests-Tier2 $0.02 (47306), DataTransfer-Out-Bytes $0.12 (1)
- `14:30:01` 2026-10-19 account total (usage, before credits) $20.76 (estimated)
- `14:30:01` 2026-10-20 account total (usage, before credits) $13.74 (estimated)
- `14:30:01` 2026-10-21 account total (usage, before credits) $22.06 (estimated)
- `14:30:01` 2026-10-22 account total (usage, before credits) $32.55 (estimated)
- `14:30:01` 2026-10-23 account total (usage, before credits) $9.79 (estimated)
- `14:30:01` 2026-10-24 account total (usage, before credits) $0.87 (estimated)
- `14:30:01` Oct 23: 83148 Standard-IA PUTs ($0.83), Batch Operations $0.00 -> back to the archive's daily copying
## C. S3 Batch Operations jobs

- `14:30:01` job 646ccb69 S3ReplicateObject created 2026-10-22 16:13: Complete succeeded 2086159 / 2086159, failed 0 backfill #2 (ops 6612)
- `14:30:01` job 523b902a S3ReplicateObject created 2026-10-21 02:20: Complete succeeded 1282030 / 1282030, failed 0 backfill #1 (ops 6583)
- `14:30:01` job de1a80a2 S3ReplicateObject created 2026-10-20 23:41: Cancelled succeeded 0 / 0, failed 0 
## D. replication configuration

- `14:30:02` rules: [('warehouse-archive-all', 'Enabled', '', 'STANDARD_IA')]
## E. fix: AWS feedback on the backfill anomaly

- `14:30:02` ✅ 4ba9ae1d-eb3 (S3, 2026-10-21, total $38.88) marked PLANNED_ACTIVITY: the approved archive backfills
- `14:30:02` ✅ the anomaly is the two approved archive backfills (Oct 21-22); nothing is still running
