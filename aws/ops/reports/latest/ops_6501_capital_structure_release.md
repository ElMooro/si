# ops 6501 - capital-structure refresh: review and release

```json
{
 "ran_at": "2026-10-08T16:34:43.461736+00:00",
 "failed_control": {
  "contract": "capital-structure-recurring-refresh.v1",
  "status": "failed",
  "request_id": "scheduled-capital-structure:36269293338",
  "run_id": "36269293338",
  "started_at": "2026-09-26T20:22:41.647016+00:00",
  "error_type": "ValueError",
  "failed_at": "2026-09-26T20:44:45.384562+00:00",
  "active_phase": "part-1",
  "phase_started_at": "2026-09-26T20:23:04.248960+00:00",
  "completed_phases": [
   "plan"
  ],
  "updated_at": "2026-09-26T20:22:45.103658+00:00"
 },
 "phase_journal": {
  "phase": "part-1",
  "status": "claimed",
  "started_at": "2026-09-26T20:23:04.349871+00:00"
 },
 "batch_journal": {
  "status": "failed",
  "n_captures": 648,
  "counts": null,
  "error_type": "ValueError"
 },
 "release": {
  "released": true,
  "request_id": "scheduled-capital-structure:36269293338",
  "failed_control": {
   "key": "audit-private/20260909-originals/share-structure-research/2eae4bff98a6d3af9d45607774bb35ccb315d8c1021be94aab8e66bbae67b24b.bin",
   "sha256": "2eae4bff98a6d3af9d45607774bb35ccb315d8c1021be94aab8e66bbae67b24b",
   "bytes": 2108
  },
  "error_type": "ValueError",
  "failed_at": "2026-09-26T20:44:45.384562+00:00",
  "active_phase": "part-1",
  "completed_phases": [
   "plan"
  ]
 },
 "after": {
  "status": "released",
  "request_id": "scheduled-capital-structure:36269293338",
  "updated_at": "2026-09-26T20:22:45.103658+00:00",
  "review": {
   "active_phase": "part-1",
   "completed_phases": [
    "plan"
   ],
   "error_type": "ValueError",
   "failed_at": "2026-09-26T20:44:45.384562+00:00",
   "failed_control": {
    "bytes": 2108,
    "key": "audit-private/20260909-originals/share-structure-research/2eae4bff98a6d3af9d45607774bb35ccb315d8c1021be94aab8e66bbae67b24b.bin",
    "sha256": "2eae4bff98a6d3af9d45607774bb35ccb315d8c1021be94aab8e66bbae67b24b"
   },
   "failed_status": "failed",
   "note": "audit 2026-10-08: run 36269293338 part-1 failure reviewed; failed control retained; new cycle permitted, failed cycle never retried",
   "ops": 6501,
   "released_at": "2026-10-08T16:34:44.391177+00:00"
  }
 }
}
```

VERDICT: PASS
