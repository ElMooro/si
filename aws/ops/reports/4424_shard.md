# ops 4424 — sharded health sweep — PASS — sweep completed in 73.8s, 13 findings, 8 repairs, cursor 36/37
- elapsed: 73.8s | timeout now 300
- issues: {"rejected_no_evidence": 1, "duplicate_acks": 4, "stuck_task": 8}
- repairs: [
 {
  "thread": "verify-batch-4407-4412",
  "action": "repost",
  "ok": true
 },
 {
  "thread": "verify-batch-4407-4412",
  "action": "nudge",
  "state": "ACK"
 },
 {
  "thread": "evidence-packs",
  "action": "nudge",
  "state": "ACK"
 },
 {
  "thread": "0805174930",
  "action": "nudge",
  "state": "ACK"
 },
 {
  "thread": "0805162503",
  "action": "nudge",
  "state": "ACK"
 },
 {
  "thread": "handshake-protocol",
  "action": "nudge",
  "state": "FILED"
 }
]
- findings: [
 {
  "thread": "verify-batch-4407-4412",
  "issue": "rejected_no_evidence",
  "by": "claude"
 },
 {
  "thread": "verify-batch-4407-4412",
  "issue": "duplicate_acks",
  "count": 5
 },
 {
  "thread": "0805175702",
  "issue": "duplicate_acks",
  "count": 6
 },
 {
  "thread": "evidence-packs",
  "issue": "duplicate_acks",
  "count": 5
 },
 {
  "thread": "0805174930",
  "issue": "duplicate_acks",
  "count": 5
 },
 {
  "thread": "verify-batch-4407-4412",
  "issue": "stuck_task",
  "state": "ACK",
  "minutes": 79
 },
 {
  "thread": "evidence-packs",
  "issue": "stuck_task",
  "state": "ACK",
  "minutes": 80
 },
 {
  "thread": "0805174930",
  "issue": "stuck_task",
  "state": "ACK",
  "minutes": 79
 }
]
- cursor: {"i": 36, "total_threads": 37, "swept": ["data/a2a/threads/backtest-infra.json", "data/a2a/threads/audit-loop-shard-002.json", "data/a2a/threads/external-ai-council.json", "data/a2a/threads/0007-code-
