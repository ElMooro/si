# Storage heartbeat scope

`storage-heartbeat.v1` monitors configured S3 metadata and twelve named bindings
observed in read-only operation 6446. It reads no provider or application bodies.
`system_status` is UNKNOWN; source observation freshness is not measured.
`storage_monitor_status` reports these limited operational checks. Legacy
`n_fresh`/`n_stale` are null; explicit storage counts replace false data-freshness
claims. All original seventeen artifact checks remain, with canonical FINRA,
enriched 8-K and XBRL-index checks added. Short-book and filings-aggregate legacy
rows identify their separate declared producers. Declarations do not identify
the last writer or certify those producers.

Head not-found, access failure, future/invalid metadata, recent/stale storage and
incomplete inventory are distinct. Prefix scans retain every received metadata
row; only a completed listing within 50,000 objects/128 pages receives complete
status. It is not an atomic namespace snapshot or a provider-population census.
Two times the configured elapsed storage interval is an operational threshold,
not a provider publication SLA or trading calendar. A weekly feed is no longer
subject to a contradictory universal 48-hour limit. Collection-completion clocks
prevent objects written during the scan from borrowing the earlier start clock.

Read-only named classic/Scheduler checks verify target, state, cadence and timezone
and distinguish denied reads from confirmed missing bindings. Multiple existing
bindings are preserved. Clients use a two-second connect/three-second read limit
and one attempt. The 85-second collection budget and twenty-second Lambda reserve
leave unknown entries when exhausted. Output still uses the original heartbeat
key. Failed publication raises instead of reporting success.

The heartbeat function itself had no observed classic/Scheduler binding in two
complete baseline passes. Its previous cadence-only declaration is retained as
unbound historical metadata; deployment neither provisions nor changes a trigger.
Other invocation routes and natural current publication are unverified. The
runtime remains Python 3.12, x86_64, 256 MiB, 120 seconds, and 512 MiB ephemeral
storage. Calls, sizing and execution permission stay false.
