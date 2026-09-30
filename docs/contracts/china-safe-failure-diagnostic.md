# China original-run failure diagnostic

The existing native producer emits a deliberately small diagnostic when retained publication fails. This read-only operation selects only that exact diagnostic prefix during the original September 30 14:30–14:45 UTC execution window. It validates the exact deployed package and receipt, original resources and schedule before and after reading the log population.

Every matching page and event is processed. Identical repeated event IDs are deduplicated; conflicting IDs, malformed events or a population above 128 refuse the complete diagnostic. Only the reviewed contract, stage, exception class, known safe reason, typed clock/count, known service error code and fixed public output identities are retained. Unknown fields and raw exception text are rejected without being echoed. A hash and byte count bind each complete diagnostic message.

The operation does not read current packets, private retained originals, account or consumer state. It does not invoke the producer, probe a provider, write native data or change the schedule. No matching diagnostic means the cause remains unknown; it does not establish successful publication. A failure context identifies a repair target, not investment qualification or source-data correctness.
