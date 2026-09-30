# Portfolio original-response integrity

The existing private holdings model consumes daily Polygon aggregate responses.
Its return definitions, exposure calculations, sample alignment and output schema
remain unchanged. This contract governs acquisition and original-input validation;
it does not qualify forecast skill, account reconciliation or trade authority.

Each response must be a complete HTTP 200 message of at most four MiB. Reads use
available-data chunks through EOF. An optional Content-Length must be one unsigned
ASCII decimal value (ordinary HTTP spaces/tabs at the edges are accepted), within
the byte bound, and equal the complete received length. Short reads are not EOF.
Oversize, truncated, nonbyte and incomplete responses fail; no prefix is accepted.

The existing twenty-second socket timeout is retained. A monotonic acceptance
deadline starts before opening the response and is checked before and after each
body read. Late headers or bodies are rejected and closed. This is not a promise
that every blocked socket or close finishes at exactly twenty seconds. There is
one request, no retry and no extra provider probe. The regular native schedule and
concurrency are unchanged.

Both the adapter and the pure model decode the complete original body as strict
UTF-8 JSON. Duplicate decoded field names, nonfinite values, malformed Unicode,
trailing content, non-object roots and nesting deeper than 128 are rejected.
Unknown fields, every array row, explicit zero, false, null and valid large integer
identifiers are preserved. Accepted original body bytes are retained verbatim as base64
with their SHA-256; parsing never rewrites those bytes. A reserved source-evidence
field in the provider document is rejected instead of being overwritten.

The pure model verifies the byte hash and compares the entire decoded source with
the supplied packet using validated, canonical JSON bytes. Python equality is not
an identity check: true must differ from 1 and false from 0. The identity is also
conservative about parsed integer versus floating-point types. Object key order
does not matter; array order and every value do. Existing source-request and
receipt-window checks remain in force. Failure withholds the affected modeled
result; it does not substitute a zero or promote a remaining input.

Fractional values retain the model's existing binary floating-point semantics;
this is not arbitrary-precision decimal validation. The original byte identity
remains separately available, including the exact numeric spelling received.

The legacy 2.0.0 output schema is compatible. The complete current compiler hashes
identify this validation change. Replaying an older compiler-bound bundle under
new code must fail; old whole code and bundles are retained inertly. Tests rebuild
the complete old synthetic inputs with the current model and compare every output
field, then freeze and replay a new current-code bundle. No archived code executes.

Acceptance consists of offline malformed-source regressions, unchanged valid
outputs, exact native package/release receipt, and preserved resources and all
original schedule bindings. Normal private publication and actual account data
are not inspected by this acceptance operation.
