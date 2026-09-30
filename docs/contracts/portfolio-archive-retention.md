# Portfolio immutable archive collision checks

The risk producer first writes the complete canonical bundle with
`IfNoneMatch='*'`. A successful new write requires no extra read. Only an existing
object precondition failure enters the collision check. Other write failures
propagate unchanged.

The check compares the entire existing body with the exact canonical candidate
bytes, in reads of at most 65,536 bytes. The expected bundle length is the byte
bound; a further EOF check detects appended bytes. If supplied, `ContentLength`
must be a non-boolean integer equal to that exact length. Unexpected content
encoding, changed/truncated/trailing bytes, late reads or I/O failure refuse
acceptance. The response body closes on every outcome. The existing archive is
never overwritten, deleted or truncated, and no prefix can qualify as a bundle.

The twenty-second monotonic acceptance budget starts before the existing-object
GET. It rejects late headers and completed reads; it does not impose hard
interruption on an SDK socket call. Existing SDK settings, schedules and native
resources stay unchanged. This read is part of normal producer operation;
acceptance tests use complete invented bundles and fake storage only.

All other existing adapter functions and the complete pure model remain
unchanged. Both complete retained synthetic bundles replay to their original
output hashes with the current reviewed compiler. Archived predecessor source
is retained as inert bytes and is never executed. The separate publication
ordering defect across native storage and the authenticated mirror remains open.
