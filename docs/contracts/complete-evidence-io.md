# Complete browser evidence acquisition

`jh-evidence-io.js` supplies strict UTF-8 JSON decoding and bounded complete byte
reads. It has no URLs, authentication policy, storage or investment authority.
The inspector and portfolio scenario adapter use the same implementation.

## Decoding

Decoding rejects duplicate keys after JSON unescaping, nonfinite numeric
overflow, malformed UTF-8, lone UTF-16 surrogates, a BOM, trailing content and
nesting beyond 128 levels. Prototype-like keys remain ordinary own properties.
Null, false, zero, empty objects, empty arrays and all returned rows remain
distinct. Numbers use JavaScript binary64 semantics; this is not arbitrary
precision decimal validation or source-provider verification.

## Acquisition and limits

The reader consumes a byte stream through EOF. Short chunks are not EOF. It
rejects over-limit bodies instead of parsing a prefix. An explicitly supplied
expected length must be an integer and match the complete body; HTTP transport
Content-Length is not assumed to describe a browser-decompressed body.

- Scenario imports and portfolio snapshots retain the four-MiB, twelve-second
  default. Their adapter cannot raise the four-MiB cap.
- Inspector source registries retain a sixteen-MiB cap and twenty-second
  acquisition deadline. A pinned registry must still match its full SHA-256.
- Inspector artifacts and captured response clones permit at most thirty-two
  MiB in each of the received and gzip-expanded byte sequences. Headers, body
  and gzip expansion share a twenty-second budget. Gzip is detected by its
  magic bytes, including when transport decoding has already produced JSON.
- The generic primitive permits explicit bounds up to sixty-four MiB and
  deadlines up to sixty seconds. Its consumers choose smaller bounds above.

Abort and timeout settle even when a source ignores cancellation. Active readers
are cancelled and released; late response bodies are cancelled. Synchronous JSON
parsing cannot be preempted by a browser timer; artifacts check elapsed time
again after parsing. Byte/depth caps bound the accepted input, not every possible
browser memory allocation or rendering cost.

## Inspector state and access

Selecting another artifact or changing owner identity cancels the previous
selection. Its late completion cannot replace the current display. Public exact
artifact identity checks, approved routes, public projection checks and owner
authentication remain mandatory. There is no alternate-path retry, public
fallback for an owner artifact, new feed request or expanded access contract.

API capture observes only clones of the page's existing contracted requests.
The newest initiated request for a request key controls that capture. An older
response cannot replace it. HTTP, network, clone or strict decoding failures
replace the previous capture with an explicit unavailable state; they do not
expose provider error text. A changed owner invalidates the capture. The original
response and errors still reach the caller. Cloned-body cancellation does not
abort that original response. This repair qualifies the inspector's capture,
not the page's separate original response parser or investment conclusion.

The build installs a content-versioned helper once, before the inspector and its
other consumers. All previous evidence inspection, pagination and field search
remain. Oversize or unsupported documents are unavailable, never described as
partially complete. Deployment and synthetic acceptance do not certify live
provider data, historical vintages, trading edge or account reconciliation.

The existing authenticated adapter currently buffers upstream bytes before it
returns its response. These reader bounds apply to the supplied response; that
earlier adapter buffer remains a separately tracked repair.
