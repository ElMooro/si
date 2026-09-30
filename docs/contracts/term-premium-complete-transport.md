# Complete Term Premium transport

The native reader consumes bounded binary fragments through EOF. A successful
single read is not evidence that the response is complete. Every S3 response
requires an integer ContentLength matching the complete received bytes. Empty,
oversized, nonbinary, interrupted, shorter and longer bodies fail. Streams close
on every return and failure, including invalid metadata before the first read.

The source workbook requires HTTP 200, the exact reviewed final URL, no partial
Content-Range and identity encoding. If Content-Length is present, it must be one
unambiguous unsigned decimal header and match the complete body. Responses with
no declared length are consumed through EOF. HTTP error bodies are closed. The
64 MiB bound, provider timeout, research calculations and schedule are unchanged.

All native object reads use this contract: previous-state preservation, immutable
readback, workbook/input/compiler replay, current compare-and-swap and the native
execution journal. Failed predecessor or acknowledgement reads cannot become
successful publication evidence. Tests use only complete invented objects; live
acceptance does not inspect those private/current paths.

The complete pre-repair store is retained as an inert fixture. Replay permits
its one exact reviewed SHA-256 only for term_premium_store.py; every mathematical
compiler still requires exact current bytes. Complete output, table, input and
compact-view binding remains mandatory. No historical code executes. Any other
store edit or substitution into a mathematical compiler is rejected.

Read-only operation 6377 verifies the latest intended source receipt and complete
native package before and after whole original public archive replay. Every
original resource and schedule field must match the accepted baseline. This
establishes deployed code and predecessor compatibility; it cannot establish a
new normal publication, current-pointer delivery, predictive validity or sizing
authority. No producer invocation or schedule acceleration is part of acceptance.
