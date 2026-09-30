# ICI complete-source transport

The predecessor accepted the first binary read as the entire object and ignored S3 ContentLength. A complete invented reproduction returns seven bytes from a longer object without an error; a wrong S3 length is also accepted. This is a source-level reproduction, not a claim about a particular live incident.

Every storage read now requires an exact positive integer S3 byte count and reads to EOF in bounded fragments. The reader retains one byte buffer, rather than one Python object per transport fragment, enforces the existing 8 MiB limit, rejects nonbinary and oversized fragments, compares the complete count and always closes the stream. Elapsed time is checked around reads; this is not process cancellation, and the original native socket timeouts remain in effect.

The two existing official ICI requests retain their URLs, attempt counts, headers and 30-second socket timeout. Partial HTTP status 206, Content-Range, encoded responses, duplicate or malformed lengths, unsupported transfer coding and mixed transfer/length framing refuse before evidence is accepted. Complete chunked, declared-length and connection-close bodies remain supported. Complete HTTP error bodies remain available to the existing private retention path; no redirect, retry, alternative provider or new request is introduced.

The independent numerical modules remain byte-identical and hash-qualified. Complete old/new native output stores and return values match under equal original inputs, clocks and compiler identities. Existing histories, conditional publication, request claims, output keys, resources and Wednesday/Thursday 16:30 UTC rule stay unchanged. The complete store and test predecessors remain fixtures.

Verification uses complete invented fixtures, including Python's actual HTTP parser, and exact public native release/package/control evidence. Operation 6395 reads no current packet, retained private original, account input, consumer, provider response or application log. It performs no native invocation, write or schedule change. A verified release is not independent confirmation of a normal scheduled publication, point-in-time source history or investment qualification.

A separate invented regression shows that a failed failure-journal write previously hid the original provider exception. The original exception now remains the native failure, with one fixed non-sensitive note if the journal cannot be verified. Diagnostic failure never becomes success or causes a retry.
