# Portfolio snapshot binding, version 1

The private risk engine adds `snapshot_binding` to its existing 2.0.0 output.
It identifies the **complete parsed snapshot used by that calculation**. The
portfolio page verifies this identity against the separately received snapshot
before displaying risk alongside holdings. Every field participates, including
fields the risk calculation does not consume. A missing binding, a changed
holding, or any other differing value withholds risk visibly. The snapshot can
still be displayed separately. There is no last-good risk fallback across books.

This is consistency evidence, not an authenticated signature, original-provider
byte hash, independent account reconciliation, or forecast qualification. The
existing private frozen bundle retains the complete calculation inputs and code
identities. Research-only permissions and all existing numerical outputs remain
unchanged. No public account endpoint or new account access is introduced.

## Binding fields

| Field | Required meaning |
|---|---|
| `contract` | `portfolio-snapshot-value.v1` |
| `key` | `portfolio/snapshot.json` |
| `generated_at` | Exact snapshot publication string, also required to match |
| `encoding` | `typed-json-binary64.v1` |
| `value_sha256` | Lowercase SHA-256 of the complete value encoding below |
| `encoded_bytes` | Positive integer length of that encoding, not HTTP length |

The object contains exactly these fields. Its encoded value is bounded to
32 MiB. Browser responses and native snapshot acquisition each read through EOF
within a 4 MiB bound, reject duplicate decoded JSON keys and malformed Unicode,
and reject nonfinite or unrepresentable values. Native acquisition also checks
the SDK's typed `ContentLength` when supplied and closes the response on failure.
An oversize packet is unavailable; a prefix is never accepted as the whole input.

## Complete value encoding

All framing characters and decimal lengths are ASCII. Strings are strict UTF-8;
lone surrogates are invalid. Depth greater than 128 is invalid. The encoding is
specified here independently of Python/JavaScript JSON formatting:

| Value | Bytes |
|---|---|
| null | `n` |
| boolean | `t` or `f` |
| number | `d` followed by eight big-endian IEEE-754 binary64 bytes |
| string | `s`, decimal UTF-8 byte length, `:`, then UTF-8 bytes |
| array | `a`, decimal element count, `:`, then each encoded value in order |
| object | `o`, decimal member count, `:`, then each encoded string key and value |

Object keys sort lexicographically by their UTF-8 bytes. Array order is retained.
Booleans, strings, missing members and null remain distinct from numbers.
Numeric spelling such as `1` versus `1.0` has the same value identity; positive
and negative zero both encode positive zero. All numbers must be finite, and an
integral numeric value outside the safe integer range ±(2^53−1) is rejected.
No unknown field, row or nested member is omitted. These rules handle the
existing private mirror's JSON reserialization without claiming its bytes equal
the S3 source bytes. The standard SHA-256 implementation hashes this encoding;
this contract does not define a new cryptographic hash algorithm.

Python and browser vectors cover subnormal values, safe-integer boundaries,
Unicode key ordering, arrays and complete synthetic native snapshots. The
retained predecessor's entire numerical output is compared against the current
model after removing only the new binding field. Archived source is not executed.

## Browser lifecycle

Both inputs must finish strict complete decoding within a twelve-second
header/body deadline. A later load cancels and invalidates earlier work; even a
fetch implementation that ignores cancellation cannot restore stale values.
Failure clears earlier account/risk display state. Risk older than the existing
four-hour display window is cleared by local age checks without fetching data.
The five-minute network cadence is unchanged, and back/forward restoration keeps
one network timer. An invalid publication cannot later become eligible solely
because the clock advances: its snapshot must also have passed binding checks.

Legacy risk packets without the new binding are withheld until normal engine
publication supplies it. Deployment acceptance checks source receipts, native
packages, resources, existing schedules and static served assets. Those checks
do not inspect actual private account inputs or claim normal-publication proof.
