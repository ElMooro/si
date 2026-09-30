# Source contract compilation

The ownership inventory and page access contracts are static source analysis.
They do not certify that an engine published a packet, that a provider is
complete, or that an investment decision is qualified.

## Loaded source names

The ownership graph resolves a module path when it loads that module's complete
source. Its repository-relative name belongs to that graph instance. Repeated
call fingerprints and evidence rows reuse that name; they do not repeat OS path
resolution. Unknown paths still resolve and must remain within the repository.
Each new graph loads its own source and bindings. Neither application source nor
retained predecessor source is executed by the analyzer.

The optimization does not memoize call arguments, captured environments,
ownership conclusions or cross-build filesystem state. Existing recursion and
uncertainty rules still apply. Whole-manifest comparison excludes only the
generated timestamp and retains every engine, candidate, unresolved issue,
call-chain proof and source line.

## Portable inventory order

Engine directories are sorted by their exact names, and candidate source files
by repository-relative POSIX strings. The compiler never relies on `Path`
comparison, which folds case on Windows and preserves it on Linux. Nested source
paths and unsupported-runtime paths also use POSIX separators. This preserves
engine identity and source evidence while making array order reproducible.

The first strict live comparison caught a pre-existing host-order difference:
all 893 named records matched, but 875 positions differed. The complete Linux
manifest is retained as a three-megabyte fixture, bound to the observed Pages
build and checked by hash. Future engine changes are not frozen to that historical
capture; synthetic mixed-case/nested-path regressions enforce the ordering rule,
and each release compares its complete newly generated inventory.

## Access policy snapshots

`access_rules()` reads the current complete checked-in private-artifact policy.
The parser cache is keyed by those bytes, rather than by process lifetime,
filename, modification time or byte count. Cached mappings and key sets are
immutable. Missing, incomplete, duplicate, malformed or unsupported declarations
fail compilation; they never supply an implicit public policy.

Policy parsing is limited to reviewed AST forms. It does not import the policy
module or execute computed expressions. The existing archive-prefix expression
is checked structurally before its literal families are reconstructed. A new
policy expression needs corresponding parser review and regression coverage.

A complete page-contract build captures one set of policy bytes and passes its
rules through output classification, dependency references, archive families,
static outputs and withheld-output counts. It checks the whole policy bytes
again before returning the compiled contract. A detected change refuses before
the generator writes its registry or installs HTML.

This check is not a filesystem lock or a transaction with another writer.
Builds should continue to run from an immutable Git checkout. AWS/Worker access
controls remain separate from these static browser contracts.

## Verification

Focused cases live in `tests/deployment/test_contract_policy_snapshot.py` and
`tests/deployment/test_source_graph_paths.py`. They cover repeated builds,
equal-length policy changes with preserved timestamps, immutable results,
different roots, missing policy, unsupported expressions, mid-build drift,
loaded path reuse, unknown-path containment and new-source bindings.

The complete predecessor compiler files are retained as inert `.py.txt` fixtures.
The fleet acceptance compares all 893 engine records and all 599 page contracts,
then runs source graph, wiring, frontend and deployment checks. Timing evidence
is diagnostic: local profiler times and separate Actions runs are not a
controlled service-level benchmark. Release acceptance requires the exact
successful Pages build and its public static manifest.
