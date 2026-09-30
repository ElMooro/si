# Worker source evidence

The manual `worker-source-evidence.yml` workflow runs operation 6354 for only
`justhodl-data-proxy`. Its existing Cloudflare credentials are confined to that
step. The direct AWS operations workflow gains no Cloudflare credentials. The
caller must supply the exact reviewed main commit; a different checkout fails.

The operation performs nine GETs to fixed control-plane endpoints: deployments,
the selected version, settings, schedules and complete source. It refuses
redirects and split-traffic deployments. Source, settings, schedules and the
selected deployment must remain equal across capture. Each request requires
complete HTTP 200 bytes, optional unambiguous exact length, identity encoding,
a 64-MiB maximum and a sixty-second acceptance budget with thirty-second socket
timeouts. The acceptance budget does not guarantee hard interruption of a
blocking socket operation. JSON is strict UTF-8 with duplicate/nonfinite/invalid
Unicode rejection. Multipart members are retained whole, including binary
members, and duplicate identities or incomplete closing boundaries fail.

The whole original source transport is retained as inert base64 after decoded
source and report metadata pass credential-pattern checks. No extracted source
is executed. Binding names/types and complete configuration hashes are retained;
binding values and raw settings are excluded. Original transport/member hashes,
the inspection commit, run ID and provider identities make this capture
inspectable. The immutable artifact is committed under
`aws/ops/reports/worker-source/6354-<run>.json`, with a selected summary or failure
at `aws/ops/reports/latest/ops_6354_worker_source_predecessor.md`. Report-push
failure makes the workflow fail.

This is a predecessor capture, **not a deployment receipt**. A stable deployment
and a script ETag do not independently prove that the source endpoint is bound
to the active version or that it equals an intended repository build. Both
`source_active_version_binding_verified` and `intended_repo_build_verified`
remain false. Exact build/deployment verification is required before accepting
the planned Worker publication repair. No Worker route, KV/DO contents, logs,
private account, current consumer packet or native producer is accessed, and
no configuration, schedule, Worker code or private state is changed.

The existing publication defect and unbounded immutable-bundle collision read
are reproduced with complete synthetic packets, retained with eleven whole
predecessor source/configuration files. A future repair must coordinate native
storage and the authenticated mirror; an S3-only compare-and-swap or KV
read-before-write is insufficient. No publication repair is claimed here.
