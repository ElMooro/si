# Public OECD object path repair

Source commit: 84929245208d8bffe969e24d5c880ea7ac7eb925. The complete deployed data-proxy module matches the pinned Wrangler 4.144.0 local build and retained runner build, with original bindings and schedules checked through the exact Worker receipt. All triggered workflows passed.

The original handler rejected the at-sign in OECD dataflow filenames before attempting the S3 read. The repair accepts that separator, raw or once URL-encoded, only under the existing public data/warm/oecd/data namespace for the documented .dat.gz filename shape. It does not generally decode escapes or widen private namespaces. Whole preceding worker source and test hooks are retained, with exact reversible deltas.

The previously rejected DSD_STES@DF_INDSERV.dat.gz object now returns HTTP 200 and its exact original key. The complete compressed body contains 380,139 CSV rows. This is a stored snapshot; it differs from the independent official response used by the chart adapter and is not claimed to be current or historically complete. Metadata replay shows 1,480 additional filenames admitted by the corrected rule, alongside all 19 previously valid filenames. Only the one named object has a live access proof in this release. No watchlist-route count was changed by this path repair.

Validation passed: 270 worker tests, 1,075 deployment checks plus 15 shell cases, private-publication boundaries and built-worker runtime checks. Denied or missing origin responses are returned once without a retry, alias, generation request or credential workaround. The automatically triggered Pages build was also checked against its complete chart/static manifest; chart source behavior is unchanged.
