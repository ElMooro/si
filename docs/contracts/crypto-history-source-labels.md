# Crypto history source labels

The Warehouse OHLC route names the supplementary adapter actually selected:
`warehouse+yahoo` or `warehouse+binance`. The `X-Source` header follows the same
label. Failed/insufficient supplements, equity requests and non-daily intervals
retain their existing warehouse-only behavior and request policy.

`history_source` names the selected supplement. `history_n` counts its input
rows; `history_added_n` counts unique dates added to the normalized warehouse
frame. `yahoo_n` remains for compatibility and is zero when no Yahoo rows are
selected; `binance_n` is its Binance counterpart. These are row counts, not
independent evidence or source-coverage scores. `warehouse_n` is unchanged.

Warehouse rows still own overlapping UTC dates and all their fields, including
missing volume. This patch changes labels and counts only. Provider identity
in a label does not establish unit consistency, adjustment consistency, original
publication dates or comparable trading sessions. No packet is made eligible
for Calls or sizing by this metadata. All tests use complete invented packets
and intercept every request; native acceptance is a separate exact-code check.
