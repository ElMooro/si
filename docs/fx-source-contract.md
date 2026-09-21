# FX original-source contract

Reviewed 2026-09-21 against the provider's [Forex Custom Bars specification](https://massive.com/docs/rest/forex/aggregates/custom-bars) and [Forex overview](https://massive.com/docs/rest/forex/overview).

The existing nineteen identities remain in scope, including both metal quote pairs. Acquisition uses the existing subscription on the AWS runner, an explicit ninety-calendar-day request window and the same daily aggregate endpoint. The request date boundaries follow the endpoint's Eastern Time description. Returned `t` is a Unix millisecond **window-start** instant; it is neither an individual quote timestamp nor a close timestamp. The capture preserves that distinction and does not independently certify bar finality.

Forex aggregates represent quoted prices. They do not establish executed transactions, market-wide volume, an investor's positions or carry financing. Generic `v`/`n` fields remain as reported without assigning an exchange-volume interpretation. Metal base-unit definitions require separate source qualification before a cash-exposure interpretation.

Every original response, including errors and empty results, is retained privately with exact byte identity. Public measurement work must retain missing rows, dated endpoints, true zeros, invalid values, duplicate timestamps and response identity failures. Request limits and pagination coverage are explicit. Missing observations are not forward-filled or removed to make a nominal return window fit. A later model must define changes by observed row offsets and actual dated spans, distinguish inverse-rate arithmetic from negation, and use matched endpoints for cross-pair comparisons.

The legacy USD proxy is not a qualified dollar index. Fixed risk-on/risk-off weights, assumed haven polarity, financing/carry language and alert verbs require separate validation. Source capture grants none of those permissions. Whole predecessor packets and dependent code are preserved before migration; consumer activation, public publication and portfolio authority are separate acceptance steps.
