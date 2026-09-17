# Calls v2 integrity contract

The AI brief is an observation service. A fluent narrative or parsed verb cannot
qualify an allocation. Successful briefs publish `ABSTAIN` with reason
`decision_model_not_validated`; failed or short generations publish `ERROR`.
Both have `decision_eligible: false` and `sizing_eligible: false`.

`aws/shared/calls_contract.py` is the shared contract. It preserves a genuine
zero Khalid score and publishes only a public projection of the private brief.
Events are immutable under `data/decisive-call-events/`; conditional S3 writes
maintain `data/decisive-call-history.json` without dropping concurrent writes or
replacing unreadable history. Existing legacy rows remain labeled as legacy.

The brief schedule is 00:05, 04:05, 08:05, 12:05, 16:05 and 20:05 UTC. The Calls
page distinguishes generation errors, abstention, expiry and overdue publication.
Its historical weighted accuracy is a legacy signal statistic, not Calls accuracy.

The position sizer abstains without producing buy/sell allocations unless a
current validated decision and a usable independent risk constraint are present.
Abstention does not liquidate existing holdings. A zero risk constraint stays zero.

The backtest publishes `data/calls-replay.json` and retains the legacy output key
`backtest/calls-results.json`. Only explicitly qualified SPY allocation targets
can execute, at a subsequent regular-session open while unexpired. UNKNOWN,
WAIT, HOLD and legacy prose cannot create an allocation. No instruction means
retain actual simulated shares/cash, not reset to full SPY exposure.

Replay output is diagnostic only: split-adjusted price returns exclude dividends,
slippage, commissions, financing and cash yield. Net return and Sharpe are null.
No qualified execution yields `no_eligible_calls` and null performance. Input
failure replaces the current read model with an explicit error, not old success.

Enabling sizing requires a separately reviewed validated decision producer with
versioned evidence, asset, exposure, observation/decision times, expiry and model
qualification. This change does not certify the existing Kelly model, scorecard,
dependency independence or any engine's predictive edge. Those remain subsequent
work; setting eligibility flags manually is not a substitute for that work.

Verification invokes must pass `suppress_alerts: true`; observation/error briefs
also suppress trade notifications. Deployment proof remains an exact commit
match in each Lambda release receipt plus runner-side output and schedule checks.
