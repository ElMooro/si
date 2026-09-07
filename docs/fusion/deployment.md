# Deployment

- Everything ships by `git push` (see `AUTONOMY.md`). `deploy-lambdas.yml` redeploys the coordinator (existing
  function) whenever its source or an `aws/shared/*.py` it imports changes. New functions are created from the runner
  by `aws/ops/pending/ops_5212_fusion_release1.py` with `_lambda_deploy_helpers.create_or_update_lambda(build_zip)`.
- Bundled config: `config/engine-registry.v1.json`, `jh-fusion-flags.json`, `jh-fusion-universe.json` are copied into
  both Lambda `source/` dirs; `TestConfig.test_bundled_configs_match_canonical` fails the suite on drift. Change the
  canonical file, copy to both `source/` dirs, push.
- Schedules (EventBridge Scheduler, never classic rules): `justhodl-jhsignal-bridge-hourly` rate(1 hour),
  `justhodl-jh-fusion-daily` cron(20 5 * * ? *) UTC. Primary fusion trigger is the coordinator route.
- DynamoDB `justhodl-jhsignal-state` is created on demand by the op and by the bridge's `ensure_table()`.
- Feature flags: SSM `/justhodl/fusion/flags` (JSON object). Kill switches: `FUSION_SIGNAL_BUS_ENABLED`,
  `FUSION_STATE_DDB_ENABLED`, `FUSION_DISABLED_ENGINES`, `FUSION_DISABLED_ENTITIES`; `FUSION_SHADOW_MODE` stays true
  until promotion.
- Rollback: disable the two schedules; the coordinator routes are inert without batch events. Nothing else in the
  fleet reads the fusion outputs yet.
- Tests: `python3 -m pytest aws/lambdas/justhodl-jhsignal-bridge/tests aws/lambdas/justhodl-jh-fusion/tests`
  (pytest + jsonschema); the op also runs them on the runner.
- Cost: ~17 GETs + ~5 small PUTs per bridge run (24/day), one fusion run per batch; append-only keys avoid the
  versioned-bucket churn that caused the Aug-2026 anomaly.
