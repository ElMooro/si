"""Execution targets stay on the previous version throughout candidate staging."""
import runpy
from pathlib import Path
from types import SimpleNamespace

protect = runpy.run_path(str(Path(__file__).resolve().parents[2] / "scripts/protect_lambda_alias.py"))["protect"]


def test_preserves_schedule_state_input_and_unrelated_targets():
    class Missing(Exception): pass
    arn = "arn:aws:lambda:us-east-1:123:function:test"
    class Lambda:
        exceptions = SimpleNamespace(ResourceNotFoundException=Missing, ResourceConflictException=Missing)
        def get_function_configuration(self, **kw):
            return dict(State="Active", LastUpdateStatus="Successful", FunctionArn=arn, RevisionId="r", CodeSha256="sha")
        def get_alias(self, **kw): raise Missing()
        def publish_version(self, **kw):
            assert kw["RevisionId"] == "r" and kw["CodeSha256"] == "sha"
            return {"Version": "7"}
        def create_alias(self, **kw): return kw
        def add_permission(self, **kw): assert kw["Qualifier"] == "live"
    class Scheduler:
        def list_schedule_groups(self, **kw): return {"ScheduleGroups": [{"Name": "default"}]}
        def list_schedules(self, **kw): return {"Schedules": [{"Name": "daily", "Target": {"Arn": arn}}, {"Name": "other", "Target": {"Arn": "other"}}]}
        def get_schedule(self, **kw):
            return dict(**kw, State="DISABLED", ScheduleExpression="rate(1 day)", FlexibleTimeWindow={"Mode": "OFF"}, Target={"Arn": arn, "Input": '{"mode":"research"}', "RoleArn": "role"})
        def update_schedule(self, **kw):
            assert kw["State"] == "DISABLED" and kw["Target"]["Input"] == '{"mode":"research"}' and kw["Target"]["Arn"] == arn + ":live"
    class Events:
        def list_rule_names_by_target(self, **kw): return {"RuleNames": []}
    result = protect(Lambda(), Scheduler(), Events(), "test")
    assert result["protected_version"] == "7" and result["schedules"] == ["daily"]
