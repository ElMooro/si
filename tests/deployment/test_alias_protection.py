"""Execution targets stay on the previous version throughout candidate staging."""
import copy
import runpy
from pathlib import Path
from types import SimpleNamespace

protect = runpy.run_path(str(Path(__file__).resolve().parents[2] / "scripts/protect_lambda_alias.py"))["protect"]
ARN = "arn:aws:lambda:us-east-1:123:function:test"


class Missing(Exception): pass


class Lambda:
    exceptions = SimpleNamespace(ResourceNotFoundException=Missing, ResourceConflictException=Missing)
    alias = None
    def get_function_configuration(self, **kw):
        return dict(State="Active", LastUpdateStatus="Successful", FunctionArn=ARN, RevisionId="r", CodeSha256="sha")
    def get_alias(self, **kw):
        if self.alias is None: raise Missing()
        return copy.deepcopy(self.alias)
    def publish_version(self, **kw):
        assert kw["RevisionId"] == "r" and kw["CodeSha256"] == "sha"
        return {"Version": "7"}
    def create_alias(self, **kw):
        self.alias = {**kw, "RevisionId": "alias-r"}
        return copy.deepcopy(self.alias)
    def add_permission(self, **kw): assert kw["Qualifier"] == "live"


class Scheduler:
    updates = None
    def list_schedule_groups(self, **kw): return {"ScheduleGroups": [{"Name": "default"}]}
    def list_schedules(self, **kw):
        return {"Schedules": [{"Name": "daily", "Target": {"Arn": ARN}}, {"Name": "other", "Target": {"Arn": "other"}}]}
    def get_schedule(self, **kw):
        return dict(**kw, State="DISABLED", ScheduleExpression="rate(1 day)", FlexibleTimeWindow={"Mode": "OFF"},
                    Target={"Arn": ARN, "Input": '{"mode":"research"}', "RoleArn": "role", "DeadLetterConfig": {"Arn": "dlq"}})
    def update_schedule(self, **kw):
        self.updates = kw
        assert kw["State"] == "DISABLED" and kw["Target"]["Input"] == '{"mode":"research"}'
        assert kw["Target"]["Arn"] == ARN + ":live" and kw["Target"]["DeadLetterConfig"] == {"Arn": "dlq"}


class Events:
    def list_event_buses(self, **kw): return {"EventBuses": [{"Name": "default"}, {"Name": "custom"}]}
    def list_rule_names_by_target(self, **kw): return {"RuleNames": []}


def test_preserves_schedule_state_input_and_unrelated_targets():
    result = protect(Lambda(), Scheduler(), Events(), "test")
    assert result["protected_version"] == "7" and result["schedules"] == ["daily"] and result["revision_id"] == "r"


def test_stale_listing_cannot_overwrite_new_unrelated_target():
    class Changed(Scheduler):
        def get_schedule(self, **kw):
            doc = super().get_schedule(**kw)
            doc["Target"]["Arn"] = "other"
            return doc
    sched = Changed()
    result = protect(Lambda(), sched, Events(), "test")
    assert sched.updates is None and result["schedules"] == []


def test_custom_event_bus_targets_preserve_input_and_dead_letter_config():
    class Custom(Events):
        updates = []
        def list_rule_names_by_target(self, **kw):
            return {"RuleNames": ["rule"] if kw["EventBusName"] == "custom" and kw["TargetArn"] == ARN else []}
        def describe_rule(self, **kw): return {"Arn": "arn:aws:events:us-east-1:123:rule/custom/rule"}
        def list_targets_by_rule(self, **kw):
            assert kw["EventBusName"] == "custom"
            return {"Targets": [{"Id": str(i), "Arn": ARN, "Input": "private-input", "DeadLetterConfig": {"Arn": "dlq"}} for i in range(12)] + [{"Id":"other", "Arn":"other"}]}
        def put_targets(self, **kw):
            assert len(kw["Targets"]) <= 10 and kw["EventBusName"] == "custom"
            for target in kw["Targets"]:
                assert target["Arn"] == ARN + ":live" and target["Input"] == "private-input" and target["DeadLetterConfig"] == {"Arn": "dlq"}
            self.updates.extend(kw["Targets"])
            return {"FailedEntryCount": 0}
    events = Custom()
    result = protect(Lambda(), Scheduler(), events, "test")
    assert len(events.updates) == 12 and result["rules"] == ["custom/rule"]


def test_concurrent_alias_change_aborts_before_code_can_be_staged():
    class Changed(Lambda):
        def get_alias(self, **kw):
            doc = super().get_alias(**kw)
            doc["RevisionId"] = "concurrent-revision"
            return doc
    try: protect(Changed(), Scheduler(), Events(), "test")
    except RuntimeError as error: assert "alias changed" in str(error)
    else: raise AssertionError("Concurrent alias edit accepted")


def test_concurrent_code_change_aborts_before_code_can_be_staged():
    class Changed(Lambda):
        reads = 0
        def get_function_configuration(self, **kw):
            self.reads += 1
            doc = super().get_function_configuration(**kw)
            if self.reads > 1: doc["RevisionId"] = "concurrent-revision"
            return doc
    try: protect(Changed(), Scheduler(), Events(), "test")
    except RuntimeError as error: assert "Function changed" in str(error)
    else: raise AssertionError("Concurrent code edit accepted")
