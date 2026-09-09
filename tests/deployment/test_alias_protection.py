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
    def __init__(self):
        self.alias = None
        self.config = dict(State="Active", LastUpdateStatus="Successful", FunctionArn=ARN,
                           RevisionId="r", CodeSha256="sha", Runtime="python3.12", Handler="handler.run",
                           Timeout=60, MemorySize=256, Layers=[{"Arn": "arn:layer:core:1"}],
                           Environment={"Variables": {"AUTH_MODE": "owner"}}, LastModified="original")
    def get_function_configuration(self, **kw):
        return copy.deepcopy(self.config)
    def get_alias(self, **kw):
        if self.alias is None: raise Missing()
        return copy.deepcopy(self.alias)
    def publish_version(self, **kw):
        assert kw["RevisionId"] == self.config["RevisionId"] and kw["CodeSha256"] == self.config["CodeSha256"]
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


class CustomEvents(Events):
    def __init__(self): self.updates = []
    def list_rule_names_by_target(self, **kw):
        return {"RuleNames": ["rule"] if kw["EventBusName"] == "custom" and kw["TargetArn"] == ARN else []}
    def describe_rule(self, **kw): return {"Arn": "arn:aws:events:us-east-1:123:rule/custom/rule"}
    def list_targets_by_rule(self, **kw):
        assert kw["EventBusName"] == "custom"
        return {"Targets": [{"Id": str(i), "Arn": ARN, "Input": "private-input", "DeadLetterConfig": {"Arn": "dlq"}} for i in range(12)] + [{"Id": "other", "Arn": "other"}]}
    def put_targets(self, **kw):
        assert len(kw["Targets"]) <= 10 and kw["EventBusName"] == "custom"
        for target in kw["Targets"]:
            assert target["Arn"] == ARN + ":live" and target["Input"] == "private-input" and target["DeadLetterConfig"] == {"Arn": "dlq"}
        self.updates.extend(kw["Targets"])
        return {"FailedEntryCount": 0}


def assert_rejected(lam, scheduler=None, events=None, match="changed"):
    try:
        protect(lam, scheduler or Scheduler(), events or Events(), "test")
    except RuntimeError as error:
        assert match in str(error), str(error)
    else:
        raise AssertionError("Concurrent edit accepted")


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
    events = CustomEvents()
    result = protect(Lambda(), Scheduler(), events, "test")
    assert len(events.updates) == 12 and result["rules"] == ["custom/rule"]


def test_concurrent_alias_change_aborts_before_code_can_be_staged():
    for change in ({"FunctionVersion": "8"}, {"RoutingConfig": {"AdditionalVersionWeights": {"8": 0.1}}},
                   {"Description": "operator changed alias"}):
        lam = Lambda()
        class Changed(Scheduler):
            def update_schedule(self, **kw):
                super().update_schedule(**kw)
                lam.alias.update(copy.deepcopy(change))
        assert_rejected(lam, Changed(), match="alias changed")


def test_concurrent_code_change_aborts_before_code_can_be_staged():
    for change in ({"CodeSha256": "new-code"}, {"Environment": {"Variables": {"AUTH_MODE": "public"}}},
                   {"Timeout": 61}, {"Handler": "other.run"}, {"Layers": [{"Arn": "arn:layer:core:2"}]},
                   {"MemorySize": 512}, {"State": "Pending"}, {"LastUpdateStatus": "InProgress"}):
        lam = Lambda()
        class Changed(Scheduler):
            def update_schedule(self, **kw):
                super().update_schedule(**kw)
                lam.config.update(copy.deepcopy(change))
        assert_rejected(lam, Changed(), match="Function changed")


def test_own_publish_create_and_permission_revision_changes_return_fresh_cas_revision():
    class OwnChanges(Lambda):
        def publish_version(self, **kw):
            result = super().publish_version(**kw)
            self.config.update(RevisionId="after-publish", LastModified="published")
            return result
        def create_alias(self, **kw):
            result = super().create_alias(**kw)
            self.config["RevisionId"] = "after-create"
            self.alias["RevisionId"] = "after-create-alias"
            return result
        def add_permission(self, **kw):
            super().add_permission(**kw)
            self.config["RevisionId"] = "after-permission"
            self.alias["RevisionId"] = "after-permission-alias"
    lam = OwnChanges()
    result = protect(lam, Scheduler(), CustomEvents(), "test")
    assert result["revision_id"] == "after-permission" == lam.config["RevisionId"]
    assert result["protected_version"] == "7" and result["rules"] == ["custom/rule"]


def test_existing_alias_permission_revision_changes_are_accepted_without_republishing():
    class Existing(Lambda):
        def publish_version(self, **kw): raise AssertionError("Existing protected version was republished")
        def create_alias(self, **kw): raise AssertionError("Existing alias was recreated")
        def add_permission(self, **kw):
            super().add_permission(**kw)
            self.config["RevisionId"] = "own-policy-function"
            self.alias["RevisionId"] = "own-policy-alias"
    lam = Existing()
    lam.alias = {"Name": "live", "FunctionVersion": "5", "RevisionId": "existing"}
    result = protect(lam, Scheduler(), CustomEvents(), "test")
    assert result["protected_version"] == "5" and result["revision_id"] == "own-policy-function"


def test_late_external_revision_only_changes_are_rejected():
    for target, match in (("config", "Function changed"), ("alias", "alias changed")):
        lam = Lambda()
        class LateChange(CustomEvents):
            def put_targets(self, **kw):
                response = super().put_targets(**kw)
                getattr(lam, target)["RevisionId"] = "late-external-edit"
                return response
        assert_rejected(lam, events=LateChange(), match=match)


def test_publish_revision_refresh_cannot_absorb_concurrent_function_semantic_changes():
    for change in ({"CodeSha256": "external-code"}, {"Timeout": 120},
                   {"Environment": {"Variables": {"AUTH_MODE": "public"}}},
                   {"Handler": "other.run"}, {"Layers": [{"Arn": "arn:layer:core:9"}]}):
        class Changed(Lambda):
            def publish_version(self, **kw):
                result = super().publish_version(**kw)
                self.config.update(copy.deepcopy(change), RevisionId="own-plus-external")
                return result
            def create_alias(self, **kw): raise AssertionError("Semantic drift should block before alias creation")
        assert_rejected(Changed(), match="Function changed")


def test_create_alias_revision_refresh_cannot_absorb_concurrent_semantic_changes():
    for target, change, match in (("config", {"Timeout": 120}, "Function changed"),
                                 ("alias", {"FunctionVersion": "9"}, "alias changed"),
                                 ("alias", {"RoutingConfig": {"AdditionalVersionWeights": {"9": .2}}}, "alias changed")):
        class Changed(Lambda):
            def create_alias(self, **kw):
                response = super().create_alias(**kw)
                getattr(self, target).update(copy.deepcopy(change), RevisionId="own-plus-external")
                return response
        assert_rejected(Changed(), match=match)


def test_permission_revision_refresh_cannot_absorb_concurrent_semantic_changes():
    for target, change, match in (("config", {"CodeSha256": "external-code"}, "Function changed"),
                                 ("config", {"Environment": {"Variables": {"AUTH_MODE": "public"}}}, "Function changed"),
                                 ("alias", {"FunctionVersion": "9"}, "alias changed"),
                                 ("alias", {"RoutingConfig": {"AdditionalVersionWeights": {"9": .2}}}, "alias changed")):
        class Changed(Lambda):
            def add_permission(self, **kw):
                super().add_permission(**kw)
                getattr(self, target).update(copy.deepcopy(change), RevisionId="own-plus-external")
        events = CustomEvents()
        assert_rejected(Changed(), events=events, match=match)
        assert events.updates == [], "Semantic drift must block before rebinding classic targets"
