"""Reusable fail-closed controls for governance operations with side effects.

The control object is deliberately dependency-free.  Callers inject both the
service client and the callable that performs an operation; this module only
authorizes the owner/token pair and accounts for the reviewed call and cost
budgets.  Approval tokens are never retained in receipts.
"""
from __future__ import annotations

import hmac
import math
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional


class GovernanceControlError(ValueError):
    """A controlled operation was not authorized or exceeded its budget."""


class GovernanceApprovalRequired(GovernanceControlError):
    """Live execution did not carry the reviewed approval token."""


class GovernanceOwnershipError(GovernanceControlError):
    """The requested owner does not match the configured governance owner."""


class GovernanceBudgetExceeded(GovernanceControlError):
    """The operation would exceed a reviewed execution budget."""


@dataclass(frozen=True)
class ExecutionBudget:
    """Hard ceilings for one controlled execution."""

    max_api_calls: int
    max_estimated_cost_usd: float

    def __post_init__(self) -> None:
        if (
            isinstance(self.max_api_calls, bool)
            or not isinstance(self.max_api_calls, int)
            or self.max_api_calls <= 0
        ):
            raise GovernanceControlError("max_api_calls must be a positive integer")
        if (
            isinstance(self.max_estimated_cost_usd, bool)
            or not isinstance(self.max_estimated_cost_usd, (int, float))
            or not math.isfinite(float(self.max_estimated_cost_usd))
            or self.max_estimated_cost_usd < 0
        ):
            raise GovernanceControlError(
                "max_estimated_cost_usd must be a finite non-negative number"
            )


class ControlledExecution:
    """Authorize and meter a sequence of dependency-injected live operations."""

    def __init__(
        self,
        *,
        expected_approval_token: str,
        expected_owner: str,
        budget: ExecutionBudget,
    ) -> None:
        if not isinstance(expected_approval_token, str) or len(expected_approval_token) < 16:
            raise GovernanceControlError(
                "expected_approval_token must contain at least 16 characters"
            )
        if not isinstance(expected_owner, str) or not expected_owner.strip():
            raise GovernanceControlError("expected_owner is required")
        if not isinstance(budget, ExecutionBudget):
            raise GovernanceControlError("budget must be an ExecutionBudget")
        self._expected_approval_token = expected_approval_token
        self.expected_owner = expected_owner.strip()
        self.budget = budget
        self.api_calls = 0
        self.estimated_cost_usd = 0.0
        self.operations = []

    def authorize(
        self,
        *,
        approval_token: Optional[str],
        owner: Optional[str],
    ) -> None:
        if not isinstance(approval_token, str) or not hmac.compare_digest(
            approval_token, self._expected_approval_token
        ):
            raise GovernanceApprovalRequired(
                "live execution requires a matching explicit approval token"
            )
        if not isinstance(owner, str) or not hmac.compare_digest(
            owner.strip(), self.expected_owner
        ):
            raise GovernanceOwnershipError(
                "live execution owner does not match the configured governance owner"
            )

    def execute(
        self,
        operation: str,
        callback: Callable[[], Any],
        *,
        approval_token: Optional[str],
        owner: Optional[str],
        api_calls: int = 1,
        estimated_cost_usd: float = 0.0,
    ) -> Any:
        """Authorize, reserve budget, then execute a supplied side effect."""
        self.authorize(approval_token=approval_token, owner=owner)
        if not isinstance(operation, str) or not operation.strip():
            raise GovernanceControlError("operation is required")
        if isinstance(api_calls, bool) or not isinstance(api_calls, int) or api_calls <= 0:
            raise GovernanceControlError("api_calls must be a positive integer")
        if (
            isinstance(estimated_cost_usd, bool)
            or not isinstance(estimated_cost_usd, (int, float))
            or not math.isfinite(float(estimated_cost_usd))
            or estimated_cost_usd < 0
        ):
            raise GovernanceControlError(
                "estimated_cost_usd must be a finite non-negative number"
            )
        next_calls = self.api_calls + api_calls
        next_cost = self.estimated_cost_usd + float(estimated_cost_usd)
        if next_calls > self.budget.max_api_calls:
            raise GovernanceBudgetExceeded("live API-call budget would be exceeded")
        if next_cost > self.budget.max_estimated_cost_usd:
            raise GovernanceBudgetExceeded("live estimated-cost budget would be exceeded")

        # Reserve before calling.  A failed remote call still consumed the reviewed
        # attempt and must not be silently retried outside the budget.
        self.api_calls = next_calls
        self.estimated_cost_usd = next_cost
        self.operations.append(operation.strip())
        return callback()

    def evidence(self) -> Dict[str, Any]:
        """Return non-secret execution evidence suitable for audit records."""
        return {
            "owner": self.expected_owner,
            "api_calls": self.api_calls,
            "max_api_calls": self.budget.max_api_calls,
            "estimated_cost_usd": round(self.estimated_cost_usd, 6),
            "max_estimated_cost_usd": float(self.budget.max_estimated_cost_usd),
            "operations": list(self.operations),
        }
