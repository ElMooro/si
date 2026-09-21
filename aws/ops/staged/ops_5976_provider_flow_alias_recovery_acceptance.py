"""Finish source acceptance after repairing exact public legacy alias routes.

Read the two completed durable publications from 5974. No repeated producer
invocations, provider requests, private account reads or notification actions.
"""
import sys
from ops_5974_provider_fund_flow_native_acceptance import main

if __name__=='__main__':
    try:main(report_name='ops_5976_provider_flow_alias_recovery_acceptance',resume_only=True)
    except Exception:sys.exit(1)
