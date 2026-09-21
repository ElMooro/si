"""Dated windows are checked against the native original-response compiler."""
from pathlib import Path
import sys
sys.path[:0]=[str(Path(__file__).resolve().parents[4]/"aws/shared/tests")]
from test_provider_flow_research import OriginalFlow, GroupFlow
