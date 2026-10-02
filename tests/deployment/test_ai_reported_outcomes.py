from pathlib import Path
import subprocess,sys
R=Path(__file__).resolve().parents[2]
def test_actual_reported_outcomes_and_lesson_adapters():
    subprocess.run([sys.executable,str(R/'aws/lambdas/justhodl-ai/tests/test_market_read_outcomes.py')],cwd=R,check=True)
def test_ai_outcome_native_acceptance():
    subprocess.run([sys.executable,str(R/'aws/ops/checks/test_ai_outcome_acceptance.py')],cwd=R,check=True)
