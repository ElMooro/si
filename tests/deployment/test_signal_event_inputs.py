from pathlib import Path
import subprocess,sys
R=Path(__file__).resolve().parents[2]
def test_actual_signal_event_input_contract():
    subprocess.run([sys.executable,str(R/'aws/shared/tests/test_signal_event_inputs.py')],cwd=R,check=True)
def test_signal_event_native_acceptance_contract():
    subprocess.run([sys.executable,str(R/'aws/ops/checks/test_signal_event_acceptance.py')],cwd=R,check=True)
