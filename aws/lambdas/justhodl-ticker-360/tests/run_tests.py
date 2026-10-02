from pathlib import Path
import subprocess,sys
R=Path(__file__).resolve().parents[4]
for name in ('test_ticker_input_reader.py','test_ticker_context_guard.py','test_ticker_coverage_context.py','test_ticker_coverage_handlers.py','test_options_coverage_publication.py'):
 subprocess.run([sys.executable,str(R/'tests'/name)],cwd=R,check=True)
