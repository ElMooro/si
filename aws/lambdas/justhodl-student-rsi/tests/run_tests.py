from pathlib import Path
import runpy
runpy.run_path(str(Path(__file__).resolve().parents[4] / 'tests/factory/test_factory.py'), run_name='__main__')
