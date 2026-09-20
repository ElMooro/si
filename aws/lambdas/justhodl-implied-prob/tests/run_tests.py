
if __name__=='__main__':
    import subprocess,sys
    from pathlib import Path
    subprocess.run([sys.executable,str(Path(__file__).with_name('native_implied_tests.py'))],check=True)
    subprocess.run([sys.executable,str(Path(__file__).resolve().parents[4]/'tests/tail_consumer_test_support.py')],check=True)
