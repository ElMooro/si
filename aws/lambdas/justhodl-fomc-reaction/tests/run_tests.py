
if __name__=='__main__':
    import subprocess,sys
    from pathlib import Path
    subprocess.run([sys.executable,str(Path(__file__).resolve().parents[4]/'tests/fedwatch_consumer_test_support.py')],check=True)
