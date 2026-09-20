from pathlib import Path
import subprocess,sys
if __name__=='__main__':
    subprocess.run([sys.executable,'-m','pytest','-q',str(Path(__file__).resolve().parent)],check=True)
