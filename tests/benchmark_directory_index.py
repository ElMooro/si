"""Optional complete synthetic index benchmark; never reads live AWS or provider data."""
from pathlib import Path
from types import ModuleType,SimpleNamespace
from unittest.mock import patch
from array import array
import ctypes,json,sys,time
R=Path(__file__).resolve().parents[1];sys.path.insert(0,str(R/'aws/lambdas/justhodl-symdir/source'))
m=ModuleType('invented_capacity');m.__file__=str(R/'aws/lambdas/justhodl-symdir/source/lambda_function.py')
with patch.dict(sys.modules,{'boto3':SimpleNamespace(client=lambda *a,**k:None),'botocore.config':SimpleNamespace(Config=lambda **k:None)}):
 exec(compile(Path(m.__file__).read_bytes(),m.__file__,'exec'),m.__dict__)
count=int(sys.argv[1]);docs=[m.doc('invented:SERIES%07d'%i,'invented','Invented regional output category %d district %d unit %d'%(i%137,i%61,i%17),'series',.5,extra={'cat':'Invented category %d'%(i%137)}) for i in range(count)]
started=time.perf_counter();derived=m.index_for_docs(docs);elapsed=time.perf_counter()-started
from directory_index import validate_docs,validate_index
validation=time.perf_counter();validate_docs({'docs':docs,'pop':array('f',[.5])*count,'built_at':'2000-01-01T00:00:00+00:00'});validate_index(docs,derived);validation=time.perf_counter()-validation
if sys.platform=='win32':
 class Memory(ctypes.Structure):
  _fields_=[('cb',ctypes.c_ulong),('PageFaultCount',ctypes.c_ulong)]+[(name,ctypes.c_size_t) for name in ('PeakWorkingSetSize','WorkingSetSize','QuotaPeakPagedPoolUsage','QuotaPagedPoolUsage','QuotaPeakNonPagedPoolUsage','QuotaNonPagedPoolUsage','PagefileUsage','PeakPagefileUsage')]
 p=Memory();p.cb=ctypes.sizeof(p);kernel=ctypes.windll.kernel32;kernel.GetCurrentProcess.restype=ctypes.c_void_p
 ctypes.windll.psapi.GetProcessMemoryInfo.argtypes=[ctypes.c_void_p,ctypes.POINTER(Memory),ctypes.c_ulong]
 assert ctypes.windll.psapi.GetProcessMemoryInfo(kernel.GetCurrentProcess(),ctypes.byref(p),p.cb)
 peak_bytes=p.PeakWorkingSetSize
else:
 import resource
 peak_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024)
print(json.dumps({'invented':True,'documents':count,'tokens':len(derived['index']),'postings':sum(map(len,derived['index'].values())),'index_build_seconds':round(elapsed,3),'validation_seconds':round(validation,3),'process_peak_working_set_bytes':peak_bytes,'environment':sys.platform+' Python '+sys.version+'; synthetic document population, not AWS capacity acceptance','actual_data_reads':0,'provider_requests':0}))
