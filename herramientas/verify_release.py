import hashlib,json,subprocess,sys,time
from pathlib import Path
from polymarket_mvp.download import atomic_json
root=Path('runs/mvp');checks=[]
for args in [['-m','unittest','discover','-s','tests/mvp','-q'],['herramientas/execute_notebook.py']]:
    path=root/('release_tests.log' if 'unittest' in args else 'release_notebook.log')
    with path.open('w') as log:
        p=subprocess.run([sys.executable,*args],stdout=log,stderr=subprocess.STDOUT)
    checks.append(dict(arguments=args,exit_code=p.returncode,log=str(path)))
    if p.returncode:raise RuntimeError(f'Check failed: {path}')
files=sorted(Path('src/polymarket_mvp').glob('*.py'))+[Path('pyproject.toml'),Path('uv.lock'),Path('mvp_polymarket_moment_lora.ipynb')]
hashes={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in files}
atomic_json(root/'implementation_verification.json',dict(passed=True,utc=time.time(),checks=checks,hashes=hashes))
print('IMPLEMENTATION_VERIFIED',len(files),flush=True)
