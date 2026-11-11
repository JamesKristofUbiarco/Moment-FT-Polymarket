import psutil
for p in psutil.process_iter(['pid','cmdline']):
    cmd=' '.join(p.info['cmdline'] or [])
    if p.info['pid']!=__import__('os').getpid() and cmd.endswith('python -u herramientas/descargar_benchmarks.py'):
        p.terminate()
