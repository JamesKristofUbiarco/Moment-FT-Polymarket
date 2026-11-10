import json
from pathlib import Path
p=Path('mvp_polymarket_moment_lora.ipynb');n=json.loads(p.read_text());cells=[]
def md(s):cells.append(dict(cell_type='markdown',metadata={},source=s.splitlines(True)))
def code(s):cells.append(dict(cell_type='code',metadata={},source=s.splitlines(True),execution_count=None,outputs=[]))
md('''# Polymarket → MOMENT → LoRA
Interfaz del MVP: originales completos, características independientes del modelo y entrenamiento verificable.

**Un benchmark, un proceso iniciado y un entrenamiento de desarrollo no completan el MVP.** La aceptación exige cobertura 164/164 y el resultado final de entrenamiento y evaluación. No hay etiquetas fiables de fraude.
''')
code('''from pathlib import Path
import os, sys, json, shutil, subprocess, shlex, time
PROJECT = Path.cwd()
assert (PROJECT / 'pyproject.toml').exists(), 'Abrir el notebook desde la raíz del proyecto.'
ROOT = PROJECT / 'runs/mvp'
ROOT.mkdir(parents=True, exist_ok=True)
UV = shutil.which('uv') or str(Path.home()/'.local/bin/uv')
PYTHON = PROJECT / '.venv/bin/python'
CONFIG = {'revision': 'efe472f1f00a62f2cab1fed2851da439d9c1fe11', 'budget_hours': 48,
          'seed': 2026, 'root': str(ROOT), 'microbatch': 256}
CONFIG
''')
md('''## Entorno local y Colab
Copiar el proyecto completo y su `uv.lock` al entorno. La siguiente celda usa un Python aislado y evita modificar el kernel de Colab. Los trabajos se ejecutan con ese mismo intérprete.
En Colab, situar los resultados durables en almacenamiento montado y verificar la compatibilidad del controlador CUDA. El runtime temporal y la disponibilidad de A100 no están garantizados.
''')
code('''if not Path(UV).exists() and not shutil.which('uv'):
    subprocess.run(['sh','-c','curl -LsSf https://astral.sh/uv/install.sh | sh'], check=True)
subprocess.run([UV, 'sync', '--frozen', '--python', '3.14'], cwd=PROJECT, check=True)
def run_python(source):
    result = subprocess.run([str(PYTHON), '-c', source], cwd=PROJECT, text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    print(result.stdout)
    if result.returncode: raise RuntimeError(f'Proceso falló: {result.returncode}')
run_python("import torch,psutil,shutil; print('CUDA',torch.cuda.is_available(),torch.version.cuda); print('RAM',psutil.virtual_memory()); print('Disco',shutil.disk_usage('.')); print(torch.cuda.get_device_name(0) if torch.cuda.is_available() else 'CPU')")
''')
md('''## Inventario fijado y progreso
Las 164 rutas son la fuente de verdad. Los 168 archivos del contador anterior no se atribuyen a una causa sin su listado original. El catálogo activo–condición se extrae durante la ingesta, sin pasada global previa.
''')
code('''run_python(f"from polymarket_mvp.download import inventory; d=inventory({str(ROOT)!r}); print(d['revision'],len(d['files']),sum(x['bytes'] for x in d['files']))")
def show_progress():
    run_python(f"from polymarket_mvp.reporting import progress; import json; print(json.dumps(progress({str(ROOT)!r}),indent=2))")
show_progress()
''')
md('''## Benchmarks completos y ejecución persistente
Los procesos locales usan tmux si está disponible. El fallback crea una sesión de proceso independiente; en Colab esto no sobrevive a la pérdida del runtime. No lanzar dos escritores sobre la misma salida.
''')
code('''def launch(module, arguments, name):
    argv=[str(PYTHON),'-u','-m',module,*arguments]
    log=ROOT/(name+'.log')
    env=dict(os.environ, POLARS_MAX_THREADS='8', OMP_NUM_THREADS='4')
    if shutil.which('tmux'):
        command=shlex.join(argv)+' >> '+shlex.quote(str(log))+' 2>&1'
        subprocess.run(['tmux','new-session','-d','-s',name,command],cwd=PROJECT,env=env,check=True)
        subprocess.run(['tmux','has-session','-t',name],check=True)
        return {'session':name,'log':str(log)}
    with log.open('a') as stream:
        process=subprocess.Popen(argv,cwd=PROJECT,env=env,stdout=stream,stderr=subprocess.STDOUT,start_new_session=True)
    time.sleep(1)
    if process.poll() is not None: raise RuntimeError(log.read_text()[-4000:])
    return {'pid':process.pid,'log':str(log),'runtime_survival':'independent of kernel; host runtime still required'}
# Ejecutar cuando no exista ya el benchmark correspondiente:
# launch('polymarket_mvp.runner',['--root',str(ROOT),'--date','2026-06-09','--tag','notebook'],'polymarket-bench-notebook')
''')
md('''## Puerta de rendimiento y FULL
La proyección usa archivos completos y throughput real de MOMENT. La política por defecto bloquea una duración proyectada superior a 48 horas o un presupuesto de disco insuficiente. Las estimaciones se distinguen de tiempos ejecutados.
''')
code('''run_python(f"from polymarket_mvp.reporting import projection; import json; print(json.dumps(projection({str(ROOT)!r}),indent=2))")
projection_path=ROOT/'projection.json'
projection=json.loads(projection_path.read_text())
print('FULL habilitado:',projection.get('performance_gate',False) and projection.get('storage_gate',False))
# Habilitar solo con las puertas verificadas; execute vuelve a comprobarlas:
# launch('polymarket_mvp.full',['--root',str(ROOT),'--projection',str(projection_path)],'polymarket-full')
''')
md('''## Características, ventanas y selección
Los Parquet guardan aperturas, cierres, extremos, actividad, estados de calidad y procedencia. No dependen del target de MOMENT.
Las ventanas usan 512 segundos y patches de ocho; no rellenan huecos. La selección de entrenamiento usa fecha, condición, fuente y actividad, con cobertura mínima por activo. La cardinalidad mínima se calcula antes de seleccionar.
''')
code('''# El FULL ejecuta estas operaciones automáticamente tras verificar 164/164.
# Para experimentos adicionales, pasar los outputs de consolidated.json:
# from polymarket_mvp.windows import build_census, select_training, pack_windows
# census = build_census(series_paths, ROOT/'full_census')
# selection = select_training(ROOT/'full_census', measured_window_budget)
for name in ['coverage.json','full_census/census.json','full_census/selection_budget.json','full_census/selection.json']:
    path=ROOT/name
    print(name, json.loads(path.read_text()) if path.exists() else 'Pendiente de ejecución integral')
''')
md('''## MOMENT, LoRA, recuperación y evaluación
Checkpoint MOMENT fijado; LoRA q/v, rango 8, alpha 16 y cabeza entrenable. El checkpoint conserva optimizador, RNG y cursor confirmado. La evaluación compara persistencia, MOMENT original y LoRA sobre los mismos objetivos.
''')
code('''for name in ['integration_complete_day_v2/result.json','integration_complete_day_v2/real_resume_verification.json','full_model/training.json','full_result.json']:
    path=ROOT/name
    if path.exists():
        result=json.loads(path.read_text())
        if name.endswith('result.json') and 'training' in result:
            result={k:v for k,v in result.items() if k!='training'} | {'training':{k:v for k,v in result['training'].items() if k!='parameter_changes'}}
        print(name, json.dumps(result,indent=2))
    else: print(name,'Pendiente')
# API directa para paquetes trasladados a Colab:
# from polymarket_mvp.model import train, evaluate
# model, report = train(train_pack, output, microbatch=256, resume=True)
''')
md("## Métricas visuales\nLa etiqueta distingue el experimento intradiario del resultado integral.")
code("run_python(\"exec(open('herramientas/plot_metrics.py').read())\")\nfrom IPython.display import Image, display\nplot=ROOT/'integration_complete_day_v2/metrics.png'\nif plot.exists(): display(Image(filename=str(plot)))\n")
md('''## Verificación final
No inferir cobertura a partir de archivos de log ni de procesos activos. El verificador coteja filas, grupos, unidades confirmadas y hashes de objetos vigentes. Los resultados del día de desarrollo son exploratorios.
''')
code('''run_python(f"from polymarket_mvp.full import verify_full; import json; r=verify_full({str(ROOT)!r}); print(json.dumps({{k:v for k,v in r.items() if k!='coverage'}},indent=2))")
final=ROOT/'full_result.json'
complete=final.exists() and json.loads(final.read_text()).get('complete',False)
print('MVP completo y verificado:',complete)
''')
for i,cell in enumerate(cells):cell['id']=f'mvp-{i:03d}'
n['cells']=cells;n['metadata']['kernelspec']={'display_name':'Python 3','language':'python','name':'python3'}
p.write_text(json.dumps(n,ensure_ascii=False,indent=1)+'\n')
