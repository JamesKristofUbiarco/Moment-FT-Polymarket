import json
from pathlib import Path
import os
os.environ.setdefault('MPLCONFIGDIR',str(Path('runs/mvp/.matplotlib').resolve()))
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
root=Path('runs/mvp/integration_complete_day_v2')
validation=json.loads((root/'evaluation_validation/evaluation.json').read_text());test=json.loads((root/'evaluation_test/evaluation.json').read_text())
history=[json.loads(x) for x in (root/'model/history.jsonl').read_text().splitlines()]
fig,axes=plt.subplots(1,3,figsize=(12,3.7),layout='constrained')
axes[0].plot([r['step'] for r in history],[r['mse'] for r in history],color='#2166ac');axes[0].set(xlabel='Actualización',ylabel='MSE',title='LoRA: 9.002 ventanas de desarrollo')
for ax,r,title in zip(axes[1:],[validation,test],['Validación exploratoria','Prueba exploratoria']):
    values=[r[n+'_mae'] for n in ['persistence','moment','lora']]
    ax.bar(['Persistencia','MOMENT','LoRA'],values,color=['#666666','#92c5de','#2166ac']);ax.set(ylabel='MAE por ventana',title=f'{title}\n{r["windows"]:,} ventanas')
    ax.tick_params(axis='x',labelrotation=15)
fig.suptitle('Día completo 2026-06-06 · resultados de desarrollo, no del dataset integral',fontsize=12)
fig.savefig(root/'metrics.png',dpi=160);plt.close(fig)
print(root/'metrics.png')
