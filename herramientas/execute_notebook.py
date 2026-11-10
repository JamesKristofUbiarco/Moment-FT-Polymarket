import nbformat
from nbclient import NotebookClient
from pathlib import Path
path=Path('mvp_polymarket_moment_lora.ipynb')
notebook=nbformat.read(path,as_version=4)
client=NotebookClient(notebook,timeout=600,kernel_name='python3',resources={'metadata':{'path':str(Path.cwd())}})
client.execute()
output=Path('runs/mvp/notebook_executed.ipynb');nbformat.write(notebook,output)
print('NOTEBOOK_EXECUTED',output,flush=True)
