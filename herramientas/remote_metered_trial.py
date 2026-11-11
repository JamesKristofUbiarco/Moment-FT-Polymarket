import psutil,time,sys
while psutil.virtual_memory().available<9*1024**3:time.sleep(10)
from polymarket_mvp.runner import main
sys.argv=['runner','--root','runs/mvp','--date','2026-06-06','--tag','metered_v8','--mode','remote','--batch-size','262144']
main()
