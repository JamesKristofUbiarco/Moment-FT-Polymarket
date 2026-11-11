import psutil,time,os
from polymarket_mvp.download import atomic_json
from polymarket_mvp.runner import main
atomic_json('runs/mvp/streamed_trial_queue.json',dict(state='waiting_memory',pid=os.getpid()))
while psutil.virtual_memory().available<10*1024**3:time.sleep(10)
atomic_json('runs/mvp/streamed_trial_queue.json',dict(state='running',pid=os.getpid()))
import sys
sys.argv=['runner','--root','runs/mvp','--date','2026-06-06','--tag','v7','--batch-size','262144','--stream-groups']
main()
atomic_json('runs/mvp/streamed_trial_queue.json',dict(state='complete',pid=os.getpid()))
