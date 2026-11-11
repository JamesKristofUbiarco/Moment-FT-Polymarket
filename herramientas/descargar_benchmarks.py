from polymarket_mvp.download import inventory,download
for date in ['2026-02-26','2026-04-26','2026-06-09']:
    entry=next(e for e in inventory('runs/mvp')['files'] if date in e['path'])
    print(date,download(entry,'runs/mvp/staging'),flush=True)
