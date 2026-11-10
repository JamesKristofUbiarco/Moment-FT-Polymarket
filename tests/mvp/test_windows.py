import tempfile
import unittest
from pathlib import Path
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from polymarket_mvp.windows import build_census,select_training,pack_windows
from polymarket_mvp.model import WindowDataset

class WindowTests(unittest.TestCase):
    def test_full_census_strict_splits_minimum_and_masks(self):
        rows=[dict(asset_id=a,condition_id='c',second=i,spread_last=.01+i%7*.001,event_ts_ms_last=i*1000,
                   file_ordinal_last=0,row_position_last=i,events=3,n_v1=0,n_v2=3) for a in ['a','b'] for i in range(1536)]
        with tempfile.TemporaryDirectory() as d:
            p=Path(d);pq.write_table(pa.Table.from_pylist(rows),p/'series.parquet')
            c=build_census([p/'series.parquet'],p/'c',{'train':(0,512),'validation':(512,1024),'test':(1024,1536)},buckets=2)
            self.assertEqual(c['split_counts'],{'train':2,'validation':2,'test':2})
            with self.assertRaises(RuntimeError):select_training(p/'c',1)
            s=select_training(p/'c',2);self.assertEqual(s['minimum_windows'],2)
            pack_windows(p/'c',p/'pack','train',True)
            w=WindowDataset(p/'pack')[0];target=w['target']
            self.assertEqual(int(w['x'][0,target:].abs().sum()),0)
            self.assertEqual(int(w['mask'][target:].sum()),0)
            self.assertEqual(int(w['input_mask'][target:target+8].sum()),8)
