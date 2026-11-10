import unittest
import polars as pl
import pyarrow as pa
from polars.testing import assert_frame_equal
from polymarket_mvp.features import partial,merge

class FeaturesTests(unittest.TestCase):
    def test_batch_invariance_with_unordered_edges_and_invalid_close(self):
        rows=[]
        for i in [20,2,9,1,5,20,35]:
            rows.append(dict(asset_id='a',condition_id='c',event_ts_ms=1000+i*100,event_type='price_change',
                best_bid=.4+i*.001,best_ask=.6,bids_json=None,asks_json=None,source_archive='v2',size=1.,price=.5,side='BUY'))
        rows.append(dict(rows[-1],event_ts_ms=4999,best_bid=0.))
        table=pa.Table.from_pylist(rows)
        a=merge(pl.concat([partial(b,0,i)[0] for i,b in [(0,table.to_batches()[0])]]))
        for size in [1,2,3]:
            parts=[];start=0
            for b in table.to_batches(max_chunksize=size):
                parts.append(partial(b,0,start)[0]);start+=b.num_rows
            b=merge(pl.concat(parts))
            assert_frame_equal(a.sort(['asset_id','condition_id','second']),b.sort(['asset_id','condition_id','second']),check_column_order=False)
        self.assertEqual(a.filter(pl.col('second')==4)['quality_last'].item(),'boundary')
