import tempfile
import unittest
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from polymarket_mvp.engine import process,consolidate

class EngineTests(unittest.TestCase):
    def test_streamed_groups_consolidation_and_retry(self):
        rows=[dict(asset_id='a',condition_id='c',event_ts_ms=1000+i,event_type='price_change',best_bid=.4+i*.001,
                   best_ask=.6,bids_json=None,asks_json=None,source_archive='v2',size=1.,price=.5,side='BUY') for i in range(30)]
        table=pa.Table.from_pylist(rows)
        for name in ['bids_json','asks_json']:
            table=table.set_column(table.schema.get_field_index(name),name,pa.array([None]*len(rows),type=pa.string()))
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);pq.write_table(table,root/'source.parquet',row_group_size=3)
            entry=dict(path='synthetic',ordinal=0,sha256='synthetic')
            r=process(entry,root/'source.parquet',root/'out',batch_size=8,groups_per_unit=4,stream_groups=True)
            self.assertEqual(r['rows_confirmed'],30);self.assertEqual(r['row_groups'],10)
            r=consolidate(root/'out');self.assertEqual(r['events'],30)
            output=pa.concat_tables([pq.read_table(f) for f in r['outputs']]).to_pylist()
            self.assertEqual(len(output),1);self.assertEqual(output[0]['row_position_last'],29)
            self.assertAlmostEqual(output[0]['bid_last'],.429)
            again=process(entry,root/'source.parquet',root/'out',batch_size=5,groups_per_unit=4,stream_groups=True)
            self.assertEqual(again['rows_this_attempt'],0)
