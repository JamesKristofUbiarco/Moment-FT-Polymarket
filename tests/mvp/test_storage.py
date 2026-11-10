import tempfile
import unittest
import pyarrow as pa
from polymarket_mvp.storage import Manifest

class PublicationTests(unittest.TestCase):
    def test_crash_retry_and_corruption(self):
        with tempfile.TemporaryDirectory() as root:
            m = Manifest(root)
            t = pa.table({'value': [1, 2]})
            with self.assertRaises(RuntimeError):
                m.publish('file:0:rg:0', {'features': t}, {'rows': 2}, 'before_commit')
            self.assertFalse(m.confirmed('file:0:rg:0'))
            self.assertEqual(len(m.orphans()), 1)
            self.assertTrue(m.publish('file:0:rg:0', {'features': t}, {'rows': 2}))
            self.assertFalse(m.publish('file:0:rg:0', {'features': t}, {'rows': 2}))
            self.assertEqual(m.verify(), 1)
            name = m.db.execute('SELECT path FROM objects').fetchone()[0]
            (m.root / name).write_bytes(b'corrupt')
            with self.assertRaises(ValueError):
                m.verify()
            m.db.close()

    def test_verified_replacement_precedes_retirement(self):
        import pyarrow.parquet as pq
        with tempfile.TemporaryDirectory() as root:
            m=Manifest(root);t=pa.table({'value':[1,2]})
            m.publish('unit',{'features':t,'relations':t},{'rows':2})
            originals=list(m.db.execute('SELECT path FROM objects'))
            replacement=m.root/'series.parquet';pq.write_table(t,replacement)
            m.compact([replacement],{'events':2},delete_originals=False)
            self.assertTrue(all((m.root/p).exists() for (p,) in originals))
            m.db.close();m=Manifest(root);self.assertEqual(m.verify(),1)
            m.compact([replacement],{'events':2},delete_originals=True)
            self.assertTrue(all((m.root/p).exists() for (p,) in originals if p.endswith('_relations.parquet')))
            self.assertTrue(all(not (m.root/p).exists() for (p,) in originals if p.endswith('_features.parquet')))
            replacement.write_bytes(b'corruption')
            with self.assertRaises(ValueError):m.verify()
            m.db.close()
