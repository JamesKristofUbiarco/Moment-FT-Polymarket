"""Crash-safe publication: immutable Parquet objects precede SQLite confirmation."""
import hashlib
import json
import os
import sqlite3
import uuid
from pathlib import Path

import pyarrow.parquet as pq


def digest(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for block in iter(lambda: f.read(8 << 20), b''):
            h.update(block)
    return h.hexdigest()


class Manifest:
    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.root / 'manifest.sqlite')
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript('''
        CREATE TABLE IF NOT EXISTS commits(unit TEXT PRIMARY KEY, metadata TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS objects(unit TEXT, path TEXT UNIQUE, sha256 TEXT, rows INTEGER);
        CREATE TABLE IF NOT EXISTS compactions(name TEXT PRIMARY KEY, metadata TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS replacements(path TEXT PRIMARY KEY, sha256 TEXT NOT NULL, rows INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS retired(path TEXT PRIMARY KEY);
        CREATE TABLE IF NOT EXISTS io(id INTEGER PRIMARY KEY, source TEXT, row_group INTEGER,
          operation TEXT, bytes INTEGER, seconds REAL, attempt TEXT);
        ''')

    def confirmed(self, unit):
        return self.db.execute('SELECT 1 FROM commits WHERE unit=?', (unit,)).fetchone() is not None

    def publish(self, unit, tables, metadata, fail_at=None):
        if self.confirmed(unit):
            return False
        objects = []
        for kind, table in tables.items():
            name = f'{uuid.uuid4().hex}_{kind}.parquet'
            path = self.root / name
            tmp = path.with_suffix('.tmp')
            from .columnar import write
            write(table,tmp)
            with open(tmp, 'rb') as f:
                os.fsync(f.fileno())
            sha = digest(tmp)
            if pq.ParquetFile(tmp).metadata.num_rows != table.num_rows:
                raise ValueError('Parquet row count mismatch')
            os.replace(tmp, path)
            objects.append((unit, name, sha, table.num_rows))
        fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        if fail_at == 'before_commit':
            raise RuntimeError('Injected crash before manifest commit')
        with self.db:
            self.db.executemany('INSERT INTO objects VALUES (?,?,?,?)', objects)
            self.db.execute('INSERT INTO commits VALUES (?,?)', (unit, json.dumps(metadata, sort_keys=True)))
        return True

    def verify(self):
        for path, sha, rows in self.db.execute('SELECT path,sha256,rows FROM objects WHERE path NOT IN (SELECT path FROM retired) UNION ALL SELECT path,sha256,rows FROM replacements'):
            p = self.root / path
            if not p.exists() or digest(p) != sha or pq.ParquetFile(p).metadata.num_rows != rows:
                raise ValueError(f'Invalid committed object: {path}')
        return self.db.execute('SELECT count(*) FROM commits').fetchone()[0]

    def orphans(self):
        known = {r[0] for r in self.db.execute('SELECT path FROM objects')}
        return sorted(p.name for p in self.root.glob('*.parquet') if p.name not in known)

    def compact(self, paths, metadata, delete_originals=False):
        """Commit verified replacements before retiring/deleting any source object."""
        objects=[]
        for path in paths:
            p=Path(path).resolve()
            objects.append((str(p),digest(p),pq.ParquetFile(p).metadata.num_rows))
        self.verify()
        with self.db:
            self.db.executemany('INSERT OR REPLACE INTO replacements VALUES (?,?,?)',objects)
            self.db.execute('INSERT OR REPLACE INTO compactions VALUES (?,?)',('final_series',json.dumps(metadata,sort_keys=True)))
            self.db.execute("INSERT OR IGNORE INTO retired SELECT path FROM objects WHERE path LIKE '%_features.parquet'")
        self.verify()
        if delete_originals:
            for (name,) in self.db.execute('SELECT path FROM retired'):
                path=self.root/name
                if not path.resolve().is_relative_to(self.root.resolve()):
                    raise ValueError('Refusing deletion outside owned parts directory')
                path.unlink(missing_ok=True)
        return len(objects)
