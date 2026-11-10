"""Lossless Parquet encoding for monotone temporal and provenance integers."""
import pyarrow as pa
import pyarrow.parquet as pq

DELTA={'second','event_ts_ms_first','event_ts_ms_last','row_position_first','row_position_last'}

def options(schema,level=3):
    delta={f.name:'DELTA_BINARY_PACKED' for f in schema if f.name in DELTA and pa.types.is_integer(f.type)}
    return dict(compression='zstd',compression_level=level,
                use_dictionary=[f.name for f in schema if f.name not in delta],column_encoding=delta)

def write(table,path,level=3):
    pq.write_table(table,path,**options(table.schema,level))
