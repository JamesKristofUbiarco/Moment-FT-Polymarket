"""Mergeable observable features. This module has no MOMENT target policy."""
import json
import math
import time
import polars as pl
import pyarrow as pa

COLUMNS = ['asset_id','condition_id','event_ts_ms','event_type','best_bid','best_ask',
           'bids_json','asks_json','source_archive','size','price','side']
STATES = ['bilateral','empty_bid','empty_ask','empty_both','boundary','null_quote','invalid_quote','unknown']
KEYS = ['asset_id','condition_id','second']
EDGE = ['bid','ask','spread','mid','quality','event_ts_ms','file_ordinal','row_position']


def snapshot(bids, asks):
    try:
        parsed=[]
        for text, bid in [(bids,True),(asks,False)]:
            if text is None: return None,None,'unknown'
            levels=json.loads(text)
            if not isinstance(levels,list): raise ValueError()
            prices=set()
            for level in levels:
                if not isinstance(level,list) or len(level)!=2: raise ValueError()
                p,s=map(float,level)
                if not math.isfinite(p) or not math.isfinite(s) or not 0<=p<=1 or s<=0 or p in prices: raise ValueError()
                prices.add(p)
            parsed.append((max(prices) if bid else min(prices)) if prices else None)
        b,a=parsed
        state='empty_both' if b is None and a is None else 'empty_bid' if b is None else 'empty_ask' if a is None else None
        return b,a,state
    except (ValueError,TypeError,OverflowError): return None,None,'invalid_quote'


def partial(batch, ordinal, row_start):
    began=time.perf_counter()
    d=pl.from_arrow(batch).with_columns(
        pl.int_range(row_start,row_start+batch.num_rows,dtype=pl.Int64).alias('row_position'),
        pl.lit(ordinal,dtype=pl.Int32).alias('file_ordinal'))
    # JSON is parsed only for snapshots; the dominant event path stays vectorized.
    books=d.filter(pl.col('event_type')=='book').select('row_position','bids_json','asks_json')
    overrides=[(i,*snapshot(b,a)) for i,b,a in books.iter_rows()]
    if overrides:
        o=pl.DataFrame(overrides,schema={'row_position':pl.Int64,'snapshot_bid':pl.Float64,
                'snapshot_ask':pl.Float64,'snapshot_state':pl.String},orient='row')
        d=d.join(o,on='row_position',how='left')
    else:
        d=d.with_columns(pl.lit(None,dtype=pl.Float64).alias('snapshot_bid'),
            pl.lit(None,dtype=pl.Float64).alias('snapshot_ask'),pl.lit(None,dtype=pl.String).alias('snapshot_state'))
    d=d.with_columns(
        pl.when(pl.col('event_type')=='book').then(pl.col('snapshot_bid')).otherwise(pl.col('best_bid')).alias('bid'),
        pl.when(pl.col('event_type')=='book').then(pl.col('snapshot_ask')).otherwise(pl.col('best_ask')).alias('ask'),
        pl.col('event_type').is_in(['book','price_change']).fill_null(False).alias('is_quote'),
        (pl.col('event_ts_ms')//1000).alias('second'))
    valid=(pl.col('bid').is_finite() & pl.col('ask').is_finite() & (pl.col('bid')>0) &
           (pl.col('ask')<1) & (pl.col('bid')<=pl.col('ask'))).fill_null(False)
    d=d.with_columns(pl.when(~pl.col('is_quote')).then(pl.lit('unknown'))
        .when(pl.col('snapshot_state').is_not_null()).then(pl.col('snapshot_state'))
        .when(valid).then(pl.lit('bilateral'))
        .when(pl.col('bid').is_null() | pl.col('ask').is_null()).then(pl.lit('null_quote'))
        .when((~pl.col('bid').is_finite()) | (~pl.col('ask').is_finite()) | (pl.col('bid')<0) | (pl.col('ask')>1) | (pl.col('bid')>pl.col('ask'))).then(pl.lit('invalid_quote'))
        .when((pl.col('bid')==0) | (pl.col('ask')==1)).then(pl.lit('boundary'))
        .otherwise(pl.lit('invalid_quote')).alias('quality'))
    d=d.with_columns(
        pl.when(pl.col('quality')=='bilateral').then(pl.col('ask')-pl.col('bid')).alias('spread'),
        pl.when(pl.col('quality')=='bilateral').then((pl.col('ask')+pl.col('bid'))/2).alias('mid'))
    keyed=pl.col('asset_id').is_not_null() & pl.col('event_ts_ms').is_not_null() & (pl.col('event_ts_ms')>=0)
    audit={'rows':batch.num_rows,'missing_key':d.filter(~keyed.fill_null(False)).height,
           'quality':dict(d.group_by('quality').len().iter_rows()),
           'types':dict(d.with_columns(pl.col('event_type').fill_null('<null>')).group_by('event_type').len().iter_rows())}
    quality_finished=time.perf_counter()
    relations=d.filter(pl.col('asset_id').is_not_null() & pl.col('condition_id').is_not_null()).group_by(['asset_id','condition_id']).agg(
        pl.col('event_ts_ms').min().alias('first_ts'),pl.col('event_ts_ms').max().alias('last_ts'),pl.len().cast(pl.UInt64).alias('events'))
    catalog_finished=time.perf_counter()
    d=d.filter(keyed).sort(['event_ts_ms','file_ordinal','row_position'])
    expr=[pl.len().cast(pl.UInt64).alias('events'),pl.col('is_quote').sum().cast(pl.UInt64).alias('quotes')]
    for state in STATES:
        expr.append(((pl.col('quality')==state)&pl.col('is_quote')).sum().cast(pl.UInt64).alias('n_'+state))
    for typ in ['price_change','book','last_trade_price','tick_size_change']:
        expr.append((pl.col('event_type')==typ).sum().cast(pl.UInt64).alias('n_'+typ))
    for source in ['v1','v2']:
        expr.append((pl.col('source_archive')==source).sum().cast(pl.UInt64).alias('n_'+source))
    trade=(pl.col('event_type')=='last_trade_price') & pl.col('size').is_finite() & (pl.col('size')>=0)
    expr.append(pl.col('size').filter(trade).sum().alias('reported_trade_size_sum'))
    for side in ['BUY','SELL']:
        expr.append(((pl.col('event_type')=='price_change')&(pl.col('side')==side)).sum().cast(pl.UInt64).alias('updates_'+side.lower()))
    for col in EDGE:
        for edge in ['first','last']:
            v=pl.col(col).filter(pl.col('is_quote'))
            expr.append(getattr(v,edge)().alias(col+'_'+edge))
    for col in ['spread','mid','price']:
        expr.extend([pl.col(col).min().alias(col+'_min'),pl.col(col).max().alias(col+'_max')])
    result=d.group_by(KEYS).agg(expr)
    audit['timings']={'quality_json_seconds':quality_finished-began,'catalog_seconds':catalog_finished-quality_finished,'group_seconds':time.perf_counter()-catalog_finished}
    return result,relations,audit


def merge(frame):
    if not frame.height: return frame
    expr=[]
    for edge in ['first','last']:
        names=[c for c in frame.columns if c.endswith('_'+edge)]
        valid=pl.col('event_ts_ms_'+edge).is_not_null()
        order=[pl.col(c+'_'+edge).filter(valid) for c in ['event_ts_ms','file_ordinal','row_position']]
        values=pl.struct(names).filter(valid).sort_by(order)
        expr.append(getattr(values,edge)().alias('_'+edge))
    for name in frame.columns:
        if name in KEYS or name.endswith(('_first','_last')): continue
        c=pl.col(name)
        if name.endswith('_min'): expr.append(c.min().alias(name))
        elif name.endswith('_max'): expr.append(c.max().alias(name))
        else: expr.append(c.sum().alias(name))
    return frame.group_by(KEYS).agg(expr).unnest('_first','_last')
