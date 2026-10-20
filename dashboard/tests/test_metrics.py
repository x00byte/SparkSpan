from dashboard.metrics import CounterHistory, aggregate, parse_metrics
import pytest


def native(output, prompt=100, cached=20, marker=1, replica='a', model='model-a'):
    return parse_metrics(f'''# HELP vllm:generation_tokens_total Generated output
vllm:generation_tokens_total{{replica="{replica}",model_name="{model}"}} {output}
vllm:generation_tokens_created{{replica="{replica}",model_name="{model}"}} {marker}
vllm:prompt_tokens_total{{replica="{replica}",model_name="{model}"}} {prompt}
vllm:prompt_tokens_cached_total{{replica="{replica}",model_name="{model}"}} {cached}
vllm:prompt_tokens_by_source_total{{replica="{replica}",model_name="{model}",source="local_compute"}} {prompt-cached}
vllm:kv_cache_usage_perc{{replica="{replica}",model_name="{model}"}} .25
vllm:request_prefill_time_seconds_sum{{replica="{replica}",model_name="{model}"}} 4
vllm:request_prefill_time_seconds_count{{replica="{replica}",model_name="{model}"}} 2
vllm:request_prefill_kv_computed_tokens_sum{{replica="{replica}",model_name="{model}"}} 800
''')


def status(*names):
    return {'backends': [{'name': n, 'healthy': True, 'draining': False, 'metrics_age_seconds': .1} for n in names]}


def test_metric_parser_separates_cache_and_compute_without_histogram_double_count():
    d=next(iter(native(25).values()))
    assert d['counters']['output']==25
    assert d['counters']['uncached']==80
    assert d['gauges']['kv_percent']==25
    assert parse_metrics('# HELP a b\nnot metrics\nvllm:generation_tokens_total{replica="a"} NaN\n')=={}


def test_irregular_wall_time_and_short_actual_window(tmp_path):
    h=CounterHistory(tmp_path/'db',poll_seconds=10)
    h.add(100,native(0,0,0),status('a'))
    h.add(110,native(100,500,100),status('a'))
    h.add(125,native(400,1000,200),status('a'))
    result=aggregate(list(h.rows),3600)
    assert result['elapsed_s']==25
    assert result['output_tps']==16
    assert result['uncached_tps']==32
    assert result['cache_percent']==20
    partial=aggregate(list(h.rows),10)
    assert partial['elapsed_s']==10
    assert partial['output_tps']==20
    h.close()


def test_reset_and_disappearing_backend_leave_gaps_not_spikes(tmp_path):
    h=CounterHistory(tmp_path/'db',poll_seconds=10)
    combine=lambda a,b:dict(a,**b)
    h.add(100,combine(native(100),native(200,replica='b')),status('a','b'))
    h.add(110,combine(native(120),native(230,replica='b')),status('a','b'))
    assert aggregate(list(h.rows),10)['output_tps']==5
    row=h.add(120,combine(native(3,marker=2),native(250,replica='b')),status('a','b'))
    assert row['resets']==1 and not row['complete']
    assert aggregate(list(h.rows),10)['output_tps'] is None
    h.add(130,native(13,marker=2),status('a','b'))
    assert aggregate(list(h.rows),10)['output_tps'] is None
    h.add(140,combine(native(23,marker=2),native(400,replica='b')),status('a','b'))
    assert aggregate(list(h.rows),10)['output_tps'] is None
    h.add(150,combine(native(33,marker=2),native(410,replica='b')),status('a','b'))
    assert aggregate(list(h.rows),10)['output_tps']==2
    h.close()


def test_draining_offline_backend_does_not_invalidate_active_production(tmp_path):
    h=CounterHistory(tmp_path/'db',poll_seconds=10)
    s=status('a','b');s['backends'][1].update(healthy=False,draining=True)
    h.add(100,native(0),s);h.add(110,native(50),s)
    assert aggregate(list(h.rows),10)['output_tps']==5
    h.close()


def test_new_model_keeps_separate_baseline_and_rates(tmp_path):
    h=CounterHistory(tmp_path/'db',poll_seconds=10)
    h.add(100,native(100,model='old'),status('a'))
    h.add(110,native(600,model='new'),status('a'))
    assert aggregate(list(h.rows),10)['output_tps'] is None
    h.add(120,native(620,model='new'),status('a'))
    assert aggregate(list(h.rows),10,model='new')['output_tps']==2
    h.close()


def test_replicated_model_cannot_report_partial_replica_rate_as_complete(tmp_path):
    h=CounterHistory(tmp_path/'db',poll_seconds=10)
    s=status('a','b')
    for n in s['backends']:n['models']=['model-a']
    h.add(100,dict(native(0),**native(0,replica='b')),s)
    h.add(110,dict(native(10),**native(20,replica='b')),s)
    assert aggregate(list(h.rows),10,model='model-a')['output_tps']==3
    h.add(120,native(20),s)
    assert aggregate(list(h.rows),10,model='model-a')['output_tps'] is None
    h.close()


def test_stale_native_metrics_are_not_fresh_measurement(tmp_path):
    h=CounterHistory(tmp_path/'db',poll_seconds=10)
    s=status('a');h.add(100,native(0),s)
    s['backends'][0]['metrics_age_seconds']=9
    h.add(110,native(50),s)
    assert aggregate(list(h.rows),10)['output_tps'] is None
    s['backends'][0]['metrics_age_seconds']=.1
    h.add(120,native(60),s)
    assert aggregate(list(h.rows),10)['output_tps'] is None
    h.add(130,native(80),s)
    assert aggregate(list(h.rows),10)['output_tps']==2
    h.close()
