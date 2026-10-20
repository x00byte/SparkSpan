"""Native counter sampling and wall-time windows for SparkSpan Dashboard."""
from collections import defaultdict, deque
import asyncio
import json
import math
from pathlib import Path
import re
import sqlite3
import time

LINE = re.compile(r'^([^\s{]+)(?:\{(.*)\})?\s+([^\s]+)(?:\s+.*)?$')
LABEL = re.compile(r'(\w+)="((?:\\.|[^"\\])*)"')
COUNTERS = {
    'vllm:generation_tokens_total': 'output',
    'vllm:prompt_tokens_total': 'prompt',
    'vllm:prompt_tokens_cached_total': 'cached',
    'vllm:request_success_total': 'completed',
    'vllm:num_preemptions_total': 'preemptions',
    'vllm:request_prefill_time_seconds_sum': 'prefill_seconds',
    'vllm:request_prefill_time_seconds_count': 'prefill_count',
    'vllm:request_prefill_kv_computed_tokens_sum': 'prefill_computed',
    'vllm:time_to_first_token_seconds_sum': 'ttft_seconds',
    'vllm:time_to_first_token_seconds_count': 'ttft_count',
    'vllm:prefix_cache_hits_total': 'cache_hits',
    'vllm:prefix_cache_queries_total': 'cache_queries',
}


def fresh_node(node):
    age = node.get('metrics_age_seconds')
    return (node.get('healthy') is True and type(age) in (int, float)
            and math.isfinite(age) and 0 <= age < 5)


def parse_metrics(text):
    series = {}
    for line in text.splitlines():
        if not line or line.startswith('#'):
            continue
        m = LINE.match(line)
        if not m:
            continue
        try:
            value = float(m[3])
            if not math.isfinite(value):
                continue
            labels = {k: json.loads('"' + v + '"') for k, v in LABEL.findall(m[2] or '')}
        except (ValueError, json.JSONDecodeError):
            continue
        replica = labels.get('replica')
        if not replica:
            continue
        model = labels.get('model_name', '')
        key = replica + '\x1f' + model
        data = series.setdefault(key, {'replica': replica, 'model': model, 'counters': {}, 'gauges': {}})
        name = m[1]
        if name in COUNTERS:
            field = COUNTERS[name]
            data['counters'][field] = data['counters'].get(field, 0) + value
        elif name == 'vllm:prompt_tokens_by_source_total' and labels.get('source') == 'local_compute':
            data['counters']['uncached'] = data['counters'].get('uncached', 0) + value
        elif name == 'vllm:kv_cache_usage_perc':
            data['gauges']['kv_percent'] = max(data['gauges'].get('kv_percent', 0), value * 100)
        elif name in ('vllm:num_requests_running', 'vllm:num_requests_waiting'):
            field = 'running' if name.endswith('running') else 'waiting'
            data['gauges'][field] = data['gauges'].get(field, 0) + value
        elif name in ('vllm:generation_tokens_created', 'process_start_time_seconds'):
            data['marker'] = value
    for data in series.values():
        c = data['counters']
        if 'uncached' not in c and 'prompt' in c and 'cached' in c:
            c['uncached'] = max(0, c['prompt'] - c['cached'])
    return {k: v for k, v in series.items() if v['counters']}


def aggregate(rows, seconds=None, now=None, replica=None, model=None):
    if not rows:
        return {'elapsed_s': 0, 'coverage': 0, 'output_tps': None, 'uncached_tps': None, 'warming': True}
    end = rows[-1]['end'] if now is None else now
    cutoff = end - seconds if seconds else rows[0]['start']
    totals = defaultdict(float)
    covered = elapsed = 0.0
    resets = gaps = 0
    samples = 0
    missing_uncached = False
    for row in rows:
        start, stop = max(cutoff, row['start']), min(end, row['end'])
        if stop <= start or not row['duration_s']:
            continue
        fraction = (stop - start) / row['duration_s']
        elapsed += stop - start
        samples += 1
        selected = [v for v in row['deltas'] if (replica is None or v['replica'] == replica)
                    and (model is None or v['model'] == model)]
        if model is not None:
            expected = set(row.get('expected_models', {}).get(model, []))
            if replica is not None:
                expected &= {replica}
            complete = bool(expected) and expected <= {d['replica'] for d in selected}
        else:
            complete = row['complete'] if replica is None else bool(selected)
        if complete:
            covered += stop - start
        else:
            gaps += 1
        resets += row.get('resets', 0)
        for item in selected:
            missing_uncached = missing_uncached or 'uncached' not in item['values']
            for key, value in item['values'].items():
                totals[key] += value * fraction
    coverage = covered / elapsed if elapsed else 0
    # Incomplete intervals are reported explicitly; do not invent zeros for an absent engine.
    complete = elapsed > 0 and coverage > .999
    ratio = lambda a, b: totals[a] / totals[b] if totals[b] else None
    return {'elapsed_s': elapsed, 'covered_s': covered, 'coverage': coverage, 'samples': samples,
            'output_tps': totals['output'] / elapsed if complete else None,
            'uncached_tps': totals['uncached'] / elapsed if complete and 'uncached' in totals and not missing_uncached else None,
            'observed_output_tps': totals['output'] / elapsed if elapsed else None,
            'output_tokens': totals['output'], 'uncached_tokens': totals.get('uncached'),
            'completed': totals['completed'], 'preemptions': totals['preemptions'],
            'cache_percent': ratio('cached', 'prompt') * 100 if totals['prompt'] else None,
            'prefix_hit_percent': ratio('cache_hits', 'cache_queries') * 100 if totals['cache_queries'] else None,
            'ttft_s': ratio('ttft_seconds', 'ttft_count'),
            'prefill_request_s': ratio('prefill_seconds', 'prefill_count'),
            'prefill_compute_tps': ratio('prefill_computed', 'prefill_seconds'),
            'resets': resets, 'gaps': gaps, 'warming': elapsed == 0,
            'approximate_boundary': seconds is not None}


class CounterHistory:
    def __init__(self, path, poll_seconds=3, retention_seconds=86400):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.path)
        self.db.execute('CREATE TABLE IF NOT EXISTS samples (at REAL PRIMARY KEY, data TEXT NOT NULL)')
        self.db.execute('DELETE FROM samples WHERE at < ?', (time.time() - retention_seconds,))
        self.db.commit()
        self.rows = deque((json.loads(r[0]) for r in self.db.execute('SELECT data FROM samples ORDER BY at')), maxlen=30000)
        self.previous = {}
        self.last = self.rows[-1]['end'] if self.rows else None
        self.poll_seconds, self.retention_seconds = poll_seconds, retention_seconds

    def add(self, at, parsed, status):
        duration = at - self.last if self.last is not None else 0
        nodes = {n['name']: n for n in status.get('backends', [])}
        expected = {n['name'] for n in nodes.values() if not n.get('draining') or n.get('running', 0)
                    or n.get('waiting', 0) or n.get('gateway_inflight', 0)}
        model_names = {s['model'] for s in [*parsed.values(), *self.previous.values()] if s['model']}
        model_names |= {m for n in nodes.values() for m in n.get('models', [])}
        expected_models = {m: sorted(n['name'] for n in nodes.values() if n['name'] in expected
                                     and (not n.get('models') or m in n['models'])) for m in model_names}
        valid_nodes, deltas, resets, current = set(), [], 0, {}
        for key, value in parsed.items():
            node = nodes.get(value['replica'], {})
            if not fresh_node(node):
                continue
            current[key] = value
            old = self.previous.get(key)
            if not old or duration <= 0 or duration > self.poll_seconds * 4:
                continue
            fields = set(value['counters']) & set(old['counters'])
            reset = (value.get('marker') != old.get('marker') or
                     any(value['counters'][f] < old['counters'][f] for f in fields))
            if reset:
                resets += 1
                continue
            if 'output' not in fields:
                continue
            valid_nodes.add(value['replica'])
            deltas.append({'replica': value['replica'], 'model': value['model'],
                           'values': {f: value['counters'][f] - old['counters'][f] for f in fields}})
        row = {'start': self.last if self.last is not None else at, 'end': at,
               'duration_s': max(0, duration), 'complete': bool(expected) and expected <= valid_nodes,
               'expected_replicas': sorted(expected), 'valid_replicas': sorted(valid_nodes),
               'expected_models': expected_models,
               'resets': resets, 'deltas': deltas}
        self.previous, self.last = current, at
        self.rows.append(row)
        self.db.execute('INSERT OR REPLACE INTO samples VALUES (?, ?)', (at, json.dumps(row)))
        self.db.execute('DELETE FROM samples WHERE at < ?', (at - self.retention_seconds,))
        self.db.commit()
        while self.rows and self.rows[0]['end'] < at - self.retention_seconds:
            self.rows.popleft()
        return row

    def close(self):
        self.db.close()


class Monitor:
    def __init__(self, client, gateway, history, poll_seconds=3):
        self.client, self.gateway, self.history = client, gateway.rstrip('/'), history
        self.poll_seconds = poll_seconds
        self.status = {'backends': [], 'capacity': 0}
        self.models, self.series = [], {}
        self.error = None
        self.updated_at = None
        self.sequence = 0
        self.changed = asyncio.Condition()

    async def sample(self):
        try:
            status, metrics, models = await asyncio.gather(*(
                self.client.get(self.gateway + path, timeout=4) for path in ['/admin/status', '/metrics', '/v1/models']))
            if status.status_code not in (200, 503):
                status.raise_for_status()
            metrics.raise_for_status()
            self.status = status.json()
            self.status['backends'] = [dict(n, healthy=False, accepting=False, metrics_stale=True)
                                      if not fresh_node(n) else n for n in self.status.get('backends', [])]
            self.models = ([{'id': m['id'], 'max_model_len': m.get('max_model_len')} for m in models.json().get('data', [])]
                           if models.status_code == 200 else [])
            self.series = parse_metrics(metrics.text)
            self.error = None
        except Exception as error:
            # Only operational error class is exposed; backend URLs/credentials are not logged.
            self.error = type(error).__name__
            self.series = {}
            self.models = []
            self.status = dict(self.status, capacity=0, backends=[dict(n, healthy=False, accepting=False) for n in self.status.get('backends', [])])
        self.updated_at = time.time()
        self.history.add(self.updated_at, self.series, self.status)
        async with self.changed:
            self.sequence += 1
            self.changed.notify_all()

    async def run(self):
        while True:
            await self.sample()
            await asyncio.sleep(self.poll_seconds)

    def snapshot(self):
        rows = list(self.history.rows)
        backend_rates = {n['name']: aggregate(rows, 15, replica=n['name']) for n in self.status.get('backends', [])}
        model_names = sorted({x['model'] for x in self.series.values() if x['model']})
        kv = defaultdict(float)
        for value in self.series.values():
            if any(n['name'] == value['replica'] and fresh_node(n) for n in self.status.get('backends', [])):
                kv[value['replica']] = max(kv[value['replica']], value['gauges'].get('kv_percent', 0))
        return {'updated_at': self.updated_at, 'sequence': self.sequence, 'poll_seconds': self.poll_seconds,
                'error': self.error, 'gateway': self.status, 'models': self.models,
                'live': aggregate(rows, 15), 'windows': {str(w): aggregate(rows, w) for w in [60, 300, 900, 3600]},
                'backend_rates': backend_rates, 'kv_percent': dict(kv),
                'model_rates': {name: aggregate(rows, 15, model=name) for name in model_names},
                'started_at': rows[0]['start'] if rows else None}


class IsolatedMonitor(Monitor):
    """A direct engine stays outside the production pool and its aggregates."""
    def __init__(self, client, target, history, poll_seconds=3):
        super().__init__(client, target['url'], history, poll_seconds)
        self.target = target

    async def sample(self):
        name = self.target['name']
        try:
            health, metric, models = await asyncio.gather(*(
                self.client.get(self.gateway + path, timeout=3) for path in ['/health', '/metrics', '/v1/models']))
            health.raise_for_status(); metric.raise_for_status(); models.raise_for_status()
            self.models = [{'id': m['id'], 'max_model_len': m.get('max_model_len')} for m in models.json()['data']]
            labelled = []
            for line in metric.text.splitlines():
                m = LINE.match(line)
                if not m or line.startswith('#'):
                    continue
                labels = (m[2] + ',') if m[2] else ''
                labelled.append(f'{m[1]}{{{labels}replica={json.dumps(name)}}} {m[3]}')
            self.series = parse_metrics('\n'.join(labelled))
            running = sum(d['gauges'].get('running', 0) for d in self.series.values())
            waiting = sum(d['gauges'].get('waiting', 0) for d in self.series.values())
            self.status = {'mode': 'isolated-test', 'capacity': self.target.get('capacity'), 'queued': 0,
                           'backends': [{'name': name, 'healthy': True, 'accepting': True, 'draining': False,
                                         'models': [m['id'] for m in self.models], 'running': running,
                                         'waiting': waiting, 'gateway_inflight': 0, 'metrics_age_seconds': 0}]}
            self.error = None
        except Exception as error:
            self.error = type(error).__name__
            self.models, self.series = [], {}
            self.status = {'mode': 'isolated-test', 'capacity': 0, 'backends': [
                {'name': name, 'healthy': False, 'accepting': False, 'draining': False, 'models': []}]}
        self.updated_at = time.time()
        self.history.add(self.updated_at, self.series, self.status)
        self.sequence += 1
