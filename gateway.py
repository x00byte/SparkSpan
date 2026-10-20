"""OpenAI chat/completion gateway for independent vLLM replicas."""
import asyncio
from collections import OrderedDict
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import time

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

HOP = {'host', 'connection', 'keep-alive', 'proxy-authenticate', 'proxy-authorization',
       'te', 'trailer', 'transfer-encoding', 'upgrade', 'content-length', 'content-encoding'}
ROUTING_HEADERS = {'x-conversation-id', 'x-session-id', 'x-spark-node'}
GENERATE = {'/v1/chat/completions', '/v1/completions'}
STATELESS = {'/tokenize', '/detokenize'}
SAMPLE = re.compile(r'^([^\s{]+)(\{[^\n]*\})?\s+(\S+)(.*)$')


def samples(text, name):
    values = []
    for line in text.splitlines():
        m = SAMPLE.match(line)
        if m and m[1] == name:
            with suppress(ValueError):
                values.append(float(m[3]))
    return sum(values)


def add_replica(text, label, seen):
    """Preserve native metric families and make every series replica-specific."""
    out = []
    for line in text.splitlines():
        if line.startswith('#'):
            if line not in seen:
                seen.add(line)
                out.append(line)
            continue
        m = SAMPLE.match(line)
        if m:
            labels = (m[2][1:-1] + ',') if m[2] and m[2] != '{}' else ''
            out.append(f'{m[1]}{{{labels}replica={json.dumps(label)}}} {m[3]}{m[4]}')
    return out


@dataclass
class Node:
    name: str
    url: str
    capacity: int = 8
    max_capacity: int = 8
    models: list[str] | None = None
    host: str | None = None
    draining: bool = False
    healthy: bool = False
    inflight: int = 0
    sent: int = 0
    running: int = 0
    waiting: int = 0
    external: int = 0
    debt: int = 0
    metrics: str = ''
    checked: float = 0
    changed: float = field(default_factory=time.monotonic)
    signature: tuple = ()
    stalled: bool = False
    model_info: dict = field(default_factory=dict)
    models_checked: float = 0

    @property
    def load(self):
        return self.external + self.inflight


class Pool:
    def __init__(self, config, client):
        self.config, self.client = config, client
        self.nodes = [Node(**n) for n in config['backends']]
        if len({n.name for n in self.nodes}) != len(self.nodes):
            raise ValueError('Backend names must be unique')
        if not self.nodes or any(type(n.capacity) is not int or type(n.max_capacity) is not int
                                 or not 1 <= n.capacity <= n.max_capacity for n in self.nodes):
            raise ValueError('Each backend needs a positive capacity within its configured maximum')
        if any(type(n.draining) is not bool for n in self.nodes):
            raise ValueError('Backend draining must be a boolean')
        self.secret = config.get('affinity_secret', 'test-only').encode()
        self.host_limits = config.get('host_limits', {})
        if not isinstance(self.host_limits, dict) or any(type(v) is not int or v <= 0 for v in self.host_limits.values()):
            raise ValueError('Host limits must be positive request counts')
        self.condition = asyncio.Condition()
        self.poll_lock = asyncio.Lock()
        self.affinity = OrderedDict()
        self.queued = 0
        self.buffered = 0
        self.closed = False
        self.completed = self.failed = 0
        self.poller = None
        self.config_path = None
        self.config_mtime = None

    async def refresh_capacity(self):
        """Apply admission tuning without restarting models or dropping streams."""
        if not self.config_path:
            return
        try:
            mtime = self.config_path.stat().st_mtime_ns
            if mtime == self.config_mtime:
                return
            updated = json.loads(self.config_path.read_text())
            nodes = updated['backends']
            if [(n['name'], n['url']) for n in nodes] != [(n.name, n.url) for n in self.nodes]:
                return  # Changing backend identity requires an explicit deployment.
            caps = [n['capacity'] for n in nodes]
            if not all(type(c) is int and 1 <= c <= n.max_capacity for c, n in zip(caps, self.nodes)):
                return
            draining = [n.get('draining', False) for n in nodes]
            if any(type(value) is not bool for value in draining):
                return
            host_limits = updated.get('host_limits', self.host_limits)
            if not isinstance(host_limits, dict) or any(type(v) is not int or v <= 0 for v in host_limits.values()):
                return
            async with self.condition:
                for node, capacity, drain in zip(self.nodes, caps, draining):
                    node.capacity = capacity
                    node.draining = drain
                self.host_limits = host_limits
                self.config_mtime = mtime
                self.condition.notify_all()
        except (OSError, ValueError, KeyError, TypeError):
            pass  # Keep the last valid limits if a file update is incomplete.

    def key(self, request, body):
        explicit = request.headers.get('x-conversation-id') or request.headers.get('x-session-id')
        meta = body.get('metadata')
        if not explicit and isinstance(meta, dict):
            explicit = meta.get('conversation_id') or meta.get('session_id')
        caller = request.headers.get('authorization', '') + '|' + (request.client.host if request.client else '')
        if explicit:
            basis = ['session', caller, body.get('model'), str(explicit)]
        else:
            # First user turn is stable across normal conversation continuations.
            messages = body.get('messages')
            first = next((m for m in (messages if isinstance(messages, list) else [])
                          if isinstance(m, dict) and m.get('role') == 'user'), None)
            basis = ['prefix', caller, body.get('model'), first or body.get('prompt'), body.get('tools')]
        return hmac.new(self.secret, json.dumps(basis, sort_keys=True, separators=(',', ':')).encode(), hashlib.sha256).hexdigest()

    async def poll(self):
        async def one(node):
            try:
                health, metric = await asyncio.gather(
                    self.client.get(node.url + '/health', timeout=3),
                    self.client.get(node.url + '/metrics', timeout=3))
                health.raise_for_status()
                metric.raise_for_status()
                if not node.model_info or time.monotonic() - node.models_checked > 30:
                    models = await self.client.get(node.url + '/v1/models', timeout=3)
                    models.raise_for_status()
                    data = models.json()['data']
                    info = {m['id']: m for m in data if isinstance(m, dict) and isinstance(m.get('id'), str)
                            and (node.models is None or m['id'] in node.models)}
                    if not info:
                        raise ValueError('Backend exposes no configured models')
                    node.model_info = info
                    node.models_checked = time.monotonic()
                text = metric.text
                running = int(samples(text, 'vllm:num_requests_running'))
                waiting = int(samples(text, 'vllm:num_requests_waiting'))
                signature = tuple(samples(text, x) for x in (
                    'vllm:prompt_tokens_total', 'vllm:generation_tokens_total', 'vllm:kv_cache_usage_perc'))
                async with self.condition:
                    now = time.monotonic()
                    if signature != node.signature or not running + waiting:
                        node.changed = now
                    node.signature = signature
                    node.running, node.waiting = running, waiting
                    # Reserve direct traffic too, without counting our own requests twice.
                    node.external = max(0, running + waiting - node.sent)
                    node.stalled = bool(running + waiting and now - node.changed > self.config.get('stall_seconds', 180))
                    node.metrics, node.checked, node.healthy = text, now, True
                    self.condition.notify_all()
            except (httpx.HTTPError, ValueError, KeyError, TypeError):
                async with self.condition:
                    node.healthy = False
                    self.condition.notify_all()
        async with self.poll_lock:
            await asyncio.gather(*(one(n) for n in self.nodes))

    async def loop(self):
        while not self.closed:
            await self.refresh_capacity()
            await self.poll()
            await asyncio.sleep(self.config.get('poll_seconds', 1))

    def healthy(self, n):
        return n.healthy and not n.stalled and time.monotonic() - n.checked < 5

    def usable(self, n):
        return self.healthy(n) and not n.draining

    def drained(self, n):
        # A failed/stale poll cannot prove the engine has finished its work.
        return n.draining and self.healthy(n) and n.inflight == 0 and n.running + n.waiting == 0

    def host_has_room(self, node):
        if node.host not in self.host_limits:
            return True
        return sum(n.load for n in self.nodes if n.host == node.host) < self.host_limits[node.host]

    def total_capacity(self, nodes):
        total, grouped = 0, {}
        for n in nodes:
            if n.host in self.host_limits:
                grouped[n.host] = grouped.get(n.host, 0) + n.capacity
            else:
                total += n.capacity
        return total + sum(min(cap, self.host_limits[host]) for host, cap in grouped.items())

    async def acquire(self, request, key, estimate, pin=None, exclude=(), model=None):
        deadline = time.monotonic() + self.config.get('queue_timeout', 120)
        async with self.condition:
            if self.queued >= self.config.get('max_queue', 32):
                raise HTTPException(429, 'Gateway queue is full', headers={'Retry-After': '2'})
            self.queued += 1
            try:
                while True:
                    if await request.is_disconnected():
                        raise HTTPException(499, 'Client disconnected')
                    matching = [n for n in self.nodes if model is None or model in n.model_info]
                    if not matching:
                        if not any(n.model_info for n in self.nodes):
                            raise HTTPException(503, 'No ready model server')
                        raise HTTPException(404, 'Requested model is not exposed by any backend')
                    if pin and not any(n.name == pin for n in matching):
                        raise HTTPException(400, 'Pinned backend does not serve the requested model')
                    healthy = [n for n in matching if self.usable(n) and n.name not in exclude and (not pin or n.name == pin)]
                    if not healthy:
                        raise HTTPException(503, 'No ready Spark replica', headers={'Retry-After': '2'})
                    free = [n for n in healthy if n.load < n.capacity and self.host_has_room(n)]
                    if free:
                        now = time.monotonic()
                        old = self.affinity.get(key)
                        preferred = old[0] if old and now - old[2] < 3600 else None
                        # Rendezvous breaks ties consistently, without storing raw session IDs.
                        def score(n):
                            reuse = min(estimate, old[1]) if old and n.name == preferred else 0
                            debt = max(0, estimate - reuse)
                            return (n.load + (n.debt + debt) / 65536 - (0.75 if n.name == preferred else 0),
                                    hashlib.sha256((key + n.name).encode()).hexdigest())
                        node = min(free, key=score)
                        reused = min(estimate, old[1]) if old and node.name == preferred else 0
                        debt = max(0, estimate - reused)
                        node.inflight += 1
                        node.debt += debt
                        self.affinity[key] = (node.name, estimate, now)
                        self.affinity.move_to_end(key)
                        while len(self.affinity) > 10000:
                            self.affinity.popitem(last=False)
                        return Lease(self, node, debt)
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise HTTPException(429, 'Timed out waiting for a Spark slot', headers={'Retry-After': '2'})
                    with suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(self.condition.wait(), min(0.25, remaining))
            finally:
                self.queued -= 1

    def status(self):
        contexts = [m['max_model_len'] for n in self.nodes for m in n.model_info.values()
                    if type(m.get('max_model_len')) is int]
        return {'mode': 'independent-replicas', 'context_per_request': min(contexts) if contexts else None,
                'capacity': self.total_capacity([n for n in self.nodes if self.usable(n)]),
                'configured_capacity': self.total_capacity([n for n in self.nodes if not n.draining]),
                'total_configured_capacity': self.total_capacity(self.nodes), 'queued': self.queued,
                'host_limits': self.host_limits,
                'completed': self.completed, 'failed': self.failed,
                'backends': [{'name': n.name, 'healthy': self.healthy(n), 'accepting': self.usable(n),
                              'draining': n.draining, 'drained': self.drained(n), 'stalled': n.stalled,
                              'models': sorted(n.model_info),
                              'host': n.host,
                              'capacity': n.capacity, 'running': n.running, 'waiting': n.waiting,
                              'metrics_age_seconds': round(max(0, time.monotonic() - n.checked), 3) if n.checked else None,
                              'gateway_inflight': n.inflight, 'admission_load': n.load,
                              'estimated_prefill_tokens': n.debt} for n in self.nodes]}


class Lease:
    def __init__(self, pool, node, debt):
        self.pool, self.node, self.debt = pool, node, debt
        self.released = self.sent = False

    def mark_sent(self):
        self.node.sent += 1
        self.sent = True

    def first_output(self):
        self.node.debt -= self.debt
        self.debt = 0

    async def release(self):
        async with self.pool.condition:
            if not self.released:
                self.released = True
                self.first_output()
                self.node.inflight -= 1
                if self.sent:
                    self.node.sent -= 1
                self.pool.condition.notify_all()


async def watch_disconnect(request, awaitable):
    """Cancel even while waiting for upstream headers or a long cold prefill."""
    task = asyncio.ensure_future(awaitable)
    try:
        while not task.done():
            await asyncio.wait({task}, timeout=0.25)
            if not task.done() and await request.is_disconnected():
                raise HTTPException(499, 'Client disconnected')
        return await task
    finally:
        if not task.done():
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task


def create_app(config=None, transport=None):
    @asynccontextmanager
    async def lifespan(app):
        settings = config or json.loads(Path(os.environ['SPARK_GATEWAY_CONFIG']).read_text())
        async with httpx.AsyncClient(transport=transport, timeout=httpx.Timeout(900, connect=5, pool=5),
                                     limits=httpx.Limits(max_connections=64, max_keepalive_connections=32),
                                     trust_env=False) as client:
            app.state.pool = Pool(settings, client)
            if config is None:
                app.state.pool.config_path = Path(os.environ['SPARK_GATEWAY_CONFIG'])
            await app.state.pool.poll()
            app.state.pool.poller = asyncio.create_task(app.state.pool.loop())
            try:
                yield
            finally:
                app.state.pool.closed = True
                app.state.pool.poller.cancel()
                with suppress(asyncio.CancelledError):
                    await app.state.pool.poller

    app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)

    @app.get('/health')
    @app.get('/ready')
    @app.get('/admin/status')
    async def health(request: Request):
        pool = request.app.state.pool
        status = pool.status()
        ready = status['capacity'] > 0
        if request.url.path == '/ready':
            ready = ready and all(pool.healthy(n) for n in pool.nodes if not n.draining)
        return JSONResponse(status, status_code=200 if ready else 503)

    @app.get('/metrics')
    async def metrics(request: Request):
        pool, seen, lines = request.app.state.pool, set(), []
        for n in pool.nodes:
            # Keep last counters on transient outages; stale gauges are marked by backend_up.
            lines.extend(add_replica(n.metrics, n.name, seen))
            for name, value in [('backend_up', int(pool.healthy(n))), ('accepting', int(pool.usable(n))),
                                ('draining', int(n.draining)), ('drained', int(pool.drained(n))), ('inflight', n.inflight),
                                ('prefill_tokens_estimate', n.debt), ('capacity', n.capacity)]:
                lines.append(f'spark_gateway_{name}{{replica={json.dumps(n.name)}}} {value}')
        lines.extend([f'spark_gateway_queue {pool.queued}', f'spark_gateway_completed_total {pool.completed}',
                      f'spark_gateway_failed_total {pool.failed}'])
        return Response('\n'.join(lines) + '\n', media_type='text/plain; version=0.0.4')

    @app.get('/v1/models')
    @app.get('/version')
    async def models(request: Request):
        pool = request.app.state.pool
        if request.url.path == '/v1/models':
            merged = {}
            for n in pool.nodes:
                if pool.usable(n):
                    for name, info in n.model_info.items():
                        # One alias may have several replicas. Advertise the smallest
                        # context limit if replicas have different allocations.
                        old = merged.get(name)
                        if old and type(old.get('max_model_len')) is int and type(info.get('max_model_len')) is int:
                            info = dict(info, max_model_len=min(old['max_model_len'], info['max_model_len']))
                        merged[name] = info
            if not merged:
                raise HTTPException(503, 'No ready model server')
            return JSONResponse({'object': 'list', 'data': list(merged.values())})
        for n in pool.nodes:
            if pool.usable(n):
                try:
                    r = await pool.client.get(n.url + request.url.path, timeout=5)
                    return Response(r.content, status_code=r.status_code, media_type='application/json')
                except httpx.HTTPError:
                    continue
        raise HTTPException(503, 'No ready Spark replica')

    @app.post('/{path:path}')
    async def forward(request: Request, path: str):
        endpoint = '/' + path
        if endpoint not in GENERATE | STATELESS:
            raise HTTPException(404, 'Supported: chat/completions, completions, tokenize and detokenize')
        pool = request.app.state.pool
        pin = request.headers.get('x-spark-node')
        if pin and pin not in {n.name for n in pool.nodes}:
            raise HTTPException(400, 'Unknown X-Spark-Node')
        size_limit = pool.config.get('max_body_bytes', 32 * 1024**2)
        total_limit = pool.config.get('max_buffer_bytes', 128 * 1024**2)
        data = bytearray()
        leased = response = None
        handed_off = False
        reserved = 0
        try:
            async for chunk in request.stream():
                if len(data) + len(chunk) > size_limit:
                    raise HTTPException(413, 'Request body too large')
                if pool.buffered + len(chunk) > total_limit:
                    raise HTTPException(429, 'Gateway request buffer is full', headers={'Retry-After': '2'})
                pool.buffered += len(chunk)
                reserved += len(chunk)
                data.extend(chunk)
            try:
                body = json.loads(data)
                if not isinstance(body, dict):
                    raise ValueError()
            except (ValueError, UnicodeDecodeError):
                raise HTTPException(400, 'Expected a JSON object')
            key = pool.key(request, body)
            model = body.get('model')
            if endpoint in GENERATE and (not isinstance(model, str) or not model):
                raise HTTPException(400, 'A non-empty model name is required')
            estimate = min(262144, max(1, len(data) // 4))
            # These IDs are routing hints. vLLM's own validation still handles the payload.
            headers = {k: v for k, v in request.headers.items() if k.lower() not in HOP | ROUTING_HEADERS}
            headers['accept-encoding'] = 'identity'
            excluded = []
            while True:
                leased = await pool.acquire(request, key, estimate, pin, excluded, model)
                node = leased.node
                upstream = pool.client.build_request('POST', node.url + endpoint, content=bytes(data), headers=headers,
                                                      params=request.query_params)
                leased.mark_sent()
                try:
                    response = await watch_disconnect(request, pool.client.send(upstream, stream=True))
                    break
                except (httpx.ConnectError, httpx.ConnectTimeout):
                    # A connection that never formed cannot have executed a generation.
                    node.healthy = False
                    excluded.append(node.name)
                    await leased.release()
                    leased = None
                    if pin or len(excluded) == len(pool.nodes):
                        raise HTTPException(503, 'Could not connect to a Spark replica')
            outgoing = {k: v for k, v in response.headers.items() if k.lower() not in HOP}
            outgoing.update({'X-Spark-Node': node.name, 'X-Accel-Buffering': 'no'})
            if 'text/event-stream' in response.headers.get('content-type', ''):
                outgoing['cache-control'] = 'no-cache'

            async def stream():
                pending = None
                first_buffer = b''
                # Mock transports may return an already-buffered response.
                iterator = response.aiter_bytes().__aiter__()
                try:
                    while True:
                        pending = asyncio.create_task(anext(iterator))
                        while not pending.done():
                            await asyncio.wait({pending}, timeout=15)
                            if not pending.done() and 'text/event-stream' in outgoing.get('content-type', ''):
                                yield b': spark-gateway keepalive\n\n'
                        try:
                            chunk = pending.result()
                        except StopAsyncIteration:
                            break
                        # Empty role chunks are not evidence of prefill completion.
                        if leased.debt:
                            first_buffer = (first_buffer + chunk)[-65536:]
                            for line in first_buffer.split(b'\n')[:-1]:
                                if line.startswith(b'data: '):
                                    with suppress(ValueError, AttributeError, TypeError):
                                        event = json.loads(line[6:])
                                        for choice in event.get('choices', []):
                                            delta = choice.get('delta', {})
                                            if choice.get('text') or any(delta.get(x) for x in ('content', 'reasoning', 'reasoning_content', 'tool_calls')):
                                                leased.first_output()
                            first_buffer = first_buffer.rsplit(b'\n', 1)[-1]
                        yield chunk
                    pool.completed += 1
                except (httpx.HTTPError, asyncio.CancelledError):
                    pool.failed += 1
                    raise
                finally:
                    # Cleanup must survive Starlette's cancellation scope.
                    async def cleanup():
                        try:
                            if pending and not pending.done():
                                pending.cancel()
                                with suppress(asyncio.CancelledError):
                                    await pending
                            await response.aclose()
                        finally:
                            await leased.release()
                    import anyio
                    with anyio.CancelScope(shield=True):
                        try:
                            await cleanup()
                        finally:
                            pool.buffered -= reserved

            handed_off = True
            return StreamingResponse(stream(), status_code=response.status_code, headers=outgoing)
        except httpx.HTTPError:
            pool.failed += 1
            raise HTTPException(502, 'Upstream connection failed; request was not replayed')
        finally:
            if not handed_off:
                pool.buffered -= reserved
                import anyio
                with anyio.CancelScope(shield=True):
                    if response:
                        await response.aclose()
                    if leased:
                        await leased.release()

    return app


app = create_app()
