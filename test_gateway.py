import asyncio
from contextlib import asynccontextmanager
import json
import socket

import httpx
import pytest
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import Response, StreamingResponse

from gateway import Node, Pool, add_replica, create_app, samples


@asynccontextmanager
async def server(app):
    sock = socket.socket()
    sock.bind(('127.0.0.1', 0))
    sock.listen(128)
    port = sock.getsockname()[1]
    runner = uvicorn.Server(uvicorn.Config(app, log_level='error', access_log=False, timeout_graceful_shutdown=2))
    task = asyncio.create_task(runner.serve(sockets=[sock]))
    for _ in range(500):
        if runner.started:
            break
        if task.done():
            await task
        await asyncio.sleep(.01)
    assert runner.started
    try:
        yield f'http://127.0.0.1:{port}'
    finally:
        runner.should_exit = True
        await task


def fake(name, model='test'):
    app = FastAPI()
    state = {'active': 0, 'peak': 0, 'requests': 0, 'cancelled': 0, 'healthy': True, 'external': 0}

    @app.get('/health')
    async def health():
        return Response(status_code=200 if state['healthy'] else 503)

    @app.get('/metrics')
    async def metrics():
        return Response(f'vllm:num_requests_running{{model_name="test"}} {state["active"] + state["external"]}\n'
                        'vllm:num_requests_waiting{model_name="test"} 0\n'
                        f'vllm:generation_tokens_total{{model_name="test"}} {state["requests"]}\n')

    @app.get('/v1/models')
    async def models():
        return {'data': [{'id': model, 'max_model_len': 262144}]}

    @app.post('/v1/chat/completions')
    async def chat(request: Request):
        body = await request.json()
        state['requests'] += 1
        if body.get('reject'):
            return Response('bad input', status_code=400)
        state['active'] += 1
        state['peak'] = max(state['peak'], state['active'])
        if body.get('wait_for_headers'):
            for _ in range(200):
                if await request.is_disconnected():
                    state['active'] -= 1
                    state['cancelled'] += 1
                    return Response(status_code=499)
                await asyncio.sleep(.01)
        async def stream():
            ended = False
            try:
                yield b'data: {"choices":[{"delta":{"role":"assistant","content":""}}]}\n\n'
                for _ in range(body.get('count', 2)):
                    await asyncio.sleep(body.get('delay', .01))
                    yield (f'data: {{"choices":[{{"delta":{{"content":"{name}"}}}}]}}\n\n').encode()
                yield b'data: [DONE]\n\n'
                ended = True
            finally:
                state['active'] -= 1
                if not ended:
                    state['cancelled'] += 1
        return StreamingResponse(stream(), media_type='text/event-stream')
    return app, state


@asynccontextmanager
async def cluster(capacity=8, **extra):
    a, sa = fake('a')
    b, sb = fake('b')
    async with server(a) as ua, server(b) as ub:
        config = {'backends': [{'name': 'a', 'url': ua, 'capacity': capacity, 'max_capacity': max(8, capacity)},
                               {'name': 'b', 'url': ub, 'capacity': capacity, 'max_capacity': max(8, capacity)}],
                  'affinity_secret': 'test', 'poll_seconds': .05, **extra}
        gateway = create_app(config)
        async with server(gateway) as url, httpx.AsyncClient(base_url=url, timeout=10) as client:
            yield client, gateway.state.pool, sa, sb


@pytest.mark.asyncio
@pytest.mark.parametrize('capacity', [6, 8, 12])
async def test_concurrent_requests_balance_and_preserve_sse(capacity):
    async with cluster(capacity=capacity) as (c, pool, a, b):
        async def call(i):
            r = await c.post('/v1/chat/completions', json={'model': 'test', 'messages': [], 'stream': True, 'delay': .1},
                             headers={'X-Conversation-ID': str(i)})
            assert r.status_code == 200
            assert r.text.endswith('data: [DONE]\n\n')
            return r.headers['x-spark-node']
        nodes = await asyncio.gather(*(call(i) for i in range(capacity * 2)))
        assert nodes.count('a') == nodes.count('b') == capacity
        assert a['peak'] == b['peak'] == capacity
        assert all(n.inflight == n.debt == 0 for n in pool.nodes)
        assert pool.buffered == 0


@pytest.mark.asyncio
async def test_affinity_failover_and_direct_traffic():
    async with cluster() as (c, pool, a, b):
        payload = {'model': 'test', 'messages': [{'role': 'user', 'content': 'hello'}]}
        r1 = await c.post('/v1/chat/completions', json=payload, headers={'X-Conversation-ID': 'session'})
        payload['messages'].append({'role': 'assistant', 'content': 'hi'})
        r2 = await c.post('/v1/chat/completions', json=payload, headers={'X-Conversation-ID': 'session'})
        assert r1.headers['x-spark-node'] == r2.headers['x-spark-node']
        # Existing direct traffic must prevent dispatch to the occupied replica.
        a['external'] = 8
        await pool.poll()
        r = await c.post('/v1/chat/completions', json=payload)
        assert r.headers['x-spark-node'] == 'b'
        a['healthy'] = False
        await pool.poll()
        assert (await c.get('/health')).status_code == 200
        assert (await c.get('/ready')).status_code == 503
        assert (await c.post('/v1/chat/completions', json=payload)).headers['x-spark-node'] == 'b'


@pytest.mark.asyncio
async def test_disconnect_closes_upstream_and_releases_slot():
    async with cluster() as (c, pool, a, b):
        async with c.stream('POST', '/v1/chat/completions', json={'model': 'test', 'messages': [], 'count': 100, 'delay': .1},
                            headers={'X-Spark-Node': 'a'}) as r:
            assert r.status_code == 200
            async for line in r.aiter_lines():
                if 'content' in line:
                    break
        for _ in range(100):
            if pool.nodes[0].inflight == a['active'] == 0:
                break
            await asyncio.sleep(.02)
        assert a['cancelled'] == 1, (a, pool.status())
        assert pool.nodes[0].inflight == pool.nodes[0].sent == pool.nodes[0].debt == 0
        assert pool.buffered == 0


@pytest.mark.asyncio
async def test_bounded_queue_and_timeout():
    async with cluster(capacity=1, max_queue=1, queue_timeout=.15) as (c, pool, a, b):
        async def long(pin):
            return await c.post('/v1/chat/completions', json={'model': 'test', 'count': 5, 'delay': .1}, headers={'X-Spark-Node': pin})
        tasks = [asyncio.create_task(long(x)) for x in ('a', 'b')]
        while not a['active'] or not b['active']:
            await asyncio.sleep(.01)
        waiting = asyncio.create_task(c.post('/v1/chat/completions', json={'model': 'test', }))
        while pool.queued == 0:
            await asyncio.sleep(.01)
        r = await c.post('/v1/chat/completions', json={'model': 'test', })
        assert r.status_code == 429 and r.headers['retry-after'] == '2'
        assert (await waiting).status_code == 429
        await asyncio.gather(*tasks)
        assert pool.queued == pool.buffered == 0


@pytest.mark.asyncio
async def test_disconnect_before_upstream_headers():
    async with cluster() as (c, pool, a, b):
        task = asyncio.create_task(c.post('/v1/chat/completions', json={'model': 'test', 'wait_for_headers': True},
                                          headers={'X-Spark-Node': 'a'}))
        while not a['active']:
            await asyncio.sleep(.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        for _ in range(100):
            if pool.nodes[0].inflight == 0 and a['cancelled']:
                break
            await asyncio.sleep(.02)
        assert a['cancelled'] == 1
        assert pool.nodes[0].inflight == pool.nodes[0].sent == pool.buffered == 0


@pytest.mark.asyncio
async def test_disconnect_in_queue_removes_waiter():
    async with cluster(capacity=1) as (c, pool, a, b):
        active = asyncio.create_task(c.post('/v1/chat/completions', json={'model': 'test', 'delay': .1, 'count': 10},
                                            headers={'X-Spark-Node': 'a'}))
        while not a['active']:
            await asyncio.sleep(.01)
        waiting = asyncio.create_task(c.post('/v1/chat/completions', json={'model': 'test', }, headers={'X-Spark-Node': 'a'}))
        while not pool.queued:
            await asyncio.sleep(.01)
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
        for _ in range(50):
            if pool.queued == 0:
                break
            await asyncio.sleep(.02)
        assert pool.queued == 0
        assert a['requests'] == 1
        await active


@pytest.mark.asyncio
async def test_validation_error_passthrough_and_metrics():
    async with cluster(max_body_bytes=1000) as (c, pool, a, b):
        assert (await c.post('/v1/chat/completions', content='x' * 1001)).status_code == 413
        assert (await c.post('/v1/chat/completions', content='bad')).status_code == 400
        assert (await c.post('/v1/chat/completions', json=[])).status_code == 400
        assert (await c.post('/v1/chat/completions', json={'model': 'test', }, headers={'X-Spark-Node': 'invalid'})).status_code == 400
        assert (await c.post('/v1/responses', json={'model': 'test', })).status_code == 404
        r = await c.post('/v1/chat/completions', json={'model': 'test', 'reject': True})
        assert r.status_code == 400 and r.text == 'bad input'
        assert a['requests'] + b['requests'] == 1
        metric = (await c.get('/metrics')).text
        assert 'replica="a"' in metric and 'replica="b"' in metric
        assert pool.buffered == 0


@pytest.mark.asyncio
async def test_only_failed_connect_is_retried():
    calls = []
    async def handler(request):
        if request.url.path == '/health':
            return httpx.Response(200)
        if request.url.path == '/metrics':
            return httpx.Response(200, text='vllm:num_requests_running 0\n')
        if request.url.path == '/v1/models':
            return httpx.Response(200, json={'data': [{'id': 'test'}]})
        calls.append(request.url.host)
        if request.url.host == 'a':
            raise httpx.ConnectError('unreachable')
        return httpx.Response(200, content=b'done')
    app = create_app({'backends': [{'name': 'a', 'url': 'http://a'}, {'name': 'b', 'url': 'http://b'}]},
                     transport=httpx.MockTransport(handler))
    async with server(app) as url, httpx.AsyncClient(base_url=url) as c:
        pool = app.state.pool
        # Set b's load so a is selected first. b remains available on the retry.
        pool.nodes[1].external = 1
        r = await c.post('/v1/chat/completions', json={'model': 'test', })
        assert r.status_code == 200 and r.content == b'done'
        assert calls == ['a', 'b']
        assert all(n.inflight == 0 for n in pool.nodes)


def test_metrics_labels_and_family_metadata():
    seen = set()
    m = '# HELP vllm:test Count\n# TYPE vllm:test counter\nvllm:test{engine="0"} 2\n'
    result = '\n'.join(add_replica(m, 'a', seen) + add_replica(m, 'b', seen))
    assert result.count('# HELP') == result.count('# TYPE') == 1
    assert samples(result, 'vllm:test') == 4


@pytest.mark.asyncio
async def test_different_models_on_one_host_share_an_endpoint_without_substitution():
    app_a, a = fake('a', 'model-a')
    app_b, b = fake('b', 'model-b')
    async with server(app_a) as ua, server(app_b) as ub:
        # Both backends deliberately live on the same machine at different ports.
        app = create_app({'backends': [{'name': 'a', 'url': ua, 'capacity': 2},
                                      {'name': 'b', 'url': ub, 'capacity': 3}]})
        async with server(app) as url, httpx.AsyncClient(base_url=url) as c:
            listing = (await c.get('/v1/models')).json()
            assert {m['id'] for m in listing['data']} == {'model-a', 'model-b'}
            ra, rb = await asyncio.gather(
                c.post('/v1/chat/completions', json={'model': 'model-a'}),
                c.post('/v1/chat/completions', json={'model': 'model-b'}))
            assert ra.headers['x-spark-node'] == 'a' and rb.headers['x-spark-node'] == 'b'
            assert a['requests'] == b['requests'] == 1
            assert (await c.post('/v1/chat/completions', json={'model': 'unknown'})).status_code == 404
            assert (await c.post('/v1/chat/completions', json={'model': 'model-a'}, headers={'X-Spark-Node': 'b'})).status_code == 400
            b['healthy'] = False
            await app.state.pool.poll()
            assert (await c.post('/v1/chat/completions', json={'model': 'model-b'})).status_code == 503
            assert a['requests'] == 1  # Never send model-b work to model-a.
            assert (await c.post('/v1/chat/completions', json={})).status_code == 400


@pytest.mark.asyncio
async def test_shared_alias_is_listed_once_with_discovered_context():
    async with cluster(capacity=8) as (c, pool, a, b):
        data = (await c.get('/v1/models')).json()['data']
        assert len(data) == 1 and data[0]['id'] == 'test'
        assert (await c.get('/admin/status')).json()['context_per_request'] == 262144


@pytest.mark.asyncio
async def test_multiple_models_on_one_gpu_obey_shared_host_budget():
    app_a, a = fake('a', 'model-a')
    app_b, b = fake('b', 'model-b')
    async with server(app_a) as ua, server(app_b) as ub:
        app = create_app({'backends': [{'name': 'a', 'url': ua, 'capacity': 3, 'host': 'one-gpu'},
                                      {'name': 'b', 'url': ub, 'capacity': 3, 'host': 'one-gpu'}],
                          'host_limits': {'one-gpu': 2}})
        async with server(app) as url, httpx.AsyncClient(base_url=url, timeout=10) as c:
            assert (await c.get('/admin/status')).json()['configured_capacity'] == 2
            tasks = [asyncio.create_task(c.post('/v1/chat/completions', json={
                'model': 'model-a' if i % 2 else 'model-b', 'delay': .1, 'count': 3})) for i in range(4)]
            peak = 0
            while not all(t.done() for t in tasks):
                peak = max(peak, a['active'] + b['active'])
                await asyncio.sleep(.005)
            assert peak == 2
            assert all(r.status_code == 200 for r in await asyncio.gather(*tasks))


@pytest.mark.asyncio
async def test_capacity_changes_drain_existing_work_and_reject_bad_config(tmp_path):
    async with cluster() as (c, pool, a, b):
        p = tmp_path / 'config.json'
        settings = {'backends': [{'name': n.name, 'url': n.url, 'capacity': 6} for n in pool.nodes]}
        p.write_text(json.dumps(settings))
        pool.config_path = p
        active = asyncio.create_task(c.post('/v1/chat/completions', json={'model': 'test', 'delay': .1, 'count': 5}))
        while not any(n.inflight for n in pool.nodes):
            await asyncio.sleep(.01)
        await pool.refresh_capacity()
        assert [n.capacity for n in pool.nodes] == [6, 6]
        assert sum(n.inflight for n in pool.nodes) == 1
        assert (await active).status_code == 200
        p.write_text('{broken')
        await pool.refresh_capacity()
        assert [n.capacity for n in pool.nodes] == [6, 6]
        settings['backends'][0]['capacity'] = 99
        p.write_text(json.dumps(settings))
        await pool.refresh_capacity()
        assert [n.capacity for n in pool.nodes] == [6, 6]


async def set_drain(pool, path, name, value):
    settings = {'backends': [{'name': n.name, 'url': n.url, 'capacity': n.capacity,
                              'draining': value if n.name == name else n.draining} for n in pool.nodes]}
    path.write_text(json.dumps(settings))
    pool.config_path = path
    await pool.refresh_capacity()


@pytest.mark.asyncio
async def test_hot_drain_finishes_existing_stream_and_resume_restores_capacity(tmp_path):
    async with cluster(capacity=1) as (c, pool, a, b):
        active = asyncio.create_task(c.post('/v1/chat/completions', json={
            'model': 'test', 'delay': .05, 'count': 10}, headers={'X-Spark-Node': 'b'}))
        while not b['active']:
            await asyncio.sleep(.01)
        path = tmp_path / 'config.json'
        await set_drain(pool, path, 'b', True)
        status = (await c.get('/admin/status')).json()
        worker = status['backends'][1]
        assert status['capacity'] == status['configured_capacity'] == 1
        assert status['total_configured_capacity'] == 2
        assert worker['healthy'] and worker['draining'] and not worker['accepting']
        assert worker['gateway_inflight'] == 1 and not worker['drained']
        assert (await c.get('/ready')).status_code == 200
        assert (await c.post('/v1/chat/completions', json={'model': 'test'},
                             headers={'X-Spark-Node': 'b'})).status_code == 503
        response = await c.post('/v1/chat/completions', json={'model': 'test'})
        assert response.headers['x-spark-node'] == 'a'
        result = await active
        assert result.status_code == 200 and result.text.endswith('data: [DONE]\n\n')
        assert b['requests'] == 1 and b['cancelled'] == 0
        await pool.poll()
        assert pool.status()['backends'][1]['drained']
        metric = (await c.get('/metrics')).text
        assert 'spark_gateway_draining{replica="b"} 1' in metric
        assert 'spark_gateway_backend_up{replica="b"} 1' in metric
        assert 'vllm:generation_tokens_total{model_name="test",replica="b"} 1' in metric
        # An invalid boolean must not silently resume a drained backend.
        await set_drain(pool, path, 'b', 'false')
        assert pool.nodes[1].draining
        await set_drain(pool, path, 'b', False)
        response = await c.post('/v1/chat/completions', json={'model': 'test'}, headers={'X-Spark-Node': 'b'})
        assert response.status_code == 200 and b['requests'] == 2


@pytest.mark.asyncio
async def test_queued_request_moves_to_remaining_replica_during_drain(tmp_path):
    async with cluster(capacity=1) as (c, pool, a, b):
        active = [asyncio.create_task(c.post('/v1/chat/completions', json={
            'model': 'test', 'delay': .05, 'count': count}, headers={'X-Spark-Node': name}))
                  for name, count in [('a', 5), ('b', 12)]]
        while not a['active'] or not b['active']:
            await asyncio.sleep(.01)
        queued = asyncio.create_task(c.post('/v1/chat/completions', json={'model': 'test'}))
        while pool.queued == 0:
            await asyncio.sleep(.01)
        await set_drain(pool, tmp_path / 'config.json', 'b', True)
        response = await queued
        assert response.status_code == 200 and response.headers['x-spark-node'] == 'a'
        assert a['requests'] == 2 and b['requests'] == 1
        assert all(r.status_code == 200 for r in await asyncio.gather(*active))
        assert pool.queued == pool.buffered == 0


@pytest.mark.asyncio
async def test_cancel_stream_and_reject_queued_pin_while_draining(tmp_path):
    async with cluster(capacity=1) as (c, pool, a, b):
        async with c.stream('POST', '/v1/chat/completions', json={
                'model': 'test', 'count': 100, 'delay': .05}, headers={'X-Spark-Node': 'b'}) as stream:
            lines = stream.aiter_lines()
            assert 'content' in await anext(lines)
            queued = asyncio.create_task(c.post('/v1/chat/completions', json={'model': 'test'},
                                               headers={'X-Spark-Node': 'b'}))
            for _ in range(200):
                if pool.queued or queued.done():
                    break
                await asyncio.sleep(.01)
            assert pool.queued, (queued.result() if queued.done() else pool.status())
            await set_drain(pool, tmp_path / 'config.json', 'b', True)
            assert (await queued).status_code == 503
        for _ in range(100):
            if pool.nodes[1].inflight == b['active'] == 0:
                break
            await asyncio.sleep(.02)
        assert b['cancelled'] == 1 and b['requests'] == 1
        assert pool.nodes[1].inflight == pool.nodes[1].debt == pool.nodes[1].sent == 0
        assert pool.buffered == pool.queued == 0


@pytest.mark.asyncio
async def test_drain_of_distinct_model_never_substitutes_another_model(tmp_path):
    app_a, a = fake('a', 'model-a')
    app_b, b = fake('b', 'model-b')
    async with server(app_a) as ua, server(app_b) as ub:
        app = create_app({'backends': [{'name': 'a', 'url': ua, 'capacity': 1},
                                      {'name': 'b', 'url': ub, 'capacity': 1}]})
        async with server(app) as url, httpx.AsyncClient(base_url=url) as c:
            pool = app.state.pool
            path = tmp_path / 'config.json'
            await set_drain(pool, path, 'b', True)
            assert {m['id'] for m in (await c.get('/v1/models')).json()['data']} == {'model-a'}
            assert (await c.post('/v1/chat/completions', json={'model': 'model-b'})).status_code == 503
            assert b['requests'] == a['requests'] == 0
            assert (await c.post('/v1/chat/completions', json={'model': 'model-a'})).status_code == 200
            b['healthy'] = False
            await pool.poll()
            assert (await c.get('/ready')).status_code == 200
            assert not pool.status()['backends'][1]['drained']  # Failed poll is not proof of idle.
            await set_drain(pool, path, 'a', True)
            assert (await c.get('/health')).status_code == 503
            assert (await c.get('/ready')).status_code == 503
            await set_drain(pool, path, 'b', False)
            b['healthy'] = True
            await pool.poll()
            assert (await c.post('/v1/chat/completions', json={'model': 'model-b'})).status_code == 200
