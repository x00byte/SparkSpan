import asyncio
from contextlib import asynccontextmanager
import json
import socket
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
import uvicorn

from dashboard.app import create_app


@asynccontextmanager
async def serve(application):
    socket_ = socket.socket()
    socket_.bind(('127.0.0.1',0))
    port=socket_.getsockname()[1]
    server=uvicorn.Server(uvicorn.Config(application,log_level='error',access_log=False,timeout_graceful_shutdown=1))
    running=asyncio.create_task(server.serve(sockets=[socket_]))
    for _ in range(300):
        if server.started:break
        if running.done():await running
        await asyncio.sleep(.01)
    assert server.started
    try:yield f'http://127.0.0.1:{port}'
    finally:
        server.should_exit=True
        await running


def fake_gateway(path):
    app=FastAPI()
    state={'active':0,'requests':0,'aborted':0,'output':0}
    @app.get('/health')
    async def health():return {'ready':True}
    @app.get('/admin/status')
    async def status():
        c=json.loads(path.read_text())
        return {'capacity':sum(n['capacity'] for n in c['backends'] if not n.get('draining')),
                'configured_capacity':12,'queued':0,'backends':[
                    {'name':n['name'],'host':n.get('host'),'capacity':n['capacity'],'healthy':True,
                     'accepting':not n.get('draining',False),'draining':n.get('draining',False),
                     'drained':n.get('draining',False) and not state['active'],
                     'models':['model-a' if n['name']=='a' else 'model-b'],
                     'running':state['active'] if n['name']=='a' else 0,'waiting':0,'gateway_inflight':state['active'],
                     'metrics_age_seconds':.1} for n in c['backends']]}
    @app.get('/v1/models')
    async def models():return {'data':[{'id':'model-a','max_model_len':1000},{'id':'model-b','max_model_len':2000}]}
    @app.get('/metrics')
    async def metrics():return Response(f'vllm:generation_tokens_total{{replica="a",model_name="model-a"}} {state["output"]}\nvllm:generation_tokens_created{{replica="a",model_name="model-a"}} 1\n',media_type='text/plain')
    @app.post('/v1/chat/completions')
    async def chat(request:Request):
        data=await request.json();name='a' if data['model']=='model-a' else 'b'
        config=json.loads(path.read_text());node=next(n for n in config['backends'] if n['name']==name)
        if node.get('draining'):return JSONResponse({'error':'draining'},status_code=503)
        if request.headers.get('x-spark-node',name)!=name:return Response(status_code=400)
        state['requests']+=1
        if not data['stream']:
            state['output']+=3
            return JSONResponse({'choices':[{'message':{'content':'test'}}],'usage':{'completion_tokens':3,'prompt_tokens':8}},headers={'X-Spark-Node':name})
        state['active']+=1
        hold='hold' in data['messages'][0]['content']
        async def send():
            complete=False
            try:
                yield 'data: {"choices":[{"delta":{"role":"assistant"}}]}\n\n'
                for _ in range(1000 if hold else 3):
                    await asyncio.sleep(.02)
                    yield 'data: {"choices":[{"delta":{"content":"a"}}]}\n\n'
                yield 'data: {"choices":[],"usage":{"completion_tokens":3,"prompt_tokens":8}}\n\n'
                yield 'data: [DONE]\n\n'
                complete=True;state['output']+=3
            finally:
                state['active']-=1
                if not complete:state['aborted']+=1
        return StreamingResponse(send(),media_type='text/event-stream',headers={'X-Spark-Node':name})
    return app,state


@asynccontextmanager
async def dashboard(tmp_path):
    config=tmp_path/'settings.json'
    config.write_text(json.dumps({'affinity_secret':'hidden-live-secret','backends':[
        {'name':'a','url':'http://internal-a:8000','host':'host-a','capacity':6},
        {'name':'b','url':'http://internal-b:8000','host':'host-b','capacity':6}], 'host_limits':{'host-a':6,'host-b':6}}))
    gateway,counters=fake_gateway(config)
    isolated,test_counters=fake_gateway(config)
    async with serve(gateway) as upstream,serve(isolated) as direct:
        app=create_app({'runtime_dir':str(tmp_path/'runtime'),'gateway_config':str(config),
                        'gateway_url':upstream,'poll_seconds':.05,'allowed_hosts':['127.0.0.1'],
                        'test_targets':[{'name':'isolated','kind':'direct-test','url':direct,'label':'Test engine'}]})
        app.state.test_counters=test_counters
        async with serve(app) as base,httpx.AsyncClient(base_url=base,timeout=10) as client:
            token=(await client.get('/api/session')).json()['csrf_token']
            headers={'Origin':base,'X-CSRF-Token':token}
            yield client,headers,app,counters,config


def bench(**kwargs):
    return dict({'confirmed':True,'target':'gateway','model':'model-a','backend':'a','concurrency':1,'requests':1,
                 'max_tokens':16,'stream':True,'temperature':0,'prompt':'A private benchmark prompt'},**kwargs)


async def wait_job(app):
    for _ in range(300):
        if app.state.benchmarks.task.done():break
        await asyncio.sleep(.01)
    assert app.state.benchmarks.task.done()
    return app.state.benchmarks.job


@pytest.mark.asyncio
async def test_read_only_start_security_and_model_separation(tmp_path):
    async with dashboard(tmp_path) as (c,h,app,state,config):
        assert state['requests']==0
        missing_target=bench();missing_target.pop('target')
        assert (await c.post('/api/benchmarks/run',json=missing_target,headers=h)).status_code==400
        assert (await c.get('/',headers={'Host':'untrusted.example'})).status_code==400
        assert (await c.get('/')).status_code==200
        assert (await c.get('/static/app.js')).status_code==200
        hero=await c.get('/static/hero.png')
        assert hero.status_code==200 and hero.headers['content-type']=='image/png'
        assert hero.content.startswith(b'\x89PNG\r\n\x1a\n')
        snap=(await c.get('/api/state')).json()
        assert {m['id'] for m in snap['models']}=={'model-a','model-b'}
        sanitized=(await c.get('/api/config')).json()
        assert 'hidden-live-secret' not in json.dumps(sanitized)
        assert (await c.post('/api/benchmarks/run',json=bench())).status_code==403
        assert (await c.post('/api/benchmarks/run',json=bench(),headers=dict(h,Origin='http://evil.test'))).status_code==403
        assert (await c.post('/api/benchmarks/run',json=bench(model='model-b',backend='a'),headers=h)).status_code==400
        assert (await c.post('/api/benchmarks/run',json=bench(confirmed=False),headers=h)).status_code==400
        assert state['requests']==0


@pytest.mark.asyncio
@pytest.mark.parametrize('stream',[True,False])
async def test_explicit_benchmark_usage_history_and_export(tmp_path,stream):
    async with dashboard(tmp_path) as (c,h,app,state,config):
        result=await c.post('/api/benchmarks/run',json=bench(stream=stream,requests=6,concurrency=6),headers=h)
        assert result.status_code==200
        job=await wait_job(app)
        assert job['state']=='complete' and job['summary']['completed']==6
        assert job['summary']['output_tokens']==18
        assert job['summary']['aggregate_output_tps']>0
        assert (job['summary']['mean_ttft_ms'] is not None)==stream
        export=await c.get('/api/benchmarks/'+job['id']+'/export')
        assert export.status_code==200 and 'A private benchmark prompt' not in export.text
        assert (await c.get('/api/benchmarks/'+job['id']+'/export?format=csv')).text.startswith('index,status,')
        assert len((await c.get('/api/benchmarks')).json()['history'])==1


@pytest.mark.asyncio
async def test_cancel_sse_closes_active_upstream_without_orphan_requests(tmp_path):
    async with dashboard(tmp_path) as (c,h,app,state,config):
        await c.post('/api/benchmarks/run',json=bench(prompt='hold',requests=6,concurrency=6),headers=h)
        for _ in range(200):
            if state['active']==6:break
            await asyncio.sleep(.01)
        assert state['active']==6
        response=await c.post('/api/benchmarks/cancel',json={},headers=h)
        assert response.json()['active']['state']=='cancelled'
        for _ in range(200):
            if state['active']==0:break
            await asyncio.sleep(.01)
        assert state['active']==0 and state['aborted']==6
        assert app.state.benchmarks.task.done()
        assert app.state.benchmarks.job['summary']['aggregate_output_tps'] is None


@pytest.mark.asyncio
async def test_admin_drain_finishes_existing_requests_and_rejects_stale_edits(tmp_path):
    async with dashboard(tmp_path) as (c,h,app,state,path):
        await c.post('/api/benchmarks/run',json=bench(),headers=h)
        for _ in range(100):
            if state['active']:break
            await asyncio.sleep(.005)
        saved=(await c.get('/api/config')).json()
        edit={'revision':saved['revision'],'backends':[{'name':n['name'],'capacity':n['capacity'],'draining':n['name']=='a'} for n in saved['config']['backends']],
              'host_limits':saved['config']['host_limits']}
        response=await c.post('/api/config/apply',json=edit,headers=h)
        assert response.status_code==200
        assert (await wait_job(app))['state']=='complete'
        assert state['aborted']==0
        assert (await c.post('/api/config/apply',json=edit,headers=h)).status_code==409
        await app.state.monitor.sample()
        assert (await c.post('/api/benchmarks/run',json=bench(),headers=h)).status_code==400
        rollback=await c.post('/api/config/rollback',json={'revision':response.json()['revision'],'backup':response.json()['saved_backup']},headers=h)
        assert rollback.status_code==200
        assert not json.loads(path.read_text())['backends'][0]['draining']
        assert json.loads(path.read_text())['affinity_secret']=='hidden-live-secret'


@pytest.mark.asyncio
async def test_isolated_benchmark_never_uses_production_router_or_aggregate(tmp_path):
    async with dashboard(tmp_path) as (c,h,app,state,path):
        await app.state.targets['isolated'].sample()
        targets=(await c.get('/api/targets')).json()['targets']
        assert targets[0]['kind']=='direct-test'
        response=await c.post('/api/benchmarks/run',json=bench(target='isolated',backend=''),headers=h)
        assert response.status_code==200
        job=await wait_job(app)
        assert job['state']=='complete' and job['options']['target'].startswith('Isolated test')
        assert app.state.test_counters['requests']==1 and state['requests']==0
        assert {n['name'] for n in (await c.get('/api/state')).json()['gateway']['backends']}=={'a','b'}
        assert (await c.post('/api/benchmarks/run',json=bench(target='http://untrusted'),headers=h)).status_code==400
