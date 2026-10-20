"""SparkSpan Dashboard: custom monitor and controls, separate from inference."""
import asyncio
from contextlib import asynccontextmanager, suppress
import hashlib
import hmac
import json
import os
from pathlib import Path
import secrets
import time

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.trustedhost import TrustedHostMiddleware

from .benchmarks import Benchmarks
from .controls import ConfigStore, ControlError
from .metrics import CounterHistory, Monitor, IsolatedMonitor

BASE = Path(__file__).parent


def create_app(settings=None, transport=None):
    @asynccontextmanager
    async def lifespan(app):
        options = settings or json.loads(Path(os.environ['SPARKSPAN_DASHBOARD_CONFIG']).read_text())
        runtime = Path(options['runtime_dir'])
        runtime.mkdir(parents=True, exist_ok=True)
        secret_file = runtime / 'csrf.key'
        if not secret_file.exists():
            fd = os.open(secret_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, 'wb') as stream:
                stream.write(secrets.token_bytes(32))
        app.state.secret = secret_file.read_bytes()
        app.state.options = options
        app.state.store = ConfigStore(options['gateway_config'], runtime)
        app.state.history = CounterHistory(runtime / 'metrics.sqlite3', options.get('poll_seconds', 3))
        async with httpx.AsyncClient(transport=transport, trust_env=False,
                                     limits=httpx.Limits(max_connections=32, max_keepalive_connections=16)) as client:
            app.state.monitor = Monitor(client, options['gateway_url'], app.state.history, options.get('poll_seconds', 3))
            app.state.targets = {}
            for target in options.get('test_targets', []):
                if target.get('kind') != 'direct-test' or not target.get('name'):
                    raise ValueError('Test targets require a name and kind=direct-test')
                if target['name'] in app.state.targets:
                    raise ValueError('Test target names must be unique')
                # Use an opaque filename; target names cannot select a filesystem path.
                key = hashlib.sha256(target['name'].encode()).hexdigest()[:16]
                history = CounterHistory(runtime / ('test-' + key + '.sqlite3'), options.get('poll_seconds', 3))
                app.state.targets[target['name']] = IsolatedMonitor(client, target, history, options.get('poll_seconds', 3))
            app.state.benchmarks = Benchmarks(client, options['gateway_url'], runtime)
            await app.state.monitor.sample()
            pollers = [asyncio.create_task(m.run()) for m in [app.state.monitor, *app.state.targets.values()]]
            try:
                yield
            finally:
                for poller in pollers:
                    poller.cancel()
                await asyncio.gather(*pollers, return_exceptions=True)
                await app.state.benchmarks.cancel()
                app.state.history.close()
                for target in app.state.targets.values():
                    target.history.close()

    app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    host_settings = settings or (json.loads(Path(os.environ['SPARKSPAN_DASHBOARD_CONFIG']).read_text())
                                 if os.environ.get('SPARKSPAN_DASHBOARD_CONFIG') else {})
    allowed = host_settings.get('allowed_hosts', ['127.0.0.1', 'localhost'])
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=allowed)

    @app.middleware('http')
    async def security(request, call_next):
        if request.method in {'POST', 'PUT', 'PATCH', 'DELETE'}:
            origin = request.headers.get('origin')
            expected = request.url.scheme + '://' + request.headers.get('host', '')
            cookie = request.cookies.get('sparkspan_session', '')
            token = request.headers.get('x-csrf-token', '')
            secret = getattr(app.state, 'secret', b'')
            valid = hmac.new(secret, cookie.encode(), hashlib.sha256).hexdigest() if cookie else ''
            if origin != expected or not token or not hmac.compare_digest(valid, token):
                return JSONResponse({'error': 'Same-origin session and CSRF token required.'}, status_code=403)
            if request.headers.get('content-type', '').split(';')[0] != 'application/json':
                return JSONResponse({'error': 'Use application/json.'}, status_code=415)
            length = request.headers.get('content-length')
            if length and (not length.isdigit() or int(length) > 131072):
                return JSONResponse({'error': 'Control request exceeds 128 KiB.'}, status_code=413)
            # Bound chunked requests as well as requests carrying Content-Length.
            data = bytearray()
            async for part in request.stream():
                data.extend(part)
                if len(data) > 131072:
                    return JSONResponse({'error': 'Control request exceeds 128 KiB.'}, status_code=413)
            request._body = bytes(data)
        response = await call_next(request)
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['X-Frame-Options'] = 'DENY'
        response.headers['Referrer-Policy'] = 'no-referrer'
        response.headers['Content-Security-Policy'] = "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'"
        response.headers['Cache-Control'] = 'no-store' if request.url.path.startswith('/api/') else 'no-cache'
        return response

    @app.exception_handler(ControlError)
    async def control_error(request, error):
        return JSONResponse({'error': str(error)}, status_code=error.status)

    async def body(request):
        try:
            data = await request.json()
            if not isinstance(data, dict):
                raise ValueError()
            return data
        except ValueError:
            raise ControlError('Expected a JSON object.')

    @app.get('/')
    async def index():
        return FileResponse(BASE / 'static/index.html')

    @app.get('/static/hero.png', include_in_schema=False)
    async def hero():
        return FileResponse(BASE.parent / 'assets/hero.png')

    @app.get('/health')
    async def health():
        return {'service': 'sparkspan-dashboard', 'status': 'running', 'port': 11002}

    @app.get('/api/session')
    async def session(request: Request):
        cookie = request.cookies.get('sparkspan_session') or secrets.token_hex(32)
        token = hmac.new(app.state.secret, cookie.encode(), hashlib.sha256).hexdigest()
        response = JSONResponse({'csrf_token': token})
        response.set_cookie('sparkspan_session', cookie, httponly=True, samesite='strict', max_age=86400,
                            secure=request.url.scheme == 'https')
        return response

    @app.get('/api/state')
    async def state():
        return app.state.monitor.snapshot()

    @app.get('/api/events')
    async def events(request: Request):
        async def generate():
            seen = -1
            while not await request.is_disconnected():
                monitor = app.state.monitor
                if seen != monitor.sequence:
                    seen = monitor.sequence
                    yield 'event: state\ndata: ' + json.dumps(monitor.snapshot(), allow_nan=False) + '\n\n'
                async with monitor.changed:
                    try:
                        await asyncio.wait_for(monitor.changed.wait(), timeout=12)
                    except asyncio.TimeoutError:
                        yield ': keepalive\n\n'
        return StreamingResponse(generate(), media_type='text/event-stream',
                                 headers={'X-Accel-Buffering': 'no', 'Cache-Control': 'no-cache'})

    @app.get('/api/history')
    async def history(seconds: int = 3600):
        if seconds not in (60, 300, 900, 3600, 86400):
            raise ControlError('Choose a 60, 300, 900, 3600 or 86400 second window.')
        rows = app.state.history.rows
        end = rows[-1]['end'] if rows else time.time()
        return {'rows': [r for r in rows if r['end'] >= end - seconds], 'window_seconds': seconds}

    @app.get('/api/config')
    async def config():
        return app.state.store.snapshot()

    @app.get('/api/targets')
    async def targets():
        return {'targets': [{'name': name, 'label': target.target.get('label', name),
                             'kind': 'direct-test', 'snapshot': target.snapshot()}
                            for name, target in app.state.targets.items()]}

    @app.post('/api/config/apply')
    async def apply(request: Request):
        return app.state.store.apply(await body(request))

    @app.post('/api/config/rollback')
    async def rollback(request: Request):
        return app.state.store.rollback(await body(request))

    @app.post('/api/config/draft')
    async def draft(request: Request):
        return app.state.store.stage(await body(request))

    @app.get('/api/benchmarks')
    async def benchmark_list():
        return {'active': app.state.benchmarks.job, 'history': app.state.benchmarks.history()}

    @app.post('/api/benchmarks/run')
    async def benchmark_run(request: Request):
        data = await body(request)
        choice = data.get('target')
        if not isinstance(choice, str) or not choice:
            raise ControlError('Choose a benchmark target explicitly; production is never the default.')
        if choice == 'gateway':
            monitor = app.state.monitor
            label = 'Production gateway'
        else:
            monitor = app.state.targets.get(choice)
            if not monitor:
                raise ControlError('Unknown benchmark target. Targets must be configured locally.')
            if data.get('backend'):
                raise ControlError('Isolated tests go directly to their configured engine; select automatic routing.')
            label = 'Isolated test · ' + monitor.target.get('label', choice)
        if monitor.error:
            raise ControlError('The selected target is unavailable; benchmark not started.', 503)
        return app.state.benchmarks.start(data, [m['id'] for m in monitor.models], monitor.status,
                                          destination=monitor.gateway, target_label=label)

    @app.post('/api/benchmarks/cancel')
    async def benchmark_cancel(request: Request):
        await body(request)
        return {'active': await app.state.benchmarks.cancel()}

    @app.get('/api/benchmarks/{identifier}/export')
    async def benchmark_export(identifier: str, format: str = 'json'):
        job = app.state.benchmarks.get(identifier)
        if format not in ('json', 'csv'):
            raise ControlError('Export format must be json or csv.')
        content = json.dumps(job, indent=2) if format == 'json' else app.state.benchmarks.csv(job)
        return Response(content, media_type='application/json' if format == 'json' else 'text/csv',
                        headers={'Content-Disposition': f'attachment; filename="sparkspan-{identifier}.{format}"'})

    app.mount('/static', StaticFiles(directory=BASE / 'static'), name='static')
    return app


app = create_app()
