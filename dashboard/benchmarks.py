"""Explicit, bounded benchmark jobs; no engine calls occur until start()."""
import asyncio
from contextlib import suppress
import csv
import hashlib
import io
import json
from pathlib import Path
import statistics
import time
import uuid

import httpx
from .controls import atomic_json, ControlError


class Benchmarks:
    def __init__(self, client, gateway, runtime):
        self.client, self.gateway = client, gateway.rstrip('/')
        self.directory = Path(runtime) / 'benchmarks'
        self.directory.mkdir(parents=True, exist_ok=True)
        self.job, self.task = None, None

    def history(self):
        results = []
        for path in sorted(self.directory.glob('*.json'), reverse=True)[:50]:
            with suppress(ValueError, OSError):
                results.append(json.loads(path.read_text()))
        return results

    def validate(self, body, available_models, status):
        if body.get('confirmed') is not True:
            raise ControlError('Confirm that this benchmark sends real inference requests.')
        if body.get('model') not in available_models:
            raise ControlError('Choose a model currently advertised by the gateway.')
        if body.get('concurrency') not in (1, 6, 12) or type(body.get('concurrency')) is not int:
            raise ControlError('Concurrency must be 1, 6 or 12.')
        count, tokens = body.get('requests'), body.get('max_tokens')
        if type(count) is not int or not 1 <= count <= 48:
            raise ControlError('Request count must be from 1 to 48.')
        if type(tokens) is not int or not 1 <= tokens <= 1024 or count * tokens > 24576:
            raise ControlError('Limit output to 1–1024 tokens per request and 24,576 tokens per job.')
        if type(body.get('stream')) is not bool:
            raise ControlError('Choose streaming or non-streaming explicitly.')
        prompt = body.get('prompt')
        if not isinstance(prompt, str) or not prompt.strip() or len(prompt) > 16000:
            raise ControlError('Prompt must contain from 1 to 16,000 characters.')
        temperature = body.get('temperature', 0)
        if type(temperature) not in (int, float) or not 0 <= temperature <= 2:
            raise ControlError('Temperature must be from 0 to 2.')
        target = body.get('backend') or None
        if target:
            node = next((n for n in status.get('backends', []) if n['name'] == target), None)
            if not node or not node.get('accepting') or body['model'] not in node.get('models', []):
                raise ControlError('That backend is not accepting this model. Benchmarks never bypass draining.')
        return {'model': body['model'], 'backend': target, 'concurrency': body['concurrency'],
                'requests': count, 'max_tokens': tokens, 'stream': body['stream'], 'temperature': temperature,
                'prompt_characters': len(prompt), 'prompt_sha256': hashlib.sha256(prompt.encode()).hexdigest()}

    def start(self, body, available_models, status, destination=None, target_label='Production gateway'):
        if self.task and not self.task.done():
            raise ControlError('A benchmark is already running. Cancel it or wait for it to finish.', 409)
        options = self.validate(body, available_models, status)
        options['target'] = target_label
        identifier = time.strftime('%Y%m%dT%H%M%S') + '-' + uuid.uuid4().hex[:8]
        self.job = {'id': identifier, 'state': 'running', 'started_at': time.time(), 'ended_at': None,
                    'options': options, 'results': [], 'progress': 0, 'summary': {}}
        self.task = asyncio.create_task(self.run(self.job, body['prompt'], destination or self.gateway))
        return self.job

    async def one(self, options, prompt, index, destination):
        began = time.monotonic()
        first = None
        usage = {}
        record = {'index': index, 'status': 'running', 'backend': None, 'output_tokens': None,
                  'ttft_ms': None, 'elapsed_s': None}
        headers = {'X-Conversation-ID': 'benchmark-' + uuid.uuid4().hex}
        if options['backend']:
            headers['X-Spark-Node'] = options['backend']
        payload = {'model': options['model'], 'messages': [{'role': 'user', 'content': prompt}],
                   'stream': options['stream'], 'max_tokens': options['max_tokens'],
                   'temperature': options['temperature'], 'chat_template_kwargs': {'enable_thinking': False}}
        if options['stream']:
            payload['stream_options'] = {'include_usage': True}
        try:
            async with asyncio.timeout(300):
                async with self.client.stream('POST', destination + '/v1/chat/completions', json=payload,
                                              headers=headers, timeout=httpx.Timeout(300, connect=5)) as response:
                    record['backend'] = response.headers.get('x-spark-node')
                    record['http_status'] = response.status_code
                    if response.status_code != 200:
                        # Never persist backend error bodies that may echo a private prompt.
                        record['status'] = 'error'
                        record['error'] = 'Upstream returned HTTP ' + str(response.status_code)
                        return record
                    if options['stream']:
                        done = False
                        async for line in response.aiter_lines():
                            if not line.startswith('data:'):
                                continue
                            data = line[5:].strip()
                            if data == '[DONE]':
                                done = True
                                break
                            try:
                                event = json.loads(data)
                            except ValueError:
                                continue
                            if event.get('error'):
                                raise ValueError('The upstream stream reported an error')
                            if event.get('usage'):
                                usage = event['usage']
                            for choice in event.get('choices', []):
                                delta = choice.get('delta', {})
                                if choice.get('text') or any(delta.get(k) for k in ['content', 'reasoning', 'reasoning_content', 'tool_calls']):
                                    if first is None:
                                        first = time.monotonic()
                        if not done:
                            raise ValueError('Stream ended without a completion marker')
                    else:
                        data = await response.aread()
                        usage = json.loads(data).get('usage', {})
                    record['status'] = 'complete'
                    count = usage.get('completion_tokens')
                    record['output_tokens'] = count if type(count) is int and count >= 0 else None
                    record['input_tokens'] = usage.get('prompt_tokens')
        except asyncio.CancelledError:
            record['status'] = 'cancelled'
            raise
        except (httpx.HTTPError, ValueError, TimeoutError) as error:
            record['status'] = 'error'
            record['error'] = type(error).__name__
        finally:
            record['elapsed_s'] = time.monotonic() - began
            record['ttft_ms'] = (first - began) * 1000 if first is not None else None
        return record

    async def run(self, job, prompt, destination):
        began = time.monotonic()
        semaphore = asyncio.Semaphore(job['options']['concurrency'])
        async def perform(index):
            async with semaphore:
                record = await self.one(job['options'], prompt, index, destination)
                job['results'].append(record)
                job['progress'] += 1
                self.summarize(job, time.monotonic() - began)
        tasks = [asyncio.create_task(perform(i)) for i in range(job['options']['requests'])]
        try:
            await asyncio.gather(*tasks)
            job['state'] = 'complete'
        except asyncio.CancelledError:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            job['state'] = 'cancelled'
        finally:
            job['ended_at'] = time.time()
            self.summarize(job, time.monotonic() - began)
            atomic_json(self.directory / (job['id'] + '.json'), job)

    @staticmethod
    def summarize(job, duration):
        finished = [r for r in job['results'] if r['status'] == 'complete']
        known = [r for r in finished if r['output_tokens'] is not None]
        tokens = sum(r['output_tokens'] for r in known)
        ttft = [r['ttft_ms'] for r in finished if r['ttft_ms'] is not None]
        exact = job['state'] == 'complete' and len(known) == job['options']['requests']
        job['summary'] = {'elapsed_s': duration, 'completed': len(finished),
                          'errors': sum(r['status'] == 'error' for r in job['results']),
                          'output_tokens': tokens, 'usage_missing': len(finished) - len(known),
                          'aggregate_output_tps': tokens / duration if duration and exact else None,
                          'partial': not exact,
                          'mean_ttft_ms': statistics.mean(ttft) if ttft else None,
                          'mean_e2e_s': statistics.mean(r['elapsed_s'] for r in finished) if finished else None}

    async def cancel(self):
        if self.task and not self.task.done():
            self.task.cancel()
            await self.task
        return self.job

    def get(self, identifier):
        if self.job and self.job['id'] == identifier:
            return self.job
        if not isinstance(identifier, str) or len(identifier) > 40 or not all(c.isalnum() or c in '-T' for c in identifier):
            raise ControlError('Invalid benchmark identifier.')
        path = self.directory / (identifier + '.json')
        if not path.exists():
            raise ControlError('Benchmark not found.', 404)
        return json.loads(path.read_text())

    def csv(self, job):
        output = io.StringIO()
        fields = ['index', 'status', 'backend', 'http_status', 'output_tokens', 'input_tokens', 'ttft_ms', 'elapsed_s', 'error']
        writer = csv.DictWriter(output, fields, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(job['results'])
        return output.getvalue()
