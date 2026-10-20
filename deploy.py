"""Reconcile only this gateway and its explicitly named replicas.

Healthy replicas are adopted without stopping them. Machine configuration and
model launch scripts live outside this repository.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import shlex
import subprocess
import time
import urllib.request


def run(node, args, **kwargs):
    if node.get('ssh'):
        args = node['ssh'] + [shlex.join(args)]
    return subprocess.run(args, check=True, text=True, capture_output=True, **kwargs)


def ready(url, alias=None):
    try:
        with urllib.request.urlopen(url + ('/v1/models' if alias else '/ready'), timeout=4) as r:
            payload = json.load(r)
        return not alias or alias in {x['id'] for x in payload['data']}
    except (OSError, ValueError, KeyError):
        return False


def validate(config):
    local = config['nodes'][0]
    run(local, ['docker', 'image', 'inspect', config['gateway_image']])
    for node in config['nodes']:
        result = run(node, ['python3', node['launcher'], '--validate-assets'])
        print(node['name'] + ': ' + result.stdout.strip(), flush=True)


def ensure_node(node, alias, log_dir):
    result = run(node, ['docker', 'ps', '-a', '--format', '{{.Names}}'])
    exists = node['container'] in result.stdout.splitlines()
    if exists:
        data = json.loads(run(node, ['docker', 'inspect', node['container']]).stdout)[0]
        if data['State']['Running']:
            if data['Image'] != node['image']:
                raise RuntimeError(node['name'] + ': unexpected replica image; left running')
            deadline = time.monotonic() + 2400
            while time.monotonic() < deadline:
                if ready(node['url'], alias):
                    print(node['name'] + ': adopted healthy replica', flush=True)
                    return
                time.sleep(5)
            raise RuntimeError(node['name'] + ': replica did not become ready')
        run(node, ['docker', 'rm', node['container']])
    command = ['python3', node['launcher']]
    if node.get('ssh'):
        command = node['ssh'] + [shlex.join(command)]
    with (log_dir / (node['name'] + '-launch.log')).open('a') as log:
        subprocess.run(command, check=True, stdout=log, stderr=subprocess.STDOUT, timeout=2500)
    if not ready(node['url'], alias):
        raise RuntimeError(node['name'] + ': expected model is not ready')
    print(node['name'] + ': started replica', flush=True)


def reconcile(config):
    log_dir = Path(config['log_dir'])
    log_dir.mkdir(parents=True, exist_ok=True)
    with ThreadPoolExecutor(max_workers=len(config['nodes'])) as pool:
        futures = [pool.submit(ensure_node, n, config['model_alias'], log_dir) for n in config['nodes']]
        for future in futures:
            future.result()
    local, name = config['nodes'][0], config['gateway_container']
    exists = name in run(local, ['docker', 'ps', '-a', '--format', '{{.Names}}']).stdout.splitlines()
    if exists:
        data = json.loads(run(local, ['docker', 'inspect', name]).stdout)[0]
        if data['Image'] != config['gateway_image']:
            raise RuntimeError('Unexpected gateway image; left running')
        if not data['State']['Running']:
            run(local, ['docker', 'start', name])
    else:
        command = ['docker', 'run', '-d', '--name', name, '--network', 'host', '--user', '1000:1000',
                   '--read-only', '--tmpfs', '/tmp:size=32m', '--cap-drop', 'ALL',
                   '--security-opt', 'no-new-privileges', '--memory', '768m', '--cpus', '1',
                   '--restart', 'unless-stopped', '--log-opt', 'max-size=20m', '--log-opt', 'max-file=3',
                   '-v', str(Path(config['gateway_config']).parent) + ':/settings:ro',
                   '-e', 'SPARK_GATEWAY_CONFIG=/settings/' + Path(config['gateway_config']).name,
                   config['gateway_image']]
        if config.get('gateway_cpuset'):
            command[2:2] = ['--cpuset-cpus', config['gateway_cpuset']]
        if config.get('gateway_listen_port'):
            command += ['uvicorn', 'gateway:app', '--host', '0.0.0.0', '--port',
                        str(config['gateway_listen_port']), '--no-access-log', '--no-proxy-headers',
                        '--timeout-graceful-shutdown', '300']
        run(local, command)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if ready(config['gateway_url']):
            if config.get('endpoint_command'):
                result = run(local, config['endpoint_command'])
                print(result.stdout.strip(), flush=True)
            print('All configured model servers and the gateway are ready: ' + config['gateway_url'] + '/v1', flush=True)
            return
        time.sleep(1)
    raise RuntimeError('Gateway did not become ready')


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--deployment', required=True)
    p.add_argument('--validate-assets', action='store_true')
    args = p.parse_args()
    config = json.loads(Path(args.deployment).read_text())
    validate(config)
    if not args.validate_assets:
        reconcile(config)
