"""Deployment recovery must preserve serving GPUs and reject image drift."""
import json
from subprocess import CompletedProcess
from unittest.mock import patch

import pytest

import deploy


NODE = {'name': 'a', 'container': 'owned-replica', 'image': 'sha256:expected',
        'url': 'http://a:8888', 'launcher': '/external/launch.py'}


def inventory(image):
    return [CompletedProcess([], 0, 'owned-replica\n'),
            CompletedProcess([], 0, json.dumps([{'Image': image, 'State': {'Running': True}}]))]


def test_healthy_replica_is_adopted_without_model_restart(tmp_path):
    with patch.object(deploy, 'run', side_effect=inventory('sha256:expected')) as run, \
         patch.object(deploy, 'ready', return_value=True):
        deploy.ensure_node(NODE, 'model', tmp_path)
    assert all(call.args[1][1] in ('ps', 'inspect') for call in run.call_args_list)


def test_unexpected_image_is_left_running(tmp_path):
    with patch.object(deploy, 'run', side_effect=inventory('sha256:unexpected')) as run:
        with pytest.raises(RuntimeError, match='left running'):
            deploy.ensure_node(NODE, 'model', tmp_path)
    assert all(call.args[1][1] in ('ps', 'inspect') for call in run.call_args_list)


def test_gateway_recovery_uses_cpu_only_container_and_directory_mount(tmp_path):
    commands = []
    def run(node, args):
        commands.append(args)
        return CompletedProcess(args, 0, '')
    config = {'log_dir': str(tmp_path), 'nodes': [NODE], 'model_alias': 'model',
              'gateway_container': 'owned-gateway', 'gateway_image': 'sha256:gateway',
              'gateway_config': '/secure/settings/config.json', 'gateway_url': 'http://gateway:8890',
              'gateway_cpuset': '0-4,10-14'}
    with patch.object(deploy, 'run', side_effect=run), \
         patch.object(deploy, 'ensure_node'), patch.object(deploy, 'ready', return_value=True):
        deploy.reconcile(config)
    command = commands[-1]
    assert command[:4] == ['docker', 'run', '--cpuset-cpus', '0-4,10-14']
    assert '--gpus' not in command and '--privileged' not in command
    assert '/secure/settings:/settings:ro' in command
    assert 'SPARK_GATEWAY_CONFIG=/settings/config.json' in command


def test_endpoint_redirect_is_enabled_only_after_gateway_ready(tmp_path):
    calls = []
    def run(node, args):
        calls.append(args)
        return CompletedProcess(args, 0, '')
    config = {'log_dir': str(tmp_path), 'nodes': [NODE], 'model_alias': 'model',
              'gateway_container': 'gateway', 'gateway_image': 'image',
              'gateway_config': '/settings/config.json', 'gateway_url': 'http://gateway',
              'endpoint_command': ['sudo', '-n', '/usr/local/sbin/qwen-hybrid-endpoint', 'enable']}
    def ready(url):
        assert calls[-1][-1] == 'image'
        return True
    with patch.object(deploy, 'run', side_effect=run), patch.object(deploy, 'ensure_node'), \
         patch.object(deploy, 'ready', side_effect=ready):
        deploy.reconcile(config)
    assert calls[-1] == config['endpoint_command']
