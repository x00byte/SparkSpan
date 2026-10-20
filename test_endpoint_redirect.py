"""The privileged helper must leave unrelated firewall state untouched."""
import io
import json
from subprocess import CompletedProcess
from unittest.mock import patch

import pytest

import endpoint_redirect as endpoint


def test_enable_checks_gateway_before_applying_atomic_rules():
    status = {'mode': 'independent-replicas', 'capacity': 12}
    calls = []
    def run(args, **kwargs):
        calls.append((args, kwargs))
        return CompletedProcess(args, 0, '', '')
    with patch.object(endpoint.subprocess, 'run', side_effect=run), \
         patch.object(endpoint.urllib.request, 'urlopen', return_value=io.BytesIO(json.dumps(status).encode())):
        endpoint.main('enable')
    assert calls[1][0] == [endpoint.NFT, '--check', '-f', '-']
    rules = calls[2][1]['input']
    assert rules.startswith('delete table ip qwen_hybrid_endpoint\n')
    assert 'flush ruleset' not in rules
    assert 'ip daddr != 127.0.0.2' in rules
    assert 'hook output' in rules and 'hook prerouting' in rules


def test_unready_gateway_does_not_change_routing():
    with patch.object(endpoint, 'exists', return_value=True), \
         patch.object(endpoint.subprocess, 'run') as run, \
         patch.object(endpoint.urllib.request, 'urlopen', return_value=io.BytesIO(b'{"capacity":0}')):
        with pytest.raises(RuntimeError, match='left unchanged'):
            endpoint.main('enable')
    assert run.call_count == 1
    assert '--check' in run.call_args.args[0]


def test_disable_deletes_only_owned_table_and_is_idempotent():
    with patch.object(endpoint, 'exists', return_value=True), patch.object(endpoint.subprocess, 'run') as run:
        endpoint.main('disable')
    run.assert_called_once_with([endpoint.NFT, 'delete', 'table', 'ip', endpoint.TABLE], check=True)
    with patch.object(endpoint, 'exists', return_value=False), patch.object(endpoint.subprocess, 'run') as run:
        endpoint.main('disable')
    run.assert_not_called()
