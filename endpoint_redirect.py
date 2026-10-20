#!/usr/bin/python3 -I
"""Scoped root helper: keep the hybrid test on the existing IPv4 API port.

Install root-owned as /usr/local/sbin/qwen-hybrid-endpoint. No caller-supplied
addresses, ports or rule files are accepted. Only this helper's table is changed.
"""
import json
import re
import subprocess
import sys
import urllib.request

TABLE = 'qwen_hybrid_endpoint'
NFT = '/usr/sbin/nft'
RULES = '''table ip qwen_hybrid_endpoint {
 chain ingress {
  type nat hook prerouting priority -105; policy accept;
  fib daddr type local tcp dport 8888 redirect to :8890
 }
 chain local {
  type nat hook output priority -105; policy accept;
  ip daddr != 127.0.0.2 fib daddr type local tcp dport 8888 redirect to :8890
 }
}
'''


def exists():
    result = subprocess.run([NFT, 'list', 'table', 'ip', TABLE], capture_output=True, text=True)
    if result.returncode and 'No such file or directory' not in result.stderr:
        raise RuntimeError(result.stderr.strip())
    return result.returncode == 0


def ruleset(present, port=8890):
    if port not in (8890, 8891):
        raise ValueError('Only the two fixed gateway ports are supported')
    return (f'delete table ip {TABLE}\n' if present else '') + RULES.replace(':8890', ':' + str(port))


def main(action):
    present = exists()
    if action == 'status':
        port = None
        if present:
            listing = subprocess.run([NFT, 'list', 'table', 'ip', TABLE], check=True, capture_output=True, text=True)
            match = re.search(r'redirect to :(889[01])\b', listing.stdout)
            port = int(match[1]) if match else None
        print(json.dumps({'enabled': present, 'public_port': 8888, 'gateway_port': port,
                          'direct_head_backend': '127.0.0.2:8888'}))
        return
    if action == 'disable':
        if present:
            subprocess.run([NFT, 'delete', 'table', 'ip', TABLE], check=True)
        return
    if action not in ('enable', 'enable-next', 'check'):
        raise ValueError('Usage: qwen-hybrid-endpoint {enable|enable-next|disable|status|check}')
    port = 8891 if action == 'enable-next' else 8890
    script = ruleset(present, port)
    subprocess.run([NFT, '--check', '-f', '-'], input=script, text=True, check=True)
    if action == 'check':
        return
    # Never redirect the production port to an unready gateway.
    with urllib.request.urlopen(f'http://127.0.0.1:{port}/ready', timeout=5) as response:
        status = json.load(response)
    if status.get('mode') != 'independent-replicas' or status.get('capacity', 0) <= 0:
        raise RuntimeError('Hybrid gateway is not ready; production routing left unchanged')
    subprocess.run([NFT, '-f', '-'], input=script, text=True, check=True)
    print('Port 8888 now routes new IPv4 connections through the hybrid gateway')


if __name__ == '__main__':
    try:
        if len(sys.argv) != 2:
            raise ValueError('Expected exactly one action')
        main(sys.argv[1])
    except (OSError, ValueError, RuntimeError, subprocess.CalledProcessError) as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1)
