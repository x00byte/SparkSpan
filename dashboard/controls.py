"""Validated, revision-checked edits to an external SparkSpan settings file."""
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
import time
from urllib.parse import urlsplit, urlunsplit
import uuid


class ControlError(ValueError):
    def __init__(self, message, status=400):
        super().__init__(message)
        self.status = status


def redact(value, key=''):
    if any(part in key.lower() for part in ('secret', 'password', 'token', 'authorization', 'api_key')):
        return '[redacted]'
    if isinstance(value, dict):
        return {k: redact(v, k) for k, v in value.items()}
    if isinstance(value, list):
        return [redact(v, key) for v in value]
    if isinstance(value, str) and key == 'url':
        p = urlsplit(value)
        if p.username or p.password or p.query or p.fragment:
            host = p.hostname or ''
            if ':' in host:
                host = '[' + host + ']'
            return urlunsplit((p.scheme, host + (':' + str(p.port) if p.port else ''), p.path, '', ''))
    return value


def atomic_json(path, value, mode=0o600):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix='.' + path.name + '-', dir=path.parent)
    try:
        os.fchmod(fd, mode)
        with os.fdopen(fd, 'w') as stream:
            json.dump(value, stream, indent=2)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(name):
            os.unlink(name)


class ConfigStore:
    def __init__(self, path, runtime):
        self.path, self.runtime = Path(path), Path(runtime)
        self.runtime.mkdir(parents=True, exist_ok=True)
        self.backups = self.runtime / 'revisions'
        self.backups.mkdir(mode=0o700, exist_ok=True)
        self.drafts = self.runtime / 'drafts'
        self.drafts.mkdir(mode=0o700, exist_ok=True)

    def read(self):
        raw = self.path.read_bytes()
        return json.loads(raw), hashlib.sha256(raw).hexdigest()

    def snapshot(self):
        config, revision = self.read()
        return {'revision': revision, 'config': redact(config),
                'hot_fields': ['backends.capacity', 'backends.draining', 'host_limits'],
                'revisions': self.revisions()}

    def revisions(self):
        return [{'id': p.stem, 'created_at': p.stat().st_mtime} for p in
                sorted(self.backups.glob('*.json'), key=lambda p: p.stat().st_mtime, reverse=True)[:20]]

    @contextmanager
    def lock(self):
        with self.path.with_name('.' + self.path.name + '.dashboard.lock').open('a') as file:
            fcntl.flock(file.fileno(), fcntl.LOCK_EX)
            yield

    def validate_hot(self, config, body):
        if set(body) - {'revision', 'backends', 'host_limits'}:
            raise ControlError('Only capacity, draining and shared host limits can be applied live. Stage identity changes as a draft.')
        nodes = body.get('backends')
        if not isinstance(nodes, list) or len(nodes) != len(config['backends']):
            raise ControlError('Provide exactly one entry for every configured backend.')
        if any(not isinstance(n, dict) or set(n) != {'name', 'capacity', 'draining'} for n in nodes):
            raise ControlError('Each backend edit must contain name, capacity and draining only.')
        by_name = {n['name']: n for n in nodes if isinstance(n.get('name'), str)}
        if len(by_name) != len(nodes) or set(by_name) != {n['name'] for n in config['backends']}:
            raise ControlError('Backend identities changed; reload the configuration before editing.')
        updated = json.loads(json.dumps(config))
        for node in updated['backends']:
            edit = by_name[node['name']]
            cap, maximum = edit['capacity'], node.get('max_capacity', 8)
            if type(cap) is not int or not 1 <= cap <= maximum:
                raise ControlError(f"{node['name']}: capacity must be an integer from 1 to {maximum}.")
            if type(edit['draining']) is not bool:
                raise ControlError('Draining must be true or false.')
            node.update(capacity=cap, draining=edit['draining'])
        limits = body.get('host_limits', config.get('host_limits', {}))
        hosts = {n.get('host') for n in config['backends']} - {None}
        if (not isinstance(limits, dict) or set(limits) - hosts or
                any(type(v) is not int or not 1 <= v <= 1024 for v in limits.values())):
            raise ControlError('Host budgets must name configured hosts and contain integers from 1 to 1024.')
        updated['host_limits'] = limits
        return updated

    def apply(self, body):
        with self.lock():
            config, revision = self.read()
            if body.get('revision') != revision:
                raise ControlError('The settings changed since this page loaded. Reload before applying.', 409)
            updated = self.validate_hot(config, body)
            backup = time.strftime('%Y%m%dT%H%M%S') + '-' + uuid.uuid4().hex[:8]
            atomic_json(self.backups / (backup + '.json'), config)
            atomic_json(self.path, updated, self.path.stat().st_mode & 0o777)
            for old in sorted(self.backups.glob('*.json'), key=lambda p: p.stat().st_mtime, reverse=True)[20:]:
                old.unlink()
        return dict(self.snapshot(), saved_backup=backup, message='Settings saved atomically. Wait for the gateway polling loop to confirm the applied limits.')

    def rollback(self, body):
        backup = body.get('backup', '')
        if not re.fullmatch(r'\d{8}T\d{6}-[0-9a-f]{8}', backup):
            raise ControlError('Invalid revision identifier.')
        path = self.backups / (backup + '.json')
        if not path.is_file():
            raise ControlError('Revision is no longer available.', 404)
        old = json.loads(path.read_text())
        current, _ = self.read()
        identity = lambda c: [{k: v for k, v in n.items() if k not in ('capacity', 'draining')} for n in c['backends']]
        if identity(old) != identity(current):
            raise ControlError('Backend identity changed since this revision. Review a draft instead of restoring it live.', 409)
        return self.apply({'revision': body.get('revision'), 'backends': [
            {'name': n['name'], 'capacity': n.get('capacity', 8), 'draining': n.get('draining', False)}
            for n in old['backends']], 'host_limits': old.get('host_limits', {})})

    def stage(self, body):
        current, revision = self.read()
        if body.get('revision') != revision:
            raise ControlError('Settings changed. Reload before staging a draft.', 409)
        draft = body.get('config')
        if not isinstance(draft, dict) or not isinstance(draft.get('backends'), list) or not draft['backends']:
            raise ControlError('A draft needs a non-empty backends array.')
        names = set()
        for node in draft['backends']:
            if not isinstance(node, dict) or not isinstance(node.get('name'), str) or not node['name'] or node['name'] in names:
                raise ControlError('Draft backend names must be non-empty and unique.')
            names.add(node['name'])
            parts = urlsplit(node.get('url', ''))
            if parts.scheme not in ('http', 'https') or not parts.hostname or parts.username or parts.password or parts.query:
                raise ControlError('Draft URLs must be HTTP(S) addresses without credentials or query strings.')
            cap, maximum = node.get('capacity', 8), node.get('max_capacity', 8)
            if type(cap) is not int or type(maximum) is not int or not 1 <= cap <= maximum <= 1024:
                raise ControlError('Draft capacity must fit its maximum, between 1 and 1024.')
            if type(node.get('draining', False)) is not bool:
                raise ControlError('Draft draining must be a boolean.')
            if 'models' in node and (not isinstance(node['models'], list) or not all(isinstance(x, str) and x for x in node['models'])):
                raise ControlError('Draft models must be a list of non-empty model names.')
        # Carry existing secrets; a sanitized browser export is never a way to overwrite them.
        for key in current:
            if redact(current[key], key) == '[redacted]':
                draft[key] = current[key]
        identifier = time.strftime('%Y%m%dT%H%M%S') + '-' + uuid.uuid4().hex[:8]
        atomic_json(self.drafts / (identifier + '.json'), draft)
        return {'id': identifier, 'config': redact(draft), 'applied': False,
                'message': 'Draft saved locally. Backend identity changes require a reviewed gateway deployment; live routing was not changed.'}
