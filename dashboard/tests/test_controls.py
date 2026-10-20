import json
from pathlib import Path
import pytest
from dashboard.controls import ConfigStore, ControlError, redact


@pytest.fixture
def store(tmp_path):
    p=tmp_path/'gateway/config.json';p.parent.mkdir()
    p.write_text(json.dumps({'affinity_secret':'very-private-secret','backends':[
        {'name':'a','url':'http://a:8000','capacity':6,'host':'host-a'},
        {'name':'b','url':'http://b:8000','capacity':6,'host':'host-b'}],
        'host_limits':{'host-a':6,'host-b':6}}))
    return ConfigStore(p,tmp_path/'state')


def edit(store):
    config,revision=store.read()
    return {'revision':revision,'backends':[{'name':n['name'],'capacity':n['capacity'],'draining':n.get('draining',False)} for n in config['backends']],
            'host_limits':config['host_limits']}


def test_atomic_edit_revision_conflict_and_rollback_preserve_secret(store):
    original_mode=store.path.stat().st_mode & 0o777
    old=edit(store);body=edit(store);body['backends'][1]['draining']=True
    response=store.apply(body)
    assert store.read()[0]['backends'][1]['draining']
    assert store.read()[0]['affinity_secret']=='very-private-secret'
    assert 'very-private-secret' not in json.dumps(response)
    with pytest.raises(ControlError,match='changed'):store.apply(old)
    restored=store.rollback({'revision':response['revision'],'backup':response['saved_backup']})
    assert not restored['config']['backends'][1].get('draining',False)
    assert store.path.stat().st_mode & 0o777 == original_mode


@pytest.mark.parametrize('change',[{'capacity':0},{'capacity':99},{'capacity':True},{'draining':'true'},{'url':'http://evil'}])
def test_invalid_live_edit_does_not_change_file(store,change):
    before=store.path.read_bytes();body=edit(store);body['backends'][0].update(change)
    with pytest.raises(ControlError):store.apply(body)
    assert store.path.read_bytes()==before


def test_bad_host_and_identity_changes_require_draft(store):
    body=edit(store);body['host_limits']['unknown']=6
    with pytest.raises(ControlError):store.apply(body)
    body=edit(store);body['backends'][0]['name']='new'
    with pytest.raises(ControlError):store.apply(body)
    before=store.path.read_bytes()
    snap=store.snapshot();snap['config']['backends'][0]['models']=['different-model']
    result=store.stage({'revision':snap['revision'],'config':snap['config']})
    assert result['applied'] is False and store.path.read_bytes()==before
    assert 'very-private-secret' not in json.dumps(result)
    assert json.loads(next(store.drafts.glob('*.json')).read_text())['affinity_secret']=='very-private-secret'


def test_redaction_includes_nested_secrets_and_url_credentials():
    out=redact({'affinity_secret':'shh','nested':{'api_key':'shh'},'backends':[{'url':'http://user:pass@server:123/v1?token=shh'}]})
    assert out['backends'][0]['url']=='http://server:123/v1'
    assert 'shh' not in json.dumps(out) and 'pass@' not in json.dumps(out)
