#!/usr/bin/env python3
"""Read-only Docker integration probe; never restarts the deployed services."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]


def run(arguments, **kwargs):
    return subprocess.run(arguments, check=True, **kwargs)


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def probe(args):
    backend = args.backend_dir.resolve()
    reader = load_module('backend_env', backend / 'scripts/sync-docker-env.py')
    values = reader.read_env(backend / '.env')
    result = run(['docker', 'compose', '--env-file', str(ROOT / '.env'), '-f',
                  str(ROOT / 'docker-compose.storage-broker.yml'), 'config', '--format', 'json'],
                 capture_output=True, text=True)
    config = json.loads(result.stdout)['services']['storage-broker']
    suffix = uuid.uuid4().hex[:8]
    network, broker = 'agora-storage-probe-' + suffix, 'agora-storage-proxy-' + suffix
    network_created = broker_created = False
    try:
        run(['docker', 'network', 'create', '--internal', network], stdout=subprocess.DEVNULL)
        network_created = True
        with tempfile.TemporaryDirectory(prefix='agora-storage-probe-') as directory:
            directory = Path(directory)
            proxy_env = directory / 'proxy.env'
            reader.atomic_write_owner_only(proxy_env, ''.join(
                f'{key}={value}\n' for key, value in config['environment'].items()))
            run(['docker', 'create', '--name', broker, '--network', network,
                 '--network-alias', 'agora-storage-broker', '--network-alias', 'agora-mssql',
                 '--network-alias', 'agora-elasticsearch', '--read-only', '--cap-drop', 'ALL',
                 '--security-opt', 'no-new-privileges:true', '--env-file', str(proxy_env),
                 '-v', str(ROOT / 'storage-broker/haproxy.cfg') + ':/usr/local/etc/haproxy/haproxy.cfg:ro',
                 config['image']], stdout=subprocess.DEVNULL)
            broker_created = True
            for storage_network in ['agora-net', 'agora-redis-ha']:
                run(['docker', 'network', 'connect', storage_network, broker])
            run(['docker', 'start', broker], stdout=subprocess.DEVNULL)
            time.sleep(3)
            run(['docker', 'exec', broker, 'haproxy', '-c', '-f', '/usr/local/etc/haproxy/haproxy.cfg'])
            client_env = directory / 'client.env'
            client_values = {key: values[key] for key in [
                'DB_NAME', 'DB_USER', 'DB_PASSWORD', 'ES_INDEX', 'ES_USER_NAME', 'ES_USER_PASSWORD',
                'REDIS_USER', 'REDIS_USER_PASSWORD', 'REDIS_SENTINELS', 'REDIS_SENTINEL_MASTER_NAME',
                'REDIS_SENTINEL_USER', 'REDIS_SENTINEL_PASSWORD', 'REDIS_BROKER_ROUTES']}
            client_values.update(DB_HOST='agora-mssql', ES_HOST='agora-elasticsearch', ES_SCHEME='https',
                                 DB_ENCRYPT='true', DB_TRUST_SERVER_CERTIFICATE='false',
                                 DB_FREETDS_CONF='/etc/freetds/freetds.conf', REDIS_TLS_ENABLED='true',
                                 ES_CA_CERT='/certs/es.crt', REDIS_TLS_CA_CERT='/certs/redis.crt')
            reader.atomic_write_owner_only(client_env, ''.join(
                f'{key}={value}\n' for key, value in client_values.items()))
            command = ['docker', 'run', '--rm', '--network', network, '--env-file', str(client_env),
                       '-v', args.cpp_build_volume + ':/tmp/agora-layer-build:ro',
                       '-v', str(backend / 'cpp/config/freetds-docker-tls.conf') + ':/etc/freetds/freetds.conf:ro']
            for key, target in [('SQL_TLS_CA_CERT', '/run/certs/sql-ca.crt'),
                                ('ES_CA_CERT', '/certs/es.crt'), ('REDIS_TLS_CA_CERT', '/certs/redis.crt')]:
                command += ['-v', values[key] + ':' + target + ':ro']
            run(command + [args.cpp_test_image, '/tmp/agora-layer-build/agora_cpp_storage_broker_fixture'])
            direct = []
            for name in ['agora-mssql', 'agora-elasticsearch']:
                obj = json.loads(run(['docker', 'inspect', name], capture_output=True, text=True).stdout)[0]
                address = obj['NetworkSettings']['Networks']['agora-net']['IPAddress']
                direct.append([address, 1433 if name == 'agora-mssql' else 9200])
            for identity in json.loads(values['REDIS_BROKER_ROUTES']):
                host, port = identity.rsplit(':', 1)
                direct.append([host, int(port)])
            script = '''import socket,json,sys
for host,port in json.loads(sys.argv[1]):
    try:
        with socket.create_connection((host,port),timeout=.4): pass
    except OSError: continue
    raise RuntimeError('Client bypassed storage broker')
assert socket.gethostbyname('agora-mssql') == socket.gethostbyname('agora-storage-broker')
assert socket.gethostbyname('agora-elasticsearch') == socket.gethostbyname('agora-storage-broker')
print('All direct storage connections blocked; TLS identities resolve to Wall')
'''
            run(['docker', 'run', '--rm', '--network', network, 'python:3.14-alpine', 'python3', '-c', script, json.dumps(direct)])
    finally:
        if broker_created:
            subprocess.run(['docker', 'rm', '-f', broker], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if network_created:
            subprocess.run(['docker', 'network', 'rm', network], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--backend-dir', type=Path, default=ROOT.parent / 'project-agora-BE')
    parser.add_argument('--cpp-test-image', required=True, help='Build environment image with the C++ runtime libraries.')
    parser.add_argument('--cpp-build-volume', required=True, help='Volume with the BUILD_TESTING=ON CMake build.')
    probe(parser.parse_args())
