#!/usr/bin/env python3
"""Prepare Wall routing using manually supplied certificates."""
import argparse
import ipaddress
import os
import secrets
import shlex
import json
from pathlib import Path
import tempfile
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]

def read_env(path):
    values = {}
    if path.exists():
        for line in path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                value = value.strip()
                if value.startswith(("'", '"')):
                    parsed = shlex.split(value, comments=True, posix=True)
                    if len(parsed) != 1: raise ValueError(f'Invalid quoted setting: {key.strip()}')
                    value = parsed[0]
                values[key.strip()] = value
    return values

def write_env(env, values):
    if env.is_symlink(): raise ValueError('Refusing a symlinked .env.')
    descriptor, temporary = tempfile.mkstemp(prefix='.env-', dir=ROOT)
    try:
        with os.fdopen(descriptor, 'w') as output:
            for key, value in values.items():
                if any(character in value for character in ('\n', '\r')) or ('\"' in value and "'" in value):
                    raise ValueError(f'Unsupported characters in {key}.')
                quote = "'" if '"' in value else '"'
                output.write(f'{key}={quote}{value}{quote}\n')
        os.replace(temporary, env)
    finally:
        if os.path.exists(temporary): os.unlink(temporary)

def storage_settings(values, backend_values, address=None):
    if address is None:
        address = ipaddress.ip_address(backend_values.get('NGINX_BACKEND_IP', '172.23.0.250'))
    subnet = ipaddress.ip_network(backend_values.get('AGORA_SERVICES_SUBNET', '172.23.0.0/16'))
    if address.version != 4 or not address.is_private or address.is_loopback or address.is_unspecified or address.is_multicast:
        raise ValueError('Storage service network requires a private IPv4 proxy address.')
    if address not in subnet:
        raise ValueError('backend-ip must belong to AGORA_SERVICES_SUBNET.')
    values['AGORA_SERVICES_SUBNET'] = str(subnet)
    routes = json.loads(backend_values.get('REDIS_BROKER_ROUTES', '{}'))
    if not isinstance(routes, dict): raise ValueError('Storage routes must be a JSON object.')
    upstream_names = {
        16379: 'STORAGE_REDIS_PRIMARY_UPSTREAM', 16380: 'STORAGE_REDIS_REPLICA_1_UPSTREAM',
        16381: 'STORAGE_REDIS_REPLICA_2_UPSTREAM', 26379: 'STORAGE_SENTINEL_1_UPSTREAM',
        26380: 'STORAGE_SENTINEL_2_UPSTREAM', 26381: 'STORAGE_SENTINEL_3_UPSTREAM',
    }
    found = set()
    for identity, dial in routes.items():
        if not isinstance(dial, dict): raise ValueError("Invalid storage broker route.")
        host, separator, port = identity.rpartition(':')
        ipaddress.ip_address(host)
        if not separator or not port.isdigit() or not 0 < int(port) < 65536:
            raise ValueError('Invalid storage broker source endpoint.')
        broker_port = dial.get('port')
        if dial.get('host') != 'agora-storage-broker' or broker_port not in upstream_names or broker_port in found:
            raise ValueError('Invalid or duplicate storage broker route.')
        found.add(broker_port)
        values[upstream_names[broker_port]] = identity
    if found != set(upstream_names):
        raise ValueError('Run BE scripts/setup-docker.py first to generate all storage broker routes.')
    # Unique storage-side service names avoid resolving this proxy's client-side aliases.
    for key, default in [('STORAGE_MSSQL_UPSTREAM', 'mssql:1433'), ('STORAGE_ES_UPSTREAM', 'elasticsearch:9200')]:
        upstream = values.get(key) or default
        host, separator, port = upstream.rpartition(':')
        if (not separator or not host or not port.isdigit() or not 0 < int(port) < 65536
                or host.strip('[]') in ('agora-storage-broker', 'agora-mssql', 'agora-elasticsearch', 'localhost', '127.0.0.1')):
            raise ValueError(f'{key} must point to a storage-side endpoint, not a broker alias.')
        values[key] = upstream
    values['NGINX_BACKEND_IP'] = str(address)
    if values.get('BROKER_TRUSTED_PROXY_IPS') == '172.21.0.250':
        values['BROKER_TRUSTED_PROXY_IPS'] = str(address)


def sync_storage(backend_dir=None):
    env = ROOT / '.env'
    if not env.is_file(): raise ValueError('Run Wall scripts/setup.py first.')
    backend = Path(backend_dir).expanduser().resolve() if backend_dir else ROOT.parent / 'project-agora-BE'
    values = read_env(env)
    storage_settings(values, read_env(backend / '.env'))
    write_env(env, values)
    print('Wall storage routes synchronized; credentials and certificates unchanged.')

def prepare(tls_dir, public_origin, backend_ip, backend_dir=None, broker_tls_dir=None):
    tls = Path(tls_dir).expanduser().resolve()
    origin = urlsplit(public_origin)
    if origin.scheme != 'https' or not origin.hostname or origin.username or origin.password or origin.query or origin.fragment or origin.path not in ('', '/'):
        raise ValueError('public-origin must be an HTTPS origin without credentials or a path.')
    address = ipaddress.ip_address(backend_ip)
    if address.version != 4 or not address.is_private or address.is_loopback or address.is_unspecified or address.is_multicast:
        raise ValueError('backend-ip must be a private IPv4 address on agora-services.')
    for name in ('fullchain.pem', 'privkey.pem', 'root-ca.pem'):
        if not (tls / name).is_file():
            raise ValueError(f'Missing manually supplied certificate: {tls / name}')
    env = ROOT / '.env'
    if env.is_symlink():
        raise ValueError('Refusing a symlinked .env.')
    values = read_env(env if env.exists() else ROOT / '.env.example')
    backend = Path(backend_dir).expanduser().resolve() if backend_dir else ROOT.parent / 'project-agora-BE'
    backend_values = read_env(backend / '.env')
    token = backend_values.get('CPP_INTERNAL_API_TOKEN') or values.get('CPP_INTERNAL_API_TOKEN', '')
    if len(token.encode()) < 32:
        raise ValueError('Configure CPP_INTERNAL_API_TOKEN in BE or Wall .env first (same value).')
    jwt_secret = backend_values.get('JWT_SECRET') or values.get('JWT_SECRET', '')
    if len(jwt_secret.encode()) < 32:
        raise ValueError('Configure the same JWT_SECRET as Spring first (at least 32 bytes).')
    storage_settings(values, backend_values, address)
    values.update(JWT_SECRET=jwt_secret, BROKER_CPP_IDENTITIES=values.get('BROKER_CPP_IDENTITIES') or '{}')
    broker_tls = Path(broker_tls_dir).expanduser().resolve() if broker_tls_dir else tls
    for name in ('fullchain.pem', 'privkey.pem'):
        if not (broker_tls / name).is_file(): raise ValueError(f'Missing broker certificate: {broker_tls / name}')
    values.update(CPP_INTERNAL_API_TOKEN=token,
                  BROKER_SECRET_KEY_BASE=values.get('BROKER_SECRET_KEY_BASE') or secrets.token_urlsafe(64),
                  BROKER_TLS_CERT_DIR=str(broker_tls),
                  BROKER_TLS_SERVER_NAME='agora-broker' if broker_tls_dir else 'agora-nginx',
                  BROKER_CPP_TARGETS=values.get('BROKER_CPP_TARGETS') or json.dumps({'8002': 'wss://agora-cpp:8002'}, separators=(',', ':')),
                  BROKER_ALLOWED_ORIGINS=public_origin.rstrip('/') + (',https://127.0.0.1:' + str(origin.port or 443) if origin.hostname == 'localhost' else ''))
    values.update(NGINX_TLS_CERT_DIR=str(tls), NGINX_HTTPS_PORT=str(origin.port or 443), NGINX_BACKEND_IP=str(address))
    write_env(env, values)
    print('Wall .env prepared; certificate files were not generated or copied.')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tls-dir')
    parser.add_argument('--sync-storage-only', action='store_true')
    parser.add_argument('--public-origin', default='https://localhost:8443')
    parser.add_argument('--backend-ip', default='172.23.0.250')
    parser.add_argument('--backend-dir')
    parser.add_argument('--broker-tls-dir', help='Optional dedicated broker certificate directory; SAN agora-broker.')
    args = parser.parse_args()
    try:
        if args.sync_storage_only: sync_storage(args.backend_dir)
        elif args.tls_dir: prepare(args.tls_dir, args.public_origin, args.backend_ip, args.backend_dir, args.broker_tls_dir)
        else: parser.error("--tls-dir is required for initial setup")
    except (OSError, ValueError) as error: parser.exit(1, f'Error: {error}\n')
