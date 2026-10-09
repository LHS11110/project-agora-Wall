#!/usr/bin/env python3
"""Validate the gateway Compose mounts and Docker network without printing secrets."""
import importlib.util
import ipaddress
import json
import os
from pathlib import Path
import subprocess
import sys
ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('wall_setup', ROOT / 'scripts/setup.py')
setup = importlib.util.module_from_spec(spec)
spec.loader.exec_module(setup)
def validate():
    if not (ROOT / '.env').is_file(): raise ValueError('Run scripts/setup.py first.')
    values = {**setup.read_env(ROOT / '.env'), **os.environ}
    if values.get('FRONTEND_MODE', 'production') not in ('development', 'production'):
        raise ValueError('FRONTEND_MODE must be development or production.')
    result = subprocess.run(['docker', 'compose', '--project-directory', str(ROOT), '--env-file', str(ROOT / '.env'), '-f', str(ROOT / 'docker-compose.nginx.yml'), 'config', '--format', 'json'], env=values, capture_output=True, text=True, timeout=20)
    if result.returncode: raise ValueError('Invalid gateway Compose configuration; review .env.')
    config = json.loads(result.stdout)
    for mount in config['services']['nginx']['volumes']:
        if mount['type'] == 'bind' and not Path(mount['source']).is_file():
            raise ValueError(f'Missing bind file for {mount["target"]}.')
    network = subprocess.run(['docker', 'network', 'inspect', 'agora-services'], capture_output=True, text=True, timeout=10)
    if network.returncode: raise ValueError('Start Wall storage-broker first to create agora-services.')
    network = json.loads(network.stdout)[0]
    address = ipaddress.ip_address(values.get('NGINX_BACKEND_IP', '172.23.0.250'))
    pools = [ipaddress.ip_network(pool['Subnet']) for pool in network['IPAM']['Config'] if pool.get('Subnet')]
    if not any(address in pool and address not in (pool.network_address, pool.broadcast_address) for pool in pools):
        raise ValueError('NGINX_BACKEND_IP is outside the agora-services subnet.')
    for container in network.get('Containers', {}).values():
        if container.get('IPv4Address', '').split('/')[0] == str(address) and container.get('Name') != 'agora-nginx':
            raise ValueError('NGINX_BACKEND_IP is occupied by another container.')
    print('Gateway mode, bind files and network validated; values hidden.')
if __name__ == '__main__':
    try: validate()
    except (OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
        print(f'Error: {error}', file=sys.stderr); sys.exit(1)
