#!/usr/bin/env python3
"""Prepare an isolated, authenticated Wall TURN relay without printing secrets."""
import json,os,secrets,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parent))
from setup import read_env, write_env
root=Path(__file__).resolve().parents[1]
def main():
    values=read_env(root/'.env')
    values['MEDIA_RELAY_SECRET']=values.get('MEDIA_RELAY_SECRET') or secrets.token_urlsafe(48)
    values['MEDIA_RELAY_URLS']=values.get('MEDIA_RELAY_URLS') or json.dumps(['turns:frelog.p-e.kr:443?transport=tcp'],separators=(',',':'))
    values['MEDIA_RELAY_CERT_DIR']=values.get('MEDIA_RELAY_CERT_DIR') or ('/etc/letsencrypt/live/frelog.p-e.kr' if Path('/etc/letsencrypt/live/frelog.p-e.kr').exists() else values.get('NGINX_TLS_CERT_DIR',''))
    cert_dir=Path(values['MEDIA_RELAY_CERT_DIR'])
    if not all((cert_dir/name).is_file() for name in ('fullchain.pem','privkey.pem')):
        raise ValueError('MEDIA_RELAY_CERT_DIR must contain fullchain.pem and privkey.pem')
    secret=values['MEDIA_RELAY_SECRET']
    if len(secret)<32 or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-' for c in secret):
        raise ValueError('MEDIA_RELAY_SECRET requires 32+ URL-safe characters')
    write_env(root/'.env',values)
    folder=root/'.local-media';folder.mkdir(mode=0o700,exist_ok=True)
    config='''listening-port=3478
tls-listening-port=5349
listening-ip=10.42.0.2
relay-ip=10.42.0.2
min-port=49160
max-port=50159
use-auth-secret
static-auth-secret={secret}
realm=frelog.p-e.kr
fingerprint
no-cli
no-tlsv1
no-tlsv1_1
no-dtls
no-tcp-relay
no-multicast-peers
denied-peer-ip=0.0.0.0-255.255.255.255
allowed-peer-ip=10.42.0.2
user-quota=64
total-quota=512
max-bps=10000000
bps-capacity=200000000
stale-nonce=600
cert=/run/tls/fullchain.pem
pkey=/run/tls/privkey.pem
pidfile=/tmp/turnserver.pid
log-file=stdout
simple-log
'''.format(secret=secret)
    path=folder/'turnserver.conf';path.write_text(config);path.chmod(0o600)
    print('Wall relay configuration prepared; credentials were not printed.')
if __name__=='__main__':main()
