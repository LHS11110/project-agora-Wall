#!/usr/bin/env python3
"""Share existing HTTPS TCP/443 with authenticated TURN/TLS, preserving client IPs."""
import re,shutil,subprocess,time,shlex
from pathlib import Path

def main():
    source=Path(__file__).resolve().parents[1]/'media/ingress.conf'
    nginx=Path('/etc/nginx/nginx.conf')
    destination=Path('/etc/nginx/streams-enabled/agora-media.conf')
    backup=Path('/var/backups/agora-media-ingress')/time.strftime('%Y%m%d-%H%M%S')
    backup.mkdir(parents=True,exist_ok=True)
    originals={nginx:nginx.read_bytes()}
    originals[destination]=destination.read_bytes() if destination.exists() else None
    pattern=re.compile(r'\blisten\s+(?:0\.0\.0\.0:)?443\s+ssl([^;]*);')
    for link in Path('/etc/nginx/sites-enabled').iterdir():
        file=link.resolve()
        if file.is_file() and pattern.search(file.read_text()):originals[file]=file.read_bytes()
    for index,(file,data) in enumerate(originals.items()):
        if data is not None:(backup/f'{index}-{file.name}').write_bytes(data)
    try:
        for file,data in originals.items():
            if file in [nginx,destination]:continue
            file.write_text(pattern.sub(r'listen 127.0.0.1:9443 ssl proxy_protocol\1;\n    set_real_ip_from 127.0.0.1;\n    real_ip_header proxy_protocol;',data.decode()))
        include='include /etc/nginx/streams-enabled/*.conf;'
        if include not in nginx.read_text():nginx.write_text(nginx.read_text()+'\n'+include+'\n')
        destination.parent.mkdir(exist_ok=True)
        shutil.copyfile(source,destination)
        subprocess.run(['nginx','-t'],check=True)
        subprocess.run(['systemctl','reload','nginx'],check=True)
    except BaseException:
        for file,data in originals.items():
            if data is None:file.unlink(missing_ok=True)
            else:file.write_bytes(data)
        subprocess.run(['nginx','-t'],check=False)
        subprocess.run(['systemctl','reload','nginx'],check=False)
        raise
    hook=Path('/etc/letsencrypt/renewal-hooks/deploy/95-agora-media')
    if hook.parent.is_dir():
        script=source.parents[1]/'scripts/renew-media-certificate.sh'
        hook.write_text('#!/bin/sh\nset -eu\ncase "${RENEWED_LINEAGE:-}" in */frelog.p-e.kr) exec '+shlex.quote(str(script))+' ;; esac\n')
        hook.chmod(0o755)
    print('HTTPS and Wall TURN/TLS ingress configured; backup:',backup)
if __name__=='__main__':main()
