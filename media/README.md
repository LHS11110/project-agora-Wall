# Wall 미디어 중계

통화·화면·시스템 오디오 모두 `iceTransportPolicy: relay`를 사용한다. 인증된 RTC 연결을 캔버스 연결에 바인딩한 뒤 Wall 브로커가 `rtc_peers.media_relay`로 단기 TURN 자격 증명을 전달한다. 공유 비밀값은 서버에만 두고 브라우저에는 만료 시각이 있는 사용자명과 HMAC 자격 증명만 보낸다. 12시간마다 인증된 소켓으로 갱신하며 새 ICE 연결로 전환한다. 중계 설정이 없으면 통화 참여를 막으며 직접 P2P로 돌아가지 않는다.

```text
브라우저 ─ WSS/443 ─ Wall Nginx ─ Phoenix: 인증·참여·SDP/ICE
브라우저 ─ TURN/TLS/443 ─ Wall Nginx stream ─ coturn
                                              │
                          격리된 중계 소켓끼리 미디어 전달
                                              │
다른 브라우저 ─ TURN/TLS/443 ─ Wall Nginx stream ─┘
```

`coturn`은 Wall이 관리하는 미디어 서비스이며 C++ API 서버를 추가 경유하지 않는다. DTLS/SRTP는 브라우저 사이에 유지되며 중계 서버가 화면·음성을 디코딩하지 않는다. SFU가 아니라 각 참가자 쌍의 TURN 중계이므로 방 인원이 늘면 연결 수는 O(P²)다.

## 외부 포트와 격리

기존 호스트 Nginx의 TCP/443을 공유한다. stream이 TLS ClientHello의 SNI/ALPN으로 HTTP와 TURN을 구분하고 TLS를 종료하지 않은 채 전달한다. HTTP의 원본 IP는 PROXY protocol로 보존한다. `frelog.p-e.kr`에 대해 h2/http/1.1을 제시하는 연결은 웹, `stun.turn` 또는 ALPN 없는 연결은 TURN이다. ALPN을 보내지 않는 구형 HTTPS 클라이언트는 이 도메인의 웹 경로를 이용할 수 없으므로 관리 도구는 HTTP ALPN을 설정해야 한다.

- 기존 HTTPS 가상 호스트: `127.0.0.1:9443 ssl proxy_protocol`
- TURN: 내부 Docker 네트워크 `10.42.0.0/24`, 서비스 `10.42.0.2:5349`
- TURN 컨테이너에 외부 공개 포트·외부 egress 없음. 추가 외부 UDP 포트 불필요.
- 모든 브라우저가 같은 중계 서버를 사용하므로 릴레이 후보도 격리 네트워크 주소를 사용한다. TURN의 peer 허용 주소는 자기 중계 주소 하나뿐이며 LAN·인터넷으로 임의 패킷을 전달하지 못한다.
- RTP/RTCP와 음성·화면·시스템 오디오는 max-bundle로 묶는다. 기본 할당 한도는 자격 증명당 64개, 전체 512개, 릴레이 포트 범위 49160–50159다.

서버가 여러 머신으로 늘면 이 설정을 그대로 복제하면 안 된다. 중계 노드 간 연결 또는 SFU와 그에 맞는 ICE/네트워크 구성을 먼저 추가해야 한다.

## 준비·시작

```sh
python3 scripts/setup-media.py
sudo apt-get install libnginx-mod-stream
# 기존 HTTPS 설정을 백업하고 변환한다. nginx -t 실패 시 원본 복구.
sudo python3 scripts/install-media-ingress.py
docker compose -f docker-compose.media.yml up -d --wait
./scripts/broker-docker.sh up
```

설정은 `.env`의 `MEDIA_RELAY_SECRET`, `MEDIA_RELAY_URLS`, `MEDIA_RELAY_CERT_DIR`와 무시되는 `.local-media/turnserver.conf`에 저장한다. 인증서 디렉터리에는 신뢰할 수 있는 `fullchain.pem`, `privkey.pem`이 필요하다. 브라우저 TURN 연결은 페이지의 개발용 인증서 예외와 별개로 인증서를 검증하므로 개발 환경에서도 신뢰 체인을 구성해야 한다.

설치 스크립트는 `/etc/letsencrypt/renewal-hooks/deploy/95-agora-media`도 등록한다. certbot 갱신 후 서비스 컨테이너를 재생성하여 교체된 인증서 파일을 다시 마운트한다. 이 시점의 활성 통화는 ICE 재연결이 발생할 수 있다.

Orchestra는 중계 설정 준비 → 중계 컨테이너 정상 여부 확인 → Phoenix 시작 순서로 실행한다. 호스트의 443 분기 설정과 신뢰할 수 있는 인증서 준비는 최초 한 번 별도로 수행한다. `10.42.0.0/24`가 기존 망과 충돌하지 않는지도 사전에 확인한다.

## 검증

FE의 `tests/integration/voice-audibility.mjs`에 `AGORA_RELAY_TEST_CONFIG`로 단기 테스트 자격 증명 JSON 경로를 전달한다. 실제 TLS/443에서 양쪽의 선택 후보가 relay임을 확인하고, 440Hz 음성·880Hz 시스템 오디오, 빨간 영상 프레임의 실제 디코딩, 독립 음량, 화면 종료 후 음성 유지, ICE 재시작과 방 재인증 뒤 복구를 검증한다. 자격 증명 파일은 저장소에 넣지 않는다.

프로토콜과 자격 증명 형식: [coturn 공식 문서](https://github.com/coturn/coturn/wiki/turnserver), [TURN REST 자격 증명](https://github.com/coturn/coturn/blob/master/examples/scripts/restapi/secure_relay_secret.sh).
