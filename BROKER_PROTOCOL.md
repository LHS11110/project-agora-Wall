# Phoenix ↔ C++ 지속 WSS 프로토콜

브라우저 → Nginx → Phoenix 브로커 → C++ 쿼리 서비스 → CanvasServiceMemory → MemoryClass → LRU/Redis 구조입니다. Nginx와 브로커 사이도 TLS를 사용합니다. Phoenix는 접속·피어·호스트·응답 대상과 브로드캐스트를 관리하고, 브로커는 사용자 JWT 인증을 수행하고 C++는 참여 권한 데이터·객체 접근 권한·CRUD·채팅·설정·저장을 처리합니다. 실제 WebRTC 미디어와 데이터 채널은 기존 경로를 유지합니다.

## 전송

각 `BROKER_CPP_TARGETS` 대상에 Gun 클라이언트가 WSS 연결 하나를 유지합니다. 기본 대상은 `wss://agora-cpp:8002`, 내부 경로는 `/broker/queries`입니다. WebSocket 최초 TLS/HTTPS upgrade 뒤에는 JSON 프레임만 교환하며 CRUD용 HTTP 엔드포인트는 없습니다. CA와 호스트명을 검증하고, upgrade에는 BE와 같은 `X-Agora-Internal-Token`을 사용합니다. 이 토큰은 최소 32바이트의 printable ASCII이며 브라우저에 전달하지 않습니다.

```json
{
  "command": "query",
  "route": {"canvas_id": 1, "connection_id": "connection", "request_id": "request-1"},
  "identity": {"canvas_id": 1, "connection_id": "connection", "expires_at": 2000000000, "user_id": 7, "nickname": "user", "tag_number": 1, "settings_revision": 0, "groups": ["default"], "is_admin": false},
  "event": {"type": "crud", "action": "patch", "item_id": "shape-1", "patch": {"x": 100}}
}
```

응답은 동일한 `route`, 요청자에게 보낼 `response`, 권한에 따라 배포할 `broadcasts`를 포함합니다. `broadcasts`의 각 항목은 `event`와 `permission`을 가집니다. `all`·`close`는 설정 변경에 따른 재접속에 사용됩니다. 브로커는 요청 ID·연결 ID·캔버스 ID가 모두 일치하는 응답만 수락합니다. 응답 순서가 요청 순서와 달라도 올바른 호출자에게 전달됩니다. 요청자가 떠난 뒤 완료된 변경도 남아 있는 참여자에게 알립니다.

브로커는 WebSocket upgrade 전에 Spring의 HS256 접속 JWT 서명·시간·canvasId·clientIp·serverHash·사용자 핸들·revision을 검증합니다. JWT_SECRET은 Spring과 브로커에만 있습니다. 검증이 실패하면 C++에 요청하지 않고 upgrade를 거절합니다.

검증 성공 시 `command: connect`의 `principal`에 `{canvas_id, nickname, tag_number, settings_revision}`을 담고 `ws_port`, `route`와 함께 전달합니다. 원본 JWT나 별도 사용자 capability는 C++에 전달하지 않습니다. C++가 현재 활성 사용자·참여자·revision을 확인해 권한별 최신 `init_items`와 identity를 반환하며 브로커는 검증한 principal과 응답 identity가 일치하는 경우에만 upgrade를 허용합니다. `query`의 identity는 브로커가 보관한 서버 측 identity에 연결 ID·만료 시각을 붙인 것입니다. 사용자 입력의 identity는 제거됩니다. C++는 인증된 내부 브로커 소켓만 신뢰하고 identity의 캔버스·연결·만료 및 데이터 접근 권한을 검사합니다.

Room은 `(ws_port, canvas_id)`로 분리하며 다른 캔버스의 identity는 가입할 수 없습니다. 응답·방송·신호는 해당 Room의 인증된 참여자에게만 전달합니다. 브로커는 내부 사용자 ID나 identity를 브라우저에 보내지 않습니다. 연결 만료(최대 5분 또는 원래 JWT 만료)와 설정 변경 시 `/access`로 재인증합니다.

## 순서와 부하 제한

캔버스별 Room은 저장 요청을 순차 처리하며 서로 다른 캔버스는 병렬 처리합니다. 호스트의 `host_item_batch`는 현재 호스트·term을 검증하고, 대기 중 호스트가 바뀐 요청은 `HOST_FENCED`로 거절합니다. 호스트의 저장 성공 응답은 C++ 적용 및 Redis flush 성공 후 반환됩니다. 클라이언트의 기존 호스트 승인·요청 카운트·P2P 동기화 동작은 유지합니다.

| 경계 | 상한 |
| --- | --- |
| 브라우저 JSON 프레임 | 12 MiB, 연결당 초당 100개 |
| Room 대기열 | 128개 / 16 MiB |
| 대상별 WSS 동시 요청 | 64개 / 32 MiB |
| C++ worker | 16개 |
| C++ 대기열 | 128개 / 32 MiB |
| C++ 브로커 연결 | 16개 |
| 단일 객체 작업 배치 | 512개 |

과부하 시 요청을 거절하고 무제한 메모리 증가를 막습니다. 느린 소비자의 소켓도 종료합니다. 대상별 응답 제한 시간은 10초입니다. 연결 단절·시간 초과 후 쓰기가 실행되었는지 확정할 수 없으면 `QUERY_OUTCOME_UNKNOWN`을 반환하고 자동 재전송하지 않습니다. 최신 스냅샷을 다시 받아 상태를 확인해야 합니다. 요청 ID는 응답 분리용이며 영구 중복 실행 방지 기록은 아닙니다.

배치의 권한·형식은 먼저 검증하지만 저장 장애 시 적용된 일부 작업을 되돌리는 트랜잭션은 제공하지 않습니다. 실패 응답의 `applied`가 있는 경우 적용된 개수를 나타내며, 확정할 수 없는 실패는 재동기화합니다. 다중 브로커 replica를 동시에 운영하려면 캔버스별 Room 소유자 및 라우팅 affinity를 별도로 구성해야 합니다. 현재 Compose는 브로커 한 개를 실행합니다.

Room은 30초마다 인증된 연결로 C++ lease를 보내며, C++는 최근 쿼리 시각으로 캔버스 수명과 부하를 계산합니다. C++에는 브라우저 소켓·온라인 사용자·RTC 피어 목록이 없습니다. HTTPS는 `/health`, Spring의 서버 부하 조회와 내부 캔버스 제어에 사용합니다. Spring은 서버 ID를 포함한 Wall Nginx 내부 경로로 호출하며 C++에 직접 연결하지 않습니다. [ID 라우팅 설정](README.md#서버-id-기반-c-api-라우팅)을 참고하세요.

## 검증

`cd broker && MIX_ENV=test mix test`는 호스트 선택·RTC 바인딩·권한별 라우팅을 검증합니다. C++는 `-DBUILD_TESTING=ON`으로 빌드한 뒤 CTest를 실행합니다. 같은 빌드의 `agora_cpp_broker_socket_fixture`에 직접 준비한 localhost SAN 인증서와 `SERVICE_TLS_CERT`·`SERVICE_TLS_KEY`를 지정해 실행하면 9448 포트에서 테스트용 WSS 서버를 엽니다. 이 fixture는 저장소를 사용하지 않고 120초 뒤 종료합니다.

fixture와 같은 네트워크에서 `BROKER_TEST_TLS_CA=/path/to/root-ca.pem MIX_ENV=test mix test`를 실행하면 40개 동시 요청의 응답 분리·지속 연결 재사용·잘못된 내부 토큰 거부를 실제 Phoenix/Gun ↔ C++ TLS 소켓으로 검증합니다. 인증서는 생성하지 않습니다.

Gun 2.7 이상은 잘못된 HTTP 요청 헤더를 기본적으로 거절합니다. cowlib의 structured-field encoder 보안 공지가 의존성 검사에 표시될 수 있으나 이 서비스는 해당 encoder에 사용자 입력을 전달하지 않으며 고정 경로·검증된 내부 토큰만 upgrade 헤더에 사용합니다. [공식 공지의 조건 및 완화 방법](https://cna.erlef.org/cves/CVE-2026-43966.html)을 참고하세요.
