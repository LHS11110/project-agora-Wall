# Phoenix ↔ C++ 지속 WSS 프로토콜

브라우저 → Nginx → Phoenix 브로커 → C++ 쿼리 서비스 → CanvasServiceMemory → MemoryClass → LRU/Redis 구조입니다. Nginx와 브로커 사이도 TLS를 사용합니다. Phoenix는 접속·피어·응답 대상과 브로드캐스트를 관리하고, 브로커는 사용자 JWT 인증을 수행하고 C++는 참여 권한 데이터·객체 접근 권한·CRUD·채팅·설정·저장을 처리합니다. WebRTC P2P는 통화 미디어에만 사용합니다. 브라우저 호스트 선출과 승인 단계는 없습니다.

## 전송

각 `BROKER_CPP_TARGETS` 대상에 Gun 클라이언트가 WSS 연결 하나를 유지합니다. 기본 대상은 `wss://agora-cpp:8002`, 내부 경로는 `/broker/queries`입니다. WebSocket 최초 TLS/HTTPS upgrade 뒤에는 JSON 프레임만 교환하며 CRUD용 HTTP 엔드포인트는 없습니다. CA와 호스트명을 검증하고, upgrade에는 BE와 같은 `X-Agora-Internal-Token`을 사용합니다. 이 토큰은 최소 32바이트의 printable ASCII이며 브라우저에 전달하지 않습니다.

```json
{
  "command": "query",
  "route": {"canvas_id": 1, "connection_id": "connection", "request_id": "request-1"},
  "identity": {"canvas_id": 1, "connection_id": "connection", "expires_at": 2000000000, "user_id": 7, "nickname": "user", "tag_number": 1, "settings_revision": 0, "groups": ["default"], "is_admin": false},
  "event": {"type": "canvas_ops", "client_id": "tab-id", "request_count": 1, "request_id": "request-1", "operations": [{"action": "patch", "id": "shape-1", "patch": {"x": 0.1}}]}
}
```

응답은 동일한 `route`, 요청자에게 보낼 `response`, 권한에 따라 배포할 `broadcasts`를 포함합니다. `broadcasts`의 각 항목은 `event`와 `permission`을 가집니다. `all`·`close`는 설정 변경에 따른 재접속에 사용됩니다. 브로커는 요청 ID·연결 ID·캔버스 ID가 모두 일치하는 응답만 수락합니다. 응답 순서가 요청 순서와 달라도 올바른 호출자에게 전달됩니다. 요청자가 떠난 뒤 완료된 변경도 남아 있는 참여자에게 알립니다.

브로커는 WebSocket upgrade 전에 Spring의 HS256 접속 JWT 서명·시간·canvasId·clientIp·serverHash·사용자 핸들·revision을 검증합니다. JWT_SECRET은 Spring과 브로커에만 있습니다. 검증이 실패하면 C++에 요청하지 않고 upgrade를 거절합니다.

검증 성공 시 `command: connect`의 `principal`에 `{canvas_id, nickname, tag_number, settings_revision}`을 담고 `ws_port`, `route`와 함께 전달합니다. 원본 JWT나 별도 사용자 capability는 C++에 전달하지 않습니다. C++가 현재 활성 사용자·참여자·revision을 확인해 권한별 최신 `init_items`와 identity를 반환하며 브로커는 검증한 principal과 응답 identity가 일치하는 경우에만 upgrade를 허용합니다. `query`의 identity는 브로커가 보관한 서버 측 identity에 연결 ID·만료 시각을 붙인 것입니다. 사용자 입력의 identity는 제거됩니다. C++는 인증된 내부 브로커 소켓만 신뢰하고 identity의 캔버스·연결·만료 및 데이터 접근 권한을 검사합니다.

Room은 `(ws_port, canvas_id)`로 분리하며 다른 캔버스의 identity는 가입할 수 없습니다. 응답·방송·신호는 해당 Room의 인증된 참여자에게만 전달합니다. 브로커는 전체 서버 identity나 원본 JWT를 다른 브라우저에 보내지 않습니다. 저장 결과와 확정 변경에는 요청 출처를 구별하는 클라이언트 ID 및 작성자 ID가 포함됩니다. 연결 만료(최대 5분 또는 원래 JWT 만료)와 설정 변경 시 `/access`로 재인증합니다.

## 순서와 부하 제한

캔버스별 Room은 저장 요청을 순차 처리하며 서로 다른 캔버스는 병렬 처리합니다. 모든 사용자는
RTC 연결이나 다른 사용자의 승인 없이 `canvas_ops`를 전송할 수 있습니다. C++는 권한과 객체를
검증한 뒤 Redis Lua로 객체 변경·전역 순번·클라이언트 처리 카운터를 함께 반영합니다.
`canvas_result`는 Redis 반영 확인이며, `canvas_commit`은 각 브라우저가 렌더링할 확정 변경입니다.
브로커는 항목별 새/이전 권한으로 데이터를 필터링하고, 권한을 잃은 항목은 삭제로 전달합니다.
빈 변경도 순번을 전달합니다. 이동만 있는 배치는 ID 목록과 방향 벡터로 공유합니다.

| 경계 | 상한 |
| --- | --- |
| 브라우저 JSON 프레임 | 16 MiB, 연결당 초당 240개 |
| Room 대기열 | 128개 / 16 MiB |
| 대상별 WSS 동시 요청 | 64개 / 32 MiB |
| C++ worker | 16개 |
| C++ 대기열 | 128개 / 32 MiB |
| C++ 브로커 연결 | 16개 |
| 단일 객체 작업 배치 | 512개 |

과부하 시 요청을 거절하고 무제한 메모리 증가를 막습니다. 느린 소비자는 재접속합니다.
대상별 일반 응답 제한 시간은 10초이며 연결 인증은 40초입니다. 결과가 불명확하면
`QUERY_OUTCOME_UNKNOWN`을 반환합니다. 클라이언트는 같은 ID·카운터·내용으로 재전송합니다.
서버의 영속 처리 기록과 SHA-256이 일치하면 저장을 반복하지 않고 기존 결과를 반환합니다.
처리 카운터는 사용자별 클라이언트 ID에 묶이며, 문서당 최대 4,096개를 보관합니다.
일반 배치는 최대 512개 작업, 이동은 최대 10,000개 ID, 확장 결과는 20,000개 객체·8MiB입니다.

첫 접속은 인증 확인에서 객체 본문을 생략하고 Room에 등록한 뒤 저장 큐에서 최신 스냅샷을
조회합니다. `canvas_snapshot`/`init_items`에는 전역 순번과 해당 클라이언트의 처리 카운터가
포함됩니다. 1초마다 `canvas_watermark`를 전달하고 순번 누락 시 같은 소켓에서
`canvas_sync`로 복구합니다. 재접속해도 저장된 이동을 다시 적용하지 않습니다.
텍스트는 공통 Automerge 스냅샷과 변경 이력을 저장하고 각 클라이언트가 병합하여 렌더링합니다.
처리 기록은 Elasticsearch 스냅샷에 인코딩하여 클라이언트마다 매핑 필드가 늘지 않게 보관합니다.

Redis 주 노드 반영은 복제·디스크 fsync 완료와 다릅니다. 기존 비동기 Redis 복제 구성은 유지합니다.

## 검증

`cd broker && MIX_ENV=test mix test`는 호스트 없는 저장·RTC 바인딩·권한별 라우팅을 검증합니다. C++는 `-DBUILD_TESTING=ON`으로 빌드한 뒤 CTest를 실행합니다. 같은 빌드의 `agora_cpp_broker_socket_fixture`에 직접 준비한 localhost SAN 인증서와 `SERVICE_TLS_CERT`·`SERVICE_TLS_KEY`를 지정해 실행하면 9448 포트에서 테스트용 WSS 서버를 엽니다. 이 fixture는 저장소를 사용하지 않고 120초 뒤 종료합니다.

fixture와 같은 네트워크에서 `BROKER_TEST_TLS_CA=/path/to/root-ca.pem MIX_ENV=test mix test`를 실행하면 40개 동시 요청의 응답 분리·지속 연결 재사용·잘못된 내부 토큰 거부를 실제 Phoenix/Gun ↔ C++ TLS 소켓으로 검증합니다. 인증서는 생성하지 않습니다.

Gun 2.7 이상은 잘못된 HTTP 요청 헤더를 기본적으로 거절합니다. cowlib의 structured-field encoder 보안 공지가 의존성 검사에 표시될 수 있으나 이 서비스는 해당 encoder에 사용자 입력을 전달하지 않으며 고정 경로·검증된 내부 토큰만 upgrade 헤더에 사용합니다. [공식 공지의 조건 및 완화 방법](https://cna.erlef.org/cves/CVE-2026-43966.html)을 참고하세요.

## 참여자 전환과 relay 거절

RTC는 `canvas_protocol: 2`, `canvas_transport: "broker-v1"` 및 캔버스 소켓의 연결 증명으로 가입합니다.
`canvas_relay`는 `cursor`, `laser`, `editor_presence`만 중계하며 저장 큐를 통과하지 않습니다.
발신자 정보는 인증된 소켓에서 만들고 같은 Room의 현재 참여자에게만 전달합니다.
호스트 메시지, 이전 `host_item_batch`, `crud_batch` 저장 경로는 브라우저에서 사용할 수 없습니다.
`rtc_signal`은 통화 연결 협상 전용입니다. 이전 버전 탭은 한 번 새로고침해야 합니다.

인증 만료·캔버스 소켓 종료·설정 변경 등 예상된 소켓 종료는 이유를 보존한 shutdown으로 처리하며 비정상 GenServer 오류로 기록하지 않습니다.

## 저장 결과 유실과 접속 순서

게이트웨이는 외부 WebSocket의 `client_id`를 내부 `/broker/websocket` 쿼리에도 전달합니다.
이 값이 없으면 임의의 ID로 첫 스냅샷을 읽어 기존 편집 세션의 처리 내역을 찾지 못합니다.

`canvas_ops`는 발신자 퇴장 후에도 큐에서 실행합니다. 일시 실패·결과 불명 시 동일 요청을
1초 후 재시도하고 인증 만료·영구 거절 시 중단합니다. 대기 중인 동일 사용자·클라이언트·카운터의
동일 요청은 합치며, 내용이 다르면 `CLIENT_REQUEST_MISMATCH`로 거절합니다.
재시도 중에도 한 캔버스의 직렬 처리를 유지하므로 새 참여자의 스냅샷이 미확정 저장을 추월하지 않습니다.
저장 성공 이후 확정 변경을 방송하며, 중복 저장 응답의 최신 순번 안내로 누락된 방송을 복구합니다.
재시도 후 결과를 확정하지 못하면 `canvas_watermark`에 `force_sync: true`를 넣어
같은 순번에서도 재동기화하게 합니다. 브로커 메모리 큐는 프로세스 재시작을 견디는 영속 로그가 아닙니다.

## Server-owned mentions and object comments

`chat` and `chat_history` accept an optional `object_id`. The C++ server derives the
private comment room and checks the current object's ACL on every request. A direct
request to an `object-comment-*` room without its object ID is rejected. Comments
use the same durable chat sequence/history path as canvas chat.

`mentions_get` returns `mentions_snapshot` with the current authenticated user's
latest 100 accessible unread notifications. `mention_read` accepts only `mention_id`
and returns a confirmation after persistence. Client-supplied user IDs are ignored.
Each stored message contains a per-recipient `mentions` boolean map; the server also
maintains a private `mention-states` index. Message append, retry receipt and mention
index creation happen atomically. Read acknowledgements update the message and index
atomically and do not reset unread state on duplicate message retries.

Server broadcasts may contain `user_ids`. These are delivered only to matching
session user IDs, including when another session has administrator privileges.
`mentions_changed` carries no notification text and instructs matching clients to
refresh their server-owned notification list. Reconnect performs the same query.

Blueprint execution and output nodes are shapes with `runtimeRole: executor|output`.
Only output nodes may carry bounded `runtimeResults` arrays. Execution follows
ordinary directed canvas connectors. Outputs are persisted and broadcast through
normal server-authoritative `canvas_ops`; code nodes do not independently execute.
