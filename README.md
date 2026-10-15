# project-agora-Wall

Agora의 외부 진입점과 서버 간 메시징 인프라를 관리하는 저장소입니다. Nginx와 Phoenix/Elixir WebSocket 메시지 브로커를 포함합니다. 브로커는 라우팅·브로드캐스트를 담당하고 C++가 데이터를 처리·저장합니다.

## 구성

| 경로 | 역할 |
| --- | --- |
| `broker/` | Phoenix 브로커와 지속 WSS C++ 클라이언트 |
| `docker-compose.broker.yml` | 브로커와 인증서 볼륨 초기화 |
| `scripts/broker-docker.sh` | 브로커 실행·설정·상태 확인 |
| `docker-compose.nginx.yml` | Nginx 컨테이너, TLS 마운트, 네트워크와 공유 볼륨 |
| `nginx/agora.conf.example` | HTTPS/WSS 라우팅의 단일 설정 템플릿 |
| `scripts/setup.py` | 직접 준비한 인증서 경로와 연결 설정을 `.env`에 기록 |
| `docker-compose.storage-broker.yml` | SQL·ES·Redis/Sentinel TCP 중계와 서비스 네트워크 생성 |
| `storage-broker/haproxy.cfg` | 저장소 listener·upstream·연결 제한 |
| `STORAGE_BROKER.md` | 저장소 브로커의 라우팅·TLS·전환·검증 |
| `scripts/preflight.py` | 실행 전 인증서 파일·Compose·네트워크 확인 |
| `scripts/nginx-docker.sh` | 개발·운영 실행, 설정 검사와 상태 확인 |
| `.env.example` | 공개 설정 예시 |

FE는 화면과 빌드 결과, BE는 Spring·C++, DB는 저장소와 `agora-net` 네트워크를 담당합니다. Nginx 이미지·실행 설정은 Wall에서 관리합니다.

## 초기 설정

Docker Engine과 Docker Compose, Python 3가 필요합니다. 저장소를 clone한 뒤 인증서를 직접 생성하거나 발급받아 아래처럼 준비합니다. 설정 스크립트는 인증서를 생성하거나 복사하지 않습니다.

```text
/path/to/certificates/
  fullchain.pem
  privkey.pem
  root-ca.pem
```

Nginx 인증서는 실제 접속 도메인을 SAN에 포함해야 합니다. 기본 로컬 실행은 `localhost`, `127.0.0.1`을 사용하며 FE의 Nginx 프록시 연결에는 `agora-nginx`도 필요합니다. `root-ca.pem`은 Nginx 인증서와 내부 `agora-spring`, `agora-cpp`, 개발 시 `agora-frontend-dev` 인증서를 검증할 수 있는 공개 CA 묶음이어야 합니다. CA 개인 키는 마운트하지 않습니다. 사설 CA는 접속하는 브라우저의 신뢰 저장소에 직접 등록합니다.

```bash
python3 scripts/setup.py --tls-dir /path/to/certificates --backend-dir ../project-agora-BE
```

`.env`를 생성하며 인증서·개인 키·실제 환경 파일은 Git에서 제외합니다. 외부 도메인을 사용할 경우 `--public-origin https://your-domain:8443`을 지정하고 FE·BE의 origin 설정도 맞춥니다.

| 설정 | 기본값 | 설명 |
| --- | --- | --- |
| `NGINX_TLS_CERT_DIR` | 직접 지정 | 인증서 디렉터리의 절대 경로 |
| `NGINX_HTTPS_PORT` | `8443` | loopback에 게시하는 HTTPS 포트 |
| `NGINX_BACKEND_IP` | `172.23.0.250` | `agora-services`의 Nginx 고정 IP |
| `FRONTEND_MODE` | `production` | `development` 또는 `production` |

Wall의 고정 IP와 BE의 `CPP_TRUSTED_PROXY_IPS`를 맞추고 Wall 서비스 네트워크 서브넷 내 빈 주소를 사용합니다. BE의 `NGINX_TLS_CERT_DIR`는 내부 서비스 인증서 초기화에도 사용하므로 유지합니다. BE의 `.env`를 먼저 준비합니다. 설정 스크립트는 `CPP_INTERNAL_API_TOKEN`을 읽어 Wall에 같은 값을 기록하고 브로커 secret을 생성합니다. 설정 스크립트는 Spring의 `JWT_SECRET`도 브로커에 전달합니다. 이 HS256 서명 검증 키는 Spring과 브로커만 공유하며 C++ 컨테이너에는 전달하지 않습니다. DB 비밀번호는 브로커에 주지 않습니다. 인증서는 직접 제공하며, 기본적으로 Nginx 인증서를 브로커에서도 사용해 내부 검증 이름을 `agora-nginx`로 지정합니다. 별도 인증서를 사용할 경우 `--broker-tls-dir`로 SAN `agora-broker` 인증서 디렉터리를 지정합니다.

## 실행

먼저 DB를 실행·초기화하고 Wall의 저장소 브로커, BE 순서로 시작합니다. 개발 모드 Nginx를 실행하면 FE와 공유하는 `agora-web` 네트워크가 생성됩니다.

```bash
./scripts/storage-broker-docker.sh up
cd ../project-agora-BE
./scripts/backend-docker.sh up
cd ../project-agora-Wall
./scripts/broker-docker.sh up
./scripts/nginx-docker.sh development
cd ../project-agora-FE
docker compose -f compose.dev.yaml up -d --build
```

접속 주소는 **https://localhost:8443**입니다. 외부 접속과 내부 upstream은 HTTPS/WSS만 사용하고 CA 및 호스트명을 검증합니다. HTTP 포트는 열지 않습니다.

| 요청 | 목적지 |
| --- | --- |
| `/api/` | Spring HTTPS `agora-spring:8080` |
| `/wss/port/8002/canvas/:id` | Phoenix WSS 객체 요청·응답·브로드캐스트 |
| `/wss/port/8002/rtc/canvas/:id` | Phoenix WSS 피어 시그널링 |
| 페이지·정적 자산 | 운영 빌드 볼륨 또는 개발 Vite HTTPS |
| `/nginx-health` | Nginx 상태 확인 |

C++ REST와 브로커의 4443 포트는 외부에 공개하지 않습니다. 외부 논리 포트는 8002–8099로 제한하고 `BROKER_CPP_TARGETS`의 허용 목록으로 C++ WSS 대상을 선택합니다. CRUD는 지속 WSS 연결에서 요청 ID로 병렬 응답을 구분합니다. [내부 프로토콜·부하 제한·테스트](BROKER_PROTOCOL.md)를 참고하세요.

기존 실행 중인 스택은 자동 재시작하지 않습니다. 변경을 반영하려면 BE를 빌드·재시작한 뒤 브로커를 시작하고 마지막으로 Nginx를 재생성합니다.

## 운영 빌드

```bash
cd ../project-agora-FE
docker compose build frontend-build
docker compose run --rm frontend-build
cd ../project-agora-Wall
./scripts/nginx-docker.sh production
```

FE가 `agora-frontend-dist` 볼륨에 빌드를 내보내고 Nginx는 `current/`를 읽기 전용으로 제공합니다. 개발 FE 컨테이너는 FE 저장소에서 별도로 중지합니다.

```bash
./scripts/nginx-docker.sh config
./scripts/nginx-docker.sh test
./scripts/nginx-docker.sh status
./scripts/nginx-docker.sh logs
./scripts/nginx-docker.sh stop
```

인증서를 갱신한 뒤 `nginx-docker.sh development` 또는 `production`을 실행하고 `docker compose -f docker-compose.nginx.yml exec nginx nginx -s reload`로 다시 읽습니다. `nginx -t`는 인증서·키와 설정을 검사하며 health check는 CA 검증을 수행합니다.

## BE에서 이전

Compose 프로젝트 이름 `agora-gateway`, 컨테이너 `agora-nginx`, 네트워크와 볼륨 이름을 유지했습니다. 기존 BE와 같은 인증서 디렉터리·포트·고정 IP를 Wall에 지정한 뒤 Wall의 실행 스크립트를 사용하면 기존 게이트웨이를 새 마운트 경로로 재생성합니다. 기존 BE Compose로 Nginx를 동시에 실행하지 않습니다. 저장소 이전만으로 실행 중인 컨테이너는 재시작하지 않습니다.

## 라이선스

이전한 프로젝트 코드는 기존과 동일하게 [MIT License](LICENSE)를 적용합니다. Nginx 공식 이미지와 그 구성 요소의 라이선스는 해당 배포물의 고지를 따릅니다.

## 서버 ID 기반 C++ API 라우팅

Spring은 C++ IP·포트를 호출 주소로 사용하지 않고 `WALL_CPP_API_ORIGIN`(기본 `https://agora-nginx:8444`)에 요청합니다. 경로는 `/internal/cpp/servers/{serverId}/api/canvas/count`이며, ID는 DB `cpp_server.server_id`입니다. Nginx가 ID를 실제 C++ HTTPS 주소로 변환합니다. Spring → Nginx와 Nginx → C++ 두 구간 모두 CA와 호스트명을 검증합니다. 내부 API 토큰과 요청 ID도 전달합니다. 미등록 ID나 라우팅 실패 시 C++ 직접 연결로 우회하지 않습니다.

[`nginx/cpp-routes.conf`](nginx/cpp-routes.conf)의 두 map에 동일한 서버 ID를 설정합니다. 첫 map 값은 IP 또는 DNS 이름과 REST 포트이며, 두 번째 값은 인증서의 SAN과 일치하는 검증 이름입니다. 예를 들어 서버 ID 42가 `172.23.0.30:8000`, 인증서 이름이 `cpp-node-42`라면 각각 `42 172.23.0.30:8000;`과 `42 cpp-node-42;`를 추가합니다. 기본 예시 ID 1은 실제 DB 등록 ID에 맞춰 수정합니다. 브로커의 WSS 대상 목록은 별도 설정입니다.

Nginx의 8444 포트는 Docker 내부에만 열며 호스트에 게시하지 않습니다. 공개 443 포트의 `/internal/`은 404를 반환합니다. 별도 네트워크에서 Spring을 실행하면 접근 가능한 Wall 내부 HTTPS origin을 명시하고 인증서 SAN에 맞춥니다. 새 C++ 서버를 등록하거나 주소를 변경하면 map을 갱신하고 다음 명령으로 검증·반영합니다.

```bash
./scripts/nginx-docker.sh test
docker compose -f docker-compose.nginx.yml exec nginx nginx -s reload
```

이 라우팅 경로를 처음 추가할 때는 새 파일 마운트를 적용해야 하므로 Nginx 컨테이너를 재생성합니다. Spring도 변경된 코드를 빌드·재시작합니다. 이후 map 내용만 수정하면 검증 후 reload로 적용할 수 있습니다.

## 브로커 인증과 캔버스 격리

Phoenix의 WebSocket `connect` 단계에서 JOSE로 HS256 서명·만료·nbf/iat·canvasId·clientIp·serverHash·사용자 핸들·settingsRevision을 검증합니다. 일반 로그인 토큰이나 다른 캔버스용 토큰은 거절합니다. JWT 자체는 암호화된 데이터가 아니라 서명된 데이터이며 `JWT_SECRET`은 그 서명을 검증하는 공유 키입니다.

검증 실패 시 C++에 요청하지 않고 업그레이드를 거절합니다. 검증 성공 후 브로커는 JWT를 전달하지 않고 검증한 principal만 내부 인증 WSS로 C++에 보내 현재 활성 사용자·참여자·revision과 권한별 최신 문서를 조회합니다. 조회 실패도 업그레이드를 거절합니다. 연결 시 C++는 사용자 JWT를 검증하거나 capability를 발급하지 않습니다. 브로커가 인증된 identity를 보관하고 요청마다 캔버스·연결·만료 시각과 함께 전달합니다. C++는 내부 브로커 토큰을 검증하며 객체 접근 권한과 데이터 처리를 담당합니다.

각 Room은 `(논리 서버 포트, canvasId)`로 분리합니다. 다른 캔버스 identity의 Room 가입은 거절하며 응답·브로드캐스트·RTC 신호는 그 Room 안에서만 라우팅합니다. 사용자가 payload에 넣은 identity·principal·route·user_id는 인증 정보로 사용하지 않습니다. 같은 캔버스 안에서도 permission 그룹을 확인해 방송합니다.

`BROKER_CPP_IDENTITIES`는 논리 포트별로 Spring에 등록된 C++ advertised host를 지정하는 JSON입니다. 기본은 `BROKER_CPP_TARGETS`의 hostname이며, 대상 DNS와 DB 등록 주소가 다르면 명시적으로 설정합니다. 예: `BROKER_CPP_IDENTITIES='{"8002":"agora-cpp"}'`. 이 이름과 논리 WS 포트로 Spring과 동일한 serverHash를 계산합니다.

브로커는 `BROKER_TRUSTED_PROXY_IPS`(기본 Nginx의 `NGINX_BACKEND_IP`)에 있는 프록시의 `X-Real-IP`만 사용합니다. 그 외 연결은 실제 peer IP로 JWT를 검증합니다. Nginx는 이 헤더를 접속 IP로 덮어씁니다. 인증된 연결도 최대 5분 또는 원래 JWT 만료 시각까지 유효하며 만료·설정 변경 시 다시 인증합니다.

기존 설정은 `scripts/setup.py`를 다시 실행해 JWT 키를 추가합니다. 인증 이전 프로토콜과 달라졌으므로 C++와 브로커를 함께 재빌드·재시작해야 합니다. Spring의 토큰 발급 형식과 프런트엔드 접속 URL은 유지합니다.

## TLS 프로토콜 정책

웹 HTTPS/WSS, Wall nginx, Phoenix 브로커, Spring, C++의 HTTPS/WSS·Redis·Elasticsearch 연결과 Redis/Sentinel·Elasticsearch 서버는 TLS 1.3을 사용합니다. TLS 1.2로의 하향 연결은 허용하지 않습니다. 기존 CA 및 인증서를 그대로 사용할 수 있으며, 설정 반영에는 해당 서비스 재빌드·재시작이 필요합니다.

예외는 현재 SQL Server 2022 Linux 컨테이너입니다. [Microsoft의 지원 문서](https://learn.microsoft.com/en-us/sql/linux/sql-server-linux-known-issues?view=sql-server-ver17#tls-13-not-supported-on-sql-server-2022)에 따라 이 의존성은 TLS 1.3을 지원하지 않으므로 SQL 연결은 인증서 검증을 수행하는 TLS 1.2 암호화를 유지합니다. SQL까지 TLS 1.3을 요구하려면 지원되는 SQL 서버 환경 및 TDS 드라이버로 별도 전환해야 합니다. SQL 호환성을 위해 JVM 전체에 TLS 1.3 전용 설정을 적용하지 않고 연결별로 제한합니다.

## 저장소 연결 중계

Spring·C++의 MS SQL·Elasticsearch·Redis/Sentinel 연결은 Wall의 TCP 저장소 브로커를 경유합니다. 애플리케이션과 저장소 네트워크를 분리하고 기존 TLS 스트림 및 인증서 검증을 유지합니다. [설정·연결 경로·전환·검증](STORAGE_BROKER.md)을 참고하세요.

내부 Nginx `/internal/search/embedding`은 Spring의 검색어를 BE 자체 호스팅 E5 worker로 전달합니다. 외부 `/internal` 차단 및 내부 토큰 검증을 유지하고 TLS 1.3·Spring 인증서 이름을 검증합니다. 문서 projection은 기존 storage-broker 경유 ES 연결을 사용합니다. [검색 아키텍처](../project-agora-DB/elasticsearch/SEARCH.md).

## 외부 리버스 프록시 연결

호스트 Nginx 등 외부 프록시를 거치면 Wall `.env`의 `NGINX_TRUSTED_PROXY_IP`에 Docker 게이트웨이에서 관찰되는 해당 프록시의 정확한 IP를 설정합니다. 외부 프록시는 `X-Real-IP`를 실제 접속자 IP로 덮어써야 합니다. Wall은 이 신뢰된 주소에서 온 헤더만 사용하며, Spring과 WebSocket 브로커가 동일한 IP로 토큰을 검증합니다. 전체 사설 네트워크를 신뢰하도록 설정하지 마세요. 변경 후 Orchestra의 `scripts/start-deploy.sh`로 적용합니다.

### Canvas collaboration routing

객체 변경은 캔버스 데이터 WebSocket의 `canvas_ops`로 C++ 서버에 전달하고 Redis에 반영합니다.
브로커는 `canvas_result`와 권한에 맞는 `canvas_commit`을 전달하며 각 브라우저가 렌더링합니다.
브라우저 호스트 선출이나 승인은 없습니다. `client_id`·요청 카운터·저장 기록으로 재전송 중복을 막고,
새 참여자 및 재접속자는 순번과 처리 카운터가 포함된 서버 스냅샷을 받습니다.

RTC 가입은 `canvas_protocol: 2`, `canvas_transport: "broker-v1"`과 데이터 소켓의 연결 증명을 사용합니다.
`canvas_relay`는 커서·레이저·편집 상태 전용이며, P2P는 통화 미디어에만 사용합니다.
이전 탭은 새로고침해야 합니다. [상세 프로토콜](BROKER_PROTOCOL.md).
