# [WIKI-0002] Flink 분산 아키텍처: JobManager와 TaskManager의 분리 이유

- **분류:** `Concept`
- **날짜:** 2026-06-25
- **작성자:** @ddev-hyeoncheol

---

## 1. 개요 (Overview)

- **핵심 정의:** Apache Flink가 분산 스트림 처리를 수행할 때 마스터(JobManager)와 워커(TaskManager)를 물리적 프로세스(또는 컨테이너) 단위로 엄격히 분리하여 구동하는 이유와 그 아키텍처적 당위성을 정의합니다.
- **배경 및 필요성:** Impala 등 대칭형 OLAP 쿼리 엔진은 하나의 프로세스(`impalad`)가 코디네이터와 익스큐터 역할을 병행하지만, Flink와 같은 무중단 실시간 스트리밍 엔진은 장애 복구 및 상태 보존(State & Checkpoint)이 핵심이기 때문에 비대칭형 아키텍처가 필요합니다.

---

## 2. 핵심 원리 (How it works)

### 2.1 각 컴포넌트의 역할
1. **JobManager (Master / Coordinator)**:
   - 데이터 흐름 그래프(JobGraph)를 물리적 실행 계획(ExecutionGraph)으로 파싱하여 자원을 할당합니다.
   - 주기적으로 분산 상태의 스냅샷(Checkpoint)을 조율하고 관리합니다.
   - 워커 노드 장애 발생 시 복구 작업(Failover)을 제어합니다.
2. **TaskManager (Worker / Executor)**:
   - JobManager로부터 할당받은 실제 태스크들을 병렬 슬롯(Task Slots) 단위로 실행합니다.
   - 실시간 스트림 데이터를 수집, 버퍼링하고 상태(State) 메모리를 직접 점유하며 계산을 진행합니다.

### 2.2 물리적 프로세스 분리 이유 (vs Impala)
1. **장애 격리 (Fault Isolation)**:
   - TaskManager는 메모리에 실시간 데이터를 상시 적재하고 무거운 연산을 돌리기 때문에 메모리 부족(OOM)으로 프로세스가 비정상 종료(Crash)할 확률이 상존합니다.
   - 만약 코디네이터와 익스큐터가 단일 프로세스에 통합되어 있다면, OOM으로 프로세스가 죽었을 때 전체 복구를 총괄해야 할 컨트롤 타워(JobManager)까지 동시에 소멸합니다.
   - Flink는 일꾼(TaskManager)이 죽어도 사장(JobManager)이 살아남아 체크포인트로부터 데이터를 재복구할 수 있도록 두 프로세스를 엄격히 차단·고립시킵니다.
2. **24/365 고가용성 (High Availability)**:
   - 일회성 쿼리를 처리하는 OLAP 엔진(Impala)과 달리 Flink는 1년 내내 중단 없이 가동되어야 하므로 장애 복구 복잡도가 극도로 높습니다.
3. **독립적 스케일 아웃 (Scalability)**:
   - 연산량이나 처리 대역폭(Throughput)이 부족할 때, 컨트롤 타워는 1대만 둔 상태에서 TaskManager의 인스턴스 개수만 유연하게 늘려서 대처할 수 있습니다.

---

## 3. 프로젝트 활용 (Implementation)

- **구현 방식 및 목적:**
  - 로컬 샌드박스 검증 단계에서도 실제 프로덕션 환경과의 정합성을 맞추기 위해 `docker-compose.yml` 상에 `jobmanager`와 `taskmanager`를 분리해 컨테이너로 기동합니다.
  - Flink SQL Client는 이 컨트롤 타워(JobManager)에 SQL을 제출(Submit)하는 게이트웨이 역할만 수행하게 됩니다.
- **관련 파일/코드:**
  - [docker-compose.yml](../../docker-compose.yml) (jobmanager, taskmanager 서비스 정의부)
  - [flink.Dockerfile](../../flink.Dockerfile) (동일한 커넥터 의존성 베이스를 공유하여 빌드)

---

## 4. 비고 (Notes)

- Flink도 단일 프로세스 기반의 로컬/임베디드 실행(Embedded Local Execution)을 테스트 용도로 지원하지만, 이 경우 스레드 간 JVM 힙 메모리를 공유하여 자원 격리가 불가능하고 JVM Crash 발생 시 전체 태스크 정보가 날아가는 한계가 있으므로 가급적 분산 컨테이너 구성을 유지합니다.
