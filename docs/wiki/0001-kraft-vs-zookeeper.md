# [WIKI-0001] KRaft 모드와 ZooKeeper의 차이 및 Kafka 4.x 아키텍처

- **분류:** `Concept`
- **날짜:** 2026-06-24
- **작성자:** @ddev-hyeoncheol

---

## 1. 개요 (Overview)

- **핵심 정의:** KRaft(Kafka Raft)는 외부 코디네이터인 ZooKeeper 없이 Kafka 노드 자체에서 내부 Raft 알고리즘을 사용해 메타데이터를 관리하는 새로운 아키텍처입니다.
- **배경 및 필요성:** ZooKeeper 모드는 관리 포인트가 이원화되어 운영 오버헤드가 컸고, 대규모 클러스터에서 파티션 확장성 한계(약 20만 개) 및 느린 컨트롤러 복구 시간 문제가 있었습니다. Kafka 4.x부터는 ZooKeeper 지원이 완전히 제거되어 KRaft가 유일한 표준이 되었습니다.

## 2. 핵심 원리 (How it works)

- **주요 메커니즘:**
  - **ZooKeeper 모드:** 클러스터 정보(토픽, 파티션 정보 등)를 외부 ZooKeeper에 보관하고, 변경 사항을 ZooKeeper ➡️ 리더 브로커 ➡️ 일반 브로커 순으로 동기화함.
  - **KRaft 모드:** 메타데이터를 Kafka 내부의 특별한 관리 토픽(`__cluster_metadata`)에 이벤트 로그 형태로 기록함. 컨트롤러 역할을 수행하는 노드들이 이 로그를 지속해서 동제하고 합의(Consensus)를 이룸.
- **핵심 키워드:**
  - **Controller Quorum:** 클러스터의 메타데이터를 관리하고 리더 선출 등을 수행하기 위한 컨트롤러 노드들의 모임.
  - **Quorum Voters:** 메타데이터의 변경 합의 및 새로운 리더 컨트롤러 투표에 참여할 컨트롤러 노드 목록.
  - **Cluster ID:** 메타데이터를 포맷팅하고 클러스터를 유일하게 식별하기 위해 필수적으로 요구되는 22자 Base64 UUID.

## 3. 프로젝트 활용 (Implementation)

- **구현 방식 및 목적:**
  - `kafka-flink-realtime-crypto` 프로젝트에서는 Apache Kafka 최신 안정 버전인 4.3.0을 적용함.
  - 별도의 ZooKeeper 컨테이너를 띄우지 않고 Kafka 단독 컨테이너 구성으로 인프라를 단순화함.
  - 개발 환경의 단순화를 위해 단일 노드 내에서 `broker` 역할과 `controller` 역할을 동시에 수행하는 `combined` 모드로 설정함.
- **관련 파일/코드:**
  - 설정 파일: [docker-compose.yml](../../docker-compose.yml) (`KAFKA_PROCESS_ROLES: broker,controller` 설정 부분)

## 4. 비고 (Notes)

- 참고 링크:
  - [Apache Kafka KRaft Design Proposal (KIP-500)](https://cwiki.apache.org/confluence/display/KAFKA/KIP-500%3A+Replace+ZooKeeper+with+a+Self-Managed+Metadata+Quorum)
  - [Official Apache Kafka Docker Configuration Guide](https://github.com/apache/kafka/blob/trunk/docker/examples/README.md)
