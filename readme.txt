카카오뱅크 사전코딩 테스트


코딩 과제
=======
- 실시간으로 다양한 규칙들의 위배 여부를 판단해 이상거래를 탐지하는 FDS(이상거래 탐지 시스템)를 개발하고자 한다.
- 이상거래 탐지에 필요한 정보가 Kafka를 통해 실시간 이벤트로 제공된다는 가정 하에, 아래의 요구사항을 준수한 프로그램을 Java로 구현하라.
- 1차 면접시 '규칙 A'에 대한 이상거래 탐지를 시연하고 동작 원리를 설명한다.

### 요구사항 ###
- Maven 기반 프로젝트
- '규칙 A'와 유사한 규칙들을 수용할 수 있는 룰 엔진 구현
- 구현한 룰 엔진을 이용해 '규칙 A' 구현
- Kafka를 실시간 이벤트 데이터 소스로 사용
- 실시간으로 유입되는 이벤트에 대해 구현한 룰 엔진을 이용해 100ms 이내에 이상거래를 탐지
- 탐지된 이상거래 내용을 탐지 즉시 Kafka 토픽 'fds.detections'에 발송
- 주어진 이벤트 데이터에 대한 가상의 부하를 만들어 룰 엔진을 테스트하는 테스트 코드 작성
- Mission Critical한 환경의 Production-Ready 수준으로 구현 (평가항목 참조)

### 규칙 A ###
- 7일 이내에 신규로 개설된 계좌로 90~100 만원이 입금된 후 2시간 이내에 출금되어 잔액이 1만원 이하가 되는 경우


### 이벤트 데이터 ###
아래의 유형으로 이벤트를 구현해 Kafka 토픽 'bank.events'에 실시간으로 Publish 한다.

* 범례
============================
p pk, not null
- not null
+ nullable (optional)
============================


* 계좌 신설 이벤트 (OpenAccount)
----------------------------
- 발생시각   : ts
- 고객번호   : customerNo
p 계좌번호   : accountNo

* 이체 이벤트 (Wire)
----------------------------
- 발생시각   : ts
- 고객번호   : customerNo
p 계좌번호   : accountNo
p 송금 계좌번호 : wireAccountNo
- 송금 이체전 계좌잔액 : beforeWireBalance
- 수취 은행  : abaNo
- 수취 계좌주 : receiverName
- 이체 금액  : wireAmount

* 입금 이벤트 (Deposit)
----------------------------
- 발생시각  : ts
- 고객번호  : customerNo
P 계좌번호  : accountNo
- 입금 금액 : depositAmount

* 출금 이벤트 (Withdrawal)
----------------------------
- 발생시각  : ts
- 고객번호  : customerNo
P 계좌번호  : accountNo
- 출금 금액 : withdrawalAmmount

** 계좌잔액 테이블 (balance)
- 발생시각  : ts
- 고객번호  : customerNo
P 계좌번호  : accountNo


### 제약사항 ###
- POJO 형태로 구현 (Spring, Guava, Lombok이나 오픈소스 Rule Engine 등의 프레임워크 일절 사용 금지)

### 평가항목 ###   
- Maven을 잘 활용했는지 여부   
- Kafka API를 제대로 사용했는지 여부   
- OOP 기반의 클래스 설계와 구조화가 잘 되었는지 여부   
- Production-Ready 수준 코드 구현 여부
  - 클린 코드
  - 오류없이 동작
  - Thread-Safety 준수
  - 성능 및 공간 최적화

### 제출방법 ###
- GitHub에 리포지토리를 생성하고 소스 코드를 등록한 뒤 해당 리포지토리 URL을 이메일로 제출


>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
>                        구현
>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

1.  -> 7일 이내 개설된 계좌로 90만원 이상 100만원 이하 입금
2.  -> 2시간 이내에 1만원 이하 시 

- 내부 구현 Memory DB 사용
  select 기능 (like where)
  key(pk), keyvalue(tuple) 형태 
  Persistency 기능 : storage save/load

- 제약사항
  insert/update/delete 가능 , transaction 없음, commit/rollback 없음.
  expiration 기능 없음.

신규계좌 정보 세션
입금 세션 (90만원 이상, 100만원 이하 금액)


Rule 과 RuleBook, RuleSession

  Rule 은 Event Type, Event Time

  RuleBook 의 조건이 모두 성립하는 경우
  alert 한다.
  
  RuleSession은 conccurentMap 으로 구현 또는 


Rule은 Rule 을 포함할수 있고,

Rule, Condition, 



# 공통 저장소
[account - storage]
//accountNO 
//opendate
balance

# rule 에 따라 이벤트 세션 생성
[현재부터 7일 전까지 계좌개설 이벤트]
accountNO
계좌 개설시각

[90만원 이상 100만원 이하 입금 이벤트 - session]
accountNO
입금 금액
입금시각

[잔액 1만원 이하 출금 이벤트]



### 규칙 A ###
- 7일 이내에 신규로 개설된 계좌로 90~100 만원이 입금된 후 2시간 이내에 출금되어 잔액이 1만원 이하가 되는 경우

출금이벤트 --> 잔액 1만원 이하 & 
+
입금 이벤트 --> 현재 시간기준 2시간 이내에 90-100만원 입금 &
+
계좌 신설 이벤트 --> 7일 이내 신규 개설


balanceDB


rulebooke에 정의되어 있는 이벤트에 맞는 sessionMap 생성

simple condition
>,=>, <,<=, ==, !=, equal, not equal, in

상위에서 지정
// join condition : 
// or, and



