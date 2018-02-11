# fds
fds code

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
