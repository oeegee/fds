package pe.exam.kafka.rule;

import java.time.Instant;
import java.util.function.Predicate;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import pe.exam.fds.store.EventStore;
import pe.exam.kafka.event.Event;
import pe.exam.kafka.event.WithdrawalEvent;
import pe.exam.memory.store.Table;

/**
 * 입금 이벤트
 * @author Jeon DeukJin
 * @param <T>
 *
 */
public class Rule<T> {
		
	private String accountNo;
	
	/**
	 * set accountNo
	 * @param accountNo
	 */
	public void setAccountNo(String accountNo) {
		this.accountNo = accountNo;
	}
	
	/**
	 * 
	 * ### 규칙 A ### 
	 * - 7일 이내에 신규로 개설된 계좌로 90~100 만원이 입금된 후  2시간 이내에 출금되어 잔액이 1만원 이하가 되는 경우 
	 */
	// 1. event : WithdrawalEvent
	public final Predicate<? super Event> A_P1 = t -> {
		try {
			return t.getClass().isAssignableFrom(Class.forName("pe.exam.kafka.event.WithdrawalEvent"));
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return false;
	};
	
	// 2. 2시간 이내 인출  (1시간 * 2) 
	public final Predicate<Comparable<?>[]> A_P2 = t -> (t[2].equals(accountNo)
			&& (((Long) Instant.now().toEpochMilli()) - (Long) t[0] <= 3600 * 2));
	
	// 3. 잔액 1만원 이하
	public final Predicate<Comparable<?>[]> A_P3 = t -> (t[2].equals(accountNo)
			&& ((Integer) t[EventStore.balanceTable.col("balance")] <= 2000000));
	
	// 4. 90만원 이상 100만원 이하 입금 2시간 이내 
	public final Predicate<Comparable<?>[]>	A_P4	= t -> (t[2].equals(accountNo)
			&& ((Long) Instant.now().toEpochMilli() - (Long) t[EventStore.depositTable.col("ts")]) <= 3600000 * 2
			&& ((Integer) t[EventStore.depositTable.col("depositAmount")] >= 900000)
			&& ((Integer) t[EventStore.depositTable.col("depositAmount")] <= 1000000));

	// 5. 7일 이내 신규 계좌 개설
	public final Predicate<Comparable<?>[]> A_P5 = t -> (
			t[2].equals(accountNo)
			&& (Long) Instant.now().toEpochMilli() - (Long) t[EventStore.openAccountTable.col("ts")] <= 3600000 * 24 * 7);
	
	// ### 규칙 B ### 
	
	public static void main(String... args) {
		// 이벤트 스토어 준비
		EventStore.initTable();
		
		// 인출 이벤트 생성
		ConsumerRecord<String, String> record = new ConsumerRecord<String, String>("bank.events", 0, 0, "withdrawalTable",
				"{\"ts\":1518247546491,\"customerNo\":\"00000000000030\",\"accountNo\":\"00000000000030\",\"withdrawalAmount\":916829}");
		Event event = new WithdrawalEvent(record);
		
//		 * - 7일 이내에 신규로 개설된 계좌로 90~100 만원이 입금된 후 2시간 이내에 출금되어 잔액이 1만원 이하가 되는 경우 
		String accountNo = "00000000000019";

		Rule rule = new Rule();
		Table t;
		
		// 1. 인출 이벤트 검사
		System.out.println("===========> 1. 이체 이벤트 발생 : WithdrawalEvent");
		Object check = rule.A_P1.test(event);
		System.out.println(" Rule.A_P1.test: "+ check);
		
		// 2. 2시간 이내 인출
		System.out.println("===========> 2. 2시간 이내 인출");
		t = EventStore.withdrawalTable.select(rule.A_P2);
		t.print();
		check = t.isExist();
		System.out.println(" Rule.A_P2.test: "+ check);
		/*
		|----------------------------------------------------------------------------------|
		|                   ts          customerNo           accountNo   withdrawalAmmount |
		|               [Long]            [String]            [String]           [Integer] |
		|----------------------------------------------------------------------------------|
		|        1518345936162      00000000000019      00000000000019             1163414 |
		|----------------------------------------------------------------------------------|*/
		
		// 3. 잔고 1만원 이하 검사
		System.out.println("===========> 3. 잔고 1만원 이하");
		t = EventStore.balanceTable.select(rule.A_P3);
		t.print();
		check = t.isExist();
		System.out.println(" Rule.A_P3.test: "+ check);
		/*
		|----------------------------------------------------------------------------------|
		|                   ts          customerNo           accountNo             balance |
		|               [Long]            [String]            [String]           [Integer] |
		|----------------------------------------------------------------------------------|
		|        1518349298724      00000000000019      00000000000019             1163414 |
		|----------------------------------------------------------------------------------|*/
		
		// 4. 입금 2시간 이내 90만원 이상 100만원 미만
		System.out.println("===========> 4. 입금 2시간 이내 90만원 이상 100만원 미만");
		t = EventStore.depositTable.select(rule.A_P4);
		t.print();
		check = t.isExist();
		System.out.println(" Rule.A_P4.test: "+ check);
		
		// 5. 7일 이내 계좌 개설
		System.out.println("===========> 5. 7일 이내 신규계좌 개설");
		t = EventStore.openAccountTable.select(rule.A_P5);
		t.print();
		check = t.isExist();
		System.out.println(" Rule.A_P5.test: "+ check);
	}
}
