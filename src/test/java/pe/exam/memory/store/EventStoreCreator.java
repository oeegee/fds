package pe.exam.memory.store;

import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Test;

import pe.exam.kafka.TestLogger;
import pe.exam.memory.store.KeyType;
import pe.exam.memory.store.Table;

public class EventStoreCreator extends TestLogger{
	
	// for stopwatch
	long startTime;
	long loopStartTime;

	/*
	 * ### 규칙 A ### - 7일 이내에 신규로 개설된 계좌로 90~100 만원이 입금된 후 2시간 이내에 출금되어 잔액이 1만원 이하가
	 * 되는 경우
	 * 
	 * 
	 * 출금이벤트 --> 잔액 1만원 이하 & : withdrawalTable + 입금 이벤트 --> 현재 시간기준 2시간 이내에 90-100만원
	 * 입금 & : depositTable + 계좌 신설 이벤트 --> 7일 이내 신규 개설 : openAccountTable
	 * 
	 * + balance
	 * 
	 */
	// 계좌 개설
	public static Table openAccountTable;
	// 입금 이벤트
	public static Table depositTable;
	// 출금 이벤트
	public static Table withdrawalTable;
	// 송금 이벤트
	public static Table wireTable;
	
	// 계좌잔액 테이블
	public static Table balanceTable;

	
	public void initTable() {
		/*
		 * openAccountTable
		 * 계좌 신설 이벤트 (OpenAccount)
		 *  ----------------------------
		 *  - 발생시각   : ts
		 *  - 고객번호   : customerNo
		 *  p 계좌번호   : accountNo
		 */
		openAccountTable = new Table("openAccountTable", "ts customerNo accountNo", "Long String String", "accountNo");
		/* depositTable
		 * 입금 이벤트 (Deposit)
		 * ----------------------------
		 * - 발생시각  : ts
		 * - 고객번호  : customerNo
		 * P 계좌번호  : accountNo
		 * - 입금 금액 : depositAmount
		 */
		depositTable = new Table("depositTable", "ts customerNo accountNo depositAmount", "Long String String Integer", "accountNo");
		
		/* withdrawalTable
		 * 출금 이벤트 (Withdrawal)
		 * ----------------------------
		 * - 발생시각  : ts
		 * - 고객번호  : customerNo
		 * P 계좌번호  : accountNo
		 * - 출금 금액 : withdrawalAmmount
		 */
		withdrawalTable = new Table("withdrawalTable", "ts customerNo accountNo withdrawalAmmount", "Long String String Integer", "accountNo");
		
		/*wireTable
		 * 이체 이벤트 (Wire)
		 * ----------------------------
		 * - 발생시각   : ts
		 * - 고객번호   : customerNo
		 * p 송금 계좌번호 : wireAccountNo
		 * - 송금 이체전 계좌잔액 : beforeWireBalance
		 * - 수취 은행  : abaNo
		 * - 수취 계좌주 : receiverName
		 * - 이체 금액  : wireAmmount
		 * p 계좌번호   : accountNo
		 */
		wireTable = new Table("wireTable", "ts customerNo accountNo wireAccountNo beforeWireBalance abaNo receiverName wireAmmount", "Long String String String Integer String String Integer", "accountNo wireAccountNo");
		
		
		/* balance 
		 * table name, column, datatype (domain), key
		 * - 발생시각  : ts
		 * - 고객번호  : customerNo
		 * P 계좌번호  : accountNo
		 * - 잔액       : balance
		 */
		balanceTable = new Table("balanceTable", "ts customerNo accountNo balance", "Long String String Integer", "accountNo");
	}
	
	
	private void generateBalance(int loop) {
//		balance = new Table("balance", "ts customerNo accountNo balance", "Long String String Integer", "accountNo");
		long timestamp = Instant.now().toEpochMilli();
		long ts;
		
		String accountNo;
		String customerNo;
		Integer balance = 0;
		
		int printStep = 1000;
		startTime = System.nanoTime();
		
		for (int i = 0; i < loop; i += 2) {
			loopStartTime = System.nanoTime();
			if( i % printStep  == 0) {
				System.out.println("  >> generateBalance count: "+ i +"/"+loop + ".  elapsed time : "+ ((System.nanoTime() - loopStartTime) ) + "ns");
			}
			
			ts = ThreadLocalRandom.current().nextLong(timestamp, timestamp+10000000);
			accountNo = String.format("%014d", i);
			customerNo = accountNo;
			balance = ThreadLocalRandom.current().nextInt(100000000);
			balanceTable.insert(new Comparable<?>[] {ts , customerNo, accountNo, balance});

			ts = ThreadLocalRandom.current().nextLong(timestamp, timestamp+10000000);
			accountNo = String.format("%014d", i+1);
			customerNo = accountNo;
			balance = ThreadLocalRandom.current().nextInt(100000000);
			balanceTable.insert(new Comparable<?>[] {ts , customerNo, accountNo, balance});
//			balance = null;
		}
		System.out.println("  >> generateBalance count total: "+ loop +".  elapsed time : "+ ((System.nanoTime() - startTime ) / 1000000000 ) +"s");

	}
	
	private void generateOpenAccount(int loop) {
//		openAccountTable = new Table("openAccountTable", "ts customerNo accountNo", "Long String String", "accountNo");
		long timestamp = Instant.now().toEpochMilli();
		long ts;
		
		String accountNo;
		String customerNo;
		
		int printStep = 1000;
		startTime = System.nanoTime();
		
		for (int i = 0; i < loop; i += 2) {
			loopStartTime = System.nanoTime();
			if( i % printStep  == 0) {
				System.out.println("  >> generateOpenAccount count: "+ (i) +"/"+loop + ".  elapsed time : "+ ((System.nanoTime() - loopStartTime) ) +"ns");
			}
			
			ts = ThreadLocalRandom.current().nextLong(timestamp, timestamp+10000000);
			accountNo = String.format("%014d", i);
			customerNo = accountNo;
			openAccountTable.insert(new Comparable<?>[] {ts , customerNo, accountNo});

			ts = ThreadLocalRandom.current().nextLong(timestamp, timestamp+10000000);
			accountNo = String.format("%014d", i+1);
			customerNo = accountNo;
			openAccountTable.insert(new Comparable<?>[] {ts , customerNo, accountNo});
		}
		System.out.println("  >> generateOpenAccount count total: "+ loop +".  elapsed time : "+ ((System.nanoTime() - startTime ) / 1000000000 ) +"s");
	}
	
	private void generateDepositTable(int loop) {
		//	depositTable = new Table("depositTable", "ts customerNo accountNo depositAmount", "Long String String Integer", "accountNo");
		long timestamp = Instant.now().toEpochMilli();
		long ts;
		
		String accountNo;
		String customerNo;
		Integer depositAmount = 0;
		Integer balance = 0;
		
		int printStep = 1000;
		startTime = System.nanoTime();
		
		for (int i = 0; i < loop; i += 2) {
			loopStartTime = System.nanoTime();
			if( i % printStep  == 0) {
				System.out.println("  >> generateDepositTable count: "+ i +"/"+loop + ".  elapsed time : "+ ((System.nanoTime() - loopStartTime) ) +"ns");
			}
			
			ts = ThreadLocalRandom.current().nextLong(timestamp, timestamp+10000000);
			accountNo = String.format("%014d", i);
			customerNo = accountNo;
			depositAmount = ThreadLocalRandom.current().nextInt(10000000);
			balance = (Integer) balanceTable.getValue(new KeyType(accountNo), "balance");
			if(null != balance) {
				if(balance >= depositAmount) {
					balance = balance + depositAmount;
				}else {
					depositAmount = balance;
					balance = 0;
				}
				depositTable.insert(new Comparable<?>[] {ts , customerNo, accountNo, depositAmount});
				balanceTable.upsert(new Comparable<?>[] {ts , customerNo, accountNo, balance});
				// deposit end
			}
			
			ts = ThreadLocalRandom.current().nextLong(timestamp, timestamp+10000000);
			accountNo = String.format("%014d", i+1);
			customerNo = accountNo;
			balance = (Integer) balanceTable.getValue(new KeyType(accountNo), "balance");
			if(null != balance) {
				if(balance >= depositAmount) {
					balance = balance - depositAmount;
				}else {
					depositAmount = balance;
					balance = 0;
				}
				depositTable.upsert(new Comparable<?>[] {ts , customerNo, accountNo, depositAmount});
				balanceTable.upsert(new Comparable<?>[] {ts , customerNo, accountNo, balance});
				// deposit end
			}
		}
		System.out.println("  >> generateDepositTable count total: "+ loop +".  elapsed time : "+ ((System.nanoTime() - startTime ) / 1000000000 ) +"s");
		depositTable.print();
//		depositTable = null;
	}
	
	private void generateWithdrawalTable(int loop) throws ClassNotFoundException, IOException {
		// 	withdrawalTable = new Table("withdrawalTable", "ts customerNo accountNo withdrawalAmmount", "Long String String Integer", "accountNo");

		// init balanceTable
		loadBalanceTable();
		
		long timestamp = Instant.now().toEpochMilli();
		long ts;
		
		String accountNo;
		String customerNo;
		Integer withdrawalAmount = 0;
		Integer balance = 0;
		
		int printStep = 1000;
		startTime = System.nanoTime();
		
		for (int i = 0; i < loop; i += 2) {
			loopStartTime = System.nanoTime();
			if( i % printStep  == 0) {
				System.out.println("  >> generateWithdrawalTable count: "+ i +"/"+loop + ".  elapsed time : "+ ((System.nanoTime() - loopStartTime) ) +"ns");
			}
			
			// withdrawal start
			ts = ThreadLocalRandom.current().nextLong(timestamp, timestamp+10000000);
			accountNo = String.format("%014d", i);
			customerNo = accountNo;
			withdrawalAmount = ThreadLocalRandom.current().nextInt(10000000);
			//check withdrawal availability with balance.
			balance = (Integer) balanceTable.getValue(new KeyType(accountNo), "balance");
			
			if(null != balance) {
				if(balance >= withdrawalAmount) {
					balance = balance - withdrawalAmount;
				}else {
					withdrawalAmount = balance;
					balance = 0;
				}
				withdrawalTable.upsert(new Comparable<?>[] {ts , customerNo, accountNo, withdrawalAmount});
				balanceTable.upsert(new Comparable<?>[] {ts , customerNo, accountNo, balance});
				// withdrawal end
			}
			
			// withdrawal start
			ts = ThreadLocalRandom.current().nextLong(timestamp, timestamp+10000000);
			accountNo = String.format("%014d", i+1);
			customerNo = accountNo;
			withdrawalAmount = ThreadLocalRandom.current().nextInt(10000000);
			//check withdrawal availability with balance.
			balance = (Integer) balanceTable.getValue(new KeyType(accountNo), "balance");
			if(null != balance) {
				if(balance >= withdrawalAmount) {
					balance = balance - withdrawalAmount;
				}else {
					withdrawalAmount = balance;
					balance = 0;
				}
				withdrawalTable.upsert(new Comparable<?>[] {ts , customerNo, accountNo, withdrawalAmount});
				balanceTable.upsert(new Comparable<?>[] {ts , customerNo, accountNo, balance});
				// withdrawal end
			}
		}
		System.out.println("  >> generateWithdrawalTable count total: "+ loop +".  elapsed time : "+ ((System.nanoTime() - startTime ) / 1000000000 ) +"s");
		
		withdrawalTable.print();
//		withdrawalTable = null;
	}
	
	private void generateWirelTable(int loop) throws ClassNotFoundException, IOException {
		// wireTable = new Table("wireTable", "ts customerNo accountNo wireAmmount", "Long String String Integer", "accountNo");
		
		loadBalanceTable();
		
		long timestamp = Instant.now().toEpochMilli();
		
		long ts;
		String customerNo;
		String wireCustomerNo;
		String accountNo;
		String wireAccountNo;
		Integer beforeWireBalance = 0;
		String abaNo;
		String receiverName;
		Integer wireAmount = 0;
		
		Integer balance = 0;
		Integer otherBalance = 0;
		
		int printStep = 1000;
		startTime = System.nanoTime();
		
		for (int i = 0; i < loop; i += 2) {
			loopStartTime = System.nanoTime();
			if( i % printStep  == 0) {
				System.out.println("  >> generateWirelTable() count: "+ i +"/"+loop + ".  elapsed time : "+ ((System.nanoTime() - loopStartTime) )+"ns");
			}
			// withdrawal start
			ts = ThreadLocalRandom.current().nextLong(timestamp, timestamp+10000000);
			accountNo = String.format("%014d", i);
			customerNo = accountNo;
			wireAccountNo = String.format("%014d", ThreadLocalRandom.current().nextInt(loop));
			wireCustomerNo = wireAccountNo;
			wireAmount = ThreadLocalRandom.current().nextInt(10000000);
			//check withdrawal availability with balance.
			balance = (Integer) balanceTable.getValue(new KeyType(accountNo), "balance");
			beforeWireBalance = balance;
			abaNo =  UUID.randomUUID().toString().substring(20);
			receiverName = UUID.randomUUID().toString().substring(25);
			otherBalance = (Integer) balanceTable.getValue(new KeyType(wireAccountNo), "balance");
			
			if(null != balance) {
				if(balance >= wireAmount) {
					balance = balance - wireAmount;
				}else {
					wireAmount = balance;
					balance = 0;
				}
				otherBalance = (otherBalance != null)? wireAmount : otherBalance + wireAmount;
				
				// sender
				wireTable.upsert(new Comparable<?>[] {ts , customerNo, accountNo, wireAccountNo, beforeWireBalance, abaNo, receiverName, wireAmount});
				balanceTable.upsert(new Comparable<?>[] {ts , customerNo, accountNo, balance});

				// receiver
				depositTable.upsert(new Comparable<?>[] {ts , wireCustomerNo, wireAccountNo, wireAmount});
				balanceTable.upsert(new Comparable<?>[] {ts , wireCustomerNo, wireAccountNo, otherBalance});
			} 
			
			// withdrawal start
			ts = ThreadLocalRandom.current().nextLong(timestamp, timestamp+10000000);
			accountNo = String.format("%014d", i+1);
			customerNo = accountNo;
			wireAccountNo = String.format("%014d", ThreadLocalRandom.current().nextInt(loop));
			wireCustomerNo = wireAccountNo;
			wireAmount = ThreadLocalRandom.current().nextInt(10000000);
			//check withdrawal availability with balance.
			balance = (Integer) balanceTable.getValue(new KeyType(accountNo), "balance");
			beforeWireBalance = balance;
			abaNo =  UUID.randomUUID().toString().substring(20);
			receiverName = UUID.randomUUID().toString().substring(25);
			otherBalance = (Integer) balanceTable.getValue(new KeyType(wireAccountNo), "balance");
			
			if(null != balance) {
				if(balance >= wireAmount) {
					balance = balance - wireAmount;
				}else {
					wireAmount = balance;
					balance = 0;
				}
				otherBalance = (otherBalance != null)? wireAmount : otherBalance + wireAmount;
				
				// sender
				wireTable.upsert(new Comparable<?>[] {ts , customerNo, accountNo, wireAccountNo, beforeWireBalance, abaNo, receiverName, wireAmount});
				balanceTable.upsert(new Comparable<?>[] {ts , customerNo, accountNo, balance});

				// receiver
				depositTable.upsert(new Comparable<?>[] {ts , wireCustomerNo, wireAccountNo, wireAmount});
				balanceTable.upsert(new Comparable<?>[] {ts , wireCustomerNo, wireAccountNo, otherBalance});
			}
		}
		System.out.println("  >> generateWirelTable count total: "+ loop +".  elapsed time : "+ ((System.nanoTime() - startTime ) / 1000000000 ) +"s");
	}
	

	public void saveBalanceTable() {
		generateBalance(100000);
		startTime = Instant.now().toEpochMilli();
		balanceTable.print();
		System.out.println("  balanceTable.print() elapsed time : "+ (Instant.now().toEpochMilli() - startTime) +"ms");
		
		startTime = Instant.now().toEpochMilli();
		balanceTable.save();
		System.out.println("  balanceTable.save() elapsed time : "+ (Instant.now().toEpochMilli() - startTime) +"ms");
		
		assertNull(null, null);
	}
	
	
	public void loadBalanceTable() throws ClassNotFoundException, IOException {
		balanceTable = Table.load("balanceTable");
		System.out.println("  balanceTable.load() elapsed time : "+ (Instant.now().toEpochMilli() - startTime) +"ms");
		
		balanceTable.print();
		
		assertNull(null, null);
	}
	
	public void saveOpenAccountTable() {
		generateOpenAccount(100000);
		startTime = Instant.now().toEpochMilli();
		openAccountTable.print();
		System.out.println("  openAccountTable.print() elapsed time : "+ (Instant.now().toEpochMilli() - startTime) +"ms");
		
		startTime = Instant.now().toEpochMilli();
		openAccountTable.save();
		System.out.println("  openAccountTable.save() elapsed time : "+ (Instant.now().toEpochMilli() - startTime) +"ms");
		
		assertNull(null, null);
	}
	
	@Test
	public void loadOpenAccountTable() throws ClassNotFoundException, IOException {
		startTime = Instant.now().toEpochMilli();
		openAccountTable = Table.load("openAccountTable");
		System.out.println("  openAccountTable.load() elapsed time : "+ (Instant.now().toEpochMilli() - startTime) +"ms");
		
		openAccountTable.print();
		
		assertNull(null, null);
	}
	
	public void saveDepositTable() {
		generateDepositTable(100000);
		
		startTime = Instant.now().toEpochMilli();
		depositTable.print();
		System.out.println("  depositTable.print() elapsed time : "+ (Instant.now().toEpochMilli() - startTime) +"ms");
		
		startTime = Instant.now().toEpochMilli();
		depositTable.save();
		System.out.println("  depositTable.save() elapsed time : "+ (Instant.now().toEpochMilli() - startTime) +"ms");
		
		assertNull(null, null);
	}
	
	public void loadDepositTable() throws ClassNotFoundException, IOException {
		startTime = Instant.now().toEpochMilli();
		depositTable = Table.load("depositTable");
		System.out.println("  depositTable.load() elapsed time : "+ (Instant.now().toEpochMilli() - startTime) +"ms");
		
		depositTable.print();
		
		assertNull(null, null);
	}
	
	@Test
	public void saveWithdrawalTable() throws ClassNotFoundException, IOException {
		generateWithdrawalTable(100000);
		
		startTime = Instant.now().toEpochMilli();
		withdrawalTable.print();
		System.out.println("  withdrawalTable.print() elapsed time : "+ (Instant.now().toEpochMilli() - startTime) +"ms");
		
		startTime = Instant.now().toEpochMilli();
		withdrawalTable.save();
		System.out.println("  withdrawalTable.save() elapsed time : "+ (Instant.now().toEpochMilli() - startTime) +"ms");
		
		assertNull(null, null);
		
	}
	
	public void loadWithdrawalTable() throws ClassNotFoundException, IOException {
		startTime = Instant.now().toEpochMilli();
		withdrawalTable = Table.load("WithdrawalTable");
		System.out.println("  WithdrawalTable.load() elapsed time : "+ (Instant.now().toEpochMilli() - startTime) +"ms");
		
		withdrawalTable.print();
		assertNull(null, null);
	}
	
	public void saveWireTable() throws ClassNotFoundException, IOException {
		generateWirelTable(100000);
		
		startTime = Instant.now().toEpochMilli();
		wireTable.print();
		System.out.println("  wireTable.print() elapsed time : "+ (Instant.now().toEpochMilli() - startTime) +"ms");
		
		startTime = Instant.now().toEpochMilli();
		wireTable.save();
		System.out.println("  v.save() elapsed time : "+ (Instant.now().toEpochMilli() - startTime) +"ms");
		
		assertNull(null, null);
	}
	
	public void loadWireTable() throws ClassNotFoundException, IOException {
		startTime = Instant.now().toEpochMilli();
		wireTable = Table.load("wireTable");
		System.out.println("  wireTable.load() elapsed time : "+ (Instant.now().toEpochMilli() - startTime) +"ms");
		
		wireTable.print();
		assertNull(null, null);
	}


	public static void main(String[] args) {
		long startTime = Instant.now().toEpochMilli();
		
		EventStoreCreator main = new EventStoreCreator();
		try {
			main.initTable();
			main.saveOpenAccountTable();
			main.saveBalanceTable();
			main.saveDepositTable();
			main.saveWithdrawalTable();
			main.saveWireTable();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

//		main.loadBalanceTable();
//		main.loadOpenAccountTable();
//		main.loadDepositTable();rr
//		main.loadWithdrawalTable();
//		main.loadWireTable();
		
		System.out.println("balance accountNo: 00000000000000: " + EventStoreCreator.balanceTable.select(new KeyType("00000000000000")).printString());
		System.out.println("balance accountNo: 00000000007964: " + EventStoreCreator.balanceTable.select(new KeyType("00000000007964")).printString());
		
		System.out.println("  RuleMain.main elapsed time : "+ (Instant.now().toEpochMilli() - startTime) +"ms");
		
		// load Table, table
//		openAccountTable = Table.load("openAccountTable");
//		depositTable = Table.load("depositTable");
//		withdrawalTable = Table.load("withdrawalTable");
//		wireTable = Table.load("wireTable");
//		balanceTable = Table.load("balanceTable");
//		
//		openAccountTable.print();
//		balanceTable.print();
	}

}
