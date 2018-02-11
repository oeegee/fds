package pe.exam.fds.store;

import org.apache.log4j.Logger;

import pe.exam.memory.store.Table;

public class EventStore {
	private static final Logger log = Logger.getLogger(EventStore.class.getName());
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

	// constructor
	public EventStore() {
	}
	
	public static void initTable() {
		String tableName;
		/*
		 * openAccountTable
		 * 계좌 신설 이벤트 (OpenAccount)
		 *  ----------------------------
		 *  - 발생시각   : ts
		 *  - 고객번호   : customerNo
		 *  p 계좌번호   : accountNo
		 */
		if(null == openAccountTable) {
			tableName = "openAccountTable";
			try {
				openAccountTable= Table.load(tableName);
			} catch (Exception e) {
				log.info("load table error [: " + tableName + "]");
				log.info("create new table [: " + tableName + "]");
				openAccountTable = new Table(tableName, "ts customerNo accountNo", "Long String String", "accountNo");
				log.info("created table [: " + tableName + "]");
			}
			log.debug(openAccountTable.printString());
		}
		
		/* depositTable
		 * 입금 이벤트 (Deposit)
		 * ----------------------------
		 * - 발생시각  : ts
		 * - 고객번호  : customerNo
		 * P 계좌번호  : accountNo
		 * - 입금 금액 : depositAmount
		 */
		if(null == depositTable) {
			tableName = "depositTable";
			try {
				depositTable= Table.load(tableName);
			} catch (Exception e) {
				log.info("load table error [: " + tableName + "]");
				log.info("create new table [: " + tableName + "]");
				depositTable = new Table(tableName, "ts customerNo accountNo depositAmount", "Long String String Integer", "accountNo");
				log.info("created table [: " + tableName + "]");
			}
			log.debug(depositTable.printString());
		}
		
		/* withdrawalTable
		 * 출금 이벤트 (Withdrawal)
		 * ----------------------------
		 * - 발생시각  : ts
		 * - 고객번호  : customerNo
		 * P 계좌번호  : accountNo
		 * - 출금 금액 : withdrawalAmmount
		 */
		tableName = "withdrawalTable";
		if(null == withdrawalTable) {
			try {
				withdrawalTable= Table.load(tableName);
			} catch (Exception e) {
				log.info("load table error [: " + tableName + "]");
				log.info("create new table [: " + tableName + "]");
				withdrawalTable = new Table(tableName, "ts customerNo accountNo withdrawalAmmount", "Long String String Integer", "accountNo");
				log.info("created table [: " + tableName + "]");
			}
			log.debug(withdrawalTable.printString());
		}
		
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
		if(null == wireTable) {
			tableName = "wireTable";
			try {
				wireTable= Table.load(tableName);
			} catch (Exception e) {
				log.warn("load table error [: " + tableName + "]");
				log.warn("create new table [: " + tableName + "]");
				wireTable = new Table(tableName, "ts customerNo accountNo wireAccountNo beforeWireBalance abaNo receiverName wireAmmount", "Long String String String Integer String String Integer", "accountNo wireAccountNo");
			}
			log.debug(wireTable.printString());
		}
		
		
		/* balance 
		 * table name, column, datatype (domain), key
		 * - 발생시각  : ts
		 * - 고객번호  : customerNo
		 * P 계좌번호  : accountNo
		 * - 잔액       : balance
		 */
		if(null == balanceTable) {
			tableName = "balanceTable";
			try {
				balanceTable= Table.load(tableName);
			} catch (Exception e) {
				log.warn("load table error [: " + tableName + "]");
				log.warn("create new table [: " + tableName + "]");
				balanceTable = new Table(tableName, "ts customerNo accountNo balance", "Long String String Integer", "accountNo");
			}
			log.debug(balanceTable.printString());
		}
	}

}
