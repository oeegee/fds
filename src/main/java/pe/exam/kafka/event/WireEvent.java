package pe.exam.kafka.event;

import java.io.IOException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * 이체 이벤트
 * @author m4600
 *
 */
public class WireEvent<K, V> extends Event<K, V> {

	private static final long serialVersionUID = 1L;

	// 송금 계좌번호
	// wireAccountNO
	private String wireAccountNO; 

	// 송금 전 잔액
	private long beforeWireBalance;
	
	// 수취은행코드
	private String abaNO;
	
	// 수취계좌주
	private String receiverName;
	
	// 이체금액
	private long wireAmount;

	public WireEvent(ConsumerRecord<String, String> record) {
		super(record);
		
		try {
			jsonNode = mapper.readTree(value);
			this.wireAccountNO = jsonNode.get("wireAccountNO").toString();
			this.beforeWireBalance = Long.parseLong(jsonNode.get("beforeWireBalance").toString());
			this.abaNO = jsonNode.get("abaNO").toString();
			this.receiverName = jsonNode.get("receiverName").toString();
			this.wireAmount = Long.parseLong(jsonNode.get("wireAmount").toString());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public long getBeforeWireBalance() {
		return beforeWireBalance;
	}

	public void setBeforeWireBalance(long beforeWireBalance) {
		this.beforeWireBalance = beforeWireBalance;
	}

	public String getAbaNO() {
		return abaNO;
	}

	public void setAbaNO(String abaNO) {
		this.abaNO = abaNO;
	}

	public String getReceiverName() {
		return receiverName;
	}

	public void setReceiverName(String receiverName) {
		this.receiverName = receiverName;
	}

	public long getWireAmount() {
		return wireAmount;
	}

	public void setWireAmount(long wireAmount) {
		this.wireAmount = wireAmount;
	}
	
	
}
