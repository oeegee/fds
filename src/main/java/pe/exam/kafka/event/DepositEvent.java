package pe.exam.kafka.event;

import java.io.IOException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * 입금 이벤트
 * @author m4600
 *
 */
public class DepositEvent<K, V> extends Event<K, V> {

	private static final long serialVersionUID = 1L;
	
	private long depositAmount;

	
	public DepositEvent(ConsumerRecord<String, String> record) {
		super(record);
		
		try {
			jsonNode = mapper.readTree(value);
			this.depositAmount = Long.parseLong(jsonNode.get("depositAmount").toString());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public long getDepositAmount() {
		return depositAmount;
	}

	public void setDepositAmount(long depositAmount) {
		this.depositAmount = depositAmount;
	}
}
