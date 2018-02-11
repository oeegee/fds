package pe.exam.kafka.event;

import java.io.IOException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.fasterxml.jackson.databind.JsonNode;

/***
 *  입금 이벤트
 * @author m4600
 *
 */
public class WithdrawalEvent<K, V> extends Event<K, V> {

	private static final long serialVersionUID = 1L;
	
	//입금금액
	private long withdrawalAmount;
	
	public WithdrawalEvent(ConsumerRecord<String, String> record) {
		super(record);
		
		JsonNode jsonNode;
		try {
			jsonNode = mapper.readTree(value);
			Object o = jsonNode.get("withdrawalAmount");
			this.withdrawalAmount = Long.parseLong(String.valueOf(o));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public long getWithdrawalAmount() {
		return withdrawalAmount;
	}
	
	public void setWithdrawalAmount(long withdrawalAmount) {
		this.withdrawalAmount = withdrawalAmount;
	}
}
