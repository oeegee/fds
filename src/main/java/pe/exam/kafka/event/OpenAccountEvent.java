package pe.exam.kafka.event;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/***
 * 계좌 신설 이벤트
 * @author m4600
 *
 */

public class OpenAccountEvent<K, V> extends Event<K, V> {
	
	private static final long serialVersionUID = 1L;

	public OpenAccountEvent(ConsumerRecord<String, String> record) {
		super(record);
	}

}
