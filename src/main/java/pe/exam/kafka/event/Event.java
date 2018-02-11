package pe.exam.kafka.event;

import java.io.Serializable;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import kafka.utils.json.JsonObject;

//import org.apache.kafka.connect.runtime.distributed.DistributedConfig;



public abstract class Event<K, V> implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	transient String key;
	// json record value
//	transient JsonObject value;
	transient String value;
	// event type
	private String eventType;
	
	// kafka offset
	private long offset;
	
	// kafka patition
	private int partition;
	
	//발생시각 kafka timestamp
	private long timestamp;
	
	// 고객번호
	private String customerID;
	
	// 계좌번호
	private String accountNO;
	
	// object converter
	transient ObjectMapper mapper = new ObjectMapper();
	transient JsonNode jsonNode = null;
	
	public Event(ConsumerRecord<String, String> record) {
		this.key = record.key();
		this.eventType = (String) record.key();
//		this.value = new JsonObject((ObjectNode) record.value());
		this.value = record.value();
		this.offset = record.offset();
		this.partition = record.partition();
		this.timestamp = record.timestamp();
	}
	
	public String getKey() {
		return this.key;
	}
	
	// getter and setter method
	public String getEventType() {
		return eventType;
	}
	
	public void setEventType(String eventType) {
		this.eventType = eventType;
	}
	
	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	public int getPartition() {
		return partition;
	}

	public void setPartition(int partition) {
		this.partition = partition;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public String getCustomerID() {
		return customerID;
	}

	public void setCustomerID(String customerID) {
		this.customerID = customerID;
	}

	public String getAccountNO() {
		return accountNO;
	}

	public void setAccountNO(String accountNO) {
		this.accountNO = accountNO;
	}
}
