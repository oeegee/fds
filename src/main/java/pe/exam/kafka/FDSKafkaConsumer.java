package pe.exam.kafka;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import pe.exam.fds.store.EventStore;
import pe.exam.kafka.event.Event;
import pe.exam.kafka.event.WithdrawalEvent;
import pe.exam.kafka.rule.RuleInstance;

/**
 * https://docs.confluent.io/4.0.0/connect/quickstart.html#goal
 * 
 * @author Jeon Deuk Jin
 * @param <K>
 * @param <V>
 *
 */
public class FDSKafkaConsumer<K, V> extends KafkaBaseClient implements Runnable {

	private static final Logger log = Logger.getLogger(FDSKafkaConsumer.class.getName());
	
	private final AtomicBoolean closed = new AtomicBoolean(false);
	org.apache.kafka.clients.consumer.KafkaConsumer<K, V> consumer;

	public FDSKafkaConsumer() {
		
//		kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		kafkaProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
		kafkaProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
		kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, this.config.getProperty("key.converter"));
		kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, this.config.getProperty("value.converter"));
		kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "fdsgroup1");
		kafkaProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "fdsgroup1");
		kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		if(log.isDebugEnabled()) {
			log.debug("  ## kafkaProps: " + kafkaProps.toString());
		}
	}
	
	public void run() {
		try {
			log.debug(" ======== kafka consumer start! --> topic["+this.config.getProperty("event.topic")+"]");
			consumer.subscribe(Arrays.asList(this.config.getProperty("event.topic")));
			final int giveUp = 100;
			int noRecordsCount = 0;
			
			ObjectMapper mapper = new ObjectMapper();
			JsonNode jsonNode;

			while (!closed.get()) {
				ConsumerRecords<K, V> records = consumer.poll(100);
				
				if (records.count() == 0) {
					noRecordsCount++;
					if (noRecordsCount > giveUp)
						break;
					else
						continue;
				}

				records.forEach(record -> {
					if (log.isDebugEnabled()) {
						log.debug(String.format("Consumer Record:(%d, %s, %d, %d)\n", record.key(), record.value(),
								record.partition(), record.offset()));
					}
					// make event object
					Event<String, String> event = new WithdrawalEvent(record);
					// save to EventStore
					saveEventStore(event);
					// check rule
					
				});

//				consumer.commitAsync();
			}
		} catch (WakeupException e) {
			consumer.unsubscribe();
			consumer.close();
			consumer = null;
			log.warn("WakeupException: Consumer Unsubscribe");
			
			if (!closed.get()) {
				throw e;
			}
		} finally {
			consumer.close();
		}
	}
	
	public boolean saveEventStore(Event event) {
		boolean result = false;
/*		
		if(event.getKey().equals("")) {
			EventStore.balanceTable.upsert("");
			result = true;
		}else if() {
			EventStore.balanceTable.upsert();
			result = true;
		}else if() {
			EventStore.balanceTable.upsert(upsertTuple);
			result = true;
		}else if() {
			EventStore.balanceTable.upsert(upsertTuple);
			result = true;
		}
		*/
		return result;
	}

	// Shutdown hook which can be called from a separate thread
	public void shutdown() {
		closed.set(true);
		consumer.wakeup();
	}
}
