package pe.exam.kafka;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;

public class TestConsumer extends TestLogger {

	MockConsumer<String, String> mockConsumer;

	@Before
	public void setUp() {
		mockConsumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
	}

	@Test
	public void testConsumer() throws IOException {
		// This is YOUR consumer object
		FDSKafkaConsumer<String, String> testConsumer = new FDSKafkaConsumer();
//		testConsumer.consumer = consumer;

		mockConsumer.assign(Arrays.asList(new TopicPartition("my_topic", 0)));

		HashMap<TopicPartition, Long> beginningOffsets = new HashMap<>();
		beginningOffsets.put(new TopicPartition("my_topic", 0), 0L);
		mockConsumer.updateBeginningOffsets(beginningOffsets);

		mockConsumer.addRecord(new ConsumerRecord<String, String>("my_topic", 0, 0L, "mykey", "myvalue0"));
		mockConsumer.addRecord(new ConsumerRecord<String, String>("my_topic", 0, 1L, "mykey", "myvalue1"));
		mockConsumer.addRecord(new ConsumerRecord<String, String>("my_topic", 0, 2L, "mykey", "myvalue2"));
		mockConsumer.addRecord(new ConsumerRecord<String, String>("my_topic", 0, 3L, "mykey", "myvalue3"));
		mockConsumer.addRecord(new ConsumerRecord<String, String>("my_topic", 0, 4L, "mykey", "myvalue4"));

//		testConsumer.consume();
	}
	

	
}
