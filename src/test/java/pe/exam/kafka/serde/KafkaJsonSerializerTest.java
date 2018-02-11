package pe.exam.kafka.serde;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import pe.exam.kafka.TestLogger;

public class KafkaJsonSerializerTest extends TestLogger {
	private static final Logger log = Logger.getLogger(KafkaJsonDeserializerTest.class.getName());
	
	private ObjectMapper objectMapper = new ObjectMapper();
	private KafkaJsonSerializer<Object> serializer;

	@Before
	public void setup() {
		serializer = new KafkaJsonSerializer<>();
		serializer.configure(Collections.<String, Object>emptyMap(), false);
	}

	@Test
	public void serializeNull() {
		assertNull(serializer.serialize("foo", null));
	}

	@Test
	public void serialize() throws Exception {
		Map<String, Object> message = new HashMap<>();
		message.put("foo", "bar");
		message.put("baz", 354.99);

		byte[] bytes = serializer.serialize("foo", message);

		Object deserialized = this.objectMapper.readValue(bytes, Object.class);
		assertEquals(message, deserialized);
	}
}
