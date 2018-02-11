package pe.exam.kafka.serde;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.junit.Test;

import pe.exam.kafka.TestLogger;

public class KafkaJsonDeserializerTest extends TestLogger{
	private static final Logger log = Logger.getLogger(KafkaJsonDeserializerTest.class.getName());

	@Test
	public void deserializeNullOrEmpty() {
		KafkaJsonDeserializer<Object> deserializer = new KafkaJsonDeserializer<>();

		Map<String, Object> props = new HashMap<>();
		props.put(KafkaJsonDeserializerConfig.JSON_KEY_TYPE, Object.class.getName());
		deserializer.configure(props, true);

		assertNull(deserializer.deserialize("topic", null));
		assertNull(deserializer.deserialize("topic", new byte[0]));
	}

	@Test
	public void deserializePojoKey() {
		KafkaJsonDeserializer<Foo> deserializer = new KafkaJsonDeserializer<Foo>();

		Map<String, Object> props = new HashMap<String, Object>();
		props.put(KafkaJsonDeserializerConfig.JSON_KEY_TYPE, Foo.class.getName());
		deserializer.configure(props, true);

		Foo foo = deserializer.deserialize(null, "{\"bar\":\"baz\"}".getBytes());
		assertNotNull(foo);
		assertEquals("baz", foo.getBar());
	}

	@Test
	public void deserializePojoValue() {
		KafkaJsonDeserializer<Foo> deserializer = new KafkaJsonDeserializer<Foo>();

		HashMap<String, Object> props = new HashMap<String, Object>();
		props.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, Foo.class.getName());
		deserializer.configure(props, false);

		Foo foo = deserializer.deserialize(null, "{\"bar\":\"baz\"}".getBytes());
		assertNotNull(foo);
		assertEquals("baz", foo.getBar());
	}

	@Test
	public void deserializeObject() {
		KafkaJsonDeserializer<Object> deserializer = new KafkaJsonDeserializer<Object>();

		HashMap<String, Object> props = new HashMap<String, Object>();
		props.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, Object.class.getName());
		deserializer.configure(props, false);

		assertEquals(45.3, deserializer.deserialize(null, "45.3".getBytes()));
		assertEquals(799, deserializer.deserialize(null, "799".getBytes()));
		assertEquals("hello", deserializer.deserialize(null, "\"hello\"".getBytes()));
		assertEquals(null, deserializer.deserialize(null, "null".getBytes()));
		assertEquals(Arrays.asList("foo", "bar"), deserializer.deserialize(null, "[\"foo\",\"bar\"]".getBytes()));

		Map<String, String> map = new HashMap<String, String>();
		map.put("foo", "bar");
		assertEquals(map, deserializer.deserialize(null, "{\"foo\":\"bar\"}".getBytes()));
	}

	public static class Foo {
		private String bar;

		public String getBar() {
			return bar;
		}

		public void setBar(String bar) {
			this.bar = bar;
		}
	}

}