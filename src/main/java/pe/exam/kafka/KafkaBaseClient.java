package pe.exam.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.log4j.Logger;

public class KafkaBaseClient {
	private static final Logger log = Logger.getLogger(KafkaBaseClient.class.getName());
	
	// config.properties
	Properties config;
	
	// kafka client properties
	protected static Properties kafkaProps = new Properties();

	public KafkaBaseClient() {
		loadProperties();
		log.debug("bootstrapServers: "+config.getProperty("bootstrapServers"));
		this.kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getProperty("bootstrapServers"));
	}

	private void loadProperties() {
		
		InputStream inStream = null;
		this.config = new Properties();

		try {
			String profile = System.getProperty("env");
			// expect local, dev, prd
			if(null == profile || (profile!="dev" && profile!="stg" && profile!="prd")) {
				inStream = ClassLoader.getSystemResourceAsStream("config.properties");
			}else {
				inStream = ClassLoader.getSystemResourceAsStream(profile+"/config.properties");
			}
			config.load(inStream);
			
			if(log.isDebugEnabled()) {
//				log.debug("  config.properties: " + config.toString());
				config.list(System.out);
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			if(null != inStream) {
				try {
					inStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			inStream = null;
		}
	}

}
