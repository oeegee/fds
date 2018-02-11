package pe.exam.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class TestLogger {
	private static final Logger log = Logger.getLogger("Test");
	public static Properties props = null;

	public TestLogger() {
		if(null == props) {
			props = new Properties();
			InputStream inStream = null;
			try {
				inStream = Thread.currentThread().getContextClassLoader().getSystemResourceAsStream("log4j-test.properties");
				props.load(inStream);
				// init log4j
				PropertyConfigurator.configure(props);
				
				// debug
				if(log.isDebugEnabled()) {
	//				log.debug("  ## log4j.properties: "+ props.toString());
					System.out.println("  >> load log4j-test.properties <<  ");
					props.list(System.out);
				}
				
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
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
}
