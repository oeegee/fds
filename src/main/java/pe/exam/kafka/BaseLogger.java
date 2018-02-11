package pe.exam.kafka;

import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class BaseLogger {
	private static final Logger log = Logger.getLogger(BaseLogger.class.getName());
	
	static {
		Properties props = new Properties();

		String profile = System.getProperty("env");
		// expect local, dev, prd
		if (null == profile || (profile != "dev" && profile != "stg" && profile != "prd")) {
			profile = "";
		}
		PropertyConfigurator.configure(profile + "/log4j.properties");

		if (log.isDebugEnabled()) {
			log.debug("  ## log4j.properties: " + props.toString());
			// props.list(System.out);
		}
	}
}
