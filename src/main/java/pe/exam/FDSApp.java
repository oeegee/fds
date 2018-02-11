package pe.exam;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import pe.exam.fds.store.EventStore;
import pe.exam.kafka.FDSKafkaConsumer;

public class FDSApp {

	public static void main(String... args) {
		
		
		// prepare event store table
		EventStore.initTable();
		
		// init rule engine 

		// consumer executor 
		ExecutorService execService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		execService.submit(new FDSKafkaConsumer());
		
	}
}
