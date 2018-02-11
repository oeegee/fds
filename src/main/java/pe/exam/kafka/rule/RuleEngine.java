package pe.exam.kafka.rule;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import pe.exam.kafka.BaseLogger;
import pe.exam.kafka.event.Event;

public class RuleEngine {
	private static final Logger log = Logger.getLogger(RuleEngine.class.getName());

	// rules
	// List<Rule> ruleList = null;

	Map<String, Event> checkResult = new ConcurrentHashMap<String, Event>();
	
	RuleInstance rule = new RuleInstance("RuleA");
	// service
	ExecutorService execService = null;

	public RuleEngine() {
		this.execService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
	}

	public void execute(Event event) throws Exception {

		// execute - threadpool & worker
		List<Future<Map<String, Event>>> futureList = new ArrayList<Future<Map<String, Event>>>();
			futureList.add(execService.submit(new RuleExecutor(rule, event)));
			
		for (Future<Map<String, Event>> future : futureList) {
			checkResult.putAll(future.get());
		}

	}

}
