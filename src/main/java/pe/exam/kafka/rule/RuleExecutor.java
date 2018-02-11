package pe.exam.kafka.rule;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import pe.exam.kafka.event.Event;


public class RuleExecutor implements Callable<Map<String, Event>>{
	private static final Logger log = Logger.getLogger(RuleExecutor.class.getName());
	
	private Event event = null;
    private RuleInstance rule;
    private Map<String, Event> checkResult = new ConcurrentHashMap<String, Event>();

    public RuleExecutor(RuleInstance rule, Event event){
        this.rule = rule;
        this.checkResult = new ConcurrentHashMap<String, Event>();
    }


    @Override
    public Map<String, Event> call() throws Exception {
    	log.info(" [ WORKER | {} check start ]");

    	boolean result = false;
    	
        if( event != null) {
        	result = rule.checkRuleA(event);
        }
        if(result == true) {
        	checkResult.put("ruleA", event);
        }

        return checkResult;
    }
}
