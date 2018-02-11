package pe.exam.kafka.rule.condition;

import pe.exam.kafka.event.Event;

/**
 * Operator : ComparisonOperator
 * 
 * @author m4600
 *
 * @param <S> attribute's value
 * @param <O> operation expr
 * @param <T> conditional value
 */
public class Condition {
	
	Event<?, ?> event;
	// attribute's value. 
//	Long x;
	Object x;
	
	// operation expr. 
	Operator operator;
	
	// conditional value.
	Object y;
	
	public Condition(Event<String, Object> event, Object x, Operator operator, Object y) {
		this.event = event;
		this.x = x;
		this.operator = operator;
		this.y = y;
	}
	
//	public Condition(Event<String, Object> event, String x, Operator operator, String y) {
//		this.event = event;
//		this.x = x;
//		this.operator = operator;
//		this.y = y;
//	}
//	
	public boolean checkCondition() {
//		if(x instanceof Long) {
//			
//		}
		
		return false;
	}
}
