package pe.exam.kafka.rule;

import java.util.Objects;

import pe.exam.fds.store.EventStore;
import pe.exam.kafka.event.Event;

public class RuleInstance<V> {

	public Rule<?> rule = new Rule();
	private String name = null;
	
	public RuleInstance(String name) {
		this.name = name;
	}
	
	/**
	 * 
	 * @param event
	 * @return
	 */
	public <K> boolean checkRuleA(Event<K, V> event){
		Objects.requireNonNull(event);
		
		this.rule.setAccountNo(event.getAccountNO());
		boolean checked = false;
		
		if(rule.A_P1.test(event)){
			event.getAccountNO();
			if(EventStore.withdrawalTable.select(rule.A_P2).isExist()){
				if(EventStore.balanceTable.select(rule.A_P3).isExist()){
					if(EventStore.depositTable.select(rule.A_P4).isExist()){
						if(EventStore.openAccountTable.select(rule.A_P5).isExist()){
							checked = true;
						}
					}
				}
			}
		}
		return checked;
	}
	
	
	/**
	 * 
	 * @param event
	 * @return
	 */
	public <K> boolean checkRuleB(Event<K, V> event){
		Objects.requireNonNull(event);
		
		boolean checked = false;
		// some code
		
		return checked;
	}
}
