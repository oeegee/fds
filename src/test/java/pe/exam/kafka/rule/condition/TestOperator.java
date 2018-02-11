
package pe.exam.kafka.rule.condition;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.log4j.Logger;
import org.junit.Test;

import pe.exam.kafka.TestLogger;

public class TestOperator extends TestLogger {
	private static final Logger log = Logger.getLogger(TestOperator.class.getName());

	@Test
	public void operatorSetAndToString() {

		Operator op = Operator.EQ;
		log.debug("Operator." + op.toString() + ":"+op.apply(20L, 10L) );
		op = Operator.GT;
		log.debug("Operator." + op.toString() + ":"+op.apply(20L, 10L) );
		op = Operator.GE;
		log.debug("Operator." + op.toString() + ":"+op.apply(20L, 10L) );
		op = Operator.LE;
		log.debug("Operator." + op.toString() + ":"+op.apply(20L, 10L) );
		op = Operator.NE;
		log.debug("Operator." + op.toString() + ":"+op.apply(20L, 10L) );
		op = Operator.LT;
		log.debug("Operator." + op.toString() + ":"+op.apply(20L, 10L) );
		
		op = Operator.CONTAINS;
		log.debug("Operator." + op.toString() + ":"+op.apply(20L, 20L) );
		log.debug("Operator." + op.toString() + ":"+op.apply("20L", "10L") );
		log.debug("Operator." + op.toString() + ":"+op.apply("20L", "20L") );
		
		assertNotNull(op);
	}

}
