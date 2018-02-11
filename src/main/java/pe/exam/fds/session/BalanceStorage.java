package pe.exam.fds.session;

import java.util.concurrent.ConcurrentHashMap;

public class BalanceStorage extends ConcurrentHashMap<String, Long>{

	private static final long serialVersionUID = 1L;
	
	public BalanceStorage() {
		super(1000000, 0.75f);
	}
	
	//TODO purge operation
	
	
	//TODO Starting : load from disk
	
	
	//TODO Shutdown : save to disk
	
}
