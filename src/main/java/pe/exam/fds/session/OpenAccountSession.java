package pe.exam.fds.session;

import java.util.concurrent.ConcurrentHashMap;

public class OpenAccountSession extends ConcurrentHashMap<String, Long> implements Session{

	private static final long serialVersionUID = 1L;

	public OpenAccountSession() {
		super(100000, 0.75f);
	}

	@Override
	public void autoPurge() {
	}
	
}
