package pe.exam.kafka.rule.condition;

public enum Operator {
	// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LegacyConditionalParameters.Conditions.html
	EQ("==") {
		@Override
		public boolean apply(final Long x, final Long y) {
			return (x == y);
		}
	},
	NE("!=") {
		@Override
		public boolean apply(final Long x, final Long y) {
			return (x != y);
		}
	},
	LE("<=") {
		@Override
		public boolean apply(final Long x, final Long y) {
			return (x <= y);
		}
	},
	LT("<") {
		@Override
		public boolean apply(final Long x, final Long y) {
			return (x < y);
		}
	},
	GE(">=") {
		@Override
		public boolean apply(final Long x, final Long y) {
			return (x >= y);
		}
	},
	GT(">") {
		@Override
		public boolean apply(final Long x, final Long y) {
			return (x > y);
		}
	},
	MATCHES("matches") {
		@Override
		public boolean apply(final String x, final String regex) {
			return x.matches(regex);
		}
	},
	CONTAINS("contains") {
		@Override
		public boolean apply(final String x, final String y) {
			return x.contains(y);
		}
	},
	NOT_CONTAINS("!contains") {
		@Override
		public boolean apply(final String x, final String y ) {
			return !x.contains(y);
		}
	},
	START_WITH("startWith") {
		@Override
		public boolean apply(final String x, final String y) {
			return x.startsWith(y);
		}
	},
	END_WITH("endsWith") {
		@Override
		public boolean apply(final String x, final String y) {
			return x.endsWith(y);
		}
	};
	
	
//	enum Type {
//		Number("Number"),
//		String("String");
//		
//		String name;
//		
//		private Type(String name) {
//			this.name = name;
//		}
//	}
	
	private String name;

	private Operator() {
	}
	
	private Operator(String name) {
		this.name = name();
	}
	
	public boolean apply(Long x, Long y) {
		return false;
	}

	public boolean apply(String x, String regex) {
		return false;
	}

//	public <T> boolean apply(T x, T y) {
//		return false;
//	}

	@Override
	public String toString() {
		return name();
	}
}