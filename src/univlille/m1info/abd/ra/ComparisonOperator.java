package univlille.m1info.abd.ra;

public enum ComparisonOperator {
	
	EQUAL, GREATER_OR_EQUAL, GREATER, LESS_OR_EQUAL, LESS;

	public String prettyString () {
		switch (this) {
		case EQUAL : return "="; 
		case GREATER_OR_EQUAL : return ">=";
		case GREATER : return ">";
		case LESS_OR_EQUAL : return "<=";
		case LESS : return "<";
		default: throw new UnsupportedOperationException("not yet implemented for " + this);
		}
	}
}
