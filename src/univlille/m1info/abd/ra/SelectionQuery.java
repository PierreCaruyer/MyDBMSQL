package univlille.m1info.abd.ra;

/** A selection operation of the relational algebra. 
 * This is a unary operation and is parameterized by a an attribute name and constant with which the comparison is made, as well as a comparison operator.
 * 
 * @author Iovka Boneva
 * This document is licensed under a Creative Commons Attribution 3.0 License: http://creativecommons.org/licenses/by/3.0/
 * 19 févr. 2016
 */
public class SelectionQuery extends UnaryRAQuery implements RAQuery {

	private final String attrName;
	private final String constantValue;
	private final ComparisonOperator operator;
	
	public SelectionQuery (RAQuery subQuery, String attributeName, ComparisonOperator operation, String constantValue) {
		super(subQuery);
		this.attrName = attributeName;
		this.constantValue = constantValue;
		this.operator = operation;
	}

	public String getAttributeName() {
		return attrName;
	}

	public String getConstantValue() {
		return constantValue;
	}

	public ComparisonOperator getComparisonOperator() {
		return operator;
	}

	@Override
	public String toString() {
		return String.format("SELECT[%s%s%s](%s)", attrName, operator.prettyString(), constantValue, getSubQuery());
	}

	@Override
	public void accept(RAQueryVisitor v) {
		v.visit(this);
	}
}
