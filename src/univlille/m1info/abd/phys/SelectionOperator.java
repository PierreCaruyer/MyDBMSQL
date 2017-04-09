package univlille.m1info.abd.phys;

import univlille.m1info.abd.ra.ComparisonOperator;
import univlille.m1info.abd.schema.RelationSchema;
import univlille.m1info.abd.schema.VolatileRelationSchema;

public class SelectionOperator extends UnaryOperator implements PhysicalOperator {

	private final ComparisonOperator comparator;
	private final String constantValue;
	private int attributeIndex = -1;
	private final RelationSchema schema;

	public SelectionOperator(PhysicalOperator operator, String attrName, String constantValue, ComparisonOperator comparator, MemoryManager mem) {
		super(operator, mem, operator.resultSchema().getSort().length);
		this.constantValue = constantValue;
		this.comparator = comparator;

		String[] sorts = operator.resultSchema().getSort();
		schema = new VolatileRelationSchema(sorts);

		for(int i = 0; i < sorts.length && attributeIndex < 0; i++)
			if(sorts[i].equals(attrName))
				attributeIndex = i;
	}
	
	@Override
	public String[] nextTuple() {
		return super.nextTuple();
	}

	/**
	 * Compares two values with the given comparison operator
	 * @param value1
	 * @param value2
	 * @return comparison test between the two values
	 */
	private boolean computeComparison(String value1, String value2) {
		int comparison = value1.compareTo(value2);

		if(comparator == ComparisonOperator.EQUAL)
			return (comparison == 0)? true : false;
		else if(comparator == ComparisonOperator.GREATER)
			return (comparison > 0)? true : false;
		else if(comparator == ComparisonOperator.GREATER_OR_EQUAL)
			return (comparison >= 0)? true : false;
		else if(comparator == ComparisonOperator.LESS)
			return (comparison < 0)? true : false;
		else if(comparator == ComparisonOperator.LESS_OR_EQUAL)
			return (comparison <= 0)? true : false;
		else
			return false;
	}

	@Override
	public RelationSchema resultSchema() {
		return schema;
	}

	@Override
	public void reset() {
		super.reset();
	}

	@Override
	public int nextPage() {
		return super.nextPage();
	}
	
	@Override
	protected String[] getComputedTuple(String[] tuple) {
		if(tuple == null)
			return null;
		if(computeComparison(tuple[attributeIndex], constantValue))
			return tuple;
		return null;
	}
}
