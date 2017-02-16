package univlille.m1info.abd.phys;

import univlille.m1info.abd.ra.ComparisonOperator;
import univlille.m1info.abd.schema.RelationSchema;
import univlille.m1info.abd.schema.VolatileRelationSchema;
import univlille.m1info.abd.simplebd.SimpleDBRelation;

public class SelectionOperator implements PhysicalOperator {

	private ComparisonOperator comparator;
	private PhysicalOperator operator;
	private String value;
	private String attributeName;
	private int attributeIndex = -1;
	private SimpleDBRelation relation;
	private RelationSchema schema;

	public SelectionOperator(PhysicalOperator operator, String attrName, String constantValue, ComparisonOperator comparator) {
		this.operator = operator;
		this.attributeName = attrName;
		this.value = constantValue;
		this.comparator = comparator;

		schema = new VolatileRelationSchema(new String[]{attributeName});
		relation = new SimpleDBRelation(schema);

		String[] sorts = operator.resultSchema().getSort();

		for(int i = 0; i < sorts.length && attributeIndex < 0; i++)
			if(sorts[i].equals(attrName))
				attributeIndex = i;
	}

	@Override
	public String[] nextTuple() {
		String[] tuple = operator.nextTuple();

		while(tuple != null && !computeComparison(tuple[attributeIndex], value))
			tuple = operator.nextTuple();

		return tuple;
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
			return (comparison == 0);
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
		relation.switchToReadMode();
	}
}
