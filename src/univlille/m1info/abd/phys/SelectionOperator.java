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

		while(tuple != null && !tuple[attributeIndex].equals(value))
			tuple = operator.nextTuple();

		return tuple;
	}

	@Override
	public RelationSchema resultSchema() {
		return relation.getRelationSchema();
	}

	@Override
	public void reset() {
		relation.switchToReadMode();
	}
}
