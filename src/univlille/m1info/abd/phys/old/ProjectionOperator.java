package univlille.m1info.abd.phys.old;

import java.util.ArrayList;
import java.util.HashMap;

import univlille.m1info.abd.schema.RelationSchema;
import univlille.m1info.abd.schema.VolatileRelationSchema;

public class ProjectionOperator implements PhysicalOperator{

	private final RelationSchema schema;
	private final String[] attributeNames;
	private final PhysicalOperator operator;

	public ProjectionOperator(PhysicalOperator operator, String ... attrNames) {
		attributeNames = attrNames;
		this.operator = operator;

		schema = new VolatileRelationSchema(attrNames);
	}

	@Override
	public String[] nextTuple() {
		String[] currentTuple = operator.nextTuple();
		HashMap<String,String> mapOperator = new HashMap<String, String>();
		ArrayList<String> tuple = new ArrayList<String>();

		if(currentTuple == null)
			return null;

		String[] sorts = operator.resultSchema().getSort();

		for (int i=0; i < sorts.length; i++){
			mapOperator.put(sorts[i], currentTuple[i]);
		}

		for (String attr : attributeNames){
			tuple.add(mapOperator.get(attr));
		}
		return tuple.toArray(new String[attributeNames.length]);
	}

	@Override
	public RelationSchema resultSchema() {
		return schema;
	}

	@Override
	public void reset() {
		operator.reset();
	}
}