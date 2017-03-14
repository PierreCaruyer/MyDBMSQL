package univlille.m1info.abd.phys;

import java.util.ArrayList;
import java.util.HashMap;

import univlille.m1info.abd.schema.RelationSchema;
import univlille.m1info.abd.schema.VolatileRelationSchema;

public class ProjectionOperator extends FilterOperator implements PhysicalOperator{

	private final RelationSchema schema;
	private final String[] attributeNames;

	public ProjectionOperator(PhysicalOperator operator, MemoryManager mem, String ... attrNames) {
		super(operator, mem, attrNames.length);
		attributeNames = attrNames;
		operatorPageAddress = -1;
		operatorTupleCount = 0;

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

	@Override
	public int nextPage() {
		return super.nextPage();
	}

	@Override
	protected String[] getComputedTuple() {
		String[] currentTuple = operator.nextTuple();
		HashMap<String,String> mapOperator = new HashMap<String, String>();
		ArrayList<String> tuple = new ArrayList<String>();
		operatorTupleCount++;
		
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
}
