package univlille.m1info.abd.phys;

import java.util.ArrayList;
import java.util.HashMap;

import univlille.m1info.abd.schema.RelationSchema;

public class ProjectionOperator extends AbstractFilterOperator implements PhysicalOperator{

	public ProjectionOperator(PhysicalOperator operator, MemoryManager mem, String ... attrNames) {
		super(operator, mem, attrNames);
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
}
