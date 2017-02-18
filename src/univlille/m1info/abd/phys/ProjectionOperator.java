package univlille.m1info.abd.phys;

import java.util.HashMap;
import java.util.Map;

import univlille.m1info.abd.schema.RelationSchema;
import univlille.m1info.abd.schema.VolatileRelationSchema;
import univlille.m1info.abd.simplebd.SimpleDBRelation;

public class ProjectionOperator implements PhysicalOperator{

	private PhysicalOperator operator;
	private String[] attributeNames;
	private SimpleDBRelation relation;
	private Map<Integer, Integer> attributesMapping;
	private RelationSchema schema;
	private int count = 0;
	
	public ProjectionOperator(PhysicalOperator operator, String ... attrNames) {
		this.operator = operator;
		this.attributeNames = attrNames;

		attributesMapping = new HashMap<>();
		schema = new VolatileRelationSchema(attributeNames);
		relation = new SimpleDBRelation(schema);
		
		String[] sorts = operator.resultSchema().getSort();
		
		for(int i = 0; i < sorts.length; i++){
			if(contains(sorts[i], attrNames)){
				this.attributeNames[i] = sorts[i];
				attributesMapping.put(count, i);
				count++;
			}
		}
	}
	
	@Override
	public String[] nextTuple() {
 		String[] tuple = new String[attributeNames.length], currentTuple = operator.nextTuple();
 		
 		if(currentTuple == null)
 			return null;
 		
 		for(int i = 0; i < count; i++)
			tuple[i] = currentTuple[attributesMapping.get(i)];
 		
 		return tuple;
	}

	@Override
	public RelationSchema resultSchema() {
		return schema;
	}

	@Override
	public void reset() {
		relation.switchToReadMode();
	}
	
	private boolean contains(String str, String[] array){
		for(String string : array)
			if(str.equals(string))
				return true;
		return false;
	}
}
