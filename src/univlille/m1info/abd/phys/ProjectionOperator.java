package univlille.m1info.abd.phys;

import java.util.ArrayList;
import java.util.List;

import univlille.m1info.abd.schema.RelationSchema;
import univlille.m1info.abd.schema.VolatileRelationSchema;
import univlille.m1info.abd.simplebd.SimpleDBRelation;

public class ProjectionOperator implements PhysicalOperator{

	private PhysicalOperator operator;
	private String[] attributeNames;
	private SimpleDBRelation relation;
	private List<Integer> attributesMapping;
	private RelationSchema schema;
	
	public ProjectionOperator(PhysicalOperator operator, String ... attrNames) {
		this.operator = operator;
		this.attributeNames = attrNames;

		attributesMapping = new ArrayList<>();
		schema = new VolatileRelationSchema(attributeNames);
		relation = new SimpleDBRelation(schema);
		
		String[] sorts = operator.resultSchema().getSort();
		
		for(int i = 0; i < sorts.length; i++){
			if(contains(sorts[i], attrNames)){
				this.attributeNames[i] = sorts[i];
				attributesMapping.add(i);
			}
		}
	}
	
	@Override
	public String[] nextTuple() {
 		String[] tuple = new String[attributeNames.length], currentTuple = operator.nextTuple();
 		
 		if(currentTuple == null)
 			return null;
 		
 		for(int i = 0; i < attributesMapping.size(); i++)
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
