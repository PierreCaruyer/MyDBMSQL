package univlille.m1info.abd.phys;

import univlille.m1info.abd.schema.RelationSchema;
import univlille.m1info.abd.schema.VolatileRelationSchema;

public class RenameOperator implements PhysicalOperator{

	PhysicalOperator operator;
	String[] attrNames = null;
	RelationSchema schema = null;
	
	public RenameOperator(PhysicalOperator operator, String attrName, String renamedAttribute) {
		this.operator = operator;
		
		String[] sorts = operator.resultSchema().getSort();
		attrNames = new String[sorts.length];
		
		for(int i = 0; i < sorts.length; i++)
			attrNames[i] = (sorts[i].equals(attrName))? renamedAttribute : sorts[i];
		
		schema = new VolatileRelationSchema(attrNames);
	}
	
	@Override
	public String[] nextTuple() {
		return operator.nextTuple();
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
		// TODO Auto-generated method stub
		return 0;
	}

}
