package univlille.m1info.abd.phys;

import univlille.m1info.abd.schema.RelationSchema;
import univlille.m1info.abd.schema.VolatileRelationSchema;

public class RenameOperator extends FilterOperator implements PhysicalOperator{

	private String[] attrNames = null;
	private RelationSchema schema = null;
	
	public RenameOperator(PhysicalOperator operator, String attrName, String renamedAttribute, MemoryManager mem) {
		super(operator, mem, operator.resultSchema().getSort().length);
		
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
		return super.nextPage();
	}
	
	@Override
	protected String[] getComputedTuple(String[] tuple) {
		return tuple;
	}

}
