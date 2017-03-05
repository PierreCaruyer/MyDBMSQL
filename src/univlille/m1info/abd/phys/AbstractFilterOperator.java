package univlille.m1info.abd.phys;

import univlille.m1info.abd.schema.RelationSchema;
import univlille.m1info.abd.schema.VolatileRelationSchema;

public abstract class AbstractFilterOperator implements PhysicalOperator{

	protected final PhysicalOperator operator;
	protected final RelationSchema schema;
	protected final MemoryManager mem;
	protected final String[] attributeNames;
	protected final FilterOperator filter;
	
	public AbstractFilterOperator(PhysicalOperator operator, MemoryManager memoryMem, String ... attributes) {
		this.operator = operator;
		this.mem = memoryMem;
		
		schema = new VolatileRelationSchema(attributes);
		
		attributeNames = attributes;
		
		filter = new FilterOperator(this);
	}
	
	public MemoryManager getMemory() {
		return mem;
	}
	
	@Override
	public abstract String[] nextTuple();
	
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
		return filter.nextPage();
	}
}
