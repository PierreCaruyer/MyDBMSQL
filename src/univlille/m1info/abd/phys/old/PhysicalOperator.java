package univlille.m1info.abd.phys.old;

import univlille.m1info.abd.schema.RelationSchema;

public interface PhysicalOperator {

	public String[] nextTuple();
	
	public void reset();
	
	public RelationSchema resultSchema();
}
