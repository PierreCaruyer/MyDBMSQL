package univlille.m1info.abd.phys.index;

import univlille.m1info.abd.phys.PhysicalOperator;
import univlille.m1info.abd.schema.RelationSchema;

public class SelectionWithIndex implements PhysicalOperator{

	
	
	@Override
	public String[] nextTuple() {
		return null;
	}

	@Override
	public RelationSchema resultSchema() {
		return null;
	}

	@Override
	public void reset() {
		
	}

	@Override
	public int nextPage() {
		return 0;
	}

}
