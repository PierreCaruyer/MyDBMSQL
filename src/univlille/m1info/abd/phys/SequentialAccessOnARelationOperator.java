package univlille.m1info.abd.phys;

import univlille.m1info.abd.schema.RelationSchema;
import univlille.m1info.abd.tp2.SimpleDBRelation;
import univlille.m1info.abd.tp2.SimpleSGBD;

public class SequentialAccessOnARelationOperator implements PhysicalOperator{

	private SimpleDBRelation relation;
	
	public SequentialAccessOnARelationOperator(SimpleSGBD sgbd, String relName) {
		relation = sgbd.getRelation(relName);
	}
	
	@Override
	public String[] nextTuple() {
		return relation.nextTuple();
	}

	@Override
	public RelationSchema resultSchema() {
		return relation.getRelationSchema();
	}

	@Override
	public void reset() {
		relation.switchToReadMode();
	}
}
