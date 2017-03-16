
package univlille.m1info.abd.phys;

import univlille.m1info.abd.schema.RelationSchema;
import univlille.m1info.abd.simplebd.SimpleDBRelation;
import univlille.m1info.abd.simplebd.SimpleSGBD;

public class SequentialAccessOnARelationOperator implements PhysicalOperator{

	private final RelationSchema schema;
	private final SimpleDBRelation relation;
	
	public SequentialAccessOnARelationOperator(SimpleSGBD sgbd, String relname) {
		relation = sgbd.getRelation(relname);
		schema = relation.getRelationSchema();
	}
	
	@Override
	public String[] nextTuple() {
		return relation.nextTuple();
	}

	@Override
	public RelationSchema resultSchema() {
		return schema;
	}

	@Override
	public void reset() {
		relation.switchToReadMode();
	}

	@Override
	public int nextPage() {
		return -1;
	}
}
