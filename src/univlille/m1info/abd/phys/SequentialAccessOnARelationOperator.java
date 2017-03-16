
package univlille.m1info.abd.phys;

import univlille.m1info.abd.schema.RelationSchema;
import univlille.m1info.abd.simplebd.DefaultRelation;

public class SequentialAccessOnARelationOperator implements PhysicalOperator{

	private final DefaultRelation relation;
	private final RelationSchema schema;
	private final MemoryManager mem;
	private int currentPageAdress;
	
	public SequentialAccessOnARelationOperator(DefaultRelation relation, MemoryManager mem, RelationSchema schema) {
		this.relation = relation;
		this.schema = schema;
		this.mem = mem;
		currentPageAdress = relation.getFirstPageAddress();
	}
	
	@Override
	public String[] nextTuple() {
		return null;
	}

	@Override
	public RelationSchema resultSchema() {
		return schema;
	}

	@Override
	public void reset() {
	}

	@Override
	public int nextPage() {
		try {
			return mem.loadPage(currentPageAdress).getAddressPage();
		} catch (NotEnoughMemoryException e) {
			return -1;
		}
	}
}
