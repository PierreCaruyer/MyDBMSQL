
package univlille.m1info.abd.phys;

import univlille.m1info.abd.memorydb.DefaultRelation;
import univlille.m1info.abd.schema.RelationSchema;

public class SequentialAccessOnARelationOperator implements PhysicalOperator {

	private final DefaultRelation relation;
	private final RelationSchema schema;
	private final MemoryManager mem;
	private int pageAddress = -1;

	public SequentialAccessOnARelationOperator(DefaultRelation relation, MemoryManager mem) {
		this.relation = relation;
		this.mem = mem;
		this.schema = relation.getRelationSchema();
	}

	@Override
	public String[] nextTuple() {
		try {
			Page page = mem.loadPage(pageAddress);
			if (page == null)
				return null;
			String[] tuple = page.nextTuple();
			mem.releasePage(page.getAddressPage(), false);
			return tuple;
		} catch (NotEnoughMemoryException e) {
			return null;
		}
	}

	@Override
	public RelationSchema resultSchema() {
		return schema;
	}

	@Override
	public void reset() {
		pageAddress = -1;
	}

	@Override
	public int nextPage() {
		try {
			if (pageAddress > -1) {
				Page currentPage = mem.loadPage(pageAddress);
				int nextPageAddress = currentPage.getAddressnextPage();
				mem.releasePage(pageAddress, true);
				pageAddress = nextPageAddress;
			} else
				pageAddress = relation.getFirstPageAddress();
			return pageAddress;
		} catch (NotEnoughMemoryException e) {
			return -2;
		}
	}
}
