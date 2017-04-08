package univlille.m1info.abd.phys;

import univlille.m1info.abd.memorydb.DefaultRelation;
import univlille.m1info.abd.schema.RelationSchema;

public class SequentialAccessOnARelationOperator implements PhysicalOperator {

	protected final DefaultRelation relation;
	protected final RelationSchema schema;
	protected final MemoryManager mem;
	protected int pageAddress;
	protected boolean pageInitialized;

	public SequentialAccessOnARelationOperator(DefaultRelation relation, MemoryManager mem) {
		this.relation = relation;
		this.mem = mem;
		this.schema = relation.getRelationSchema();
		pageAddress = -1;
		pageInitialized = false;
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
		pageInitialized = false;
	}

	@Override
	public int nextPage() {
		try {
			if (pageAddress >= 0) {
				Page currentPage = mem.loadPage(pageAddress);
				int nextPageAddress = currentPage.getAddressnextPage();
				mem.releasePage(pageAddress, false);
				pageAddress = nextPageAddress;
			} else if(!pageInitialized) {
				pageAddress = relation.getFirstPageAddress();
				pageInitialized = true;
			} else {
				pageAddress =  -1;
			}
			return pageAddress;
		} catch (NotEnoughMemoryException e) {
			return -1;
		}
	}
}
