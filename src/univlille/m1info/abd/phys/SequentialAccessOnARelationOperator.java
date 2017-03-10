
package univlille.m1info.abd.phys;

import java.util.List;

import univlille.m1info.abd.schema.RelationSchema;
import univlille.m1info.abd.simplebd.DefaultRelation;

public class SequentialAccessOnARelationOperator implements PhysicalOperator{

	private final DefaultRelation relation;
	private int currentPageIndex = -1;
	private Page currentPage;
	private final RelationSchema schema;
	private final MemoryManager mem;
	
	public SequentialAccessOnARelationOperator(DefaultRelation relation, List<String[]> tuples, MemoryManager mem, RelationSchema schema) {
		this.schema = schema;
		this.mem = mem;
		this.relation = relation;
		relation.loadTuples(tuples);
	}
	
	@Override
	public String[] nextTuple() {
		return currentPage.nextTuple();
	}

	@Override
	public RelationSchema resultSchema() {
		return schema;
	}

	@Override
	public void reset() {
		mem.releasePage(currentPageIndex, false);
		currentPageIndex = relation.getFirstPageAddress();
		try {
			currentPage = mem.loadPage(currentPageIndex);
		} catch (NotEnoughMemoryException e) {
			e.printStackTrace();
		}
	}

	@Override
	public int nextPage() {
		return currentPage.getAddressnextPage();
	}
}
