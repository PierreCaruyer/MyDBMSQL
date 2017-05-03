package univlille.m1info.abd.memorydb;

import java.util.Arrays;
import java.util.List;

import univlille.m1info.abd.index.Index;
import univlille.m1info.abd.phys.MemoryManager;
import univlille.m1info.abd.phys.NotEnoughMemoryException;
import univlille.m1info.abd.phys.Page;
import univlille.m1info.abd.schema.RelationSchema;

public class DefaultRelation {

	protected int firstPageAddress = -1;
	protected int lastPageAddress = -1;
	protected final RelationSchema schema;
	protected final MemoryManager mem;
	protected final int sortsCount;
	protected final String relationName;
	protected final SchemawithMemory sgbd;

	public DefaultRelation(RelationSchema schema, SchemawithMemory sgbd) {
		this.mem = SchemawithMemory.mem;
		this.schema = schema;
		this.sgbd = sgbd;
		this.sortsCount = schema.getSort().length;
		this.relationName = schema.getName();
	}

	public RelationSchema getRelationSchema() {
		return this.schema;
	}

	public int getFirstPageAddress() {
		return firstPageAddress;
	}

	public int getLastPageAddress() {
		return lastPageAddress;
	}

	protected void setFirstPageAddress(int pageAddress) {
		this.firstPageAddress = pageAddress;
	}

	protected void setLastPageAddress(int pageAddress) {
		this.lastPageAddress = pageAddress;
	}

	public void loadTuples(List<String[]> tuples) {
		if (tuples.isEmpty())
			return;
		try {
			Index relationIndex = sgbd.getUniqueIndex(schema);
			Page currentPage = mem.NewPage(sortsCount), lastPage = null;

			currentPage.SetPrevAdd(-1);
			firstPageAddress = currentPage.getAddressPage();

			for (int i = 0; i < tuples.size(); i++) {
				currentPage.AddTuple(tuples.get(i));
				if(relationIndex != null)
					relationIndex.addElement(Arrays.toString(tuples.get(i)), currentPage.getAddressPage());
				if (currentPage.isFull() && i != tuples.size() - 1) {
					lastPage = currentPage;

					currentPage = mem.NewPage(sortsCount);
					currentPage.SetPrevAdd(lastPage.getAddressPage());
					lastPage.SetNextAdd(currentPage.getAddressPage());

					mem.PutinMemory(lastPage, lastPage.getAddressPage());
					mem.releasePage(lastPage.getAddressPage(), true);
				}
			}
			currentPage.SetNextAdd(-1);
			mem.PutinMemory(currentPage, currentPage.getAddressPage());

			lastPageAddress = currentPage.getAddressPage();

			mem.releasePage(currentPage.getAddressPage(), true);
		} catch (NotEnoughMemoryException e) {
			e.printStackTrace();
		}
	}
}