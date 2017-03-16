package univlille.m1info.abd.simplebd;

import java.util.List;

import univlille.m1info.abd.phys.MemoryManager;
import univlille.m1info.abd.phys.NotEnoughMemoryException;
import univlille.m1info.abd.phys.Page;
import univlille.m1info.abd.phys.SchemawithMemory;
import univlille.m1info.abd.schema.RelationSchema;

public class DefaultRelation {

	private int firstPageAddress = -1;
	private int lastPageAddress = -1;
	private final RelationSchema schema;
	private final MemoryManager mem;
	private final int sortsCount;
	
	public DefaultRelation (RelationSchema schema, MemoryManager mem) {
		this.mem = mem;
		this.schema = schema;
		this.sortsCount = schema.getSort().length;
	}
	
	public RelationSchema getRelationSchema () {
		return this.schema;
	}
	
	public int getFirstPageAddress () {
		return firstPageAddress;
	}
	
	public int getLastPageAddress () {
		return lastPageAddress;
	}

	protected void setFirstPageAddress (int pageAddress) {
		this.firstPageAddress = pageAddress;
	}
	
	protected void setLastPageAddress (int pageAddress) {
		this.lastPageAddress = pageAddress;
	}
	
    public void loadTuples(List<String[]> tuples){
    	if(tuples.isEmpty())
    		return;
    	try {
			Page currentPage = mem.NewPage(sortsCount), lastPage = null;
			currentPage.SetPrevAdd(-1);
			firstPageAddress = currentPage.getAddressPage();
			for(int i = 0; i < tuples.size(); i++) {
				currentPage.AddTuple(tuples.get(i));
				if(currentPage.getNumberofTuple() == SchemawithMemory.PAGE_SIZE) {
					lastPage = currentPage;
					mem.releasePage(currentPage.getAddressPage(), true);
					currentPage = mem.NewPage(sortsCount);
					currentPage.SetPrevAdd(lastPage.getAddressPage());
					lastPage.SetNextAdd(currentPage.getAddressPage());
				}
				else if(i == tuples.size() - 1) {
					mem.releasePage(currentPage.getAddressPage(), true);
				}
			}
			lastPageAddress = currentPage.getAddressPage();
		} catch (NotEnoughMemoryException e) {
			e.printStackTrace();
		}
    }
 }