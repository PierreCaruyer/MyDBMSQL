package univlille.m1info.abd.memorydb;

import java.util.List;

import univlille.m1info.abd.phys.MemoryManager;
import univlille.m1info.abd.phys.NotEnoughMemoryException;
import univlille.m1info.abd.phys.Page;
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
			
			int n = 0;
			for(int i = 0; i < tuples.size(); i++) {
				currentPage.AddTuple(tuples.get(i));
				n++;
				if ( n == SchemawithMemory.PAGE_SIZE && i != tuples.size()-1 ) {
					lastPage = currentPage;
					
					currentPage = mem.NewPage(sortsCount);
					currentPage.SetPrevAdd(lastPage.getAddressPage());
					lastPage.SetNextAdd(currentPage.getAddressPage());
					
					mem.PutinMemory(lastPage, lastPage.getAddressPage());
					mem.releasePage(lastPage.getAddressPage(), true);
					n = 0;
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