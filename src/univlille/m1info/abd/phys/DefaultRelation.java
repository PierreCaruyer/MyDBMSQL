package univlille.m1info.abd.phys;

import java.util.List;

import univlille.m1info.abd.schema.RelationSchema;

public class DefaultRelation {

	private int firstPageAddress = -1;
	private int lastPageAddress = -1;
	private final RelationSchema schema;
	private final MemoryManager mem ;
	
	public DefaultRelation (RelationSchema schema, MemoryManager mem) {
		this.schema = schema;
		this.mem = mem;
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
			Page currentPage = mem.NewPage(tuples.get(0).length);
			Page lastPage = null;
			currentPage.SetPrevAdd(-1);
			firstPageAddress = currentPage.getAddressPage();
			for(int i = 1; i < tuples.size(); i++) {
				currentPage.AddTuple(tuples.get(i));
				if(currentPage.isFull()) {
					mem.releasePage(currentPage.getAddressPage(), true);
					lastPage = currentPage;
					currentPage = mem.NewPage(tuples.get(i+1).length);
					currentPage.SetPrevAdd(lastPage.getAddressPage());
					lastPage.SetNextAdd(currentPage.getAddressPage());
				}
			}
			lastPageAddress = currentPage.getAddressPage();
		} catch (NotEnoughMemoryException e) {
			e.printStackTrace();
		}
    }
 }