package univlille.m1info.abd.phys;

import java.util.ArrayList;
import java.util.Arrays;

import univlille.m1info.abd.schema.DefaultRelationSchema;
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
	
    public void loadTuples(ArrayList<String> tuples){
    // TODO Implement load Tuples into the MemoryManager mem; 
    }
 }