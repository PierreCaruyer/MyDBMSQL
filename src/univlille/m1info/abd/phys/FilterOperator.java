package univlille.m1info.abd.phys;

import univlille.m1info.abd.memorydb.SchemawithMemory;
import univlille.m1info.abd.schema.RelationSchema;

public abstract class FilterOperator implements PhysicalOperator{ // Equivalent to UnaryRAQUery
	
	protected int operatorPageAddress = -1, pageAddress = -1;
	protected int pageTupleCount, operatorTupleCount;
	protected final MemoryManager mem;
	protected final int sortsLength;
	protected final PhysicalOperator operator;
	
	public FilterOperator(PhysicalOperator operator, MemoryManager mem, int sortsLength) {
		this.operator = operator;
		this.mem = mem;
		this.sortsLength = sortsLength;
	}
	
	@Override
	public abstract String[] nextTuple();
	
	@Override
	public abstract RelationSchema resultSchema();
	
	@Override
	public void reset() {
		operator.reset();
	}
	
	@Override
	public int nextPage() {
		if(operatorPageAddress < 0) //page address hasn't been initialized
			operatorPageAddress = operator.nextPage();
		if(operatorPageAddress < 0)
			return operatorPageAddress;
		try {
			int prevPageAddress = -1;
			Page operatorPage = mem.loadPage(operatorPageAddress);
			operatorPage.switchToReadMode();
			Page page = mem.NewPage(sortsLength);
			String[] operatorTuple = null, tuple = null, firstTuple = null;
			
			//Goes on until page is full
			while(page.getNumberofTuple() != SchemawithMemory.PAGE_SIZE && prevPageAddress != operatorPageAddress) {
				operatorTuple = operatorPage.nextTuple();
				
				//Page.nextTuple() loops : when it reaches its end, it rewinds to the first tuple of the page 
				if(operatorTuple == firstTuple || operatorTuple == null) {
					mem.releasePage(operatorPageAddress, false);
					prevPageAddress = operatorPageAddress;
					operatorPageAddress = operator.nextPage();
					if(operatorPageAddress < 0)
						break;
					operatorPage = mem.loadPage(operatorPageAddress);
					operatorPage.switchToReadMode();
					continue;
				}
				
				//Sets the first tuple of the current page
				if(firstTuple == null)
					firstTuple = operatorTuple;
				
				tuple = getComputedTuple(operatorTuple);
				//if computation failed
				if(tuple == null)
					continue;
				page.AddTuple(tuple);
			}
			mem.PutinMemory(page, page.getAddressPage());//page update since it has been modified
			mem.releasePage(page.getAddressPage(), false); //freeing memory
			
			return page.getAddressPage();
		} catch (NotEnoughMemoryException e) {
			return -2;
		}
	}
	
	public void printTuple(String[] t) {
		System.out.print("[");
		for(String a : t)
			System.out.print(a + ", ");
		System.out.println("]");
	}
	
	protected abstract String[] getComputedTuple(String[] tuple);
}
