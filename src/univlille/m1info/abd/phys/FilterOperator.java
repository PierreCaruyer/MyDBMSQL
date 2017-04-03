package univlille.m1info.abd.phys;

import univlille.m1info.abd.schema.RelationSchema;

public abstract class FilterOperator implements PhysicalOperator{ // Equivalent to UnaryRAQUery
	
	protected int operatorPageAddress = -1, pageAddress = -1;
	protected int pageTupleCount, operatorTupleCount;
	protected final MemoryManager mem;
	protected final int sortsLength;
	protected final PhysicalOperator operator;
	protected int operatorTuplePtr = 0;
	
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
		if(operatorPageAddress < 0/* || operatorTuplePtr == 0*/) //page address isn't initialized
			operatorPageAddress = operator.nextPage();
		if(operatorPageAddress < 0)
			return operatorPageAddress;
		try {
			int prevPageAddress = -1;
			Page operatorPage = mem.loadPage(operatorPageAddress);
			Page page = mem.NewPage(sortsLength);
			String[] operatorTuple = null, tuple = null, firstTuple = null;

			operatorPage.switchToReadMode();
			for(; operatorTuplePtr >= 0; operatorTuplePtr--)
				operatorPage.nextTuple();
			
			//Goes on until the page is full
			while(!page.isFull() && prevPageAddress != operatorPageAddress) {
				operatorTuple = operatorPage.nextTuple();
				operatorTuplePtr++;
				
				/*Page.nextTuple() loops : when it reaches its end, it rewinds to the first tuple of the page
				 * Therefore if the current tuple has the same reference than the first tuple,
				 * all the tuples of this page have been computed 
				 */
				if(operatorTuple == firstTuple || operatorTuple == null) {
					prevPageAddress = operatorPageAddress;
					mem.releasePage(operatorPageAddress, false);
					operatorPageAddress = operator.nextPage();
					if(operatorPageAddress < 0) //gets out of the loop
						break;
					firstTuple = null;
					operatorTuplePtr = 0;
					operatorPage = mem.loadPage(operatorPageAddress);
					operatorPage.switchToReadMode();
					continue;
				}
				
				//Sets the first tuple of the current page
				if(firstTuple == null)
					firstTuple = operatorTuple;
				
				tuple = getComputedTuple(operatorTuple);
				
				if(page == null)
					System.out.println("null");
				
				if(tuple != null)
					page.AddTuple(tuple);
			}
			
			int pageAddress = page.getAddressPage();
			
			mem.PutinMemory(page, page.getAddressPage());//page update since it has been modified
			mem.releasePage(page.getAddressPage(), false); //freeing memory
			
			return pageAddress;
		} catch (NotEnoughMemoryException e) {
			return -2;
		}
	}
	
	protected abstract String[] getComputedTuple(String[] tuple);
}
