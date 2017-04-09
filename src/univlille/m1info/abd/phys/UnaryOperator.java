package univlille.m1info.abd.phys;

import univlille.m1info.abd.schema.RelationSchema;

/**
 * Abstraction of all unary operators (equivalent to UnaryRAQueries) such as
 * RenameOperator, ProjectionOperator, SelectionOperator, etc ...
 */
public abstract class UnaryOperator implements PhysicalOperator{ // Equivalent to UnaryRAQUery
	
	protected int operatorPageAddress = -1, pageAddress = -1;
	protected int pageTupleCount, operatorTupleCount;
	protected final MemoryManager mem;
	protected final int sortsLength;
	protected final PhysicalOperator operator;
	protected int operatorTuplePtr;
	
	public UnaryOperator(PhysicalOperator operator, MemoryManager mem, int sortsLength) {
		this.operator = operator;
		this.mem = mem;
		this.sortsLength = sortsLength;
		this.operatorTuplePtr = 0;
	}
	
	@Override
	public String[] nextTuple() {
		return null;
	}
	
	@Override
	public abstract RelationSchema resultSchema();
	
	@Override
	public void reset() {
		pageAddress = -1;
		operatorTuplePtr = 0;
		operatorPageAddress = -1;
		operator.reset();
	}
	
	@Override
	public int nextPage() {
		if(operatorPageAddress == -1) //page address isn't initialized
			operatorPageAddress = operator.nextPage();
		if(operatorPageAddress == -1)
			return -1;
		try {
			Page operatorPage = mem.loadPage(operatorPageAddress);
			Page page = mem.NewPage(sortsLength);
			String[] operatorTuple = null, tuple = null;

			operatorPage.switchToReadMode();
			for(; operatorTuplePtr > 0; operatorTuplePtr--)
				operatorPage.nextTuple();
			
			//Goes on until the page is full
			while(!page.isFull()) {
				operatorTuple = operatorPage.nextTuple();
				/*Page.nextTuple() loops : when it reaches its end, it rewinds to the first tuple of the page
				 * Therefore if the current tuple has the same reference than the first tuple,
				 * all the tuples of this page have been computed 
				 */
				if(operatorTuple == null) {
					mem.releasePage(operatorPageAddress, false);
					operatorPageAddress = operator.nextPage();
					if(operatorPageAddress == -1)//gets out of the loop
						break;
					operatorTuplePtr = 0;
					operatorPage = mem.loadPage(operatorPageAddress);
					operatorPage.switchToReadMode();
					operatorTuple = operatorPage.nextTuple();
				}
				operatorTuplePtr++;
				tuple = getComputedTuple(operatorTuple);
				
				if(tuple != null)
					page.AddTuple(tuple);
			}
			if(page.getNumberofTuple() != 0) {
				int address = page.getAddressPage();
				
				mem.PutinMemory(page, page.getAddressPage());//page update since it has been modified
				mem.releasePage(page.getAddressPage(), false); //freeing memory
				
				return address;
			}
			return -1;
		} catch (NotEnoughMemoryException e) {
			return -1;
		}
	}
	
	protected abstract String[] getComputedTuple(String[] tuple);
}
