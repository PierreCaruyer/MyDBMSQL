package univlille.m1info.abd.phys;

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
		if(operatorPageAddress < 0)
			operatorPageAddress = operator.nextPage();
		if(operatorPageAddress < 0)
			return operatorPageAddress;
		try {
			int prevPageAddress = -1;
			Page operatorPage = mem.loadPage(operatorPageAddress);
			operatorPage.switchToReadMode();
			Page page = mem.NewPage(sortsLength);
			String[] operatorTuple = null, tuple = null, firstTuple = null;
			
			while(page.getNumberofTuple() != SchemawithMemory.PAGE_SIZE && operatorPageAddress > -1 && prevPageAddress != operatorPageAddress) {
				operatorTuple = operatorPage.nextTuple();
				
				if(operatorTuple == firstTuple || operatorTuple == null) {
					mem.releasePage(operatorPageAddress, false);
					prevPageAddress = operatorPageAddress;
					operatorPageAddress = operator.nextPage();
					operatorPage = mem.loadPage(operatorPageAddress);
					operatorPage.switchToReadMode();
					continue;
				}
				
				if(firstTuple == null)
					firstTuple = operatorTuple;
				
				tuple = getComputedTuple(operatorTuple);
				if(tuple == null)
					continue;
				page.AddTuple(tuple);
			}
			mem.PutinMemory(page, page.getAddressPage());
			mem.releasePage(page.getAddressPage(), false);
			
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
