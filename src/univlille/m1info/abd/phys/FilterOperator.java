package univlille.m1info.abd.phys;

import univlille.m1info.abd.schema.RelationSchema;

public abstract class FilterOperator implements PhysicalOperator{ // Equivalent to UnaryRAQUery
	
	protected int operatorPageAddress, pageAddress;
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
		operatorPageAddress = operator.nextPage();
		if(operatorPageAddress == -1)
			return -1;
		
		try {
			Page operatorPage = mem.loadPage(operatorPageAddress);
			Page page = mem.NewPage(sortsLength);
			String[] operatorTuple = null, tuple = null;
			
			while(page.getNumberofTuple() != SchemawithMemory.PAGE_SIZE && operatorPageAddress > -1) {
				operatorTuple = operatorPage.nextTuple();
				if(operatorTuple == null) {
					mem.releasePage(operatorPageAddress, false);
					operatorPageAddress = operator.nextPage();
					operatorPage = mem.loadPage(operatorPageAddress);
					continue;
				}
				tuple = getComputedTuple(operatorTuple);
				if(tuple == null)
					continue;
			}
			mem.PutinMemory(page, page.getAddressPage());
			mem.releasePage(page.getAddressPage(), false);
			
			return pageAddress;
		} catch (NotEnoughMemoryException e) {
			return -2;
		}
	}
	
	protected void updateOperatorPage(boolean release) throws NotEnoughMemoryException { //safely gets next operator's page
	}

	protected abstract String[] getComputedTuple(String[] tuple);
}
