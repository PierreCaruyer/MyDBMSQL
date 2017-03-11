package univlille.m1info.abd.phys;

public abstract class FilterOperator implements PhysicalOperator{
	
	protected int operatorTupleCount;
	protected int operatorPageAddress;
	protected int pageTupleCount;
	protected Page page, operatorPage;
	protected final MemoryManager mem;
	protected final int sortsLength;
	protected final PhysicalOperator operator;
	
	public FilterOperator(PhysicalOperator operator, MemoryManager mem, int sortsLength) {
		this.operator = operator;
		this.mem = mem;
		this.sortsLength = sortsLength;
	}
	
	public int nextPage() {
		try {
			if(operatorPage != null && operatorPage.getNumberofTuple() == operatorTupleCount) //If page is at end
				updateOperatorPage(true);
			else if(operatorPage == null)
				updateOperatorPage(false);
			
			if(operatorPageAddress < 0)
				return operatorPageAddress;
			else {
				//Operator page has been allocated and all of its tuples haven't been used yet
				if(page == null)
					page = mem.NewPage(sortsLength);
				
				String tuple[] = new String[sortsLength];
				
				while(page.getNumberofTuple() < SchemawithMemory.PAGE_SIZE && tuple != null) {
					tuple = getComputedTuple();
					if(tuple == null) {
						updateOperatorPage(true);
						if(operatorPageAddress < 0 && page.getNumberofTuple() == 0){
							mem.releasePage(page.getAddressPage(), false);
							return operatorPageAddress;
						}
						tuple = getComputedTuple();
					}
					if(tuple != null)
						page.AddTuple(tuple);
				}
				return page.getAddressPage();
			}
		} catch (NotEnoughMemoryException e) {
			return -2;
		}
	}
	
	protected void updateOperatorPage(boolean release) throws NotEnoughMemoryException { //safely gets next operator's page
		operatorPageAddress = operator.nextPage();
		if(release)
			mem.releasePage(operatorPage.getAddressPage(), false);
		if(operatorPageAddress < 0)
			operatorPage = null;
		else
			operatorPage = mem.loadPage(operatorPageAddress);
	}

	protected abstract String[] getComputedTuple();
}
