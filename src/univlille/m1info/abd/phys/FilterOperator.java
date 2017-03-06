package univlille.m1info.abd.phys;

public class FilterOperator{

	private final AbstractFilterOperator operator;
	private final MemoryManager mem;
	
	public FilterOperator(AbstractFilterOperator operator) {
		this.operator = operator;
		mem = operator.getMemory();
	}

	public int nextPage() {
		try {
			Page p = mem.NewPage(operator.attributeNames.length);
			String[] tuple;
			while(!(p.isFull() || (tuple = operator.nextTuple()) == null))
				p.AddTuple(tuple);
			mem.PutinMemory(p, p.getAddressPage());
			return p.getAddressPage();
		} catch (NotEnoughMemoryException e) {
			e.printStackTrace();
		}
		return -1;
	}

}
