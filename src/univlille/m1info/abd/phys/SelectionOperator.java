package univlille.m1info.abd.phys;

import univlille.m1info.abd.ra.ComparisonOperator;
import univlille.m1info.abd.schema.RelationSchema;
import univlille.m1info.abd.schema.VolatileRelationSchema;

public class SelectionOperator implements PhysicalOperator {

	private final ComparisonOperator comparator;
	private final String constantValue;
	private int attributeIndex = -1;
	private int operatorTupleCount;
	private int operatorPageAddress;
	private int pageTupleCount;
	private final RelationSchema schema;
	private final PhysicalOperator operator;
	private Page page, operatorPage;
	private MemoryManager mem;
	private final int sortsLength;

	public SelectionOperator(PhysicalOperator operator, String attrName, String constantValue, ComparisonOperator comparator, MemoryManager mem) {
		this.constantValue = constantValue;
		this.comparator = comparator;
		this.operator = operator;
		this.mem = mem;
		
		operatorTupleCount = 0;
		pageTupleCount = 0;
		operatorPageAddress = 0;
		page = null;
		operatorPage = null;

		String[] sorts = operator.resultSchema().getSort();
		sortsLength = sorts.length;
		schema = new VolatileRelationSchema(sorts);

		for(int i = 0; i < sorts.length && attributeIndex < 0; i++)
			if(sorts[i].equals(attrName))
				attributeIndex = i;
	}

	@Override
	public String[] nextTuple() {
		String[] tuple = operator.nextTuple();

		while(tuple != null && !computeComparison(tuple[attributeIndex], constantValue))
			tuple = operator.nextTuple();

		return tuple;
	}

	/**
	 * Compares two values with the given comparison operator
	 * @param value1
	 * @param value2
	 * @return comparison test between the two values
	 */
	private boolean computeComparison(String value1, String value2) {
		int comparison = value1.compareTo(value2);

		if(comparator == ComparisonOperator.EQUAL)
			return (comparison == 0)? true : false;
		else if(comparator == ComparisonOperator.GREATER)
			return (comparison > 0)? true : false;
		else if(comparator == ComparisonOperator.GREATER_OR_EQUAL)
			return (comparison >= 0)? true : false;
		else if(comparator == ComparisonOperator.LESS)
			return (comparison < 0)? true : false;
		else if(comparator == ComparisonOperator.LESS_OR_EQUAL)
			return (comparison <= 0)? true : false;
		else
			return false;
	}

	@Override
	public RelationSchema resultSchema() {
		return schema;
	}

	@Override
	public void reset() {
		operator.reset();
	}

	@Override
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

	private void updateOperatorPage(boolean release) throws NotEnoughMemoryException { //safely gets next operator's page
		operatorTupleCount = 0;
		operatorPageAddress = operator.nextPage();
		if(release)
			mem.releasePage(operatorPage.getAddressPage(), false);
		operatorPage = mem.loadPage(operatorPageAddress);
	}

	private String[] getComputedTuple() {
		String[] tuple = operatorPage.nextTuple();
		operatorTupleCount++;
		while(tuple != null && !computeComparison(tuple[attributeIndex], constantValue)) {
			tuple = operator.nextTuple();
			operatorTupleCount++;
		}
		return tuple;
	}
}
