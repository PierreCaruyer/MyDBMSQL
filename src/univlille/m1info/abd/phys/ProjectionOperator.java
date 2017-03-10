package univlille.m1info.abd.phys;

import java.util.ArrayList;
import java.util.HashMap;

import univlille.m1info.abd.schema.RelationSchema;
import univlille.m1info.abd.schema.VolatileRelationSchema;

public class ProjectionOperator implements PhysicalOperator{

	private final RelationSchema schema;
	private final MemoryManager mem;
	private final String[] attributeNames;
	private Page page, operatorPage;
	private final PhysicalOperator operator;
	private int operatorPageAddress;
	private int operatorTupleCount;

	public ProjectionOperator(PhysicalOperator operator, MemoryManager mem, String ... attrNames) {
		attributeNames = attrNames;
		this.operator = operator;
		this.mem = mem;
		operatorPageAddress = -1;
		operatorTupleCount = 0;

		schema = new VolatileRelationSchema(attrNames);
	}

	@Override
	public String[] nextTuple() {
		String[] currentTuple = operator.nextTuple();
		HashMap<String,String> mapOperator = new HashMap<String, String>();
		ArrayList<String> tuple = new ArrayList<String>();

		if(currentTuple == null)
			return null;

		String[] sorts = operator.resultSchema().getSort();

		for (int i=0; i < sorts.length; i++){
			mapOperator.put(sorts[i], currentTuple[i]);
		}

		for (String attr : attributeNames){
			tuple.add(mapOperator.get(attr));
		}
		return tuple.toArray(new String[attributeNames.length]);
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
			
			String[] tuple = new String[schema.getSort().length];
			
			while(page.getNumberofTuple() == SchemawithMemory.PAGE_SIZE && tuple != null) {
				
			}

			return page.getAddressPage();
		} catch (NotEnoughMemoryException e) {
			return -2;
		}
	}

	private void updateOperatorPage(boolean release) throws NotEnoughMemoryException { //safely gets next operator's page
		operatorPageAddress = operator.nextPage();
		if(release)
			mem.releasePage(operatorPage.getAddressPage(), false);
		if(operatorPageAddress < 0)
			operatorPage = null;
		else
			operatorPage = mem.loadPage(operatorPageAddress);
	}

	private String[] getNextPageTuple() {
		String[] currentTuple = operator.nextTuple();
		HashMap<String,String> mapOperator = new HashMap<String, String>();
		ArrayList<String> tuple = new ArrayList<String>();
		operatorTupleCount++;
		
		if(currentTuple == null)
			return null;

		String[] sorts = operator.resultSchema().getSort();

		for (int i=0; i < sorts.length; i++){
			mapOperator.put(sorts[i], currentTuple[i]);
		}

		for (String attr : attributeNames){
			tuple.add(mapOperator.get(attr));
		}
		return tuple.toArray(new String[attributeNames.length]);
	}
}
