package univlille.m1info.abd.phys;

import java.util.ArrayList;
import java.util.Arrays;

import univlille.m1info.abd.schema.RelationSchema;
import univlille.m1info.abd.schema.VolatileRelationSchema;

public class JoinOperator implements PhysicalOperator {

	private String[] leftTuple;
	private final PhysicalOperator right;
	private final PhysicalOperator left;
	private Page leftPage, rightPage;
	private final RelationSchema schema;
	private final RelationSchema schemaLeft;
	private final RelationSchema schemaRight;
	private ArrayList<String> joinSorts;
	private final String[] leftSorts;
	private final String[] rightSorts;
	private final MemoryManager mem;
	private boolean workComplete;
	
	public JoinOperator(PhysicalOperator right, PhysicalOperator left, MemoryManager mem) {
		this.left = left;
		this.right = right;
		this.mem = mem;
		schemaLeft = left.resultSchema();
		schemaRight = right.resultSchema();
		leftSorts = schemaLeft.getSort();
		rightSorts = schemaRight.getSort();
		workComplete = false;

		/*
		 * Gives the output relation the attributes of both input relations
		 */
		joinSorts = new ArrayList<String>(Arrays.asList(leftSorts));
		for ( String attributeName : rightSorts ) {
			if ( !joinSorts.contains(attributeName)) joinSorts.add(attributeName);
		}

		schema = new VolatileRelationSchema(joinSorts.toArray(new String[joinSorts.size()]));
			
		leftTuple = left.nextTuple(); //Initialising leftTuple to a default value
		leftPage = loadPage(left.nextPage());
	}
	
	@Override
	public String[] nextTuple() {
		/*
		 * For each, left tuple, attempt to find a non-null right tuple.
		 */
		String[] rightTuple = rightPage.nextTuple();

		/*
		 * If right tuple is null, then the right physical operator must be rewinded
		 * so we can try to join the tuples of the right physical with the next tuple 
		 * of the left physical operator. 
		 */
		if(rightTuple == null){
			right.reset();
			leftTuple = leftPage.nextTuple();
			rightTuple = rightPage.nextTuple();
		}
		
		/*
		 * If the left operator returns a left tuple, then all tuple combinations have been tested.
		 */
		if(leftTuple == null || rightTuple == null) {
			workComplete = true;
			return null;
		}
			
		// Find the common attributes between left and right
		ArrayList<String> inter = new ArrayList<String>();
		for ( String attr : rightSorts ) {
			if ( Arrays.asList(leftSorts).contains(attr) ) inter.add(attr); 
		}
		// Check the joint
		boolean isJoin = true;
		for ( String attr : inter ) {
			String value1 = schemaLeft.getAttributeValue(leftTuple, attr);
			String value2 = schemaRight.getAttributeValue(rightTuple, attr);
			if ( value1 != value2 ) {
				isJoin = false;
				break;
			}
		}
		// Prepare the next tuple
		ArrayList<String> next_tuple = new ArrayList<String>(joinSorts.size());
		next_tuple.addAll(Arrays.asList(leftTuple));
		// If no junction return null
		if ( !isJoin ) return null;
		else {
			for ( String attr : rightSorts ) {
				String value = null;
				if ( !Arrays.asList(leftSorts).contains(attr) ) {
					value = schemaRight.getAttributeValue(rightTuple, attr);
					next_tuple.add(value);
				}
			}
		}
		return next_tuple.toArray(new String[joinSorts.size()]);
	}

	@Override
	public RelationSchema resultSchema() {
		return schema;
	}

	@Override
	public void reset() {
		left.reset();
		right.reset();
	}
	
	private Page loadPage(int pageAddress) {
		Page p = null;
		try {
			if(pageAddress > -1) {
				p = mem.loadPage(pageAddress);
				p.switchToReadMode();
			}
		} catch (NotEnoughMemoryException e) {
			e.printStackTrace();
		}
		return p;
	}

	@Override
	public int nextPage() {
		if(leftPage == null) { 
			leftPage = loadPage(left.nextPage());
			if(leftPage == null)
				return -1;
		}
		
		if(rightPage == null) {
			rightPage = loadPage(right.nextPage());
			if(rightPage == null) {
				right.reset();
				rightPage = loadPage(right.nextPage());
				if(rightPage == null)
					return -2;
			}
		}
		mem.releasePage(rightPage.getAddressPage(), false);//Might change
		mem.releasePage(leftPage.getAddressPage(), false);
		Page currentPage = null;
		
		try {
			currentPage = mem.NewPage(schema.getSort().length);
		} catch (NotEnoughMemoryException e) {
			return -3;
		}
		
		String[] tuple = null;
		while((!currentPage.isFull()) || (((tuple = nextTuple()) != null) && !workComplete))
			currentPage.AddTuple(tuple);
		
		return currentPage.getAddressPage();
	}
}
