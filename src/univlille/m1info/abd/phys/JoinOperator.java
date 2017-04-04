package univlille.m1info.abd.phys;

import java.util.ArrayList;
import java.util.Arrays;

import univlille.m1info.abd.schema.RelationSchema;
import univlille.m1info.abd.schema.VolatileRelationSchema;
import univlille.m1info.abd.tp6.TP6;

public class JoinOperator implements PhysicalOperator {

	private String[] leftTuple;
	private final PhysicalOperator right;
	private final PhysicalOperator left;
	private final RelationSchema schema;
	private final RelationSchema schemaLeft;
	private final RelationSchema schemaRight;
	private ArrayList<String> joinSorts;
	private final String[] leftSorts;
	private final String[] rightSorts;
	private final MemoryManager mem;

	private int leftPageAddress = -1, rightTupleCount = 0;
	private int rightPageAddress = -1, leftTupleCount = 0;

	public JoinOperator(PhysicalOperator right, PhysicalOperator left, MemoryManager mem) {
		this.left = left;
		this.right = right;
		this.mem = mem;
		schemaLeft = left.resultSchema();
		schemaRight = right.resultSchema();
		leftSorts = schemaLeft.getSort();
		rightSorts = schemaRight.getSort();

		/*
		 * Gives the output relation the attributes of both input relations
		 */
		joinSorts = new ArrayList<String>(Arrays.asList(leftSorts));
		for (String attributeName : rightSorts) {
			if (!joinSorts.contains(attributeName))
				joinSorts.add(attributeName);
		}

		schema = new VolatileRelationSchema(joinSorts.toArray(new String[joinSorts.size()]));
		//leftTuple = left.nextTuple(); // Initialising leftTuple to a default
										// value
	}

	@Override
	public String[] nextTuple() {
		/*
		 * For each, left tuple, attempt to find a non-null right tuple.
		 */
		String[] rightTuple = right.nextTuple();

		/*
		 * If right tuple is null, then the right physical operator must be
		 * rewinded so we can try to join the tuples of the right physical with
		 * the next tuple of the left physical operator.
		 */
		if (rightTuple == null) {
			right.reset();
			leftTuple = left.nextTuple();
			rightTuple = right.nextTuple();
		}

		if (leftTuple == null || rightTuple == null)
			return null;

		return getComputedTuples(leftTuple, rightTuple);
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

	@Override
	public int nextPage() {
		if(leftPageAddress < 0)
			return leftPageAddress;
		if(rightPageAddress < 0 || rightTupleCount == 0) {
			rightPageAddress = right.nextPage();
			if(rightPageAddress < 0) {
				right.reset();
				rightPageAddress = right.nextPage();
				if(rightPageAddress < 0)
					return rightPageAddress;
			}
		}
		try{
			int leftPrevPage = -1, rightPrevPage = -1;
			Page page = mem.NewPage(joinSorts.size());
			System.out.println(page.getAddressPage());
			Page leftPage = mem.NewPage(left.resultSchema().getSort().length);
			Page rightPage = mem.NewPage(right.resultSchema().getSort().length);
			String[] leftTuple = null, rightTuple = null, tuple = null, firstRightTuple = null, firstLeftTuple = null;
			
			//Set the first tuples
			firstRightTuple = rightPage.nextTuple();
			rightTupleCount = (rightTupleCount > 0)? rightTupleCount - 1 : rightTupleCount;
			
			firstLeftTuple = leftPage.nextTuple();
			leftTupleCount = (leftTupleCount > 0)? leftTupleCount -1 : leftTupleCount;
				
			//Initialize operator pages and their iterator
			rightPage.switchToReadMode();
			for(; rightTupleCount > 0; rightTupleCount--)
				rightPage.nextTuple();
			
			leftPage.switchToReadMode();
			for(; leftTupleCount > 0; leftTupleCount--)
				leftPage.nextTuple();
			
			while(!page.isFull() && leftPrevPage != leftPageAddress && rightPrevPage != rightPageAddress) {
				rightTuple = rightPage.nextTuple();
				
				if(rightTuple == firstRightTuple || rightTuple == null) {
					mem.releasePage(rightPageAddress, false);
					rightPrevPage = rightPageAddress;
					rightPageAddress = right.nextPage();
					if(rightPageAddress < 0) {
						right.reset();
						rightPrevPage = rightPageAddress;
						rightPageAddress = right.nextPage();
						if(rightPageAddress < 0) {
							mem.releasePage(leftPageAddress, false);
							break;
						}
						leftTuple = leftPage.nextTuple();
						if(leftTuple == firstLeftTuple || leftTuple == null) {
							mem.releasePage(leftPageAddress, false);
							leftPageAddress = left.nextPage();
							if(leftPageAddress < 0)
								break;
							leftTupleCount = 0;
							firstLeftTuple = null;
							leftPrevPage = leftPageAddress;
							leftPage = mem.loadPage(leftPageAddress);
							leftPage.switchToReadMode();
							continue;
						}
					}
					rightTupleCount = 0;
					firstRightTuple = null;
					rightPage = mem.loadPage(rightPageAddress);
					rightPage.switchToReadMode();
					continue;
				}
				
				if(firstRightTuple == null)
					firstRightTuple = rightTuple;
				if(firstLeftTuple == null)
					firstLeftTuple = leftTuple;
				
				tuple = getComputedTuples(leftTuple, rightTuple);
				
				if(tuple != null)
					page.AddTuple(tuple);
			}
			
			int pageAddress = page.getAddressPage();
			
			mem.PutinMemory(page, page.getAddressPage());
			mem.releasePage(page.getAddressPage(), false);
			
			tuple = leftTuple = rightTuple = null;
			rightPage = leftPage = page = null;
			
			return pageAddress;
		}catch(NotEnoughMemoryException e) {
			return -2;
		}
	}

	private String[] getComputedTuples(String[] tuple1, String[] tuple2) {
		// Find the common attributes between left and right
		ArrayList<String> inter = new ArrayList<String>();
		for (String attr : rightSorts) {
			if (Arrays.asList(leftSorts).contains(attr))
				inter.add(attr);
		}
		System.out.print("leftTuple ");
		TP6.printTuple(tuple1);
		System.out.print("rightTuple ");
		TP6.printTuple(tuple2);
		// Check the joint
		boolean isJoin = true;
		for (String attr : inter) {
			String value1 = schemaLeft.getAttributeValue(tuple1, attr);
			String value2 = schemaRight.getAttributeValue(tuple2, attr);
			if (value1 != value2) {
				isJoin = false;
				break;
			}
		}
		// Prepare the next tuple
		ArrayList<String> next_tuple = new ArrayList<String>(joinSorts.size());
		next_tuple.addAll(Arrays.asList(tuple1));
		// If no junction return null
		if (!isJoin)
			return null;
		else {
			for (String attr : rightSorts) {
				String value = null;
				if (!Arrays.asList(leftSorts).contains(attr)) {
					value = schemaRight.getAttributeValue(tuple2, attr);
					next_tuple.add(value);
				}
			}
		}
		return next_tuple.toArray(new String[joinSorts.size()]);
	}
}
