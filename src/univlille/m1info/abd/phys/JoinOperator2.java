package univlille.m1info.abd.phys;

import java.util.ArrayList;
import java.util.Arrays;

import univlille.m1info.abd.schema.RelationSchema;
import univlille.m1info.abd.schema.VolatileRelationSchema;

public class JoinOperator2 implements PhysicalOperator {
	private final PhysicalOperator right;
	private final PhysicalOperator left;
	private final RelationSchema schema;
	private final RelationSchema schemaLeft;
	private final RelationSchema schemaRight;
	private ArrayList<String> joinSorts;
	private final String[] leftSorts;
	private final String[] rightSorts;
	private final MemoryManager mem;
	
	private int leftPageAddress = -1;
	private int rightPageAddress = -1;
	
	public JoinOperator2(PhysicalOperator right, PhysicalOperator left, MemoryManager mem) {
		super();
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RelationSchema resultSchema() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub

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
			Page page = mem.NewPage(joinSorts.size());
			Page leftPage = mem.NewPage(left.resultSchema().getSort().length);
			Page rightPage = mem.NewPage(right.resultSchema().getSort().length);
			String[] leftTuple = null, rightTuple = null, tuple = null, firstRightTuple = null, firstLeftTuple = null;
			int leftPrevPage = -1, rightPrevPage = -1;
			
			//Set the first tuples
			firstRightTuple = rightPage.nextTuple();
			rightTupleCount = (rightTupleCount > 0)? rightTupleCount - 1 : rightTupleCount;
			
			firstLeftTuple = leftPage.nextTuple();
			leftTupleCount = (leftTupleCount > 0)? leftTupleCount -1 : leftTupleCount;
				
			//Initialize operator pages and their iterator
			rightPage.switchToReadMode();
			for(; rightTupleCount >= 0; rightTupleCount--)
				rightPage.nextTuple();
			
			leftPage.switchToReadMode();
			for(; leftTupleCount >= 0; leftTupleCount--)
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
	}

}
