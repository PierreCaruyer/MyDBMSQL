package univlille.m1info.abd.phys;

import java.util.ArrayList;
import java.util.Arrays;

import univlille.m1info.abd.memorydb.SchemawithMemory;
import univlille.m1info.abd.schema.RelationSchema;
import univlille.m1info.abd.schema.VolatileRelationSchema;
import univlille.m1info.abd.tp6.TestTP6;

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

	private int leftPageAddress = -1;
	private int rightPageAddress = -1;

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
		if (leftPageAddress < 0)
			leftPageAddress = left.nextPage();
		if (leftPageAddress < 0)
			return leftPageAddress;
		if (rightPageAddress < 0) {
			rightPageAddress = right.nextPage();
			if (rightPageAddress < 0) {
				right.reset();
				rightPageAddress = right.nextPage();
				if (rightPageAddress < 0)
					return rightPageAddress;
			}
		}
		try {
			Page leftPage = mem.loadPage(leftPageAddress), rightPage = mem.loadPage(rightPageAddress);
			leftPage.switchToReadMode();
			rightPage.switchToReadMode();
			Page page = mem.NewPage(joinSorts.size());
			String[] leftTuple = leftPage.nextTuple(), tuple = null, firstLeftTuple = null, rightTuple = null, firstRightTuple = null;

			while (page.getNumberofTuple() != SchemawithMemory.PAGE_SIZE && firstRightTuple != rightTuple && firstLeftTuple != leftTuple) {
				rightTuple = rightPage.nextTuple();
				
				if(rightTuple == firstRightTuple || rightTuple == null) {//right page is at end
					mem.releasePage(rightPageAddress, false);
					rightPageAddress = right.nextPage();
					
					if(rightPageAddress < 0) {//all right operator's pages were read
						right.reset();
						rightPageAddress = right.nextPage();
						if(rightPageAddress < 0) // no page found
							break;
						rightPage = mem.loadPage(rightPageAddress);
						rightPage.switchToReadMode();
						leftTuple = leftPage.nextTuple();
						if(leftTuple == firstLeftTuple || leftTuple == null) {
							mem.releasePage(leftPageAddress, false);
							leftPageAddress = left.nextPage();
							if(leftPageAddress < 0)
								break;
							leftPage = mem.loadPage(leftPageAddress);
							leftPage.switchToReadMode();
						}
					}
					continue;
				}
				
				if(firstLeftTuple == null)
					firstLeftTuple = leftTuple;
				if(firstRightTuple == null)
					firstRightTuple = rightTuple;

				if((tuple = getComputedTuples(leftTuple, rightTuple)) != null)
					page.AddTuple(tuple);
			}
			
			mem.PutinMemory(page, page.getAddressPage());
			mem.releasePage(page.getAddressPage(), false);

			return page.getAddressPage();
		} catch (NotEnoughMemoryException e) {
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
		TestTP6.printTuple(tuple1);
		System.out.print("rightTuple ");
		TestTP6.printTuple(tuple2);
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
