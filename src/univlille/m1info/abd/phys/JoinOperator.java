package univlille.m1info.abd.phys;

import java.util.ArrayList;
import java.util.Arrays;

import univlille.m1info.abd.schema.RelationSchema;
import univlille.m1info.abd.schema.VolatileRelationSchema;

public class JoinOperator implements PhysicalOperator {

	protected String[] leftTuple;
	protected final PhysicalOperator right;
	protected final PhysicalOperator left;
	protected final RelationSchema schema;
	protected final RelationSchema schemaLeft;
	protected final RelationSchema schemaRight;
	protected ArrayList<String> joinSorts;
	protected final String[] leftSorts;
	private final String[] rightSorts;
	protected final MemoryManager mem;
	
	protected int leftPageAddress = -1, rightTupleCount = 0;
	protected int rightPageAddress = -1, leftTupleCount = 0;

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
		// leftTuple = left.nextTuple(); // Initialising leftTuple to a default
		// value
		leftPageAddress = left.nextPage();
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
		rightTupleCount = 0;
		leftTupleCount = 0;
		leftPageAddress = -1;
		rightPageAddress = -1;
		left.reset();
		right.reset();
	}

	@Override
	public int nextPage() {
		if (leftPageAddress == -1)
			return -1;
		if (rightPageAddress == -1 || rightTupleCount == 0) {
			rightPageAddress = right.nextPage();
			if (rightPageAddress == -1) {
				right.reset();
				rightPageAddress = right.nextPage();
				if (rightPageAddress == -1)
					return -1;
			}
		}
		try {
			Page page = mem.NewPage(joinSorts.size());
			Page leftPage = mem.loadPage(leftPageAddress);
			Page rightPage = mem.loadPage(rightPageAddress);
			String[] leftTuple = null, rightTuple = null, tuple = null;

			// Set the first tuples
			leftPage.switchToReadMode();
			leftTuple = leftPage.nextTuple();
			leftTupleCount = (leftTupleCount > 0) ? leftTupleCount - 1 : leftTupleCount;

			rightPage.switchToReadMode();
			rightTupleCount = (rightTupleCount > 0) ? rightTupleCount - 1 : rightTupleCount;

			// Initialize operator pages and their iterator
			for (; rightTupleCount > 0; rightTupleCount--)
				rightPage.nextTuple();

			for (; leftTupleCount > 0; leftTupleCount--)
				leftPage.nextTuple();

			while (!page.isFull() && leftPageAddress != -1) {
				rightTuple = rightPage.nextTuple();
				rightTupleCount++;
				if (rightTuple == null) {
					mem.releasePage(rightPageAddress, false);
					rightPageAddress = right.nextPage();
					if (rightPageAddress == -1) {
						right.reset();
						rightPageAddress = right.nextPage();
						if (rightPageAddress == -1) { //reachable only if the right operator has no tuple at all 
							mem.releasePage(leftPageAddress, false); // right page has already been released at this point
							break;
						}
						leftTuple = leftPage.nextTuple();
						leftTupleCount++;
						if (leftTuple == null) {
							mem.releasePage(leftPageAddress, false);
							leftPageAddress = left.nextPage();
							if (leftPageAddress == -1) {
								System.out.println("Left page address set to -1");
								break;
							}
							leftTupleCount = 0;
							leftPage = mem.loadPage(leftPageAddress);
							leftPage.switchToReadMode();
							leftTuple = leftPage.nextTuple();
							leftTupleCount++;
						}
					}
					rightTupleCount = 0;
					rightPage = mem.loadPage(rightPageAddress);
					rightPage.switchToReadMode();
					rightTuple = rightPage.nextTuple();
					rightTupleCount++;
				}
				tuple = getComputedTuples(leftTuple, rightTuple);

				if (tuple != null)
					page.AddTuple(tuple);
			}

			int pageAddress = page.getAddressPage();
			if (page.getNumberofTuple() != 0) {
				mem.PutinMemory(page, page.getAddressPage());
				mem.releasePage(page.getAddressPage(), false);

				tuple = leftTuple = rightTuple = null;
				rightPage = leftPage = page = null;
			} else
				pageAddress = -1;
			return pageAddress;
		} catch (NotEnoughMemoryException e) {
			return -1;
		}
	}

	private String[] getComputedTuples(String[] tuple1, String[] tuple2) {
		if(tuple1 == null || tuple2 == null)
			return null;
		
		// Find the common attributes between left and right
		ArrayList<String> inter = new ArrayList<String>();
		for (String attr : rightSorts) {
			if (Arrays.asList(leftSorts).contains(attr))
				inter.add(attr);
		}
		// Check the joint
		boolean isJoin = true;
		for (String attr : inter) {
			String value1 = schemaLeft.getAttributeValue(tuple1, attr);
			String value2 = schemaRight.getAttributeValue(tuple2, attr);
			if (!value1.equals(value2)) {
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
