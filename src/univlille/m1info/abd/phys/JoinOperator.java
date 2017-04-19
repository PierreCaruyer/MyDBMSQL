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
	protected int rightPageAddress = -1, leftTupleCount = 0, rightFirstPage = -1;

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
		for (String attributeName : rightSorts)
			if (!joinSorts.contains(attributeName))
				joinSorts.add(attributeName);

		schema = new VolatileRelationSchema(joinSorts.toArray(new String[joinSorts.size()]));
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
		rightPageAddress = -1;
		left.reset();
		right.reset();
		leftPageAddress = left.nextPage();
	}

	@Override
	public int nextPage() {
		if (leftPageAddress == -1)
			return -1;
		if (rightPageAddress == -1 || rightTupleCount == 0) {
			rightPageAddress = right.nextPage();
			if (rightPageAddress == -1) {
				resetRightOperator();
				if (rightPageAddress == -1)
					return -1;
			}
		}
		if (rightFirstPage == -1)
			rightFirstPage = rightPageAddress;
		try {
			Page page = mem.NewPage(joinSorts.size());
			Page leftPage = loadOperatorPage(leftPageAddress);
			Page rightPage = loadOperatorPage(rightPageAddress);
			String[] leftTuple = null, rightTuple = null, tuple = null;
			
			// Set the first tuples
			leftTuple = leftPage.nextTuple();
			leftTupleCount = (leftTupleCount > 0) ? leftTupleCount - 1 : leftTupleCount;

			// Initialize operator pages and their iterator
			for (; rightTupleCount >0; rightTupleCount--)
				rightPage.nextTuple();

			for (; leftTupleCount > 0; leftTupleCount--)
				leftPage.nextTuple();
			
			while (!page.isFull()) {				
				if ((rightTuple = nextOperatorTuple(right, rightPage, false)) == null) {
					rightPageAddress = releaseOperatorPage(right, rightPageAddress);
					if (rightPageAddress == -1) {
						resetRightOperator();
						if (rightPageAddress == -1) { // reachable only if the right operator has no tuple at all
							mem.releasePage(leftPageAddress, false); // right page has already been released at this point
							break;
						}
						if ((leftTuple = nextOperatorTuple(left, leftPage, false)) == null) {
							leftPageAddress = releaseOperatorPage(left, leftPageAddress);
							if (leftPageAddress == -1)
								break;
							leftPage = loadOperatorPage(leftPageAddress);
							leftTuple = nextOperatorTuple(left, leftPage, true);
						}
					}
					rightPage = loadOperatorPage(rightPageAddress);
					rightTuple = nextOperatorTuple(right, rightPage, true);
				}

				if ((tuple = getComputedTuples(leftTuple, rightTuple)) == null)
					continue;
				
				page.AddTuple(tuple);
			}

			if (page.getNumberofTuple() == 0)
				return -1;
			
			mem.PutinMemory(page, page.getAddressPage());
			mem.releasePage(page.getAddressPage(), false);
			
			return page.getAddressPage();
		} catch (NotEnoughMemoryException e) {
			return -1;
		}
	}

	private String[] nextOperatorTuple(PhysicalOperator op, Page page, boolean reset) {
		if (op == right) {
			if (reset)
				rightTupleCount = 0;
			rightTupleCount++;
		} else {
			if (reset)
				leftTupleCount = 0;
			leftTupleCount++;
		}
		return page.nextTuple();
	}

	private int releaseOperatorPage(PhysicalOperator operator, int pageAddress) {
		mem.releasePage(pageAddress, false);
		return operator.nextPage();
	}

	private void resetRightOperator() {
		right.reset();
		rightPageAddress = right.nextPage();
	}

	private Page loadOperatorPage(int pageAddress) throws NotEnoughMemoryException {
		Page page = mem.loadPage(pageAddress);
		page.switchToReadMode();
		return page;
	}

	private String[] getComputedTuples(String[] tuple1, String[] tuple2) {
		if (tuple1 == null || tuple2 == null)
			return null;

		// Find the common attributes between left and right
		ArrayList<String> inter = new ArrayList<String>();
		for (String attr : rightSorts)
			if (Arrays.asList(leftSorts).contains(attr))
				inter.add(attr);

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
