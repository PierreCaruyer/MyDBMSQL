package univlille.m1info.abd.phys;

import java.util.ArrayList;
import java.util.Arrays;

import univlille.m1info.abd.schema.RelationSchema;
import univlille.m1info.abd.schema.VolatileRelationSchema;

public class JoinOperator implements PhysicalOperator {

	private String[] leftTuple;
	private final PhysicalOperator right;
	private final PhysicalOperator left;
	private Page leftPage, rightPage, currentPage;
	private final RelationSchema schema;
	private final RelationSchema schemaLeft;
	private final RelationSchema schemaRight;
	private ArrayList<String> joinSorts;
	private final String[] leftSorts;
	private final String[] rightSorts;
	private final MemoryManager mem;
	private int leftPageAddress, rightPageAddress, currentPageAddress;
	private int rightCount = 0, leftCount = 0, currentCount = 0;
	private int sortsLength = 0;

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
		sortsLength = joinSorts.size();

		schema = new VolatileRelationSchema(joinSorts.toArray(new String[joinSorts.size()]));

		leftTuple = left.nextTuple(); // Initialising leftTuple to a default
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

		/*
		 * If the left operator returns a left tuple, then all tuple
		 * combinations have been tested.
		 */
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
		try {
			if (leftCount == SchemawithMemory.PAGE_SIZE) {
				leftPageAddress = left.nextPage();
				mem.releasePage(leftPage.getAddressPage(), false);
				leftCount = 0;
			}
			if (leftPageAddress == -1)
				return -1;

			if (rightCount == SchemawithMemory.PAGE_SIZE) {
				rightPageAddress = right.nextPage();
				mem.releasePage(rightPage.getAddressPage(), false);
				rightCount = 0;
			}
			if (rightPageAddress == -1) {
				right.reset();
				leftPageAddress = left.nextPage();
				rightPageAddress = right.nextPage();
				if (leftPageAddress == -1 || rightPageAddress == -1)
					return -1;
			}

			if (currentPage == null) {
				currentPage = mem.NewPage(sortsLength);
				currentPageAddress = currentPage.getAddressPage();
			}
			
			if(leftCount == 0)
				leftPage = mem.loadPage(leftPageAddress);
			if(rightCount == 0)
				rightPage = mem.loadPage(rightPageAddress);
			
			String[] tuple1, tuple2;
			
			while(currentCount != SchemawithMemory.PAGE_SIZE) {
				tuple1 = leftPage.nextTuple();
				tuple2 = rightPage.nextTuple();
				currentPage.AddTuple(getComputedTuples(tuple1, tuple2));
				currentPage = mem.NewPage(sortsLength);
				currentPageAddress = currentPage.getAddressPage();
			}
			
		} catch (NotEnoughMemoryException e) {
			e.printStackTrace();
		}

		return left.nextPage();
	}

	private String[] getComputedTuples(String[] tuple1, String[] tuple2) {
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

	/*
	 * private void sortRelations() { List<String[]> leftTuples, rightTuples;
	 * ArrayList<String> interSorts = getInterSorts(); leftTuples =
	 * getOperatorTuples(left); rightTuples = getOperatorTuples(right);
	 * 
	 * leftTuples.sort(new Comparator<String[]>(){
	 * 
	 * @Override public int compare(String[] o1, String[] o2) { return
	 * o1[userIndex].compareTo(o2[userIndex]); } });
	 * 
	 * rightTuples.sort(new Comparator<String[]>(){
	 * 
	 * @Override public int compare(String[] o1, String[] o2) { return
	 * o1[userIndex].compareTo(o2[userIndex]); } });
	 * rememberSortedTuples(leftTuples, rightTuples); }
	 * 
	 * private void rememberSortedTuples(List<String[]> leftTuples,
	 * List<String[]> rightTuples) { DefaultRelation rightDefRelation = new
	 * DefaultRelation(schemaRight, mem); DefaultRelation leftDefRelation = new
	 * DefaultRelation(schemaLeft, mem);
	 * 
	 * rightDefRelation.loadTuples(rightTuples);
	 * leftDefRelation.loadTuples(leftTuples); }
	 * 
	 * private List<String[]> getOperatorTuples(PhysicalOperator o) {
	 * List<String[]> operatorTuples = new ArrayList<>(); String[] currentTuple
	 * = null; int currentPageAddr = -1; Page currentPage = null;
	 * 
	 * while((currentPageAddr = o.nextPage()) > 0) { try { currentPage =
	 * mem.loadPage(currentPageAddr); while((currentTuple =
	 * currentPage.nextTuple()) != null) operatorTuples.add(currentTuple);
	 * mem.releasePage(currentPageAddr, false); } catch
	 * (NotEnoughMemoryException e) { e.printStackTrace(); } }
	 * 
	 * return operatorTuples; }
	 */
}
