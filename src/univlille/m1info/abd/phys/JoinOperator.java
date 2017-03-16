package univlille.m1info.abd.phys;

import java.util.ArrayList;
import java.util.Arrays;

import univlille.m1info.abd.schema.RelationSchema;
import univlille.m1info.abd.schema.VolatileRelationSchema;

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

		leftTuple = left.nextTuple(); // Initialising leftTuple to a default
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
		return -1;
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
