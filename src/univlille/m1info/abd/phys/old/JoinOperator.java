package univlille.m1info.abd.phys.old;

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
	
	public JoinOperator(PhysicalOperator right, PhysicalOperator left) {
		this.left = left;
		this.right = right;
		schemaLeft = left.resultSchema();
		schemaRight = right.resultSchema();
		leftSorts = schemaLeft.getSort();
		rightSorts = schemaRight.getSort();

		/*
		 * Gives the output relation the attributes of both input relations
		 */
		joinSorts = new ArrayList<String>(Arrays.asList(leftSorts));
		for ( String attributeName : rightSorts ) {
			if ( !joinSorts.contains(attributeName)) joinSorts.add(attributeName);
		}

		schema = new VolatileRelationSchema(joinSorts.toArray(new String[joinSorts.size()]));
			
		leftTuple = left.nextTuple(); //Initialising leftTuple to a default value
	}
	
	@Override
	public String[] nextTuple() {
		/*
		 * For each, left tuple, attempt to find a non-null right tuple.
		 */
		String[] rightTuple = right.nextTuple();

		/*
		 * If right tuple is null, then the right physical operator must be rewinded
		 * so we can try to join the tuples of the right physical with the next tuple 
		 * of the left physical operator. 
		 */
		if(rightTuple == null){
			right.reset();
			leftTuple = left.nextTuple();
			rightTuple = right.nextTuple();
		}
		
		/*
		 * If the left operator returns a left tuple, then all tuple combinations have been tested.
		 */
		if(leftTuple == null || rightTuple == null)
			return null;
			
		// Find the common attributes between left and right
		ArrayList<String> inter = new ArrayList<>(); 
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
}
