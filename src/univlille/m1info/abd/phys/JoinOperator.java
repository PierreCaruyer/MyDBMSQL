package univlille.m1info.abd.phys;

import univlille.m1info.abd.schema.RelationSchema;
import univlille.m1info.abd.schema.VolatileRelationSchema;

public class JoinOperator implements PhysicalOperator {

	private int nextTupleCalls = 0;
	private int sortLength = 0;
	private int joinIndex = -1;
	private RelationSchema schema;
	private String[] attributeNames;
	private PhysicalOperator right, left;
	private String[] rightTuple;
	
	public JoinOperator(PhysicalOperator right, PhysicalOperator left) {
		int i = 0, j = 0;
		this.left = left;
		this.right = right;
		String[] rightSorts = this.right.resultSchema().getSort(), leftSorts = this.left.resultSchema().getSort();

		/*
		 * Récupère l'indice de l'attribut commun sur lequel on fera la jointure.
		 */
		for(i = 0; i < rightSorts.length && joinIndex < 0; i++)
			for(j = 0; j < leftSorts.length && joinIndex < 0; j++)
				if(rightSorts[i].equals(leftSorts[j]))
					joinIndex = j;

		/*
		 * Associe à la relation de sortie les attributs des relations en entrée.
		 */
		sortLength = rightSorts.length + leftSorts.length - 1;
		attributeNames = new String[sortLength];
		for(i = 0; i < rightSorts.length; i++)
			attributeNames[i] = rightSorts[i];
		for(j = 0; j < leftSorts.length; j++)
			if(j != joinIndex)
				attributeNames[i++] = leftSorts[j];
			
		//Initialising rightTuple to a defaultValue
		rightTuple = right.nextTuple();
		
		schema = new VolatileRelationSchema(attributeNames);
	}
	
	@Override
	public String[] nextTuple() {
		boolean leftTupleIsNull = false;
		String[] tuple = new String[sortLength], leftTuple = left.nextTuple();

		if(nextTupleCalls == 0 && (rightTuple == null || leftTuple == null))
			return null;
		
		nextTupleCalls++;
		
		if(leftTuple == null){
			left.reset();
			leftTuple = left.nextTuple();
			rightTuple = right.nextTuple();
			leftTupleIsNull = true;
		}
		
		if(right == null && leftTupleIsNull)
			return null;
		
		return tuple;
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
