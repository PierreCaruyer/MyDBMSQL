package univlille.m1info.abd.phys;

import univlille.m1info.abd.schema.RelationSchema;
import univlille.m1info.abd.schema.VolatileRelationSchema;

public class JoinOperator implements PhysicalOperator {

	private int i = 0, j = 0, sortLength = 0, rightJoinIndex = -1;
	private String[] attributeNames, leftTuple;
	
	private PhysicalOperator right, left;
	private RelationSchema schema;	
	
	public JoinOperator(PhysicalOperator right, PhysicalOperator left) {
		this.left = left;
		this.right = right;
		String[] rightSorts = this.right.resultSchema().getSort(), leftSorts = this.left.resultSchema().getSort();

		/*
		 * Récupère l'indice de l'attribut commun sur lequel on fera la jointure.
		 */
		for(i = 0; i < leftSorts.length && rightJoinIndex < 0; i++)
			for(j = 0; j < rightSorts.length && rightJoinIndex < 0; j++)
				if(rightSorts[j].equals(leftSorts[i]))
					rightJoinIndex = j;

		/*
		 * Associe à la relation de sortie les attributs des relations en entrée.
		 */
		sortLength = rightSorts.length + leftSorts.length - 1;
		attributeNames = new String[sortLength];
		for(i = 0; i < leftSorts.length; i++)
			attributeNames[i] = leftSorts[i];
		for(j = 0; j < rightSorts.length; j++)
			if(j != rightJoinIndex)
				attributeNames[i++] = rightSorts[j];
			
		//Initialising rightTuple to a defaultValue
		leftTuple = left.nextTuple();
		
		schema = new VolatileRelationSchema(attributeNames);
	}
	
	@Override
	public String[] nextTuple() {
		String[] rightTuple = right.nextTuple();

		if(rightTuple == null){
			right.reset();
			leftTuple = left.nextTuple();
			if(leftTuple == null)
				return null;
			rightTuple = right.nextTuple();
		}
		
		if(leftTuple == null || rightTuple == null)
			return null;
		
		String[] tuple = new String[sortLength];
		
		for(i = 0; i < leftTuple.length; i++)
			tuple[i] = leftTuple[i];
		for(j = 0; j < rightTuple.length; j++)
			if(j != rightJoinIndex)
				tuple[i] = rightTuple[j];
		
		return (tuple[rightJoinIndex].equals(rightTuple[rightJoinIndex]))? tuple : null;
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
