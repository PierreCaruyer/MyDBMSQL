package univlille.m1info.abd.phys;

import univlille.m1info.abd.schema.RelationSchema;
import univlille.m1info.abd.schema.VolatileRelationSchema;

public class JoinOperator implements PhysicalOperator {

	private int i = 0, j = 0, nextTupleCalls = 0, sortLength = 0, joinIndex = -1;
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
		for(i = 0; i < leftSorts.length && joinIndex < 0; i++)
			for(j = 0; j < rightSorts.length && joinIndex < 0; j++)
				if(rightSorts[j].equals(leftSorts[i]))
					joinIndex = j;

		/*
		 * Associe à la relation de sortie les attributs des relations en entrée.
		 */
		sortLength = rightSorts.length + leftSorts.length - 1;
		attributeNames = new String[sortLength];
		for(i = 0; i < leftSorts.length; i++)
			attributeNames[i] = leftSorts[i];
		for(j = 0; j < rightSorts.length; j++)
			if(j != joinIndex)
				attributeNames[i++] = rightSorts[j];
			
		//Initialising rightTuple to a defaultValue
		leftTuple = left.nextTuple();
		
		schema = new VolatileRelationSchema(attributeNames);
	}
	
	@Override
	public String[] nextTuple() {
		String[] rightTuple = right.nextTuple(), tuple = null;

		if((joinIndex < 0) || (nextTupleCalls == 0 && (rightTuple == null || leftTuple == null)))
			return null;
		
		nextTupleCalls++;
		
		if(rightTuple == null){
			return null;
			/*right.reset();
			rightTuple = right.nextTuple();
			if(leftTuple == null)
				return  null;*/
		}
		
		tuple = new String[sortLength];
		
		for(i = 0; i < leftTuple.length; i++)
			tuple[i] = leftTuple[i];
		for(j = 0; j < rightTuple.length; j++)
			if(j != joinIndex)
				tuple[i] = rightTuple[j];
		
		return (tuple[joinIndex].equals(rightTuple[joinIndex]))? tuple : null;
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
