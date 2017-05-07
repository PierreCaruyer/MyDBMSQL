package univlille.m1info.abd.phys.old;

import univlille.m1info.abd.schema.RelationSchema;

/** An operator for evaluating one or several operations of the relational algebra.
 * Such an operator allows to iterate over the result tuples.
 * 
 * @author Iovka Boneva
 * This document is licensed under a Creative Commons Attribution 3.0 License: http://creativecommons.org/licenses/by/3.0/
 * 1 févr. 2017
 */
public interface PhysicalOperator {

	/** The next tuple of the result of this operator.
	 * 
	 * @return
	 */
	public String[] nextTuple();

	/** The schema of the result relation for this operator.
	 * Optional method.
	 */
	public RelationSchema resultSchema();
	
	/** Resets the result. 
	 * A subsequent call of {@link #nextTuple()} will return the first tuple of the result. 
	 */
	public void reset();
}
