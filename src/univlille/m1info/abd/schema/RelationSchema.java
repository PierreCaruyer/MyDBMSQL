package univlille.m1info.abd.schema;

/** The schema of a (single) relation.
 * Proposes utility methods for manipuliting tuples of the relation being described.
 * 
 * @author Iovka Boneva
 * This document is licensed under a Creative Commons Attribution 3.0 License: http://creativecommons.org/licenses/by/3.0/
 * 20 janv. 2017
 */
public interface RelationSchema {
	
	/** The name of the relation.
	 * 
	 * @return
	 */
	public String getName();
	
	/** The names of the attributes of the relation.
	 * 
	 * @return
	 */
	public String[] getSort();
	
	
	
	
	/** Retrieves the value of an attribute in a given tuple.
	 * Retruns {@code null} if the given attributeName is not part of this relation schema.
	 * Utility method.
	 * 
	 * @param tuple The tuple in which the value is retrieved. Is supposed to be a tuple of this relation, otherwise the result can be arbitrary.
	 * @param attributeName The name of the attribute which value is retrieved.
	 * @return 
	 */
	public String getAttributeValue(String[] tuple, String attributeName);

	/** Changes the value of an attribute for a given tuple.
	 * Utility method.
	 * 
	 * @param newValue The new value for the attribute.
	 * @param tuple The tuple in which the value is changed. Is supposed to be a tuple of this relation, otherwise the result can be arbitrary.
	 * @param attributeName The name of the attribute which value is changed.
	 */
	public void setAttributeValue(String newValue, String[] tuple, String attributeName);

	/** Creates a tuple with the appropriate size to be a tuple of this relation.
	 * All the values is the tuple are {@code null}
	 * Utility method.
	 * 
	 * @return
	 */
	public String[] newEmptyTuple();

	
	
}
