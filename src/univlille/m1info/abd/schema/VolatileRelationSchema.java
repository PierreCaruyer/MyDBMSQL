package univlille.m1info.abd.schema;


/** Allows to represent a relation schema with the empty string as relation name.
 * To be used only when the relation does not need to be stored.
 * 
 * @author Iovka Boneva
 * This document is licensed under a Creative Commons Attribution 3.0 License: http://creativecommons.org/licenses/by/3.0/
 * 1 févr. 2017
 */
public class VolatileRelationSchema extends DefaultRelationSchema {

	public VolatileRelationSchema(String[] attributeNames) {
		super("", attributeNames);
	}
	
	@Override
	protected void checkCorrectRelationName(String name) {
		if (name.length() != 0)
			throw new IllegalArgumentException("An anonymous relation always has empty name.");
	}

}
