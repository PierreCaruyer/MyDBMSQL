package univlille.m1info.abd.ra;

/** An operation of the relational algebra used as a leaf.
 * Represents a relation name.
 * 
 * @author Iovka Boneva
 * This document is licensed under a Creative Commons Attribution 3.0 License: http://creativecommons.org/licenses/by/3.0/
 * 19 févr. 2016
 */
public class RelationNameQuery implements RAQuery {

	private final String relName;
	
	public RelationNameQuery(String relationName) {
		this.relName = relationName;
	}

	public String getRelationName() {
		return relName;
	}

	@Override
	public String toString() {
		return relName;
	}

	@Override
	public void accept(RAQueryVisitor v) {
		v.visit(this);
	}
}
