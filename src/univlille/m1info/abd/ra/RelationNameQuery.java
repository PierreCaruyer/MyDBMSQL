package univlille.m1info.abd.ra;

import univlille.m1info.abd.schema.RelationSchema;
import univlille.m1info.abd.simplebd.SimpleDBRelation;

/** An operation of the relational algebra used as a leaf.
 * Represents a relation name.
 * 
 * @author Iovka Boneva
 * This document is licensed under a Creative Commons Attribution 3.0 License: http://creativecommons.org/licenses/by/3.0/
 * 19 févr. 2016
 */
public class RelationNameQuery implements RAQuery {

	private final String relName;
	private SimpleDBRelation relation;
	
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
	public String[] nextTuple() {
		return relation.nextTuple();
	}

	@Override
	public void reset() {
		relation.switchToReadMode();
	}

	@Override
	public RelationSchema resultSchema() {
		return relation.getRelationSchema();
	}
}
