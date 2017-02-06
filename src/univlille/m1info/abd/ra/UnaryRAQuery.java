package univlille.m1info.abd.ra;

import univlille.m1info.abd.schema.RelationSchema;
import univlille.m1info.abd.simplebd.SimpleDBRelation;

/** An abstract class for the three unary operations of the relational algebra: projection, selection and rename.
 *  
 * @author Iovka Boneva
 * This document is licensed under a Creative Commons Attribution 3.0 License: http://creativecommons.org/licenses/by/3.0/
 * 1 févr. 2017
 */
public abstract class UnaryRAQuery implements RAQuery {

	private final RAQuery subQuery;
	private SimpleDBRelation relation;
	
	public UnaryRAQuery(RAQuery subQuery) {
		this.subQuery = subQuery;
	}
	
	public String[] nextTuple(){
		return subQuery.nextTuple();
	}
	
	public void reset(){
		relation.switchToReadMode();
	}
	
	public RelationSchema resultSchema(){
		return subQuery.resultSchema();
	}
	
	public RAQuery getSubQuery() {
		return subQuery;
	}
}
