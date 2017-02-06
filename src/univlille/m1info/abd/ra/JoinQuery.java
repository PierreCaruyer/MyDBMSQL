package univlille.m1info.abd.ra;

import univlille.m1info.abd.schema.RelationSchema;

/** The natural join operation of the relational algebra. 
 * 
 * @author Iovka Boneva
 * This document is licensed under a Creative Commons Attribution 3.0 License: http://creativecommons.org/licenses/by/3.0/
 * 19 févr. 2016
 */
public class JoinQuery implements RAQuery {

	private RAQuery leftSubQuery;
	private RAQuery rightSubQuery;
		
	public JoinQuery(RAQuery leftSubQuery, RAQuery rightSubQuery) {
		this.leftSubQuery = leftSubQuery;
		this.rightSubQuery = rightSubQuery;
	}
	
	public RAQuery getLeftSubQuery() {
		return leftSubQuery;
	}

	public RAQuery getRightSubQuery() {
		return rightSubQuery;
	}

	@Override
	public String toString() {
		return String.format("JOIN(%s, %s)", leftSubQuery, rightSubQuery);
	}

	@Override
	public String[] nextTuple() {
		return null;
	}

	@Override
	public void reset() {
		leftSubQuery.reset();
		rightSubQuery.reset();
	}

	@Override
	public RelationSchema resultSchema() {
		return null;
	}
}
